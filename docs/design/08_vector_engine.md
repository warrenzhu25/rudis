# Component 08: Vector Search Engine: HNSW, SQ8 & Product Quantization (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/vector.rs`  
> **Implementation Reference**: [`docs/internal/08_vector_engine.md`](../internal/08_vector_engine.md)  
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
Traditional in-memory datastores encounter severe scalability barriers on modern multi-core, high-throughput cloud hardware. Single-threaded architectures (such as Redis) saturate a single CPU core while leaving the remaining 95%+ of server cores idle. Multi-threaded mutex architectures (such as Memcached) suffer from heavy spinlock contention, CPU cache line bouncing, and global memory allocator lock bottlenecks.

### 1.2 The Rudis Solution
Rudis implements the **Thread-Per-Core (Shared-Nothing)** architectural paradigm natively on Linux `io_uring` via Monoio. Each physical CPU core owns its own isolated event loop, its own thread-local memory database, and its own kernel `SO_REUSEPORT` listener. Operations on local keys execute in nanoseconds with zero locks, zero atomic operations, and zero cross-core cache invalidations.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model
High-dimensional vector indexing using Hierarchical Navigable Small World (HNSW) graphs. SIMD-accelerated distance metrics (AVX2/SSE2) with optional SQ8 scalar quantization and Product Quantization.

### 2.2 Design Rationale (The "Why")
Float32 vectors consume massive memory (5.12MB per 10k 128-dim vectors). SQ8 quantization compresses vectors by 75% with negligible recall loss, fitting multi-million embedding datasets into standard instances.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **Owner-shard routing (`VSET` & standalone `HnswIndex`)**:
   - **Redis 8 Vector Sets (`RudisValue::VectorSet`)** live directly inside `RudisTable` as first-class keyspace values (`TYPE` returns `vectorset`), supporting standard `DEL`, `EXPIRE`, `RENAME`, `DUMP`/`RESTORE`, and automatic owner-shard routing via `target_shard_of_cmd`.
   - **Standalone `HnswIndex` (`ShardDb.vector_indexes`)** routes `VADD`, `VQUERY`, `VSIM`, `VDEL`, and `VINFO` deterministically by the index name to its owner shard over the lock-free cross-shard mesh (`src/mailbox.rs`), ensuring consistent multi-shard visibility.
2. **Runtime SIMD tier selection, x86_64 only**: `dot_product`/`l2_distance_sq`/`cosine_distance`
   probe `is_x86_feature_detected!("avx512f")` first and use a 512-bit (two-accumulator,
   32-floats-per-iteration) FMA kernel if present; otherwise they probe
   `"avx2"`/`"fma"` and use a 256-bit (16-floats-per-iteration) FMA kernel; otherwise they fall
   back to a portable 8-lane-unrolled scalar implementation. Binary quantization (`BinaryVector`, `BIN`) uses native hardware `popcnt` (`u64::count_ones`) over 64-bit packed sign bitwords for Hamming distance.
3. **All metrics return a "smaller is closer" distance, not a raw similarity score**:
   `VectorMetric::IP` (inner product) returns `-dot_product(a, b)` specifically so that, like
   `L2` and `Cosine`, a smaller returned value always means "more similar" — letting
   `search_layer`'s single min/max-heap logic work identically regardless of metric.
4. **NVMe-backed `.vtier` vector storage drops in-RAM `Vec<f32>`**:
   When a vector is inserted with `VADD ... TIERED`, `HnswIndex` computes an in-RAM `QuantizedVector` (SQ8, 1 byte/dim) for fast graph traversal, appends the raw IEEE-754 little-endian `f32` bytes to `<tier_path>.<index>.vtier` (or `<RUDIS_VECTOR_TIER_DIR>/rudis_vec_<index>.vtier`), records `tier_offset: Option<u64>`, and frees `HnswNode.vector` (`Vec::new()`). During `VQUERY ... RERANK`, RDB snapshots, AOF rewrite, or `VSIM`, `HnswIndex::node_vector_cow` reads back only the needed top-$K$ full-precision vectors via positional `pread` (`FileExt::read_exact_at`).
5. **Diversity-aware HNSW heuristic & delete repair**:
   `prune_neighbors` applies the Malkov & Yashunin heuristic (preferring candidates closer to the base node than to any already-selected neighbor, backfilling with next-nearest candidates if needed), `remove` repairs orphaned neighbor links and recomputes `entry_point`/`max_layer`, and `search_layer` reuses a thread-local epoch-stamped visited table (`TLS_VISITED`) to avoid per-query `HashSet` allocations.

---

## 3. High-Level Architecture & Workflow Diagram

```
Layer 2:  [Node A] ───────────────────────► [Node D]
                      │                                 │
       Layer 1:  [Node A] ──────► [Node B] ──────► [Node D]
                      │              │                  │
       Layer 0:  [Node A] ─► [N1] ─► [Node B] ─► [N2] ─► [Node D]
```

---

## 4. Performance Guarantees & Theoretical Complexity

- Distance kernels are SIMD-accelerated on x86_64 in three runtime-selected tiers:
  AVX-512 (32 floats/iteration, two `__m512` FMA accumulators) when
  `is_x86_feature_detected!("avx512f")`, else AVX2+FMA (16 floats/iteration, two `__m256`
  accumulators), else a portable 8-lane-unrolled scalar fallback.
- SQ8 scoring reuses AVX2 kernels against `u8` data (`dot_f32_u8_avx2` / `l2_f32_u8_avx2`).
- Binary quantization (`BIN`) packs 64 dimensions per `u64` word (`8x` smaller than SQ8, `32x` smaller than FP32) and scores via XOR + hardware `count_ones()`.

### 4.1 Quantization & Tiering Trade-offs: SQ8 vs. BIN vs. PQ vs. NVMe Tiered

- **SQ8 (`QuantizedVector`, requested via `QUANTIZE`/`SQ8`/`Q8`)**: linear 8-bit scalar quantization
  per vector, independently scaled to `[min, max]`. 4x smaller than FP32 (1 byte/dim vs. 4), with `sum_q`/`sum_q_sq` precomputed so dot-product/L2/cosine can be reconstructed algebraically without dequantizing.
- **Binary Quantization (`BinaryVector`, requested via `BIN` in Redis 8 Vector Sets)**: 1-bit sign quantization (`x >= 0.0 -> 1`), packed into `Vec<u64>` and scored via Hamming distance.
- **Random Projection (`RandomProjection`, requested via `REDUCE <dim>` in Redis 8 Vector Sets)**: Johnson-Lindenstrauss orthogonal projection (`1/sqrt(target_dim)` scaled Rademacher matrix) reducing high-dimensional input embeddings on ingestion and query.
- **NVMe-Tiered Full-Precision Reranking (`TIERED` + `RERANK`)**: combines in-RAM SQ8 graph navigation with disk-resident `.vtier` full-precision vectors. Graph traversal never touches disk; only the final `ef_search.max(k * 3)` candidates are fetched from the `.vtier` file via positional `pread` when `RERANK` is requested.
- **PQ (`ProductQuantizer`/`PQVector`, requested via `PQ`)**: splits each vector into `m` subvectors and encodes each subvector as a 1-byte centroid index using ADC lookup tables.

### 4.2 Supported Commands & Dual Index Model

Rudis supports **two complementary vector APIs** in `src/vector.rs`:
1. **Redis 8 Native Vector Sets (`RudisValue::VectorSet`)**: stored directly in `RudisTable` as first-class keys with optional JSON attributes, `FILTER` expressions, `Q8`/`BIN`/`NOQUANT` quantization, and `REDUCE <dim>` projection.
2. **Standalone Named HNSW Indexes (`ShardDb.vector_indexes`)**: supports `VADD ... [SQ8|PQ|TIERED]`, `VQUERY ... [RERANK]`, `VSIM`, `VDEL`, and `VINFO`, persisted in the extended `RDBX` trailer and `BGREWRITEAOF`.

| Command | Parameters | Behavior |
| :--- | :--- | :--- |
| `VADD key [REDUCE dim] (FP32 blob \| VALUES n f…) elem [CAS] [NOQUANT\|Q8\|BIN] [EF n] [SETATTR json] [M n] [TIERED]` | Redis 8 Vector Set or legacy `VADD idx key f… [METRIC] [SQ8\|PQ\|TIERED]` | Inserts/updates an element in a Redis 8 Vector Set (or standalone HNSW index). Supports `Q8`, `BIN`, `REDUCE`, JSON attributes (`SETATTR`), and `.vtier` disk spilling (`TIERED`). |
| `VSIM key (ELE elem \| FP32 blob \| VALUES n f…) [WITHSCORES] [WITHATTRIBS] [COUNT k] [EF ef] [FILTER expr] [FILTER-EF ef] [TRUTH] [EPSILON d]` | Redis 8 k-NN similarity search or legacy pair distance `VSIM idx k1 k2 [METRIC]` | Runs Redis 8 k-NN search with optional attribute `FILTER` expressions, exact `TRUTH` brute-force scan, and `EPSILON` radius threshold; or computes pairwise distance in legacy 3-arg form. |
| `VQUERY index k v1 v2 ... [RERANK]` | Index name, result count, query vector, optional exact-rerank flag | Approximate k-NN search (`search_tiered`); `RERANK` re-scores candidates against exact `f32` vectors (reading from `.vtier` disk via `pread` for `TIERED` nodes). |
| `VREM key elem` / `VDEL index key` | Vector set key & element (or index & key) | Removes an element and repairs HNSW neighbor links. |
| `VCARD key` / `VDIM key` / `VISMEMBER key elem` | Vector set key (& element) | Returns cardinality, projected vector dimension, or membership (`1`/`0`). |
| `VEMB key elem [RAW]` | Vector set key, element, optional `RAW` | Returns the reconstructed (or raw quantized/packed) embedding for `elem`. |
| `VLINKS key elem [WITHSCORES]` | Vector set key, element | Inspects HNSW neighbor links across all layers for `elem`. |
| `VRANDMEMBER key [count]` | Vector set key, optional signed count | Returns one or more random elements from the vector set. |
| `VSETATTR key elem json` / `VGETATTR key elem` | Vector set key, element, JSON string | Attaches or retrieves per-element JSON metadata used by `VSIM ... FILTER`. |
| `VINFO key` | Vector set key or standalone index name | Returns Redis 8 Vector Set metadata (`quant-type`, `vector-dim`, `size`, `max-level`, `hnsw-m`, `tiered-bytes`, etc.). |

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/08_vector_engine.md`**](../internal/08_vector_engine.md): Low-level implementation and code reference.
* **Source Files**: `src/vector.rs`
