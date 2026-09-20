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
1. **Thread-local, not cross-shard**: consistent with the rest of the codebase, `HnswIndex`
   instances live inside one shard's `ShardDb` with no locks — but unlike the key-value
   store, there is no `ShardMessage` variant to reach a vector index on another shard at all
   (see §1). This isn't a locking decision, it's simply unimplemented cross-shard support.
2. **Runtime SIMD tier selection, x86_64 only**: `dot_product`/`l2_distance_sq`/`cosine_distance`
   probe `is_x86_feature_detected!("avx512f")` first and use a 512-bit (two-accumulator,
   32-floats-per-iteration) FMA kernel if present; otherwise they probe
   `"avx2"`/`"fma"` and use a 256-bit (16-floats-per-iteration) FMA kernel; otherwise they fall
   back to a portable 8-lane-unrolled scalar implementation. The SQ8 asymmetric-distance helpers
   (`dot_f32_u8_avx2`/`l2_f32_u8_avx2`) only have an AVX2 tier (no AVX-512 variant) — SQ8-scored
   candidates never get the AVX-512 speedup even on hardware that supports it. There is **no
   ARM/NEON code path** — only `#[cfg(target_arch = "x86_64")]` SIMD kernels exist; any other
   architecture always takes the portable path.
3. **All three metrics return a "smaller is closer" distance, not a raw similarity score**:
   `VectorMetric::IP` (inner product) returns `-dot_product(a, b)` specifically so that, like
   `L2` and `Cosine`, a smaller returned value always means "more similar" — letting
   `search_layer`'s single min/max-heap logic work identically regardless of metric.
4. **Product Quantization codebooks are not trained on data.** `ProductQuantizer::new`
   generates each subvector's 256 centroids deterministically: centroid 0 is the zero
   vector, centroids `1..=d_sub` are positive unit basis vectors, `d_sub+1..=2*d_sub` are
   negative unit basis vectors, and the remainder are filled by a fixed SplitMix64-style
   hash of `(centroid_id, subvector_id, dim_id)` mapped into `[-1, 1]`. There is no k-means
   or any training pass over real vectors — every `ProductQuantizer` for a given `(dim, m)`
   produces byte-for-byte identical codebooks. Real PQ implementations cluster the actual
   data distribution; this one does not, which will cost recall accordingly.
5. **HNSW layer assignment is deterministic across index instances.** `HnswIndex::new`
   seeds a custom xorshift64 PRNG (`rng_state`) with the fixed constant
   `0x853c49e6748fea9b` every time — not from OS randomness, the clock, or the index name.
   Two indexes built by inserting the same vectors in the same order will have identical
   graph topology.

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

- Distance kernels are genuinely SIMD-accelerated on x86_64, in three runtime-selected tiers:
  AVX-512 (32 floats/iteration, two `__m512` FMA accumulators) when
  `is_x86_feature_detected!("avx512f")`, else AVX2+FMA (16 floats/iteration, two `__m256`
  accumulators), else a portable 8-lane-unrolled scalar fallback — no unconditional `unsafe` on
  unsupported hardware.
- SQ8 scoring reuses AVX2 kernels against `u8` data (no AVX-512 tier for the quantized path — see
  the SIMD-tier invariant in §2.3), so approximate scoring during graph traversal is not meaningfully slower
  per-comparison than exact float scoring on hardware that has at least AVX2.
- No numbers in this document are benchmarked — the previous version's "\>3,500 vectors/sec",
  "\<400µs p99", and "75% RAM reduction" figures were unsourced and have been removed rather
  than repeated unverified. SQ8's memory reduction ratio (4 bytes/dim -> 1 byte/dim, i.e. 4x
  smaller for the quantized copy, kept *alongside* the original `Vec<f32>` per §3's note) is
  the one ratio derivable directly from the type definitions, not from measurement. Likewise,
  Product Quantization's compression ratio (e.g. a 128-dim float32 vector at 512 bytes down to
  `m` PQ code bytes — 16 bytes for the default `m = dim/8` in `add_quantized_ext`, i.e. 32x) is
  derived from the type definitions (`PQVector.codes: Vec<u8>`, one byte per subvector), not
  measured.

### 4.1 Quantization trade-offs: SQ8 vs. PQ vs. full precision

Both quantization schemes are *additive* to the full-precision vector — `HnswNode.vector:
Vec<f32>` is always retained (§3), so neither scheme reduces the graph's own memory floor by
itself; they exist to make *candidate scoring during traversal* cheaper and, if `TIERED` is also
set, to give tiered/cold nodes a compact resident representation that avoids touching disk on
every comparison.

- **SQ8 (`QuantizedVector`, requested via `QUANTIZE`/`SQ8`)**: linear 8-bit scalar quantization
  per vector, independently scaled to that vector's own `[min, max]` range. 4x smaller than the
  float vector it approximates (1 byte/dim vs. 4), with `sum_q`/`sum_q_sq` precomputed so exact
  dot-product/L2/cosine can be reconstructed algebraically without dequantizing (§4.2 of the
  internal doc). Recall loss is typically small because the quantization error is bounded by
  `scale/2` per dimension and errors partially cancel across dimensions in the dot product.
- **PQ (`ProductQuantizer`/`PQVector`, requested via `PQ`)**: splits each vector into `m`
  subvectors (`m = (dim/8).clamp(1, 16)` if not already initialized) and encodes each subvector
  as a single byte (one of 256 centroid indices) — far higher compression (16 bytes for a
  128-dim vector at the default `m`) at the cost of coarser per-subvector approximation. **The
  codebooks are not trained on the dataset** (§2.3, invariant 4) — they are a fixed deterministic basis, so
  PQ's usual advantage (recall close to SQ8 at a fraction of the size, because centroids are fit
  to the real data distribution) does not fully apply here; expect measurably worse recall than
  a textbook trained-PQ implementation at the same compression ratio.
- **Full precision (neither flag set)**: `dist_to_node` always falls through to `compute_distance`
  on the exact `f32` vectors — no compression, exact recall relative to the metric.
- **`RERANK`** (on `VQUERY`) is the mechanism for recovering exact-recall ordering cheaply on top
  of either quantization scheme: the approximate top `ef_search.max(k*3)` candidates are re-scored
  with `compute_distance` on their exact float vectors and re-sorted, trading a wider initial
  candidate set for exact final ordering — this only helps if the *set* of true top-k is well
  approximated by quantized distances, which is generally true for SQ8, less reliably so for this
  implementation's untrained PQ.

### 4.2 Supported Commands & Index Model

Rudis exposes a dedicated `V*`-prefixed command family for vector search — a standalone index
type (`HnswIndex`), independent of the `FT.CREATE`-based RediSearch schema engine (Component 09).
There is no `VECTOR` field type inside `FT.CREATE`; the two search subsystems do not interoperate.

| Command | Parameters | Behavior |
| :--- | :--- | :--- |
| `VADD index key v1 v2 ... [QUANTIZE\|SQ8] [PQ] [TIERED]` | Index name, member key, float vector components, optional flags | Inserts/updates a vector; lazily creates the index (`HnswIndex::new`, metric defaults to `COSINE`) on first use. `QUANTIZE`/`SQ8` attaches an SQ8 copy; `PQ` attaches a PQ code (initializing the index's shared `ProductQuantizer` on first use); `TIERED` marks the node as tiered-storage-eligible and forces an SQ8 copy if PQ wasn't also requested. |
| `VQUERY index k v1 v2 ... [RERANK]` | Index name, result count, query vector, optional exact-rerank flag | Approximate k-NN search (`search_tiered`); `RERANK` re-scores the wider candidate set against exact float vectors before truncating to `k` (§4.1). |
| `VSIM index key1 key2 [METRIC]` | Index name, two member keys, optional one-off metric override | Computes the exact distance between two already-indexed vectors under the index's configured metric, or `METRIC` if supplied for that single call only — does not alter the index's stored metric. |
| `VDEL index key` | Index name, member key | Removes a vector from the index (tombstones its node; see §4.6 of the internal doc). |
| `VINFO index` | Index name | Returns cardinality, dimensionality, metric name, and current `max_layer`. |

Per-call metric override is **not** accepted on `VADD` — `Command::Vadd::metric` is always parsed
as `None` by the RESP layer, so an index's distance metric (`COSINE` default, `L2`, or `IP`) is
fixed at first-use time and can only be read back, not changed, without deleting and recreating
the index.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/08_vector_engine.md`**](../internal/08_vector_engine.md): Low-level implementation and code reference.
* **Source Files**: `src/vector.rs`
