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
2. **Runtime AVX2 detection, x86_64 only**: `dot_product`/`l2_distance_sq` check
   `is_x86_feature_detected!("avx2")`/`"fma"` at call time and fall back to a portable
   8-lane-unrolled scalar implementation otherwise. There is **no ARM/NEON code path** —
   only `#[cfg(target_arch = "x86_64")]` SIMD kernels exist; any other architecture always
   takes the portable path.
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

- Distance kernels are genuinely AVX2+FMA accelerated at 16 floats/iteration when the CPU
  supports it, with a correct portable fallback otherwise — no unconditional `unsafe` on
  unsupported hardware.
- SQ8 scoring reuses the same AVX2 kernels against `u8` data, so approximate scoring during
  graph traversal is not meaningfully slower per-comparison than exact float scoring.
- No numbers in this document are benchmarked — the previous version's "\>3,500 vectors/sec",
  "\<400µs p99", and "75% RAM reduction" figures were unsourced and have been removed rather
  than repeated unverified. SQ8's memory reduction ratio (4 bytes/dim -> 1 byte/dim, i.e. 4x
  smaller for the quantized copy, kept *alongside* the original `Vec<f32>` per §3's note) is
  the one ratio derivable directly from the type definitions, not from measurement.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/08_vector_engine.md`**](../internal/08_vector_engine.md): Low-level implementation and code reference.
* **Source Files**: `src/vector.rs`
