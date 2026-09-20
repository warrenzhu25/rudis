# Component 08: Vector Search Engine: HNSW, SQ8 & Product Quantization (Design)

> **Source Files**: `src/vector.rs`


---

### 1. Architectural Purpose & Scope

`src/vector.rs` implements an in-memory approximate nearest-neighbor (ANN) vector index:
a **Hierarchical Navigable Small World (HNSW)** graph (`HnswIndex`), an **8-bit scalar
quantization** scheme (`QuantizedVector`), and a **Product Quantization with Asymmetric
Distance Computation** scheme (`ProductQuantizer`/`PQVector`). It is exposed to clients
through five bespoke commands parsed in `src/resp.rs` and dispatched in `src/connection.rs`:
`VADD`, `VQUERY`, `VSIM`, `VDEL`, `VINFO`. There is no `FT.SEARCH ... KNN` integration —
that syntax does not exist anywhere in this codebase; full-text search (`src/search.rs`,
Component 09) is a separate engine with no code-level link to this one.

**Each shard owns a completely independent set of named indexes** (`ShardDb.vector_indexes:
HashMap<String, HnswIndex>`), and every vector command only ever touches
`router.local_db` — there is no cross-shard routing for `VADD`/`VQUERY`/`VSIM`/`VDEL`/`VINFO`
at all (confirmed: none of the five appear in `target_shard_of_cmd`, and none of `Router`'s
methods reference the vector engine). This means an index named `"products"` on shard 0 and
an index named `"products"` on shard 1 are two entirely separate, unrelated HNSW graphs —
which shard a given connection lands on (decided by the kernel via `SO_REUSEPORT`, per
Component 01) silently determines which index a `VADD`/`VQUERY` actually reads or writes.
There is no fan-out, no merge, and no consistency check across shards. Treat this as the
single most important operational caveat for this subsystem.

---

### 2. Key Invariants & Concurrency Constraints

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

### 3. Performance Characteristics

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
