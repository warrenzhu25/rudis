# Component 08: Vector Search Engine: HNSW, SQ8 & Product Quantization (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/vector.rs`  
> **High-Level Design Spec**: [`docs/design/08_vector_engine.md`](../design/08_vector_engine.md)  
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/vector.rs` | Core implementation and logic | Primary data structures and algorithms |

---

### 3. Component Architecture & Data Structures

```
                 VADD index key <floats...> [QUANTIZE|SQ8] [PQ] [TIERED]
                 VQUERY index k <floats...> [RERANK]
                 VSIM index key1 key2 [METRIC ...]
                 VDEL index key
                 VINFO index
                                     │
                        ShardDb.vector_indexes["index"]  (per-shard, independent)
                                     │
                                     ▼
                              HnswIndex
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
         nodes: Vec<Option<HnswNode>>          key_to_id: HashMap<Bytes, usize>
         (tombstoned via None on delete,          (external key -> internal id)
          never compacted)
                    │
                    ▼
         HnswNode { vector: Vec<f32>, quantized: Option<QuantizedVector>,
                     pq: Option<PQVector>, neighbors: Vec<Vec<usize>> }
                     (neighbors[layer] = adjacency list at that layer)
```

#### Real core types

```rust
pub enum VectorMetric { Cosine, L2, IP }   // Cosine is Default
pub enum VectorQuantType { NoQuant, Q8, Bin }

pub struct VectorSetValue {
    pub index: Box<HnswIndex>,
    pub quant_type: VectorQuantType,
    pub reduce_dim: Option<usize>,
    pub raw_dim: usize,
    pub projection: Option<RandomProjection>,
    pub attributes: HashMap<Bytes, String>, // element -> JSON metadata string
}

pub struct HnswIndex {
    pub name: String,
    pub dim: usize,
    pub metric: VectorMetric,
    pub m: usize,               // default 16 — max neighbors per node, layer > 0
    pub m0: usize,              // default 32 — max neighbors per node, layer 0
    pub ef_construction: usize, // default 64
    pub ef_search: usize,       // default 32
    pub ml: f64,                // 1 / ln(m) — level-generation scale
    pub entry_point: Option<usize>,
    pub max_layer: usize,
    pub nodes: Vec<Option<HnswNode>>,
    pub free_ids: Vec<usize>,   // recycled slot indices on deletion
    pub key_to_id: HashMap<Bytes, usize>,
    pub pq_quantizer: Option<ProductQuantizer>,
    pub tier_path: Option<PathBuf>, // optional .vtier disk spill file
    pub tiered_bytes: u64,          // current bytes appended to .vtier
    rng_state: u64,
}

pub struct HnswNode {
    pub id: usize,
    pub key: Bytes,
    pub vector: Vec<f32>,             // empty (`Vec::new()`) when spilled to .vtier disk
    pub quantized: Option<QuantizedVector>,  // SQ8 (1 byte/dim)
    pub binary: Option<BinaryVector>,        // 1-bit sign quantization (64 dims/u64)
    pub pq: Option<PQVector>,                // PQ codes (1 byte/subvector)
    pub is_tiered: bool,
    pub tier_offset: Option<u64>,     // byte offset in .vtier file when `is_tiered`
    pub neighbors: Vec<Vec<usize>>,   // one adjacency list per layer this node exists on
}
```

When `is_tiered` is `false`, `HnswNode.vector` retains the full `Vec<f32>`. When `is_tiered` is `true` (`VADD ... TIERED`), `spill_node_to_tier` appends the raw little-endian `f32` bytes to the index's `.vtier` backing file, sets `tier_offset: Some(offset)`, and clears `node.vector = Vec::new()` so only the compact `QuantizedVector` (SQ8, 4x smaller) remains in RAM for HNSW graph traversal.

---

### 4. Execution Algorithms & Code Logic

#### 4.1 SIMD distance kernels

```rust
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") {
            return unsafe { dot_product_avx512(a, b) };
        }
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { dot_product_avx2(a, b) };
        }
    }
    dot_product_portable(a, b)
}
```

`dot_product`/`l2_distance_sq`/`cosine_distance` each dispatch through three runtime-selected
tiers:
1. **AVX-512** (`dot_product_avx512`/`l2_distance_sq_avx512`/`cosine_distance_avx512`, gated on
   `is_x86_feature_detected!("avx512f")`): two `__m512` accumulators (`acc0`/`acc1`), 32 floats/iteration.
2. **AVX2+FMA** (`dot_product_avx2`/`l2_distance_sq_avx2`/`cosine_distance_avx2`, gated on
   `"avx2"` **and** `"fma"`): two `__m256` accumulators via `_mm256_fmadd_ps`, 16 floats/iteration.
3. **Portable fallback** (`dot_product_portable`/`l2_distance_sq_portable`/
   `cosine_distance_portable`): `a.as_chunks::<8>()` manual 8-lane unrolling, zero `unsafe`.

`dot_f32_u8_avx2`/`l2_f32_u8_avx2` provide AVX2 acceleration for a `f32` query against an SQ8 `u8` vector, and `BinaryVector::hamming_distance` uses XOR + hardware `u64::count_ones()` across packed 64-bit sign words.

#### 4.2 SQ8 quantization: precomputed sums avoid dequantizing on every comparison

`QuantizedVector::quantize` linearly maps each float into `[0, 255]` using the vector's own
min/max (`scale = (max - min) / 255`), and precomputes `sum_q`/`sum_q_sq` once at insert
time. `compute_distance` reconstructs the dot product / L2 / cosine algebraically from `dot_u8` plus the precomputed sums without materializing a dequantized `Vec<f32>`.

#### 4.3 HNSW insertion & diversity-aware heuristic (`prune_neighbors`)

`prune_neighbors` implements the Malkov & Yashunin diversity heuristic: candidates are sorted by distance to the base node, and a candidate `c` is selected if it is closer to the base node than to every already-selected neighbor `s` in `selected`. Any remaining slots up to `max_neighbors` are backfilled with the closest rejected candidates to preserve connectivity.

#### 4.4 Search (`search_tiered` / `search_filtered`) & NVMe `.vtier` positional rerank

```rust
pub fn search_tiered(&self, query: &[f32], k: usize, rerank: bool) -> Vec<(Bytes, f32)>
```

During traversal (`search_layer`), a thread-local epoch-stamped visited array (`TLS_VISITED`) avoids per-query `HashSet` allocations, and `dist_to_node` scores candidates using their in-RAM representation (`BinaryVector`, `PQVector`, `QuantizedVector`, or in-RAM `vector`). When `rerank` is `true`, the top `ef_search.max(k * 3)` candidates are re-scored using `self.node_vector_cow(node)`:
- If `node.vector` is in RAM, returns `Cow::Borrowed(&node.vector)`.
- If `node.vector` was spilled to `.vtier` disk (`node.tier_offset == Some(offset)`), reads `self.dim * 4` bytes directly from `self.tier_path` via `std::os::unix::fs::FileExt::read_exact_at` (`pread`) without mutating file offsets or requiring a mutex.
- Falls back to `QuantizedVector::dequantize()` if the backing file is unavailable.

#### 4.5 Deletion with neighbor repair & slot recycling (`remove`)

`remove(key)` repairs the neighborhood of every former neighbor at each layer by reconnecting it to the deleted node's other neighbors (pruned via `prune_neighbors`), pushes the freed node index onto `self.free_ids` for reuse on subsequent insertions, and recomputes `self.max_layer` and `self.entry_point` from the highest-layer surviving node if the entry point was deleted.

---

### 5. Cross-Component Interactions

- **`src/resp.rs`**: parses both the Redis 8 Vector Sets command suite (`VADD`, `VSIM`, `VREM`, `VCARD`, `VDIM`, `VEMB`, `VLINKS`, `VRANDMEMBER`, `VSETATTR`, `VGETATTR`, `VISMEMBER`, `VINFO`) and legacy `VADD`/`VQUERY`/`VSIM`/`VDEL`/`VINFO` forms.
- **`src/table.rs` & `src/shard.rs`**: `RudisValue::VectorSet(Box<VectorSetValue>)` is stored directly in `RudisTable`, while standalone HNSW indexes live in `ShardDb.vector_indexes` and route across shards by index name (`target_shard_of_cmd`).
- **`src/rdb.rs` & `src/aof.rs`**: both `RudisValue::VectorSet` (`RDB_TYPE_VECTOR_SET = 24`) and standalone `ShardDb.vector_indexes` / `ShardDb.search_indexes` (`RDBX` trailer chunk in `save_extended_rdb_chunk`) are persisted across RDB snapshots and `BGREWRITEAOF`, and replicated to replicas via `PSYNC`.
- **`src/search.rs`** (Component 09): embeds `HnswIndex` inside `VectorFieldIndex::Hnsw` for `FT.CREATE ... SCHEMA ... VECTOR HNSW` and `FT.SEARCH` / `FT.HYBRID` queries.

---

### 7. Future Improvements

- **Medium — train PQ codebooks with k-means.** `ProductQuantizer::new` still uses a deterministic basis rather than Lloyd's k-means clustering over inserted embeddings.
- **Low — add `FLOAT16` / `BFLOAT16` / `INT8` SIMD distance kernels** on x86_64 and ARM NEON.

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Distance kernels use explicit AVX2 FMA instructions for cosine and L2 distance.
* **Gotcha 2**: Exact float reranking can be combined with quantized search for optimal recall.
* **Gotcha 3**: Vector deletion rewires neighbor graph edges incrementally.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
