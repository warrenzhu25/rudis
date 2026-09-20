# Component 08: Vector Search Engine: HNSW, SQ8 & Product Quantization (Implementation)

> **Source Files**: `src/vector.rs`


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

pub struct HnswIndex {
    pub name: String,
    pub dim: usize,
    pub metric: VectorMetric,
    pub m: usize,               // 16 — max neighbors per node, layer > 0
    pub m0: usize,              // 32 — max neighbors per node, layer 0
    pub ef_construction: usize, // 64
    pub ef_search: usize,       // 32
    pub ml: f64,                // 1 / ln(m) — level-generation scale
    pub entry_point: Option<usize>,
    pub max_layer: usize,
    pub nodes: Vec<Option<HnswNode>>,
    pub key_to_id: HashMap<Bytes, usize>,
    pub pq_quantizer: Option<ProductQuantizer>,
    rng_state: u64,             // fixed-seed xorshift64, see §2.5
}

pub struct HnswNode {
    pub id: usize,
    pub key: Bytes,
    pub vector: Vec<f32>,             // full-precision vector always kept
    pub quantized: Option<QuantizedVector>,  // SQ8, if requested
    pub pq: Option<PQVector>,                // PQ codes, if requested
    pub is_tiered: bool,
    pub neighbors: Vec<Vec<usize>>,   // one adjacency list per layer this node exists on
}

pub struct QuantizedVector {
    pub min_val: f32,
    pub scale: f32,      // (max - min) / 255
    pub sum_q: f32,      // precomputed for fast cosine norm
    pub sum_q_sq: f32,
    pub data: Vec<u8>,
}

pub struct PQVector { pub codes: Vec<u8> }   // one byte (0-255) per subvector

pub struct ProductQuantizer {
    pub dim: usize,
    pub m: usize,                       // number of subvectors
    pub d_sub: usize,                   // dim / m
    pub codebooks: Vec<Vec<Vec<f32>>>,  // [subvector][centroid 0..256][d_sub floats]
}
```

Note the node always keeps its full `Vec<f32>` regardless of whether SQ8/PQ is also
enabled — quantization here is an additional fast-path structure for candidate scoring,
not a memory-savings replacement for the raw vector (the "reranking" pass in §4.3 depends
on the exact float vector still being present).

---

### 4. Execution Algorithms & Code Logic

#### 4.1 SIMD distance kernels

```rust
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    { if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
        return unsafe { dot_product_avx2(a, b) };
    } }
    dot_product_portable(a, b)
}
```

`dot_product_avx2`/`l2_distance_sq_avx2` process 16 floats per iteration (two accumulated
`__m256` lanes via `_mm256_fmadd_ps`), with an 8-wide tail and a scalar remainder — real
FMA-fused AVX2, not a placeholder. `dot_f32_u8_avx2`/`l2_f32_u8_avx2` do the same for a
`f32` query against a `u8`-quantized vector (`_mm256_cvtepu8_epi32` widening + convert),
used by `QuantizedVector::compute_distance` so SQ8-accelerated scoring is itself
SIMD-accelerated, not just smaller.

`compute_distance` unifies the three metrics into one "smaller is closer" scale:

```rust
pub fn compute_distance(a: &[f32], b: &[f32], metric: VectorMetric) -> f32 {
    match metric {
        VectorMetric::L2 => l2_distance_sq(a, b).sqrt(),
        VectorMetric::IP => -dot_product(a, b),
        VectorMetric::Cosine => {
            let dot = dot_product(a, b);
            let norm_a = dot_product(a, a).sqrt();
            let norm_b = dot_product(b, b).sqrt();
            if norm_a == 0.0 || norm_b == 0.0 { 1.0 }
            else { (1.0 - (dot / (norm_a * norm_b))).max(0.0) }
        }
    }
}
```

#### 4.2 SQ8 quantization: precomputed sums avoid dequantizing on every comparison

`QuantizedVector::quantize` linearly maps each float into `[0, 255]` using the vector's own
min/max (`scale = (max - min) / 255`), and precomputes `sum_q`/`sum_q_sq` once at insert
time. `compute_distance` (query vs. stored SQ8 vector) then reconstructs the true dot
product / L2 / cosine algebraically from `dot_u8` (float·u8 SIMD dot) plus the precomputed
sums, without ever materializing a dequantized `Vec<f32>` — e.g. inner product is
`min_val * sum_query + scale * dot_u8`. `dequantize()` (full float reconstruction) exists
but is not on this hot path.

#### 4.3 HNSW insertion (`add_quantized_ext`)

```rust
let target_level = self.random_level();     // -ln(rand) * ml, capped at 16
...
// 1. Greedy descent from entry_point through layers ABOVE target_level
for lc in (target_level + 1..=self.max_layer).rev() { /* follow nearest neighbor at lc */ }
// 2. From target_level down to 0: beam search (search_layer) at each layer,
//    keep the nearest m_max candidates (m0 at layer 0, m elsewhere) as new neighbors,
//    link bidirectionally, and prune any neighbor whose list now exceeds m_max
for lc in (0..=target_level.min(self.max_layer)).rev() { ... self.prune_neighbors(...) }
if target_level > self.max_layer { self.max_layer = target_level; self.entry_point = Some(new_id); }
```

`prune_neighbors` prunes by **plain nearest-distance truncation** (sort candidates by
distance to the node, keep the closest `max_neighbors`) — not the original HNSW paper's
diversity-aware neighbor-selection heuristic. Simpler, but can leave the graph less
navigable under adversarial insertion orders than the paper's algorithm.

#### 4.4 Search (`search`/`search_tiered`) with optional exact rerank

```rust
pub fn search_tiered(&self, query: &[f32], k: usize, rerank: bool) -> Vec<(Bytes, f32)> {
    // 1. Greedy descent through layers max_layer..=1 (single nearest-neighbor hop per layer)
    // 2. Beam search at layer 0 via search_layer(query, curr_obj, search_ef, 0)
    //    where search_ef = ef_search.max(if rerank { k*3 } else { k })
    if rerank {
        // 3. Re-score every candidate with compute_distance() on the EXACT f32 vector
        //    (bypassing any SQ8/PQ approximation used during graph traversal), re-sort, truncate to k
    } else {
        // return the approximate candidates as-is, truncated to k
    }
}
```

`dist_to_node` (used throughout traversal) prefers `PQVector`+`ProductQuantizer` ADC if
both are present, else `QuantizedVector` SQ8, else the exact float vector — so a node
built with `PQ` or `QUANTIZE` is scored approximately during graph traversal, and only
gets compared against the true vector if the caller passes `RERANK` (`Command::Vquery {
rerank, .. }`).

#### 4.5 PQ encode + ADC scoring

```rust
pub fn compute_distance_table(&self, query: &[f32]) -> Vec<[f32; 256]> {
    // one 256-entry L2 table per subvector, precomputed once per query
}
pub fn compute_distance_adc(&self, table: &[[f32; 256]], pq: &PQVector) -> f32 {
    pq.codes.iter().enumerate().map(|(m, &c)| table[m][c as usize]).sum()
}
```

Classic ADC: encode the query's distance to all 256 centroids per subvector once, then
score every stored PQ-coded vector as a sum of table lookups — no per-candidate float
math, at the cost of the codebook-quality caveat in §2.4.

#### 4.6 Deletion leaves tombstoned slots, no compaction

`remove(key)` walks every layer's neighbor list to strip references to the removed id,
sets `self.nodes[id] = None` (leaving a hole — ids are never reused or compacted), and if
the removed node was the entry point, picks the first `Some` slot in `nodes` as the new
one (`self.nodes.iter().position(|n| n.is_some())`) — not necessarily a well-connected or
central node, just the first surviving slot.

---

### 5. Cross-Component Interactions

- **`src/resp.rs`**: parses `VADD`/`VQUERY`/`VSIM`/`VDEL`/`VINFO` into `Command` variants;
  `VADD`'s `metric` field is always parsed as `None` (per-call metric override isn't
  actually accepted on `VADD` — the metric is fixed at index-creation time only).
- **`src/shard.rs`**: `ShardDb::vadd` lazily creates the `HnswIndex` on first use
  (`vector_indexes.entry(index_name).or_insert_with(...)`), defaulting the metric to
  `VectorMetric::Cosine` if the index didn't already exist; `vsim` allows a one-off
  `metric_override` for that single comparison without changing the index's stored metric.
- **`src/connection.rs`**: dispatches all five commands straight to
  `router.local_db.borrow()[_mut]()` — no `target_shard_of_cmd` entry, no remote path (§1).
- **`src/search.rs`** (Component 09): no code-level relationship — separate engine, despite
  both being "search" subsystems.
- **`src/table.rs`**: no relationship — vector data lives entirely in `ShardDb.vector_indexes`,
  not in `RudisValue`/`RudisTable` at all.

---

### 7. Future Improvements

- **High — give vector indexes cross-shard reach (§1).** This is the single most important gap in this subsystem: an index name is currently silently scoped to whichever shard happened to receive the `VADD`/`VQUERY` connection, with no fan-out, no merge, and no error telling the caller their view is partial. At minimum, either (a) route all vector commands for a given index name to one designated "owner" shard (hash the index name, forward via a new `ShardMessage::Vector*` variant, mirroring how `src/block.rs`/`src/search.rs` already accept a narrow cross-shard exception for subsystems that need global visibility), or (b) document loudly at the protocol level (a startup warning, or a real error if `VADD`/`VQUERY` land on different shards for the same index) so this isn't a silent correctness surprise.
- **Medium — train PQ codebooks on real data (§2.4).** The current fixed deterministic basis (never trained via k-means or any clustering pass) will under-perform a real Product Quantization implementation on actual data distributions — recall will be measurably worse than the "PQ" name implies. A one-time or periodic k-means pass over inserted vectors (even a simple mini-batch k-means) per subvector would bring this in line with what PQ is normally expected to deliver.
- **Medium — support index persistence.** No `Cross-Component Interactions` entry connects `HnswIndex`/`ShardDb.vector_indexes` to the RDB save/restore path (Component 05/14) — a restart appears to lose all vector indexes with no explicit warning. Either wire `VADD`-built indexes into the RDB chunk format so they survive a restart, or document explicitly (in `VINFO`'s output, and in user-facing docs) that vector indexes are ephemeral today.
- **Low — implement the paper's diversity-aware neighbor selection instead of plain nearest-distance pruning (§4.3).** `prune_neighbors`'s simple truncation is simpler and cheaper but can leave the HNSW graph less navigable under adversarial or highly-clustered insertion orders than the original algorithm's heuristic — worth revisiting if recall on real workloads underperforms expectations.
- **Low — seed `rng_state` from something other than a fixed constant outside of test contexts (§2.5)**, so two indexes inserting the same vectors in the same order don't necessarily produce identical graph topology in production — currently a reasonable choice for reproducible tests, but worth an explicit opt-out for real deployments if graph-topology diversity ever matters for load distribution or resilience.

---
---
