# Component 08: Vector Search Engine — HNSW, SQ8/PQ/Binary Quantization, Redis 8 Vector Sets & Semantic Cache (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/vector.rs` (3,305 lines — engine logic is lines 1–2938; lines 2940–3305 are `#[cfg(test)] mod tests` with 9 test functions)
> **High-Level Design Spec**: [`docs/design/08_vector_engine.md`](../design/08_vector_engine.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

This document was rewritten from scratch against the current source. The file grew from ~1,378 to 3,305 lines since the previous pass. The major additions are: a full Redis 8 **Vector Sets** command suite (`VADD`/`VSIM`/`VREM`/`VINFO`/`VCARD`/`VDIM`/`VEMB`/`VLINKS`/`VRANDMEMBER`/`VSETATTR`/`VGETATTR`/`VISMEMBER`) with a hand-rolled `VSIM ... FILTER` expression language, a real Product Quantizer trainer (k-means++ seeding + Lloyd's iterations, replacing the old deterministic-basis codebook), an HNSW diversity-neighbor selection heuristic plus proper delete-time graph repair and node-slot reuse, 1-bit binary (sign) quantization, NVMe-backed `.vtier` disk spilling for the HNSW graph itself, `VECTOR_RANGE`-style radius search, and a first-class `SemanticCache` type used by the `SEMANTIC.*` command family. See §0 for the commit-by-commit history.

---

## 0. What Landed Since The Last Pass (`git log --oneline -- src/vector.rs`)

```
f456197 feat(vector): add NVMe-backed tiered vector reranking and sync AI-native docs/coverage
647efc9 feat(search): support @field:[VECTOR_RANGE radius $blob] queries and $YIELD_DISTANCE_AS
a67d51a feat(vector): k-means++ and Lloyd codebook training for Product Quantizer
9ec3e1c feat(vector): HNSW diversity neighbor heuristic, delete graph repair and slot reuse
e71631a feat(vector): Redis 8 Vector Sets command suite, FILTER evaluator and shard routing
1350170 feat(semantic): add first-class semantic cache commands for LLM/agent workloads
5099d5b feat(search): pre-filtered hybrid KNN (ad-hoc brute force + filtered HNSW traversal)
f0ce2a9 feat(search): full FT.CREATE VECTOR attributes, FLAT index, TYPE-aware decoding and binary-safe HASH ingest
784ccb4 feat(search): multi-match json_nummultby and HNSW vector index integration in RediSearch
8bddd7b feat(vector): implement AVX-512 and fused AVX2/FMA SIMD distance acceleration for HNSW vector search
3c1157a refactor: complete clippy and code quality optimization sweep
5a63370 Implement Geospatial Engine, RedisBloom Probabilistic Structures, and Product Quantization with ADC
5c9fecb feat: Add zero-copy I/O, AVX2 SIMD vector acceleration, and RedisJSON engine
6d60ac6 feat: add kTLS support, SQ8 vector quantization with rerank, active-active CRDTs with tombstone GC, and vector...
684cc79 Implement zero-copy snapshots, HNSW vector search, Redis 7 RESP3/functions/tracking, and jemalloc profiling
```

**Where things actually live (re-verified, corrects a stale assumption)**: `SemanticCache`/`SemanticEntry`/`SemanticHit` (§2.6) are genuinely defined and implemented **in this file** (`src/vector.rs:2765–2938`), not in a separate `src/semcache.rs` — **no such file exists in the tree**. `src/agent.rs` only *consumes* `HnswIndex` directly (`use crate::vector::{HnswIndex, VectorMetric}`, `AgentMemorySession.index: Option<HnswIndex>`) for token-budgeted episodic recall over conversation turns — it does not define its own vector index type, and has nothing to do with `SemanticCache`. `src/mcp.rs` exposes `SEMANTIC.*`/`VADD`/`VSIM` indirectly as MCP tools but defines no vector data structures of its own. There is also **no `RudisValue::VectorSet` enum variant and no `VectorSetValue` wrapper struct** — despite what an older doc pass implied, Redis 8 Vector Sets are just `HnswIndex` instances living in `ShardDb.vector_indexes: HashMap<String, HnswIndex>`, distinguished from legacy/FT-internal `HnswIndex` instances purely by the `is_redis_vset: bool` flag plus the `quant`/`projection`/`attributes` fields on `HnswIndex` itself (§2.2).

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/vector.rs` | SIMD distance kernels, SQ8/PQ/binary quantization, HNSW graph (build/search/delete), exact `FlatIndex`, the `VSIM ... FILTER` expression parser, `SemanticCache` | `VectorMetric`, `QuantizedVector`, `ProductQuantizer`/`PQVector`, `HnswNode`, `HnswIndex`, `FlatIndex`, `VectorFieldIndex`, `SemanticCache`, `dot_product`/`l2_distance_sq`/`cosine_distance`/`compute_distance` |
| `src/shard.rs` | Owns `ShardDb.vector_indexes: HashMap<String, HnswIndex>` and `ShardDb.semantic_caches: HashMap<Bytes, SemanticCache>`; command-level glue (`vadd`/`vadd_ext`/`vsim_ext`/`vdel`/`vinfo`/`vsetattr`/`semantic_info`, …) | `ShardDb::vadd_ext`, `ShardDb::vsim_ext`, `ShardDb::semantic_info` (lines 3392–3760) |
| `src/resp.rs` | Parses the legacy and Redis 8 `V*` command grammars and the `SEMANTIC.*` grammar into `Command` variants | `"VADD"`/`"VSIM"`/`"VDEL"`/`"VREM"`/`"VINFO"`/`"VCARD"`/`"VDIM"`/`"VEMB"`/`"VLINKS"`/`"VRANDMEMBER"`/`"VSETATTR"`/`"VGETATTR"`/`"VISMEMBER"` arms (lines 9556–10153), `"SEMANTIC.SET"`/`"SEMANTIC.GET"`/`"SEMANTIC.DEL"`/`"SEMANTIC.FLUSH"`/`"SEMANTIC.INFO"` arms (lines 12224–12441) |
| `src/connection.rs` | Dispatches `Command::Vadd/Vsim/Vdel/Vinfo/.../Semantic*` to the `ShardDb` methods above and writes RESP2/RESP3 replies | `Command::Vsim { .. }` arm (line 18385), `command_name_for` V*/SEMANTIC mappings (lines 4201–4218) |
| `src/search.rs` (Component 09) | Embeds `FlatIndex`/`HnswIndex` from this file inside `InvertedIndex.vector_indices: HashMap<String, VectorFieldIndex>` for `FT.CREATE ... VECTOR` fields | `build_vector_index` (search.rs:642–678) |
| `src/agent.rs` (Component 20) | Embeds a private `HnswIndex` per agent session for episodic-memory KNN recall (`AGENT.MEM.*`) — a separate consumer, unrelated to `SemanticCache` | `AgentMemorySession.index: Option<HnswIndex>` |
| `src/aof.rs` | AOF rewrite re-emits every live vector-set element as a `VADD` command and every live semantic-cache entry as a `SEMANTIC.SET` command | lines 2371–2429 |

---

## 2. Component Architecture & Data Structures

```
  VADD idx ele FP32 <32B blob> [Q8|BIN|NOQUANT] [EF n] [M n] [REDUCE d] [TIERED] [SETATTR json]
  VSIM idx ELE|FP32|VALUES <target> [COUNT n] [EF n] [FILTER expr] [FILTER-EF n] [TRUTH] [WITHSCORES]
  FT.CREATE idx SCHEMA f VECTOR FLAT|HNSW TYPE FLOAT32 DIM d DISTANCE_METRIC m [M n] [EF_CONSTRUCTION n]
  SEMANTIC.SET ns id prompt response VECTOR d f1..fd [EX s] [SCOPE s] [QUANTIZE]
  AGENT.MEM.* (src/agent.rs)  →  private per-session HnswIndex, unrelated to the above
                         │                    │                              │
                         ▼                    ▼                              ▼
       ShardDb.vector_indexes["idx"]   InvertedIndex.vector_indices   ShardDb.semantic_caches[ns]
         : HnswIndex (is_redis_vset=true)  ["field"]: VectorFieldIndex    : SemanticCache { index: HnswIndex, entries }
                         │             (Flat(FlatIndex) | Hnsw(HnswIndex))            │
                         └───────────────────────┬────────────────────────────────────┘
                                                  ▼
                                   All three ultimately wrap the SAME
                                   HnswIndex/FlatIndex types below —
                                   there is exactly one HNSW/FLAT implementation
                                   in the whole codebase.
```

### 2.1 `VectorMetric` (lines 6–35)

```rust
pub enum VectorMetric { Cosine /* default */, L2, IP }
```

`FromStr` accepts `COSINE`, `L2`/`EUCLIDEAN`, `IP`/`DOT`/`INNERPRODUCT` (case-insensitive). `compute_distance` (line 505) maps each metric to a **distance** (smaller = closer), not a raw score:
- `L2` → `sqrt(l2_distance_sq(a, b))`
- `IP` → `-dot_product(a, b)` (negated so smaller distance still means "more similar")
- `Cosine` → `cosine_distance(a, b)` = `(1 - cos_sim).max(0.0)`, range `[0, 2]`

### 2.2 `HnswIndex` — the one graph implementation used by every consumer (lines 1010–1043)

```rust
pub struct HnswIndex {
    pub name: String,
    pub dim: usize,
    pub metric: VectorMetric,
    pub m: usize,                          // max neighbors/node above layer 0 (default 16)
    pub m0: usize,                         // max neighbors/node at layer 0 (default 32 = 2*m)
    pub ef_construction: usize,            // beam width during insertion (default 64)
    pub ef_search: usize,                  // beam width during query (default 32)
    pub ml: f64,                           // 1 / ln(m) — level-generation scale
    pub entry_point: Option<usize>,
    pub max_layer: usize,
    pub nodes: Vec<Option<HnswNode>>,      // tombstoned via None on delete, slots recycled via free_ids
    pub free_ids: Vec<usize>,
    pub key_to_id: HashMap<Bytes, usize>,
    pub pq_quantizer: Option<ProductQuantizer>,
    pub pq_trained: bool,
    pub quant: VQuant,                     // Redis 8 vset quantization mode: NoQuant | Q8 | Bin
    pub attributes: HashMap<Bytes, String>,// per-element JSON metadata (VSETATTR / VSIM...FILTER)
    pub projection: Option<Vec<f32>>,      // row-major dim×input_dim random-projection matrix (VADD...REDUCE)
    pub input_dim: usize,                  // client-facing dim before projection (0 = no projection)
    pub uid: u64,                          // process-unique id (VINFO vset-uid), from an AtomicU64 counter
    pub is_redis_vset: bool,               // true only for indexes created via Redis 8 VADD syntax
    pub tier_path: Option<PathBuf>,        // NVMe .vtier backing file (VADD...TIERED)
    pub tiered_bytes: u64,                 // total bytes ever appended to tier_path
    rng_state: u64,                        // private xorshift64 state (layer assignment, VRANDMEMBER)
}
```

`HnswIndex::new(name, dim, metric)` (line 1046) hard-codes `m=16`, `m0=32`, `ef_construction=64`, `ef_search=32`, `ml=1/ln(16)`. `HnswIndex::with_params(name, dim, metric, m, ef_construction, ef_runtime)` (line 1079, used by `FT.CREATE ... VECTOR HNSW` — §8) overrides `m`/`ef_construction`/`ef_search` but **always recomputes `m0 = m*2`** regardless of the caller — there is no independent `M0` knob anywhere in the codebase; RediSearch's `M0` attribute, if ever added, would have nowhere to plug in.

```rust
pub struct HnswNode {
    pub id: usize,
    pub key: Bytes,
    pub vector: Vec<f32>,                  // empty Vec::new() once spilled to .vtier
    pub quantized: Option<QuantizedVector>,// SQ8, 1 byte/dim
    pub binary: Option<Vec<u64>>,          // 1-bit sign quantization, dim.div_ceil(64) u64 words
    pub pq: Option<PQVector>,              // PQ codes, 1 byte/subvector
    pub is_tiered: bool,
    pub tier_offset: Option<u64>,          // byte offset into tier_path
    pub neighbors: Vec<Vec<usize>>,        // neighbors[layer] = adjacency list at that layer
}
```

A node can carry **more than one** compressed representation simultaneously (e.g. both `quantized` and `pq`, or `quantized` with `vector` empty when tiered) — `dist_to_node` (§3.3) picks exactly one, in a fixed priority order, per call.

### 2.3 Quantized payload types

```rust
pub struct QuantizedVector { pub min_val: f32, pub scale: f32, pub sum_q: f32, pub sum_q_sq: f32, pub data: Vec<u8> }
pub struct PQVector { pub codes: Vec<u8> }
pub struct ProductQuantizer { pub dim: usize, pub m: usize, pub d_sub: usize, pub codebooks: Vec<Vec<Vec<f32>>> }
pub enum VQuant { NoQuant /* f32, default */, Q8 /* int8 */, Bin /* bin */ }
```
`codebooks` is `m` subspaces × 256 centroids × `d_sub` floats — i.e. every subspace always has a full 256-entry (one-byte-code) codebook, trained or not (§4.3).

### 2.4 `Candidate` / `FurthestCandidate` — the two heap orderings (lines 936–981)

Two nearly-identical wrapper structs exist purely to get opposite `Ord` directions out of `std::collections::BinaryHeap` (a max-heap): `Candidate::cmp` reverses the comparison so `BinaryHeap<Candidate>` pops the **smallest**-distance element first (used as the traversal frontier), while `FurthestCandidate::cmp` is the natural order so `BinaryHeap<FurthestCandidate>` pops the **largest**-distance element first (used as the bounded "current best-`ef`" result set `W`, so the worst element is always at the top and cheap to evict).

### 2.5 `FlatIndex` — exact brute-force, structure-of-arrays (lines 2511–2650)

```rust
pub struct FlatIndex {
    pub name: String, pub dim: usize, pub metric: VectorMetric,
    keys: Vec<Bytes>,
    data: Vec<f32>,        // contiguous keys.len() * dim row-major storage
    key_to_pos: HashMap<Bytes, usize>,
}
```
`add` does an in-place overwrite for an existing key or an `O(dim)` append; `remove` is an `O(dim)` swap-remove (last row copied into the removed slot, `key_to_pos` patched for the moved key — no tombstoning, no fragmentation, unlike `HnswIndex.nodes`). `search_filtered`/`range_filtered` are a straight `O(N·dim)` scan through `compute_distance`, with `search_filtered` maintaining a bounded `BinaryHeap<FurthestCandidate>` of size `k` (an `O(N log k)` top-k selection) rather than sorting the full result set.

### 2.6 `VectorFieldIndex` — the exact type `search.rs` wraps per `VECTOR` field (lines 2655–2763)

```rust
pub enum VectorFieldIndex { Flat(FlatIndex), Hnsw(HnswIndex) }
```
Every method (`add`/`remove`/`get_vector`/`search`/`search_filtered`/`range_filtered`/`memory_usage`/`metric`/`dim`/`len`/`algorithm`) is a two-arm `match` that forwards to the wrapped type. This confirms the finding from the Component 09 doc pass: `InvertedIndex.vector_indices: HashMap<String, VectorFieldIndex>` is a genuine per-field `FlatIndex`/`HnswIndex`, built by `search.rs::build_vector_index` (§8) directly from `FieldType::Vector`'s `algorithm` (`"FLAT"` vs. everything else) and `VectorFieldAttrs` (`m`/`ef_construction`/`ef_runtime`/`initial_cap`) — the **same struct types** as Redis 8 Vector Sets, just owned by a different collection (`InvertedIndex.vector_indices` vs. `ShardDb.vector_indexes`) with independently-configured parameters (§9).

### 2.7 `SemanticCache` — genuinely lives here, not in a separate file (lines 2765–2938)

```rust
pub struct SemanticEntry { pub id: Bytes, pub prompt: Bytes, pub response: Bytes, pub scope: Option<Bytes>, pub expire_at: Option<Instant>, pub tokens: u64 }
pub struct SemanticHit   { pub id: Bytes, pub prompt: Bytes, pub response: Bytes, pub score: f32 }
pub struct SemanticCache {
    pub namespace: String,
    pub index: HnswIndex,                  // always VectorMetric::Cosine (hard-coded, §4.6)
    pub entries: HashMap<Bytes, SemanticEntry>,
    pub hits: u64, pub misses: u64, pub tokens_saved: u64, pub evicted_expired: u64,
}
```
One `SemanticCache` = one `SEMANTIC.*` namespace = one private `HnswIndex` plus a parallel metadata map keyed by the same `Bytes` id (§4.6).

---

## 3. SIMD Distance Kernels — Three-Tier Runtime Dispatch (lines 37–512)

`dot_product`, `l2_distance_sq`, and `cosine_distance` each do **the same three-way runtime check** (via `is_x86_feature_detected!`, no compile-time target requirement — the binary ships all three tiers and picks one per call):

```rust
pub fn dot_product(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f") { return unsafe { dot_product_avx512(a, b) }; }
        if is_x86_feature_detected!("avx2") && is_x86_feature_detected!("fma") {
            return unsafe { dot_product_avx2(a, b) };
        }
    }
    dot_product_portable(a, b)
}
```

1. **AVX-512** (`*_avx512`, gated on `avx512f` only): two `__m512` accumulators (`acc0`/`acc1`), 32 `f32` lanes/iteration via `_mm512_fmadd_ps`, horizontally reduced by `hsum512_ps` (itself built on top of `hsum256_ps`). Cosine's AVX-512 kernel accumulates `dot`/`na`/`nb` in one pass (three accumulators, 16 lanes/iteration — it does *not* get the 32-lane double-accumulator treatment `dot_product`/`l2` get).
2. **AVX2+FMA** (`*_avx2`, gated on **both** `avx2` **and** `fma` — a CPU can have one without the other): two `__m256` accumulators, 16 lanes/iteration via `_mm256_fmadd_ps`, reduced by `hsum256_ps` (extract+add+shuffle chain down to a scalar).
3. **Portable fallback** (`*_portable`, always compiled, zero `unsafe`): `a.as_chunks::<8>()` manual 8-lane unrolling plus a scalar remainder loop. This is what runs on non-x86_64 targets (e.g. ARM) — **there is no NEON kernel**; ARM always takes the scalar-unrolled path.

`cosine_distance_{avx2,avx512,portable}` all share the identical final step: `norm = (na*nb).sqrt(); if norm == 0.0 { 1.0 } else { (1.0 - dot/norm).max(0.0) }` — a zero vector is defined to have cosine distance `1.0` from everything (not `0.0`/`NaN`).

Two more AVX2-only kernels exist purely to accelerate **asymmetric** float-query-vs-`u8`-quantized-data distance without dequantizing: `dot_f32_u8_avx2` (`_mm256_cvtepu8_epi32` + `_mm256_cvtepi32_ps` to widen 8 `u8` lanes to `f32`, then FMA against the float query) and `l2_f32_u8_avx2` (same widening, plus `min_val`/`scale` dequantization fused into the FMA via `_mm256_fmadd_ps(f32_v, scale_vec, min_vec)`). Both are gated on `avx2 && fma`; their fallback is a plain scalar loop (§4.1) — **there is no AVX-512 asymmetric SQ8 kernel**, despite AVX-512 existing for full-precision distances.

`quantize_binary`/`binary_hamming_cosine_distance` (lines 912–934) have **no SIMD path at all** — `quantize_binary` packs sign bits into `u64` words with a scalar loop, and `binary_hamming_cosine_distance` computes `(wa ^ wb).count_ones()` per word using the hardware `POPCNT`-backed `u32::count_ones` intrinsic (fast, but not vectorized across words) then normalizes to `2*diff_bits/dim` (range `[0, 2]`, matching the Cosine-distance convention).

---

## 4. Quantization — Exact Encoding, Byte Layout & Recall Tradeoffs

### 4.1 SQ8 Scalar Quantization — `QuantizedVector` (lines 524–643)

`QuantizedVector::quantize(v)`: finds `min_val`/`max_val` over the vector, sets `scale = (max_val - min_val) / 255.0` (or `1.0` if the vector is constant), then for each component computes `q = ((x - min_val) / scale).round().clamp(0, 255) as u8`. Two running sums — `sum_q = Σq`, `sum_q_sq = Σq²` — are accumulated **once, at quantize time**, specifically so `compute_distance` never has to touch every byte twice. Byte layout: `min_val: f32` + `scale: f32` + `sum_q: f32` + `sum_q_sq: f32` (16 bytes of header) + `data: Vec<u8>` (`dim` bytes) — **75% memory reduction vs. `f32`** (4 bytes/dim → 1 byte/dim, ignoring the fixed 16-byte header).

`compute_distance(query, metric)` reconstructs each metric **algebraically from the precomputed sums**, without ever materializing a dequantized `Vec<f32>`:
- `IP`: `dot = min_val * Σquery + scale * dot_u8(query, data)`, distance `= -dot`.
- `L2`: `l2_sq(query, data, min_val, scale) = Σ(query_i - (min_val + data_i*scale))²`, distance `= sqrt(l2_sq)`.
- `Cosine`: same `dot` as `IP`, plus `‖b‖² = dim*min_val² + 2*min_val*scale*sum_q + scale²*sum_q_sq` (algebraic expansion of `Σ(min_val + data_i*scale)²` using only the precomputed sums), `‖a‖²` via `dot_product(query, query)`; distance `= (1 - dot/(‖a‖·‖b‖)).max(0.0)`.

`dot_u8`/`l2_sq` each try the AVX2 asymmetric kernel first (`dot_f32_u8_avx2`/`l2_f32_u8_avx2`, §3) and fall back to a scalar per-element loop — **no AVX-512 path** for either.

### 4.2 Binary (1-bit sign) Quantization (lines 912–934)

`quantize_binary(v)`: `words = dim.div_ceil(64)` `u64`s, bit `i` set iff `v[i] > 0.0` (strictly greater — a value of exactly `0.0` is treated as the negative/unset bit). Distance is Hamming-XOR popcount normalized to `[0, 2]` (§3). This is the most aggressive compression tier: `dim.div_ceil(64) * 8` bytes total — e.g. 128 dims → 2 `u64` words → **16 bytes**, a 96.9% reduction vs. 512-byte `f32` storage, matching PQ's compression ratio but with zero training and much coarser recall (1 bit/dim vs. PQ's typically-8-bits/subvector).

### 4.3 Product Quantization (PQ) with Asymmetric Distance Computation (lines 653–895)

`ProductQuantizer::new(dim, m)`: `m = m.max(1)`, `d_sub = (dim/m).max(1)`. **Important edge case**: if `dim % m != 0`, the trailing `dim - m*d_sub` dimensions of every vector are simply never read by `encode`/`compute_distance_table` (both slice `[start..(start+d_sub).min(v.len())]`) — those dimensions contribute **nothing** to PQ-based distance, silently.

Before training, `new` seeds each of the 256 centroids per subspace **deterministically**, not randomly: centroid `0` is the zero vector, centroids `1..=d_sub` are positive unit basis vectors, `d_sub+1..=2*d_sub` are negative unit basis vectors, and the remaining `256 - 2*d_sub - 1` centroids come from a SplitMix64-style hash of `(code, subspace, component)` mapped into `[-1, 1]`. This untrained codebook is what every PQ-encoded vector uses until `train`/`train_pq` runs.

`encode(v)`: for each of the `m` subspaces, linear-scan all 256 centroids for the minimum squared-L2 distance, store the winning centroid index as one `u8` — so `PQVector.codes.len() == m`, i.e. **`dim` floats (4·dim bytes) compress to `m` bytes**. E.g. `dim=128, m=16` (the auto-derived default, see below) → 512 bytes → 16 bytes = **96.9% compression**, matching the module doc comment.

**Asymmetric Distance Computation (ADC)**: `compute_distance_table(query)` precomputes, once per query, an `m × 256` table of squared distances from each query subvector to every centroid in that subspace (`Vec<[f32; 256]>`, `m` allocations of 1KB each). `compute_distance_adc(table, pq)` then sums `table[subspace][pq.codes[subspace]]` for each of the `m` codes — an `O(m)` table lookup per candidate after the one-time `O(m·256·d_sub) = O(dim·256)` table build. `compute_distance_with_vec` is the convenience wrapper that builds the table and immediately looks up one vector (used by `dist_to_node`, which does **not** reuse a shared table across candidates within a single query — every `dist_to_node` call for a PQ node rebuilds the full `m×256` table from scratch, an easy-to-miss `O(dim·256)` cost multiplied by every candidate visited during a beam search).

`ProductQuantizer::train(dim, m, samples, max_iters)` (lines 791–894) is a genuine two-phase trainer:
1. **Furthest-first k-means++ seeding** (deterministic, not randomized): centroid 0 = `samples[0]`'s subvector; each subsequent centroid up to `k_active = samples.len().clamp(1, 256)` is the sample with the **maximum** current min-distance-to-any-chosen-centroid (greedy farthest-point sampling), tracked incrementally via a running `min_sq_dist` vector — this is `O(k_active · samples.len())`, not the full `O(k²·N)` naive re-scan. Seeding stops early (`actual_k < k_active`) if the farthest remaining point is within `1e-12` of an existing centroid (degenerate/duplicate data).
2. **Lloyd's k-means iterations** (up to `max_iters`, early-exits on a fixed point / no reassignments): standard assign-to-nearest-centroid + recompute-centroid-as-mean loop over `0..actual_k` centroids; centroids beyond `actual_k` (i.e. `actual_k..256`) are **left at their original deterministic-basis seed values** — a partially-trained codebook whenever `samples.len() < 256`.

`HnswIndex` auto-triggers training **once**, inside `add_quantized_ext`, the first time the index's element count reaches **15** after a PQ-enabled insert (`!self.pq_trained && self.len() >= 15`), using `max_iters = 10` hard-coded and `m = (dim/8).clamp(1, 16)` if no `ProductQuantizer` exists yet. `distortion(samples)` (mean squared reconstruction error) exists purely for the test suite's assertion that training reduces error — it is not used by any production code path.

**Known training gap** (§6 item 1): `enable_pq(m)` only installs a fresh `ProductQuantizer` and clears `pq_trained` — it does **not** retroactively PQ-encode any already-inserted node. `train_pq(max_iters)` (the explicit, non-auto retraining path) only re-encodes nodes whose `node.pq` is **already** `Some(..)` — nodes that were inserted via plain `add()`/`add_quantized()` (i.e. without `quantize_pq=true`) never get a `node.pq` value from `enable_pq`+`train_pq` alone.

---

## 5. HNSW Graph — Construction, Search & Deletion (lines 1045–2097)

### 5.1 Layer assignment (`random_level`, lines 1312–1323)

Private `rng_state: u64` xorshift64 (`x ^= x<<13; x ^= x>>7; x ^= x<<17`), reseeded to a fixed constant (`0x853c49e6748fea9b`) in `new` — **deterministic across process restarts for a freshly-created index** (not seeded from OS entropy or time). `random_level()` draws `r = next_random_f64().max(1e-15)` and returns `floor(-ln(r) * ml)`, capped at `16` — standard HNSW exponential-decay level assignment, `ml = 1/ln(m)` tuned so the expected number of layers is `O(log N)`.

### 5.2 Insertion — `add_quantized_ext` (lines 1425–1596)

1. If `key` already exists, `remove(&key)` first (full delete-then-reinsert; there is no in-place vector update — any change to an existing key's embedding pays the full deletion-repair cost of §5.5 before re-insertion).
2. Draw `target_level`; allocate `new_id` from `free_ids.pop()` or `nodes.len()` (slot reuse).
3. Build the node's compressed payload(s): SQ8 `quantized` if `quantize_sq8 || self.quant == Q8 || (tiered && !quantize_pq)`; `binary` if `self.quant == Bin`; `pq` if `quantize_pq` (lazily creating/auto-training the `ProductQuantizer` per §4.3).
4. **First element in an empty index**: becomes `entry_point` directly, `max_layer = target_level`, no graph search needed.
5. **Otherwise**: greedy single-best-neighbor descent from `entry_point` down through layers `max_layer..=target_level+1` (one neighbor-improves-distance hop at a time, no beam) to find the best entry point at `target_level+1`.
6. **Insert the node**, then for each layer `min(target_level, max_layer)..=0` (descending): run `search_layer(vector, curr_obj, ef_construction, layer)` for a beam of candidates, run `select_neighbors_heuristic` (§5.3) to pick up to `m0` (layer 0) or `m` (layer > 0) neighbors, wire the new node's `neighbors[layer]`, and **reciprocally** add the new node to each chosen neighbor's adjacency list — if that pushes a neighbor over its `m_max`, immediately call `prune_neighbors` on it (§5.3) rather than deferring.
7. If `target_level > max_layer`, the new node becomes the new `entry_point` and `max_layer` is raised.
8. If `tiered`, `spill_node_to_tier` runs **after** graph linking (§5.6).

### 5.3 Neighbor selection — Malkov & Yashunin Algorithm 4, `keepPrunedConnections = true` (lines 1370–1406, 1598–1623)

```rust
fn select_neighbors_heuristic(&self, candidates: &[Candidate], m_max: usize) -> Vec<usize> {
    // candidates already sorted by ascending distance to the base point
    for cand in candidates {
        let is_diverse = selected.iter().all(|&sel_id| {
            let dist_to_sel = self.dist_to_node(&node_vector_cow(sel_id), cand_node);
            cand.distance <= dist_to_sel   // cand is closer to the base than to any already-picked neighbor
        });
        if is_diverse { selected.push(cand.id) } else { pruned.push(cand.id) }
    }
    // backfill remaining slots up to m_max from `pruned`, closest-first
}
```
A candidate is accepted only if it is closer to the **base point** than to every neighbor already selected — this is what spreads edges across distinct directions around a cluster instead of connecting a node to `m` near-duplicates of the same nearby point. Rejected candidates are not discarded outright: once `selected` runs out of genuinely diverse candidates, the heuristic backfills remaining `m_max` slots from the pruned list (closest-first) so node degree doesn't collapse under an aggressively diverse neighborhood. `prune_neighbors(node_id, layer, max_neighbors)` re-runs the exact same heuristic on a node's *current* neighbor list whenever an insertion or deletion pushes it over budget — it is the single re-pruning code path used by both insertion (§5.2 step 6) and deletion repair (§5.5).

### 5.4 Search — `search_layer` / `search_layer_filtered` / `search_filtered` (lines 1625–1873)

`search_layer(query, entry_point, ef, layer)`: classic bounded best-first search — a `Candidate` min-heap frontier, a `FurthestCandidate` max-heap of the current best-`ef` (`W`), a `HashSet<usize>` **freshly allocated per call** (no thread-local/epoch-stamped visited-set reuse — every query allocates and drops its own `HashSet`). Expansion stops early once the frontier's closest unvisited candidate is farther than `W`'s current worst member and `W` is already full (`w.len() >= ef`).

`search_layer_filtered` is a separate, near-duplicate implementation (not a generic closure parameter on `search_layer`) specialized to layer 0 only: rejected-by-`filter` nodes are still **traversed** (pushed onto the candidate frontier) so the graph stays navigable through a selective filter, but only accepted nodes enter `W` / the final result — this is what lets `VSIM ... FILTER` and `SemanticCache::get`'s scope filter stay approximately correct instead of returning too few results when the filter rejects most of the graph's neighborhood.

Top-level `search_filtered(query, k, ef_runtime, rerank, filter)`: greedy single-hop descent through layers `max_layer..=1` (same shape as insertion step 5), then layer-0 beam search with `search_ef = ef_runtime.unwrap_or(ef_search).max(1)`, widened to `.max(k*3)` when `rerank` else `.max(k)`. If `rerank`, the beam's candidates are **all** re-scored with `compute_distance(query, node_vector_cow(node), metric)` — i.e. full-precision, bypassing whatever `quantized`/`binary`/`pq` shortcut `dist_to_node` would otherwise take — sorted, and truncated to `k`; the graph-traversal distances that got the candidates onto the beam in the first place are discarded. Without `rerank`, the beam's own (possibly-quantized) distances are returned as-is.

`range_filtered(query, radius, epsilon, filter)` (lines 1877–1956): same layer `1..=max_layer` descent to find an entry point, then seeds a fresh best-first search with `search_layer(.., ef_search.max(32), 0)` before switching to an unbounded BFS/priority expansion that keeps visiting any neighbor within `radius * (1 + epsilon)` (`epsilon` defaults to `0.01`) and only **emits** neighbors within the exact `radius`. The seed-then-expand structure exists specifically so a radius search that happens to be far from `entry_point` still reaches the query's true neighborhood.

### 5.5 Distance dispatch — `dist_to_node` priority order (lines 1348–1364)

```rust
if node.pq.is_some() && self.pq_quantizer.is_some()  { /* PQ ADC */ }
else if node.binary.is_some()                        { /* Hamming */ }
else if node.quantized.is_some()                      { /* SQ8 algebraic */ }
else                                                   { /* compute_distance on node_vector_cow (full precision, possibly read from .vtier) */ }
```
A node with **both** `quantized` and `pq` set (possible — nothing in `add_quantized_ext` prevents it) always takes the PQ branch; the SQ8 payload becomes dead weight for search purposes on that node (though it's still returned by `VEMB ... RAW`, §7).

### 5.6 NVMe `.vtier` disk spilling (lines 1097–1154)

`spill_node_to_tier(node_id, full_vector)`: lazily creates `tier_path` at `{tmp}/rudis-vtier-{pid}-{uid}.bin` on first use, **re-opens the file with `OpenOptions::new().create(true).append(true)` on every single spill call** (no cached/long-lived file handle), appends the vector's raw little-endian `f32` bytes, records the pre-write file length as `node.tier_offset`, then **clears `node.vector = Vec::new()`** so only the compact SQ8 payload remains resident for graph traversal. `node_vector_cow` (lines 1099–1122) is the read side: returns `Cow::Borrowed` if `node.vector` is non-empty; otherwise, if tiered, re-opens the file **again** (a fresh `std::fs::File::open` per call) and does a positional `read_exact_at` (`pread`, no seek, no mutex) of exactly `dim * 4` bytes at `tier_offset`; falls back to `QuantizedVector::dequantize()` if the file is unavailable; falls back to an all-zero vector as a last resort.

This is a deliberately minimal, ad-hoc append-only file — **it shares nothing with the `ShardTierManager`/`SmallBins`/`fallocate`-hole-punching machinery in `src/tiering.rs`** (Component 07). Bytes for a removed or re-added (delete-then-reinsert, §5.2 step 1) vector are never reclaimed; `tier_path` only grows for the lifetime of the index.

### 5.7 Deletion — `remove`, graph repair & slot reuse (lines 1959–2024)

1. Pop `key_to_id[key]`, tombstone `nodes[id] = None`, push `id` onto `free_ids` (reused by the next `add_quantized_ext` call, so `nodes.len()` only grows when there are no free slots — confirmed by the `test_hnsw_diversity_heuristic_and_delete_slot_reuse` test, §10).
2. For every layer the deleted node participated in: strip `id` from every former neighbor's adjacency list at that layer, then **repair connectivity** — for each former neighbor `u` below its `m_max` at that layer, compute distances from `u` to every *other* former neighbor `v` of the deleted node (excluding ones `u` is already linked to), sort by distance, and greedily backfill `u`'s adjacency list up to `m_max`. This directly rewires the deleted node's former neighborhood into a (partial) clique rather than leaving dangling degree-reduced nodes.
3. If the deleted node was the `entry_point` (or the index just became empty), rescans **every remaining node** (`O(N)`) for the one with the highest `neighbors.len() - 1` (used as a proxy for "highest layer a node exists on") and promotes it to the new `entry_point`/`max_layer`.

---

## 6. Known Bugs, Limitations & Dead Code (verified by reading, not carried over from memory)

1. **`enable_pq(m)` does not retroactively PQ-encode existing vectors.** It only installs a fresh, untrained `ProductQuantizer` and clears `pq_trained`. `train_pq` only re-encodes nodes whose `node.pq` is already `Some(..)` (§4.3) — nodes inserted via plain `add()`/`add_quantized()` before `enable_pq` was called never gain PQ codes from this pair of calls alone; only elements inserted *after* with `quantize_pq=true` participate.
2. **`vsim_ext`'s rerank heuristic looks inverted.** `src/shard.rs:3586`: `let rerank = idx.quant == crate::vector::VQuant::NoQuant;`. Reranking (§5.4) only has a real effect when the graph's own per-candidate distances are lossy (`Q8`/`Bin`) — for `NoQuant` sets, `dist_to_node` already uses full-precision `compute_distance`, so reranking a `NoQuant` set recomputes the same numbers it already has. As written, **quantized (`Q8`/`Bin`) Redis 8 Vector Sets never get the exact-distance rerank pass**, while `NoQuant` sets rerank pointlessly. This looks backwards from the typical "quantize for fast traversal, rerank exact for final accuracy" pattern the tiering code (§5.6) and `search_tiered`/`search_ext`'s `rerank` parameter were clearly built to support.
3. **`VSIM`/`VLINKS` similarity score formula assumes a Cosine-shaped `[0, 2]` distance range regardless of the index's actual metric.** Both `vsim_ext` (`shard.rs:3601`) and `HnswIndex::links` (`vector.rs:1279`) compute `score = (1.0 - dist / 2.0).clamp(0.0, 1.0)`. This is exactly right for `Cosine` (whose distance range is genuinely `[0, 2]`, §2.1), but `L2` distance is `sqrt(l2_sq)` (unbounded, commonly ≫ 2 for real embeddings) and `IP` distance is `-dot_product` (unbounded, either sign) — for non-Cosine vector sets, `WITHSCORES`/`VLINKS` output is either meaninglessly clamped to `0.0` or otherwise not a genuine similarity in `[0, 1]`.
4. **`VSIM ... NOTHREAD` is parsed but discarded.** `resp.rs` parses the flag into `Command::Vsim.no_thread`, but `connection.rs:18396` destructures it as `no_thread: _` — there is no alternate code path it could even select (the whole shard model is already single-threaded-per-core, §1 of Component 01), so the flag is a pure no-op kept only for client compatibility.
5. **PQ silently drops trailing dimensions when `dim % m != 0`.** `encode`/`compute_distance_table` both slice to `(start + d_sub).min(v.len())` per subspace (§4.3) — any dimensions beyond `m * d_sub` never influence PQ-based distance at all, with no error, warning, or documentation of the precision loss.
6. **`.vtier` spilled bytes are never reclaimed.** Deleting or re-adding (delete-then-reinsert, §5.2) a tiered vector leaves its old bytes orphaned in the append-only `tier_path` file forever — there is no compaction, free-list, or hole-punching (contrast with the real `ShardTierManager` in `src/tiering.rs`, Component 07).
7. **`VectorFieldIndex::memory_usage` (and `HnswIndex`'s inline sum backing it) only counts `n.vector.len() * 4`** for the in-RAM float payload — it does not add the bytes used by `quantized`/`binary`/`pq`, so an index that is entirely `Q8`/`Bin`/`PQ`/tiered (where `vector` is empty or was never populated) reports near-zero vector memory, understating actual RSS.
8. **`VSIM ... FILTER` string literals don't decode escape sequences.** `FilterParser::parse_primary`'s string-literal branch does `s.push(esc as char)` for any `\X` escape — i.e. `\n` inside a filter string literal becomes the literal character `n`, not a newline; there is no translation table for `\n`/`\t`/`\\`/`\"` etc., only "consume the backslash and keep the next byte verbatim."
9. **PQ's `compute_distance_with_vec` rebuilds the full `m×256` ADC table from scratch on every call** (§4.3) — `dist_to_node` calls it per-candidate during a beam search with no shared per-query table, unlike a typical ADC implementation that builds the table once per query and reuses it across every candidate compared.
10. **`node_vector_cow`/`spill_node_to_tier` open a new `std::fs::File` handle on every call** — no cached/pooled file descriptor for `tier_path`, so a tiered index under heavy rerank load pays a fresh `open()` syscall per candidate per query.

---

## 7. `V*` / `SEMANTIC.*` Command Surface (verified against `src/resp.rs` lines 9556–10153, 12224–12441)

| Command | Parsed Syntax | Notes |
| :--- | :--- | :--- |
| `VADD` | `<idx> [REDUCE d] FP32 <blob>\|VALUES n f1..fn <ele> [CAS] [TIERED] [NOQUANT\|Q8\|BIN] [EF n] [SETATTR json] [M n]` | `REDUCE d` installs a random projection (`set_projection`, §2.2) on first creation; default quant (no flag) is `Q8` (`quant.unwrap_or(Q8) == Q8` at `resp.rs:9706`); `M` must be `>= 2`. Also accepts a legacy `VADD idx key f1 f2 ... [QUANTIZE] [PQ] [TIERED]` positional form (not shown above). |
| `VSIM` | `<idx> ELE <ele>\|FP32 <blob>\|VALUES n f1..fn [WITHSCORES] [WITHATTRIBS] [COUNT n=10] [EPSILON e∈[0,1]] [EF n] [FILTER expr] [FILTER-EF n] [TRUTH] [NOTHREAD]` | `TRUTH` runs `search_exact` (brute force, §5.4 is bypassed entirely); `FILTER` is validated with `validate_vset_filter` at parse time (§8) and evaluated per-element against `HnswIndex.attributes` (§2.2) at query time; see §6 items 2–4 for scoring/rerank caveats. Also doubles as `VSIM idx key1 key2 [METRIC]` (→ `Command::Vdist`) when the 3rd arg isn't `ELE`/`FP32`/`VALUES`. |
| `VDEL` / `VREM` | `<idx> <ele>` | Removes the element and its `VSETATTR` attribute; if the index becomes empty, the whole `HnswIndex` entry is dropped from `ShardDb.vector_indexes`. |
| `VINFO` | `<idx>` | `(len, dim, metric.as_str(), max_layer)` — a 4-field summary, not a full attribute dump. |
| `VCARD` / `VDIM` | `<idx>` | Element count / dimensionality. |
| `VEMB` | `<idx> <ele> [RAW]` | Non-`RAW`: dequantized `f32` vector. `RAW`: `raw_embedding` returns `(quant_type: "q8"\|"bin"\|"fp32", raw_bytes, norm, q8_range)` — `fp32` mode stores the **normalized** vector (`v / norm`) as raw little-endian bytes, not the original magnitude. |
| `VLINKS` | `<idx> <ele> [WITHSCORES]` | Per-layer adjacency list of `ele`, each neighbor scored via the Cosine-shaped formula in §6 item 3. |
| `VRANDMEMBER` | `<idx> [count]` | Positive count: partial Fisher-Yates over a pre-sorted key list (distinct elements, capped at index size). Negative count: `count.abs()` **independent** draws with replacement (duplicates possible). |
| `VSETATTR` / `VGETATTR` | `<idx> <ele> <json>` / `<idx> <ele>` | `VSETATTR` validates the JSON syntactically (`serde_json::from_str`) but not against any schema; empty string clears the attribute. |
| `VISMEMBER` | `<idx> <ele>` | Membership check. |
| `SEMANTIC.SET` | `<ns> <id> <prompt> <response> VECTOR d f1..fd [EX s\|PX ms] [SCOPE s] [QUANTIZE\|SQ8] [TOKENS n]` | `VECTOR` also accepts a raw binary blob in place of `d f1..fd` (dispatches through `search::decode_vector`). `TOKENS` overrides the auto-estimate `response.len().div_ceil(4).max(1)` used for `tokens_saved` telemetry (§2.7). |
| `SEMANTIC.GET` | `<ns> VECTOR d f1..fd [THRESHOLD t=0.90∈[0,1]] [SCOPE s] [WITHSCORE] [WITHPROMPT] [WITHID]` | Always internally searches the top **4** HNSW candidates (`SemanticCache::get`, hard-coded `k=4`, §2.7) regardless of any count, filters by `scope`, and returns at most the single best hit `>= threshold`. |
| `SEMANTIC.DEL` | `<ns> <id> [id...]` | Multi-id delete. |
| `SEMANTIC.FLUSH` / `SEMANTIC.INFO` | `<ns>` | `FLUSH` clears `entries` and rebuilds a fresh `HnswIndex` at the same `dim`; `INFO` (`ShardDb::semantic_info`) returns `(len, dim, hits, misses, tokens_saved, evicted_expired)`. |

`FT.CREATE ... VECTOR` does **not** go through this table at all — it's parsed by `search.rs`/Component 09's own grammar and built via `build_vector_index` (§8) directly into a `VectorFieldIndex`, bypassing `ShardDb.vector_indexes` entirely.

---

## 8. Cross-Component Interactions

- **`src/shard.rs`**: owns `ShardDb.vector_indexes: HashMap<String, HnswIndex>` (Redis 8 Vector Sets + legacy standalone HNSW) and `ShardDb.semantic_caches: HashMap<Bytes, SemanticCache>`; `vadd`/`vadd_ext`/`vsim_ext`/`vdel`/`vinfo`/`vsetattr`/`vgetattr`/semantic-cache glue all live here (lines 3392–3760) and are the only callers that mutate `HnswIndex`/`SemanticCache` on behalf of client commands. `vadd_ext` is also where a fresh Redis 8 vset's `is_redis_vset`, `quant` (default `Q8`), `m`/`m0`/`ml`, `ef_construction`, and `projection` get set at index-creation time (§2.2), and where re-`VADD`ing into an existing index is rejected if the requested `quant`/`reduce` conflicts with what the index already has.
- **`src/resp.rs`**: parses every `V*`/`SEMANTIC.*` grammar in §7 into `Command` variants, including validating `VSIM ... FILTER` expression syntax eagerly via `crate::vector::validate_vset_filter` (§6 item 8 applies equally at parse time) before the command is ever queued.
- **`src/connection.rs`**: dispatches the parsed commands to the `ShardDb` methods above and encodes results as RESP2/RESP3 (`Command::Vsim`'s arm alone handles four independent WITHSCORES×WITHATTRIBS reply shapes, lines 18385–18471, including a RESP3 map (`%N\r\n`) vs. RESP2 flat-array distinction).
- **`src/search.rs`** (Component 09): `build_vector_index` (lines 642–678) constructs a `VectorFieldIndex::Flat(FlatIndex::new(..))` or `VectorFieldIndex::Hnsw(HnswIndex::with_params(..))` directly from `FT.CREATE`'s parsed `FieldType::Vector`/`VectorFieldAttrs`, stored in `InvertedIndex.vector_indices` (§2.6) — completely independent of `ShardDb.vector_indexes`, with independently-tunable `M`/`EF_CONSTRUCTION`/`EF_RUNTIME` defaults that differ from this file's own (§9). `knn_search`/`vector_range_search` in `search.rs` call `VectorFieldIndex::search_filtered`/`range_filtered`, which forward straight into this file's `HnswIndex`/`FlatIndex` methods (§5.4) — and also call `crate::vector::compute_distance` directly for their own brute-force fallback branches.
- **`src/agent.rs`** (Component 20): `AgentMemorySession.index: Option<HnswIndex>` is a **third, independent** consumer — one private `HnswIndex::new(session_id, dim, Cosine)` per agent session, lazily created on the first turn with a non-empty embedding, used for `AGENT.MEM.RECALL`'s KNN over conversation turns. It shares the type but not the instance with `SemanticCache`/`ShardDb.vector_indexes`.
- **`src/mcp.rs`** (Component 20): exposes `VADD`/`VSIM`/`SEMANTIC.*` as MCP tool calls but defines no vector types of its own — thin `Command` builders only.
- **`src/aof.rs`**: `BGREWRITEAOF`-style rewrite (lines 2371–2429) walks every live `HnswIndex` in `db.vector_indexes` and re-emits each element as a synthetic `Command::Vadd` (carrying its **current** metric/quant/M/tiered/setattr state, via `command_to_resp`), and every live, non-expired `SemanticCache` entry as a synthetic `Command::SemanticSet` (with `ttl` recomputed as the remaining duration from `now`) — so replaying the rewritten AOF reconstructs both structures element-by-element rather than via a binary snapshot format.

---

## 9. Concrete Numbers

| Quantity | Value | Source |
| :--- | :--- | :--- |
| `src/vector.rs` total / engine / test lines | 3,305 / ~2,938 (lines 1–2938) / ~365 (lines 2940–3305, 9 `#[test]` fns) | `wc -l` |
| `HnswIndex::new` defaults | `M=16, M0=32 (=2*M), ef_construction=64, ef_search=32, ml=1/ln(16)≈0.361` | lines 1046–1076 |
| `HnswIndex::with_params` | `m0` always forced to `m*2`; `m.max(2)`, `ef_construction.max(1)`, `ef_runtime.max(1)` | lines 1079–1095 |
| `FT.CREATE VECTOR HNSW` defaults (`VectorFieldAttrs`, `search.rs`) | `M=16, EF_CONSTRUCTION=200, EF_RUNTIME=10, INITIAL_CAP=1024` | `search.rs:87-98` — **different `EF_CONSTRUCTION`/`EF_RUNTIME` from this file's own 64/32 defaults** |
| `random_level` cap | `min(level, 16)` layers | line 1322 |
| SQ8 compression | 4 bytes/dim (`f32`) → 1 byte/dim + 16-byte fixed header = **75% reduction** | §4.1 |
| Binary compression | 1 bit/dim, packed into `u64` words = **96.9%** reduction at dim=128 (16 bytes vs. 512) | §4.2 |
| PQ compression (default `m=(dim/8).clamp(1,16)`) | `dim` floats → `m` bytes, e.g. dim=128→m=16 → **96.9% reduction** | §4.3 |
| PQ codebook size | `m` subspaces × 256 centroids × `d_sub` floats, always fully allocated whether trained or not | lines 654–698 |
| PQ auto-train trigger | first insert once `len() >= 15`, `max_iters=10` hard-coded | lines 1462–1489 |
| `VSIM` default `COUNT` / `EPSILON` range | `10` / `[0.0, 1.0]` | `resp.rs:9918,9958` |
| `SEMANTIC.GET` default `THRESHOLD` / internal search `k` | `0.90` / `4` (hard-coded, independent of caller) | `resp.rs:12335`, `vector.rs:2906` |
| `VADD` default quant when unspecified | `Q8` | `resp.rs:9705-9706` |
| `FlatIndex` search complexity | `O(N·dim)` scan + `O(N log k)` top-k heap maintenance | §2.5 |
| `HnswIndex` insert complexity | `O(ef_construction · M)` per touched layer, `O(log N)` expected layers | §5.2 |
| `HnswIndex` search complexity | `O(ef_search · M)` at layer 0 + `O(M)` greedy hops per upper layer | §5.4 |
| `HnswIndex` delete complexity | `O(M² )` local neighbor repair per layer touched; `O(N)` **only** if the deleted node was `entry_point` | §5.7 |
| PQ ADC per-candidate cost | `O(m)` table lookup, but `O(dim·256)` table **rebuilt every call** (§6 item 9) | §4.3 |
| AVX-512 lanes/iteration | 32 (`dot_product`/`l2`, two `__m512` accumulators) or 16 (`cosine`, one pass, 3 accumulators) | lines 68–227 |
| AVX2+FMA lanes/iteration | 16 (`dot_product`/`l2`, two `__m256` accumulators) or 8 (`cosine`, one pass) | lines 150–309 |
| Portable fallback unroll | 8 lanes via `as_chunks::<8>()` | lines 404–501 |

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: There is exactly **one** HNSW/FLAT implementation in the codebase (`HnswIndex`/`FlatIndex`, this file). Redis 8 Vector Sets (`ShardDb.vector_indexes`), RediSearch `VECTOR` fields (`InvertedIndex.vector_indices`), `SemanticCache.index`, and `AgentMemorySession.index` are four **independent instances/owners** of the same types, each with its own `M`/`ef_construction`/`ef_search`/metric — changing one does not affect the others, and their defaults already differ (§9).
* **Gotcha 2**: `dist_to_node`'s priority order is PQ > binary > SQ8 > full-precision (§5.5). If a node somehow has more than one compressed payload set, the cheapest/lossiest one that's present wins — check which `Option` fields are actually `Some` before assuming a given node is using the distance metric you expect.
* **Gotcha 3**: Reranking (`rerank: bool` on `search_ext`/`search_filtered`/`search_tiered`) is wired backwards for Redis 8 Vector Sets (`vsim_ext`, §6 item 2) — if you're debugging unexpectedly poor `VSIM` recall on a `Q8`/`BIN` vector set, this is the first thing to check, not the HNSW graph itself.
* **Gotcha 4**: `VSIM`/`VLINKS` similarity scores are only meaningful for `Cosine`-metric indexes (§6 item 3) — don't trust `WITHSCORES` output on an `L2`- or `IP`-metric vector set without checking the raw distance first.
* **Gotcha 5**: Deleting then re-inserting a `TIERED` element leaks bytes in the `.vtier` file forever (§6 item 6) — this file has no compaction; if you're chasing unexpected disk growth on a churny tiered vector set, check `idx.tiered_bytes` against the actual file size on disk, not `idx.len()`.
* **Gotcha 6**: `enable_pq`/`train_pq` do not retroactively encode pre-existing vectors (§4.3/§6 item 1) — to PQ-compress an already-populated index, elements must be re-added with `quantize_pq=true` (`add_quantized_ext`), not just `enable_pq`+`train_pq`.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run vector.rs's own unit tests (9 tests covering HNSW CRUD/search, SQ8 quantize+rerank,
#    PQ encode+ADC, SIMD parity across AVX-512/AVX2/portable, semantic cache TTL/scope/telemetry,
#    Redis 8 FILTER+quantization, HNSW diversity heuristic+delete slot reuse,
#    PQ k-means training, and NVMe tiered rerank-from-disk)
cargo test --lib vector:: -- --test-threads=1

# 4. Run the full unit suite
cargo test --lib -- --test-threads=1
```
