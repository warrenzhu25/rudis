# Component 09: RediSearch Full-Text Engine & Hybrid Vector Fusion (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/search.rs` (4,117 lines — engine logic is lines 1–3035; lines 3037–4117 are `#[cfg(test)] mod tests` with 14 test functions)
> **High-Level Design Spec**: [`docs/design/09_redisearch.md`](../design/09_redisearch.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

This document was rewritten from scratch against the current source. The file grew from ~2,488 to 4,117 lines since the previous pass; the biggest change is that `InvertedIndex` now owns **real** `FlatIndex`/`HnswIndex` vector indices per vector field (`vector_indices: HashMap<String, VectorFieldIndex>`) instead of doing a brute-force linear scan over `DocMeta.vector_fields` — KNN queries now genuinely traverse HNSW graphs (with an ad-hoc brute-force fallback), and a full RRF/Linear hybrid-fusion layer, `VECTOR_RANGE` queries, multi-vector (JSON chunk) fields, `FT.ALTER`, `FT._LIST`, and `FT.PROFILE` were added. See §0 for a commit-by-commit summary.

---

## 0. What Landed Since The Last Pass (`git log --oneline -- src/search.rs`)

```
c22d300 feat(search): add FT.HYBRID (RRF/Linear), FT.ALTER, FT._LIST, FT.PROFILE, and multi-vector JSON chunks
8f52a3a feat(search): FT.CREATE backfill, FT.INFO vector stats, and DIALECT/TIMEOUT options
647efc9 feat(search): support @field:[VECTOR_RANGE radius $blob] queries and $YIELD_DISTANCE_AS
cd3e6e5 feat(search): wire Reciprocal Rank Fusion (RRF) and WITHSCORES into FT.SEARCH
5099d5b feat(search): pre-filtered hybrid KNN (ad-hoc brute force + filtered HNSW traversal)
8ff96f4 feat(search): full KNN clause with $K, EF_RUNTIME, AS alias and distance-ordered results
f0ce2a9 feat(search): full FT.CREATE VECTOR attributes, FLAT index, TYPE-aware decoding and binary-safe HASH ingest
784ccb4 feat(search): multi-match json_nummultby and HNSW vector index integration in RediSearch
b8320b8 feat(search): FT.AGGREGATE execution pipeline with GROUPBY, REDUCE, APPLY, SORTBY, and LIMIT
0b2c1ed feat(search): balanced RangeTree for O(log N + K) numeric indexing and search
5a8b4b4 feat(search): shard-local InvertedIndex and parallel scatter-gather FT.SEARCH with top-K heap merging
a29ab71 refactor(search): dense 32-bit DocId indexing and term-directed O(1) document deletion
26ca813 feat(search): implement secondary indexing over RedisJSON with JSONPath extraction and field aliases
b000092 feat: implement RediSearch full-text search and AF_XDP eBPF kernel bypass engine
```

**Not found**: despite `src/mcp.rs` introducing an "AI-Native Agent Runtime, Semantic Cache & MCP Server" subsystem (Component 20 — `SemanticCache` lives in `src/vector.rs`, `AgentMemoryBank`/MCP server in `src/agent.rs`/`src/mcp.rs`), `search.rs` itself has **no** semantic-cache or vector-set (`VADD`/`VSIM`/`VCARD`) code. The only cross-reference is `src/mcp.rs` importing `crate::search::SearchOptions` to expose one MCP tool, `rudis_ft_search` (§5.9) — a thin wrapper that builds a `Command::FtSearch` with only `index`/`query`/`limit` (no filters, sort, or scoring knobs exposed to the LLM agent).

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/search.rs` | Index data structures, tokenization, query parsing/evaluation, BM25 scoring, KNN/VECTOR_RANGE, RRF/Linear fusion, aggregation pipeline | `InvertedIndex`, `IndexSchema`, `QueryAst`, `AggregateOptions`, `bm25_score`, `parse_query`, `execute_search`, `reciprocal_rank_fusion`, `linear_score_fusion` |
| `src/shard.rs` | Per-shard index partition (`ShardDb.search_indices`) and its local indexing/query hooks | `ShardDb::init_search_index`, `ShardDb::reindex_hash_local`, `ShardDb::delete_document_local`, `ShardDb::index_json_document_local` |
| `src/router.rs` | Cross-shard scatter/gather for `FT.SEARCH`/`FT.AGGREGATE`, hybrid RRF/Linear orchestration, index create/alter/drop broadcast | `Router::ft_search`, `Router::ft_aggregate`, `Router::create_search_index`, `Router::alter_search_index` |
| `src/resp.rs` | `FT.*` argument parsing into the structs below | `"FT.CREATE"`, `"FT.SEARCH"`, `"FT.AGGREGATE"`, `"FT.ALTER"`, `"FT.PROFILE"`, `"FT.HYBRID"`, `"FT._LIST"` arms (lines 11199–12223) |
| `src/connection.rs` | `FT.*` command dispatch, `HSET`/`JSON.SET`/delete indexing hooks | `Command::FtCreate/FtSearch/FtAggregate/FtInfo/FtDropIndex/FtExplain/FtAdd/FtList/FtAlter/FtProfile` arms (lines 11395–~11900), `reindex_hash_for_search` |
| `src/server.rs` | Remote-shard handler for cross-shard `SearchQuery`/`InitSearchIndex`/`DropSearchIndex` messages | `ShardMessage::SearchQuery`/`InitSearchIndex`/`DropSearchIndex` arms (~line 1636) |
| `src/vector.rs` | `FlatIndex`, `HnswIndex`, `VectorFieldIndex` enum, and the SIMD `compute_distance`/`cosine_distance`/`l2_distance_sq`/`dot_product` kernels this file calls directly (Component 08) | `VectorFieldIndex::{search_filtered, range_filtered}`, `compute_distance` |
| `src/mcp.rs` | Exposes `FT.SEARCH` as the `rudis_ft_search` MCP tool (Component 20) | `"rudis_ft_search"` tool-call arm (line 429) |

---

## 2. Component Architecture & Data Structures

```
     Raw Document: HSET "doc:1" title "Rust Systems" body "Distributed io_uring" emb <32 raw bytes>
                                    │
                    reindex_hash_for_search() (src/connection.rs:12724)
                                    │
                  ┌─────────────────┴──────────────────┐
                  ▼                                     ▼
     db.reindex_hash_local(key)              crate::search::index_document_hook(key, raw)
     → ShardDb.search_indices[*]             → global SEARCH_INDICES[*] mirror
     (THIS shard's partition only,            (every index, process-wide, RwLock-protected)
      no lock — shard-thread-confined)
                  │                                     │
                  └──────────────┬──────────────────────┘
                                 ▼           add_hash_document() → decode_vector() per VECTOR field,
                                             tokenize_text()+simple_stem() per TEXT field, …
                  ┌───────────────────┬───────────────────┬────────────────────┐
                  ▼                   ▼                   ▼                    ▼
            Text Fields          Tag Fields          Numeric Fields      Vector Fields
       ["rust","system"]      {"tech","db"}              99.5          Vec<f32> (+ chunks)
        (stemmed)                                                             │
                  │                   │                   │                    ▼
                  ▼                   ▼                   ▼         vector_indices[alias]:
     inverted: HashMap<String,   tag_fields on       numeric_trees:   VectorFieldIndex::Flat|Hnsw
       Vec<Posting>>             each DocMeta       HashMap<String,   (actual FlatIndex / HnswIndex
     "rust"->[Posting{…}]                            RangeTree>        from src/vector.rs — see §2.4)
```

### 2.1 Real Data Structures (`src/search.rs`)

```rust
pub type DocId = u32;                                                          // line 5

pub enum FieldType {                                                           // lines 8-27
    Text { weight: f64, sortable: bool, nostem: bool },
    Numeric { sortable: bool },
    Tag { separator: char, casesensitive: bool },
    Vector { dim: usize, distance_metric: String, algorithm: String, attrs: VectorFieldAttrs },
}

pub enum VectorDataType {                                                      // lines 31-39
    Float32 /* default */, Float64, Float16, BFloat16, Int8, Uint8,
}
impl VectorDataType {
    pub fn elem_size(&self) -> usize { /* 8, 4, 2, 2, 1, 1 bytes respectively */ }
}

pub struct VectorFieldAttrs {                                                  // lines 77-85
    pub data_type: VectorDataType,   // default Float32
    pub m: usize,                    // default 16       (HNSW)
    pub ef_construction: usize,      // default 200      (HNSW)
    pub ef_runtime: usize,           // default 10       (HNSW)
    pub initial_cap: usize,          // default 1024     (FLAT + HNSW)
    pub epsilon: f64,                // default 0.01     (parsed, UNUSED — see §6)
    pub block_size: usize,           // default 1024     (parsed, UNUSED — see §6)
}

pub struct SchemaField {                                                       // lines 248-253
    pub identifier: String, // e.g. "$.title" (JSON path) or "title" (hash field)
    pub alias: String,      // the name queries/results use (via FT.CREATE ... AS <alias>)
    pub field_type: FieldType,
}

pub struct IndexSchema {                                                       // lines 255-262
    pub name: String,
    pub on_type: String,                      // "HASH" or "JSON"
    pub prefixes: Vec<String>,
    pub fields: HashMap<String, FieldType>,   // identifier AND alias both map here (O(1) lookup)
    pub schema_fields: Vec<SchemaField>,      // ordered, for FT.INFO/introspection
}

pub struct Posting { pub doc_id: DocId, pub term_freq: u32 }                    // lines 264-268, 8 bytes

pub struct DocMeta {                                                           // lines 270-280
    pub key: Bytes,
    pub doc_len: usize,                                    // whole-document token count, not per-field
    pub fields: HashMap<String, String>,
    pub numeric_fields: HashMap<String, f64>,
    pub tag_fields: HashMap<String, HashSet<String>>,
    pub vector_fields: HashMap<String, Vec<f32>>,          // first/only vector per field
    pub multi_vector_fields: HashMap<String, Vec<Vec<f32>>>, // NEW: all chunks, chunk 0 == vector_fields entry
    pub terms: Vec<String>,                                // drives O(terms-in-doc) deletion
}

pub struct OrderedF64(pub f64);  // total_cmp-based Ord/PartialOrd so f64 can key a BTreeMap  (lines 282-299)

pub struct RangeTree {                                                         // lines 303-306
    pub entries: std::collections::BTreeMap<OrderedF64, Vec<DocId>>,
    pub total_entries: usize,
}

pub struct InvertedIndex {                                                     // lines 373-392
    pub schema: Option<IndexSchema>,
    pub inverted: HashMap<String, Vec<Posting>>,                   // term -> postings
    pub numeric_trees: HashMap<String, RangeTree>,                 // numeric field -> balanced tree
    pub key_to_id: HashMap<Bytes, DocId>,                          // document key -> dense id
    pub id_to_meta: HashMap<DocId, DocMeta>,                       // dense id -> metadata
    pub vector_indices: HashMap<String, crate::vector::VectorFieldIndex>,  // NEW: real FLAT/HNSW indices
    pub next_doc_id: DocId,
    pub free_ids: Vec<DocId>,                                      // recycled ids from deleted documents
    pub total_docs: usize,
    pub total_terms: usize,
    pub indexing_failures: usize,                                  // NEW: hash_indexing_failures (FT.INFO)
}
```

**§2.1 re-verification of the prior pass's two central claims:**
1. **Dense `u32` `DocId` with a free-list** — still true, unchanged (`next_doc_id`/`free_ids`, lines 386–387, recycled in `add_document` line 898 and pushed back in `remove_document` line 969).
2. **Per-shard partitioning, not one global lock** — still true, but the mechanism is now fully visible: `ShardDb.search_indices: HashMap<String, InvertedIndex>` (`src/shard.rs:621`, plain owned `InvertedIndex`, **no** `Arc`/`RwLock`, because `ShardDb` is thread-confined to its shard's core) is what real `FT.SEARCH`/`FT.AGGREGATE` query. A **separate, fully independent** process-wide mirror, `static SEARCH_INDICES: RwLock<HashMap<String, Arc<RwLock<InvertedIndex>>>>` (line 1028), duplicates every document via a **double write** on every mutation — see §2.3. This is the same two-copy design as before; what changed is that `vector_indices` is now part of what gets duplicated in both copies.

### 2.2 `VectorFieldAttrs` / `VectorDataType` — binary-safe, typed vector decoding (new)

`decode_vector(bytes, data_type, dim)` (line 231) is the single entry point used by both HASH and JSON ingestion:

```rust
pub fn decode_vector(bytes: &[u8], data_type: VectorDataType, dim: usize) -> Option<Vec<f32>> {
    let text = parse_vector_text(bytes);              // only succeeds if EVERY byte is ASCII
                                                        // digit/'.'/','/'-'/'+'/'e'/'E'/'['/']'/whitespace
    let v = match text {
        Some(t) if dim == 0 || t.len() == dim => t,    // prefer the text parse when it's plausible
        _ => {
            if dim != 0 && bytes.len() != dim * data_type.elem_size() { return None; }
            decode_typed_blob(bytes, data_type)?       // binary little-endian per TYPE
        }
    };
    if (dim != 0 && v.len() != dim) || v.iter().any(|x| !x.is_finite()) { return None; }
    Some(v)
}
```

`decode_typed_blob` (line 168) handles all six `VectorDataType` variants: `FLOAT32`/`FLOAT64` via `f32/f64::from_le_bytes`, `FLOAT16`/`BFLOAT16` via hand-written `f16_to_f32`/bit-shift conversions (lines 102–165), `INT8`/`UINT8` via a direct byte-to-float cast. Because `parse_vector_text` rejects any byte string containing a non-ASCII-numeric byte, a genuine binary blob (effectively random bytes) almost never round-trips as text — this **resolves** the prior pass's "4-byte ambiguity" finding about the older `parse_vector_blob`-only decoder (still present at line 633, but now only used as a `FLOAT32`/`dim=0` fallback inside `extract_json_fields` for JSON string-encoded vectors, not the primary decode path).

### 2.3 Two Storage Copies, One Query Path — Precisely (confirmed unchanged in shape)

`Router::ft_search` (`src/router.rs:2675`) is the real entry point:

```rust
pub async fn ft_search(&self, index: &str, ast: &QueryAst, opts: &SearchOptions) -> (usize, Vec<SearchHit>) {
    // ... hybrid RRF/Linear short-circuit, see §4.8 ...
    let (local_total, local_hits) = {
        let db = self.local_db.borrow();
        if let Some(idx) = db.search_indices.get(index) {
            crate::search::execute_search(idx, ast, &scatter_opts)             // per-shard partition
        } else if let Some(idx_arc) = crate::search::get_search_index(index) {
            crate::search::execute_search(&idx_arc.read().unwrap(), ast, &scatter_opts)  // fallback: mirror
        } else { (0, Vec::new()) }
    };
    if self.num_shards > 1 {
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                sender.send(ShardMessage::SearchQuery { index, ast: Box::new(ast.clone()),
                                                          options: Box::new(scatter_opts.clone()), responder: tx });
                pending.push(rx);
            }
        }
        for rx in pending { /* recv_async, accumulate all_hits/all_totals */ }
    }
    // merge-sort by sortby or score desc, re-paginate by offset/limit
}
```

Every remote shard answers `ShardMessage::SearchQuery` by querying **only its own local partition** — `db.search_indices.get(&index)` in `src/server.rs`'s shard-message loop (~line 1651) — and never consults the mirror. The local-shard mirror fallback (`get_search_index`) fires only if this shard's own `search_indices` map is missing the index entirely, which does not happen in normal operation because `create_search_index` synchronously broadcasts `ShardMessage::InitSearchIndex` to **every** shard before returning (§5). With a single shard (`num_shards == 1`), the scatter loop never runs at all — the whole cross-shard path is skipped.

`FT.INFO` (`Command::FtInfo`, `src/connection.rs:11482`) always reads the **mirror** directly via `get_search_index`, never a shard's local partition — it reports process-wide `num_docs`/`num_terms`/`hash_indexing_failures`/`vector_index_sz_mb` without any scatter/gather, because the mirror already has every document (in principle — see the `FT.ADD` divergence in §6).

### 2.4 Vector indices are now real `FlatIndex`/`HnswIndex`, not a brute-force scan (prior-pass finding now reversed)

`build_vector_index` (line 642) constructs the concrete backing index from a schema's `VECTOR` field at index-creation time (`InvertedIndex::new`, lines 693–698) **and** lazily the first time a document with that vector field is added if the index wasn't pre-built (lines 914–923, using the first document's vector length as a dimension hint when `DIM` was `0`):

```rust
fn build_vector_index(index_name: &str, ftype: &FieldType, dim_hint: usize) -> Option<VectorFieldIndex> {
    let FieldType::Vector { dim, distance_metric, algorithm, attrs } = ftype else { return None };
    let dim = if *dim == 0 { dim_hint } else { *dim };
    let metric = metric_from_str(distance_metric);
    Some(if algorithm.eq_ignore_ascii_case("FLAT") {
        VectorFieldIndex::Flat(FlatIndex::new(index_name.to_string(), dim, metric, attrs.initial_cap))
    } else {
        VectorFieldIndex::Hnsw(HnswIndex::with_params(index_name.to_string(), dim, metric,
                                                       attrs.m, attrs.ef_construction, attrs.ef_runtime))
    })
}
```

`InvertedIndex.vector_indices: HashMap<String, VectorFieldIndex>` is keyed by the field's **alias**. `VectorFieldIndex` (`src/vector.rs:2655`) is a thin enum dispatch over `FlatIndex`/`HnswIndex`, exposing `.search_filtered(query, k, ef_runtime, filter)`, `.range_filtered(query, radius, epsilon, filter)`, `.add`, `.remove`, `.metric()`, `.len()`, `.dim()`, `.memory_usage()`. Real ANN traversal (`HnswIndex::search_filtered`) is used whenever there is no candidate-set filter, or the filtered candidate set is large enough (§4.5's `KNN_ADHOC_BF_RATIO` heuristic); `FlatIndex` is always brute-force-exact regardless.

---

## 3. Indexing Algorithms

### 3.1 Tokenization — `tokenize_text` / `simple_stem` (lines 593–630, unchanged)

```rust
pub fn tokenize_text(text: &str, stem: bool) -> Vec<String> {
    for raw in text.split(|c: char| !c.is_alphanumeric()) {
        let lower = raw.to_ascii_lowercase();
        if ENGLISH_STOP_WORDS.contains(lower.as_str()) { continue; }   // ~120-word fixed English stop list
        tokens.push(if stem { simple_stem(&lower) } else { lower });
    }
}
pub fn simple_stem(word: &str) -> String {
    // -ing (len>5) strip 3; -ies (len>4) -> "y"; -es (len>4) strip 2;
    // -ed (len>3) strip 2; trailing -s (len>3, not -ss) strip 1; else unchanged
}
```

This same pipeline runs at index time (`add_document`) and query time (`parse_query`'s bare-word branch, and `parse_knn_clause`/etc. do not tokenize), so stored and queried terms are normalized identically. Not a real Porter/Snowball stemmer — five hand-written suffix rules.

### 3.2 Indexing a HASH document — `add_hash_document` → `add_document` (lines 732–957)

`add_hash_document(key, raw: &[(Bytes, Bytes)])` is the **binary-safe** entry point used for `HSET`/`HMSET` (added in `f0ce2a9`): for every `(field, value)` pair, if the field matches a schema `VECTOR` field's identifier/alias it is decoded via `decode_vector` with that field's declared `TYPE`/`DIM` — a decode failure sets `failed = true`, and the **whole document is rejected** (all other fields discarded too) and `indexing_failures` incremented (lines 748–763); otherwise the raw bytes are lossily converted to `String` (`String::from_utf8_lossy`) for text/numeric/tag processing. `add_document` then does the real work:

1. For each `VECTOR` schema field, resolve the vector from the caller-supplied `vectors` map (preferred) or by decoding the field's string value (lines 808–822); a dimension mismatch on an **extra chunk** (`field#1`, `field#2`, …) rejects the whole document (lines 832–835, `indexing_failures += 1`).
2. For `TEXT` fields: `tokenize_text` → accumulate `term_positions: HashMap<String, u32>` (term frequency) and `doc_len`.
3. For `NUMERIC`: `f64::parse` into `numeric_fields`.
4. For `TAG`: split on `separator`, trim, lower-case unless `casesensitive`, into a `HashSet<String>`.
5. Fields **not declared in the schema** (except the literal JSON-root marker `"$"`) are still indexed as stemmed text — an untyped fallback (lines 888–895).
6. Allocate/reuse a `DocId` from `free_ids` or `next_doc_id` (line 898).
7. Push a `Posting { doc_id, term_freq }` per term into `self.inverted` (lines 904–912).
8. For each vector field: lazily build the `VectorFieldIndex` if missing (using this doc's vector length as the dim hint), call `vi.add(key_bytes.clone(), vec.clone())` for the primary vector, then for every extra chunk `vi.add(Bytes::from(format!("{}\x00{}", doc_id_str, chunk_idx)), chunk_vec)` — **chunk keys use a `\x00` (NUL) separator between the document key and the chunk index** (lines 914–933).
9. Insert each numeric value into `self.numeric_trees[field].add(doc_id, value)` (lines 935–940).
10. Store `DocMeta` and bump `total_docs`/`total_terms`.

Re-indexing an existing key (`key_to_id.contains_key`) first calls `remove_document` (full remove-then-readd, line 794–796).

**Multi-vector chunks are JSON-only in practice.** `add_document`'s chunk-detection loop (lines 826–838) looks for extra vectors under the keys `"{alias}#{chunk_idx}"`/`"{identifier}#{chunk_idx}"` inside the `vectors` map passed in. `extract_json_fields` (§3.3) populates exactly these keys when a JSONPath (e.g. `$.chunks[*].emb`) matches multiple array elements. **`add_hash_document`, however, only ever inserts a single `sf.alias -> vec` entry into its `vectors` map** (line 750) — it never scans raw HASH fields for a `field#1`/`field#2` naming convention. So a `HSET`-ingested document can never produce more than one chunk per vector field, even though the storage/query-side dedup machinery (§4.6) is fully format-agnostic.

### 3.3 Indexing a JSON document — `extract_json_fields` (lines 1100–1238)

Walks `schema.schema_fields`, resolves each field's JSONPath via `crate::json::parse_json_path`/`query_json_path`, and for `Vector` fields collects **every** matched value as a candidate vector (`serde_json::Value::Array` → `f32` components, or `String` → `parse_vector_blob`). The **first** matched vector becomes the field's primary value (`extracted_vectors[alias]`); every **subsequent** match becomes a numbered chunk (`extracted_vectors["{alias}#{chunk_idx}"]`, lines 1212–1215) — this is what backs true multi-vector JSON chunk fields. The full JSON document is also always stored verbatim under the `"$"` key (line 1109).

### 3.4 Deleting a document — `remove_document` (lines 959–1004, unchanged in shape)

`O(terms in that document)`, not a full vocabulary scan, because `DocMeta.terms` records exactly which terms the document contributed. Also now cleans up `vector_indices`: for each vector field, `hnsw.remove(&meta.key)` for the primary vector plus `hnsw.remove(&format!("{}\x00{}", key, chunk_idx))` for every recorded chunk (lines 992–1001).

### 3.5 BM25 Scoring — `bm25_score` (lines 1006–1024, formula unchanged, re-verified)

```rust
pub fn bm25_score(&self, term: &str, posting: &Posting, doc_len: usize) -> f64 {
    let n = self.inverted.get(term).map(|v| v.len()).unwrap_or(0);
    if n == 0 || self.total_docs == 0 { return 0.0; }
    let idf = ((self.total_docs as f64 - n as f64 + 0.5) / (n as f64 + 0.5) + 1.0).ln();
    if idf <= 0.0 { return 0.0001; }                          // floor, not in the classic formula
    let (k1, b) = (1.2, 0.75);
    let avgdl = self.avg_doc_len().max(1.0);
    let freq = posting.term_freq as f64;
    let tf = (freq * (k1 + 1.0)) / (freq + k1 * (1.0 - b + b * (doc_len as f64 / avgdl)));
    idf * tf
}
```

$k_1=1.2$, $b=0.75$, textbook Okapi BM25 IDF smoothing. `doc_len` is the whole document's combined token count across every indexed text field (not per-field). **`FieldType::Text.weight` is accepted and stored by `FT.CREATE`/`FT.ALTER` but is never read anywhere in `bm25_score`, `add_document`, or any caller** — re-verified still true; the per-field weight is pure metadata, only echoed back by `FT.INFO`.

---

## 4. Query Pipeline

### 4.1 `QueryAst` (lines 1270–1314)

```rust
pub enum QueryAst {
    Term(String), Prefix(String), Exact(String),
    FieldScope { field: String, inner: Box<QueryAst> },
    NumericRange { field: String, min: f64, max: f64 },
    TagFilter { field: String, tags: Vec<String> },
    And(Vec<QueryAst>), Or(Vec<QueryAst>), Not(Box<QueryAst>),
    KnnVector { field: String, k: usize, query_vec: Vec<f32>, param_name: String,
                k_param: Option<String>, ef_runtime: Option<String>, yield_as: Option<String> },
    VectorRange { field: String, radius: f32, radius_param: Option<String>, param_name: String,
                  epsilon: Option<String>, yield_as: Option<String> },
    MatchAll,
}
```

`KnnVector`/`VectorRange` now carry `$param`-style deferred references (`k_param`, `radius_param`, `ef_runtime`, `epsilon` are all `Option<String>` holding either a literal or a `$name`) resolved at **execution** time against `SearchOptions.params` via `resolve_usize_param`/`resolve_f32_param`/`resolve_str_param` (lines 1386–1405) — these try both the bare name and a `$`-prefixed name as map keys.

`QueryAst::Exact` is still **defined and handled in `evaluate_ast`, but `parse_query` never constructs it** — a quoted phrase like `"rust systems"` is parsed as an `And` of individually stemmed terms (line 1677–1692), not a distinguished phrase match. Confirmed still dead code from the parser's side.

### 4.2 Parser — `parse_query` (lines 1548–1704)

Hand-written, single-pass over whitespace-split tokens (with ad-hoc bracket/brace re-joining for multi-token clauses). Recognizes, in dispatch order:

- `base=>[KNN <k|$K> @field [$param] [EF_RUNTIME e] [AS alias]]` — hybrid vector suffix (`parse_knn_clause`, lines 1433–1488, also accepts the legacy `=>{$YIELD_DISTANCE_AS: alias; $EF_RUNTIME: n}` attribute-block syntax).
- `@field:[VECTOR_RANGE <radius|$param> $blob [EPSILON e] [AS alias]]` — range vector query (`parse_vector_range_clause`, lines 1492–1546).
- `@field:[min max]` — numeric range (falls back to `NEG_INFINITY`/`INFINITY` on unparsable bound).
- `@field:{tag1|tag2|...}` — tag filter; **tags are always lowercased at parse time** (line 1647, `.to_ascii_lowercase()`) regardless of the field's `CASESENSITIVE` schema attribute — see §6.
- `@field:...` — recursive `FieldScope` sub-query.
- `word1 | word2` — top-level `Or` (only recognized as a bare `|` token, splits the remaining query into left/right).
- `word*` — `Prefix`.
- `-term` — `Not` (recursive).
- bare word(s) — `tokenize_text(..., stem=true)`; multiple sub-tokens (e.g. from punctuation splitting inside one arg) become an `And` of `Term`s.

`strip_outer_parens` (line 1407) strips one layer of balanced parentheses before parsing, so `(foo bar)=>[KNN ...]` and `*=>[KNN ...]` both work as the base of a hybrid query.

### 4.3 `evaluate_ast` — recursive AST walk to `HashMap<DocId, f64>` (lines 1747–1930)

- `MatchAll` → every `id_to_meta` key, score `1.0`.
- `Term`/`Prefix`/`Exact` → postings lookup (`Prefix` scans **every** term in `self.inverted`, i.e. `O(vocabulary size)`), scored via `bm25_score`, summed on duplicate prefix matches.
- `FieldScope` → evaluates `inner`, then filters to docs that actually have that field present.
- `NumericRange` → `get_numeric_tree` (§4.4) range scan if a tree exists for the field (trying both the bare name and a `$.`-stripped/prefixed variant), else a full `O(N)` linear scan fallback.
- `TagFilter` → `O(N)` linear scan of all docs' `tag_fields` (no inverted tag index — every tag query is a full scan).
- `And` → **hybrid pre-filtering**: if one sub-AST is a `KnnVector`/`VectorRange`, the *other* sub-ASTs are evaluated first as a `filter: HashMap<DocId, f64>`, and the vector clause is evaluated **restricted to that filter** (§4.5) — this is RediSearch's "pre-filtered hybrid KNN". Otherwise, plain set intersection with additive score accumulation, short-circuiting once the running set is empty.
- `Or` → union with additive score accumulation.
- `Not` → full-index-minus-excluded-set.
- `KnnVector`/`VectorRange` (top-level, no filter) → `knn_search`/`vector_range_search` with `filter: None`.

### 4.4 `RangeTree` — balanced numeric range index (lines 303–371, algorithm re-verified unchanged)

`BTreeMap<OrderedF64, Vec<DocId>>` (values grouped by exact numeric value, since ties are common). `add`/`remove` do a `binary_search` within the per-value `Vec<DocId>` bucket (`O(log bucket_size)`) plus a `BTreeMap` insert/lookup (`O(log distinct_values)`). `range(min, max)` does `self.entries.range(OrderedF64(min)..=OrderedF64(max))`, a genuine **O(log N + K)** B-tree range scan (`K` = number of matching doc-value pairs) — this is the "Dragonfly-inspired" RangeTree the doc comment (line 301) references, and it's a real balanced-tree range query, not a linear scan. `OrderedF64` wraps `f64` with `total_cmp`-based `Ord` so NaN/±0.0 order consistently as a BTreeMap key.

### 4.5 KNN search — `knn_search` (lines 2070–2174)

```rust
const KNN_ADHOC_BF_RATIO: f64 = 0.1;   // line 1934

// (vi_opt, filter) dispatch:
(None, None)        => brute_force(all doc ids)                           // FLAT-less field: scan DocMeta
(None, Some(f))      => brute_force(f.keys())
(Some(vi), None)     => dedup_min_distance(vi.search(query, k_fetch, ef))          // full ANN/FLAT search
(Some(vi), Some(f))  => {
    let small = f.len() as f64 <= (vi.len() as f64 * 0.1).max(k as f64);
    if small || matches!(vi, VectorFieldIndex::Flat(_)) {
        brute_force(f.keys())                                            // ADHOC_BF: filtered set too small
    } else {
        let found = dedup_min_distance(vi.search_filtered(query, k_fetch, ef, Some(&pred)));
        if found.len() < k.min(f.len()) { brute_force(f.keys()) }        // traversal starved: fall back exact
        else { found }
    }
}
```

This is RediSearch's real "ADHOC_BF" heuristic: when the pre-filter set is smaller than `10%` of the index (or at least `k`), or the index is a `FLAT` (always-exact) index, brute-force over just the filtered candidates beats a filtered graph traversal; otherwise it does a real filtered `HnswIndex::search_filtered` traversal, with a starvation fallback to brute force if the traversal returns fewer than `min(k, filter_size)` results (a poorly-connected filtered region of the graph). `k_fetch` is `k * 4` when any document has more than one indexed chunk for the field (`has_multi_chunks`, lines 2109–2118), over-fetching so that deduplicating multiple chunks down to one hit per parent document (`dedup_min_distance`, lines 1978–1993 — keeps the **minimum** distance per `DocId`) still yields `k` distinct documents.

Vector index key → `DocId` resolution (`resolve_vec_key_doc_id`, lines 1943–1953) first tries an exact `key_to_id` lookup, then strips at the first `\x00` byte to recover the parent document's key for chunk keys.

### 4.6 `VECTOR_RANGE` search — `vector_range_search` (lines 1995–2068)

Structurally identical dispatch to KNN (`vi.range_filtered(query, radius, epsilon, filter)` vs. brute force with a `d <= radius` predicate), but no `k` truncation — returns every match within `radius`. `distance_to_similarity` (lines 1936–1941) converts a raw distance to a `(0,1]`-ish similarity score for ranking: `Cosine → (1.0 - dist).max(0.0)`, all other metrics (`L2`, `IP`) → `1.0 / (1.0 + dist.max(0.0))`.

### 4.7 `execute_search` — the non-hybrid, single-AST path (lines 2260–2418)

1. Short-circuits into the RRF/Linear hybrid path (§4.8) if `opts.rrf_k`/`opts.linear_weights` is set **and** `ast.as_hybrid_rrf()` recognizes the AST shape (an `And` containing exactly one `KnnVector`/`VectorRange` plus at least one other non-`MatchAll` clause). **If the option is set but the AST doesn't match that shape (e.g. a pure-KNN query with `RRF` tacked on, or no vector clause at all), the RRF/Linear option is silently ignored** and the normal scoring path below runs instead.
2. `evaluate_ast` → candidate scores.
3. `knn_distances` (lines 2208–2252) computes the exact vector distance for every candidate (not just the KNN winners) when the AST has a KNN/VECTOR_RANGE clause, for sorting/`YIELD_DISTANCE_AS` purposes.
4. Sort: by KNN distance ascending if the query has a vector clause and no conflicting explicit `SORTBY` (`sort_by_knn`), else by `SORTBY` field (numeric, defaults to `NEG_INFINITY` if missing/unparsable), else BM25 descending.
5. Paginate (`offset`/`limit`), then build `SearchHit`s: when sorting by KNN distance the reported `score` is **`-distance`** (line 2373, so descending-score merge across shards still produces nearest-first order) and, if the caller also passed an explicit `SORTBY`, `sort_val` additionally carries the raw distance. The KNN distance field is injected into `fields` under its `AS`/`$YIELD_DISTANCE_AS` alias or the default `"__{field}_score"` (`format_distance`, lines 2254–2258, uses `format!("{}", d)` — shortest round-trip `Display`, **different formatting** from the `WITHSCORES` path which always uses `format!("{:.6}", score)`, §6).

### 4.8 Reciprocal Rank Fusion & Linear Fusion (lines 2420–2546)

```rust
pub fn reciprocal_rank_fusion(bm25_hits: &[SearchHit], vector_hits: &[SearchHit], k: f64) -> Vec<SearchHit> {
    // for each list, for each hit at rank r (0-based): score += 1.0 / (k + r as f64 + 1.0)
    // summed per doc_id across both lists, sorted descending
}
```

Textbook RRF, `k` caller-supplied (`FT.SEARCH RRF <k>` / `SCORER RRF K <k>`, default `60.0`). `linear_score_fusion(bm25_hits, vector_hits, alpha, beta)`: min-max normalizes each list's scores to `[0,1]` independently (`(score - min) / (max - min)`, or `1.0` if the list has zero span), converting vector "scores" (which arrive as `-distance <= 0.0` from `execute_search`, line 2373) back to a `(0,1]` similarity via `1.0/(1.0 + (-score))` first, then computes `alpha * norm_bm25 + beta * norm_vec` per document (missing from one list contributes `0` from that list only). Both fusion functions merge each side's `fields` map, preferring whichever side's field value was seen first (`or_insert_with`), with `linear_score_fusion` additionally back-filling any field present in the vector-side hit but missing from the (first-seen) merged row (lines 2519–2526).

**Orchestration is two layers deep**: `execute_search` runs the hybrid fusion **within a single shard's index** (used by the per-shard/remote-shard scatter targets), while `Router::ft_search` (`src/router.rs:2681`) runs an **independent, coarser** hybrid path: it detects the same `as_hybrid_rrf()` shape, then recursively calls `self.ft_search` **twice** — once for the BM25-only sub-AST, once for the KNN/VectorRange-only sub-AST, each with its own full scatter-gather across all shards (`limit = max(offset+limit, knn_k, 100)`, `rrf_k`/`linear_weights` cleared on the recursive calls to avoid infinite hybrid-detection) — and fuses the two **already-merged, cross-shard** hit lists client-side on the coordinating shard. **This means a hybrid `FT.SEARCH`/`FT.HYBRID` query issues roughly double the cross-shard messages of a plain query** (two independent `(N-1)`-shard scatters instead of one).

### 4.9 `FT.AGGREGATE` pipeline — `execute_aggregate_pipeline` (lines 2846–3034)

```rust
pub enum Reducer { Count{alias}, Sum{field,alias}, Avg{field,alias}, Min{field,alias}, Max{field,alias} }
pub struct GroupByStage { pub fields: Vec<String>, pub reducers: Vec<Reducer> }
pub struct ApplyStage   { pub expr: String, pub alias: String }
pub struct SortByStage  { pub fields: Vec<(String, bool)>, pub max: Option<usize> }
pub enum AggregateStage { Group(GroupByStage), Apply(ApplyStage), Sort(SortByStage), Limit{offset,num}, Filter(String) }
pub type AggregateRow = Vec<(String, String)>;   // ordered key/value pairs, not a HashMap
```

`Router::ft_aggregate` (`src/router.rs:2808`) first calls `parse_query`, then `self.ft_search` with `SearchOptions { limit: 100_000, offset: 0, sortby: None, return_fields: options.load_fields, .. }` — i.e. it fetches up to **100,000** matching rows via the normal cross-shard scatter/gather (§2.3), fully merged, **before** running any pipeline stage. `execute_aggregate_pipeline` then runs **single-threaded, on the coordinating shard only** — there is no per-stage distribution; `GROUPBY`/`REDUCE`/`APPLY`/`SORTBY`/`FILTER`/`LIMIT` all operate on the in-memory `Vec<AggregateRow>` sequentially, in the order the stages appeared in the command:

- **`Filter`**: `evaluate_filter` (lines 2800–2844) — a 3-token `@field op value` comparator (`==`/`=`, `!=`, `>`, `>=`, `<`, `<=`), numeric if both sides parse as `f64`, else lexicographic string comparison. No `&&`/`||` boolean composition.
- **`Apply`**: `evaluate_expr` — a small recursive-descent arithmetic parser/evaluator (`tokenize_expr`/`parse_expr_addition`/`parse_expr_multiplication`/`parse_expr_primary`, lines 2615–2798) supporting `+ - * /`, parens, unary minus, numeric literals, and `@field` references (missing/unparsable fields evaluate to `0.0`, not an error). On a parse error, falls back to copying `@expr`'s own value verbatim if `expr` is itself a bare field reference. Numeric results are formatted as an integer string if `fract() == 0.0`, else `{:.4}` trimmed of trailing zeros.
- **`Group`**: buckets rows into a `HashMap<Vec<String>, Vec<AggregateRow>>` keyed by the group-by fields' string values, then computes each reducer over each bucket (`Sum`/`Avg`/`Min`/`Max` skip non-numeric values silently; `Min`/`Max` on an empty numeric set emit `"0"`).
- **`Sort`**: stable multi-key sort (numeric if both sides parse, else `str::cmp`), optional `max` truncation.
- **`Limit`**: `skip(offset).take(num)`.

---

## 5. Cross-Shard Scatter/Gather — Exact Mechanism

| Operation | Mechanism |
| :--- | :--- |
| `FT.CREATE` | `Router::create_search_index` (router.rs:2560): registers in the global mirror first, backfills+initializes the **local** shard's partition (`ShardDb::init_search_index`, scanning that shard's own `RudisTable`/`JsonStore` entries and matching `PREFIX`es), then broadcasts `ShardMessage::InitSearchIndex { schema }` to every other shard via `flume::bounded(1)` request/response, awaiting each `rx.recv_async()` before returning `Ok(())` — **synchronous fan-out**, so once `FT.CREATE` returns, every shard has an initialized (possibly backfilled) local partition. |
| `FT.ALTER` | `Router::alter_search_index` (router.rs:2619): merges new fields into the existing schema, calls `crate::search::reset_search_index` (replaces the **mirror** with a brand-new empty `InvertedIndex`, discarding its prior contents), re-initializes the **local** shard via `init_search_index` — which re-backfills matching documents from *that shard's own* local table/JSON store AND pushes each one into the now-empty mirror (`shard.rs:686-690/705-709/724-728`) — then broadcasts `ShardMessage::InitSearchIndex` with the merged schema to every other shard, each of which does the same local-backfill-plus-mirror-push for its own slice of the keyspace. Net effect: the mirror ends up fully reconstructed from every shard's local data once all broadcasts complete, not permanently emptied — but it is briefly incomplete (only the initiating shard's documents) while the broadcast is in flight. |
| `FT.SEARCH`/`FT.HYBRID` | `Router::ft_search` (§2.3): query local partition, scatter `ShardMessage::SearchQuery` to every other shard (skipped entirely if `num_shards == 1`), gather, merge-sort, re-paginate. Hybrid (RRF/Linear) queries do this **twice** (§4.8). |
| `FT.AGGREGATE` | One `ft_search` scatter/gather (limit 100,000, no pagination) then a fully local, single-shard pipeline (§4.9) — **not** distributed. |
| `FT.DROPINDEX` | `Router::drop_search_index`: drops local partition, broadcasts `ShardMessage::DropSearchIndex { name }` to every other shard, then drops the global mirror entry (`crate::search::drop_search_index`). |
| `FT.INFO`/`FT._LIST` | No scatter/gather — reads the global mirror directly (`get_search_index`/`list_search_indices`), which every shard's writes keep in sync via the double-write in §2.3/§6. |
| `FT.ADD` | **No scatter/gather, no local-shard write at all** — writes directly (and only) to the global mirror via `get_search_index` (`connection.rs:11623-11627`). See §6 for the consequence. |

All inter-shard messages use the same `flume::bounded(1)` request/oneshot-response pattern as the rest of the router (Component 04's `ShardMessage` mesh) — a query message plus a dedicated response channel per remote shard, awaited concurrently (one `rx` per shard, not sequentially).

---

## 6. Known Bugs, Limitations & Dead Code (verified by reading, not carried over from memory)

1. **`FT.ADD` documents are invisible to `FT.SEARCH`/`FT.AGGREGATE` once the index exists on every shard.** `Command::FtAdd` (`connection.rs:11617-11631`) calls `idx_arc.write().unwrap().add_document(...)` **only** on the global mirror (`crate::search::get_search_index`). But `Router::ft_search` only falls back to the mirror when the querying shard's **own** `search_indices` map lacks the index (§2.3) — and after `FT.CREATE`, every shard always has a local entry. So a document added purely via legacy `FT.ADD` is counted in `FT.INFO`'s `num_docs` (which reads the mirror) but **never returned by `FT.SEARCH`/`FT.AGGREGATE`** (which read the per-shard partitions) — a real, user-visible count/result mismatch. `FT.ADD` also always passes `vectors: None` (line 11626), so it can never index vector fields even when it does work.
2. **`FieldType::Text.weight` (`WEIGHT` attribute) is parsed and stored (`FT.CREATE`/`FT.ALTER`, and echoed by `FT.INFO`) but never read by `bm25_score` or anywhere in the scoring path** — re-verified still true. All text fields contribute to relevance identically regardless of declared weight.
3. **`VECTOR ... EPSILON <e>` (schema-level default) and `VECTOR FLAT ... BLOCK_SIZE <n>` are parsed into `VectorFieldAttrs.epsilon`/`.block_size` (`resp.rs:11387-11403`) but never read anywhere in `search.rs` or `vector.rs`** — dead configuration. Only the **per-query** `$EPSILON` inside a `VECTOR_RANGE ... => {$EPSILON: ...}` clause has any effect (§4.6).
4. **`DIALECT` is range-validated (`1..=4`) and stored in `SearchOptions.dialect`, but the field is never read anywhere after being set** (`grep` confirms the only reference to `.dialect` in the whole crate is the assignment at `resp.rs:11592`) — pure validate-and-discard; there is only one query-parsing behavior regardless of declared dialect.
5. **`TIMEOUT <ms>` is parsed into `SearchOptions.timeout_ms` but the value is explicitly discarded at the one place it's pattern-matched** (`connection.rs:10604`, `timeout_ms: _`) — no query timeout is ever enforced; a pathological query (e.g. a `Prefix` scan over a huge vocabulary, or `TagFilter`'s full linear scan) runs to completion regardless of the requested timeout.
6. **`TAG` query matching ignores the schema's `CASESENSITIVE` attribute on the query side.** Indexing (`add_document`, lines 869-885) respects `casesensitive` (stores tags with original case when set). But `parse_query`'s tag-filter branch **always** lowercases the query's tag literals (line 1647, unconditional `.to_ascii_lowercase()`), so a `CASESENSITIVE` tag field that actually contains mixed-case values (e.g. `"Books"`) can **never** be matched by `@field:{Books}` — it is silently lowercased to `"books"` before the `HashSet::contains` check and will never match unless the original tag happened to already be lowercase.
7. **Multi-vector (chunked) fields only work for JSON-indexed documents** (§3.2/§3.3) — `extract_json_fields` produces the `"{alias}#{n}"` extra-chunk keys that `add_document` looks for, but `add_hash_document` never does, so a HASH document's vector field is always exactly one chunk regardless of how many raw `field#N` HASH fields are set.
8. **`QueryAst::Exact` is still dead code** — defined and handled in `evaluate_ast`, never constructed by `parse_query`; quoted phrases are parsed as an `And` of stemmed terms, not an exact/positional phrase match (re-verified unchanged from the prior pass).
9. **Hybrid RRF/Linear silently no-ops if the AST doesn't have the expected shape.** If a caller sets `RRF`/`LINEAR`/`SCORER` on `FT.SEARCH` but the query string doesn't parse into an `And` containing exactly one KNN/VECTOR_RANGE clause plus at least one other clause (`as_hybrid_rrf`, lines 1360-1382), the fusion option is dropped and the plain BM25/KNN-sort path runs instead — no error is returned.
10. **`FT.EXPLAIN` returns the `Debug`-formatted `QueryAst`**, not a real, human-oriented query execution plan (iterator tree, estimated cardinalities, etc.) — `connection.rs:11611-11615`, `format!("{:?}", ast)`.
11. **`FT.DROPINDEX ... DD`** (which in real RediSearch also deletes the indexed documents themselves) **parses the `DD` flag but discards it** (`Command::FtDropIndex { index, dd: _ }`, `connection.rs:11604`) — only the index metadata/postings are dropped; underlying keys are never touched.
12. **`WITHSCORES` and `YIELD_DISTANCE_AS` use different, inconsistent numeric formatting**: `FT.SEARCH`/`FT.PROFILE`'s score output is always `format!("{:.6}", score)` (fixed 6 decimals), while the KNN distance field injected via `format_distance` (line 2255) uses `format!("{}", d)` (shortest round-trip `Display`, e.g. `"0"` not `"0.000000"`).
13. **`Prefix` and `TagFilter` are both full scans** — `Prefix` is `O(vocabulary size)` over `self.inverted`, `TagFilter` is `O(N documents)` over `id_to_meta` — there is no trie/prefix index for terms and no inverted tag index; only exact `Term` lookups and numeric `RangeTree` queries are sub-linear.
14. **Double indexing cost persists**: every document write still pays full tokenization/posting-list/vector-index cost twice — once into the per-shard partition, once into the process-wide mirror (§2.3) — unchanged from the prior pass's finding.

---

## 7. `FT.*` / `SEARCH.*` Command Surface (verified against `src/resp.rs` lines 11199–12223 and `src/connection.rs`)

| Command | Parsed Syntax | Notes |
| :--- | :--- | :--- |
| `FT.CREATE` | `<index> [ON HASH\|JSON] [PREFIX n p1 ...] SCHEMA (<id> [AS <alias>] TEXT [WEIGHT w] [SORTABLE] [NOSTEM] \| NUMERIC [SORTABLE] \| TAG [SEPARATOR c] [CASESENSITIVE] \| VECTOR <FLAT\|HNSW> <n_args> TYPE <FLOAT32\|FLOAT64\|FLOAT16\|BFLOAT16\|INT8\|UINT8> DIM n DISTANCE_METRIC <L2\|IP\|COSINE> [INITIAL_CAP c] [BLOCK_SIZE b (FLAT)] [M m (HNSW)] [EF_CONSTRUCTION efc (HNSW)] [EF_RUNTIME efr (HNSW)] [EPSILON e (HNSW)])+` | `TYPE`/`DIM`/`DISTANCE_METRIC` are mandatory for `VECTOR` (returns an error if missing); backfills all matching existing keys on all shards (§5). |
| `FT.SEARCH` | `<index> <query> [NOCONTENT] [WITHSCORES] [RRF [k]] [LINEAR [a b]] [SCORER RRF\|LINEAR ...] [LIMIT off n] [SORTBY field [ASC\|DESC]] [RETURN n f1 ...] [PARAMS n k v ...] [DIALECT 1-4] [TIMEOUT ms]` | `RRF`/`LINEAR`/`SCORER` set `SearchOptions.rrf_k`/`.linear_weights`; effective only if the query AST matches a hybrid shape (§6 item 9). |
| `FT.HYBRID` | `<index> <text_query> <vector_query> [RRF [k]] [LINEAR [a b]] [SCORER RRF\|LINEAR ...] [LIMIT off n] [RETURN n f1 ...] [PARAMS n k v ...] [WITHSCORES] [NOCONTENT] [DIALECT] [TIMEOUT]` | **Syntactic sugar, not a distinct execution path**: stitches `text_query` and `vector_query` into one combined query string (`"({text})=>{vec_tail}"`) and returns a `Command::FtSearch` with `rrf_k: Some(60.0)` by default — reuses `FT.SEARCH`'s exact hybrid-fusion code (§4.8). |
| `FT.AGGREGATE` | `<index> <query> [LOAD n @f1 ...] [GROUPBY n @f1 ... [REDUCE COUNT\|SUM\|AVG\|MIN\|MAX n args [AS alias]]...] [APPLY expr AS alias] [SORTBY n @f1 [ASC\|DESC] ... ] [LIMIT off n] [FILTER expr] [DIALECT] [TIMEOUT]` | Stages execute in command order (§4.9); scatter/gather happens once, before any stage (§5). |
| `FT.ALTER` | `<index> SCHEMA ADD (<id> [AS <alias>] <type>...)+` | Merges fields into the schema, rebuilds+backfills all copies (§5). |
| `FT._LIST` | *(no args)* | `Command::FtList` → sorted `list_search_indices()` from the mirror. |
| `FT.PROFILE` | `<index> SEARCH [LIMITED] QUERY <query> [NOCONTENT] [WITHSCORES] [LIMIT off n] [PARAMS n k v ...] [DIALECT] [TIMEOUT]` | Returns a 2-element RESP array `[search_results, profile_details]`; timing is wall-clock `Instant::now()` deltas around `parse_query` and `Router::ft_search` (parse time and total time, both formatted as seconds with 6 decimals) plus the `Debug`-formatted AST — not a real per-iterator profile. |
| `FT.INFO` | `<index>` | Reads the mirror only (§2.3); reports `num_docs`, `num_terms` (`inverted.len()`, i.e. distinct terms), `total_inverted_index_blocks` (actually `total_terms`, the sum of all doc lengths — a naming mismatch with real RediSearch's field), `vector_index_sz_mb` (`Σ VectorFieldIndex::memory_usage()`), `hash_indexing_failures`, per-attribute detail including live `num_vectors`/`dim` for `VECTOR` fields. |
| `FT.DROPINDEX` | `<index> [DD]` | `DD` parsed but ignored (§6 item 11). |
| `FT.EXPLAIN` | `<index> <query>` | Returns `Debug`-formatted `QueryAst` (§6 item 10); `<index>` argument is accepted but unused. |
| `FT.ADD` | `<index> <doc_id> <score> FIELDS f1 v1 ...` | Legacy single-document add; mirror-only write, no vectors (§6 item 1). |

`VECTOR` fields are backed by `InvertedIndex.vector_indices: HashMap<String, VectorFieldIndex>` wrapping either `FlatIndex` or `HnswIndex` from `src/vector.rs` (§2.4). Multi-vector JSON chunk arrays index each chunk under `<doc_key>\x00<chunk_idx>` in the vector index and deduplicate to the parent document by minimum distance across chunks during KNN/`VECTOR_RANGE` evaluation (§3.2, §4.5).

---

## 8. Cross-Component Interactions

- **`src/connection.rs`**: `Command::FtCreate/FtSearch/FtAggregate/FtInfo/FtDropIndex/FtExplain/FtAdd/FtList/FtAlter/FtProfile` arms call the corresponding `Router`/global-registry functions (§5/§7). `HSET`/`HMSET` and root-path `JSON.SET` call `reindex_hash_for_search`/`index_json_document_local`+`index_json_document_hook` after a successful write, gated by `db.has_search_indices() || crate::search::has_active_search_indices()` so the check is cheap (an atomic load, `SEARCH_INDICES_COUNT`) when no index exists anywhere.
- **`src/router.rs`**: `create_search_index`/`alter_search_index`/`drop_search_index` keep the per-shard/mirror duality in sync at schema-change time (§5); `ft_search`/`ft_aggregate` are the real cross-shard query paths (§2.3/§4.9).
- **`src/shard.rs`**: owns the per-shard `search_indices: HashMap<String, InvertedIndex>` and the `init_search_index`/`reindex_hash_local`/`delete_document_local`/`index_json_document_local` methods that mutate it directly with no locking (shard-thread-confined) — `init_search_index` also mirrors every backfilled document into the global registry (§2.3).
- **`src/resp.rs`**: parses every `FT.*` grammar in §7 into the real `IndexSchema`/`FieldType`/`VectorFieldAttrs`/`SearchOptions`/`AggregateOptions` values from §2/§4.9, including `PARAMS`, keyed by the bare parameter name with no `$` prefix (`resolve_str_param` tries both bare and `$`-prefixed forms, so either convention on the query-string side works).
- **`src/vector.rs`** (Component 08): supplies `VectorFieldIndex`/`FlatIndex`/`HnswIndex` (now genuinely owned per vector field, §2.4) and the SIMD `compute_distance`/`cosine_distance`/`l2_distance_sq`/`dot_product` kernels called directly by the brute-force branches of `knn_search`/`vector_range_search`/`min_doc_vector_distance` (via `crate::vector::compute_distance`, runtime-dispatched to AVX-512 → AVX2+FMA → portable). `HnswIndex`'s own ANN traversal is now genuinely reused here too (not independent, as the prior pass found) — see §2.4's reversal of that finding.
- **`src/json.rs`**: JSONPath parsing/querying (`parse_json_path`/`query_json_path`) backs `extract_json_fields` (§3.3); nested objects/arrays that aren't the matched leaf value are stringified via `.to_string()`, not recursively flattened into dotted-path fields.
- **`src/mcp.rs`** (Component 20): the `rudis_ft_search` MCP tool (line 429) is a minimal wrapper over `Command::FtSearch` — `index`/`query`/`limit` only, `SearchOptions::default()` otherwise (no `RETURN`, `SORTBY`, `RRF`, or `PARAMS` exposed to MCP callers).

---

## 9. Concrete Numbers

| Quantity | Value | Source |
| :--- | :--- | :--- |
| `DocId` width | `u32` | line 5 |
| `Posting` size | 8 bytes (`DocId` + `u32` term_freq) | lines 264–268 |
| BM25 `k1` / `b` | `1.2` / `0.75` | line 1017–1018 |
| BM25 IDF floor | `0.0001` (when `idf <= 0.0`) | line 1014 |
| Default RRF `k` | `60.0` | `resp.rs` RRF/HYBRID default, `SearchOptions.rrf_k` |
| Default linear `(alpha, beta)` | `(0.5, 0.5)` | `resp.rs` LINEAR default |
| `KNN_ADHOC_BF_RATIO` | `0.1` (10% of index size threshold for ADHOC brute force) | line 1934 |
| KNN chunk over-fetch multiplier | `k * 4` when multi-chunk vectors present | lines 2114-2118 |
| `VectorFieldAttrs` defaults | `m=16, ef_construction=200, ef_runtime=10, initial_cap=1024, epsilon=0.01, block_size=1024` | lines 87-98 |
| `FT.AGGREGATE` internal fetch cap | `100_000` hits before pipeline stages run | `router.rs:2816` |
| `DIALECT` accepted range | `1..=4` (validated, otherwise unused — §6) | `resp.rs:11589` |
| `RangeTree` range query complexity | `O(log N + K)` | §4.4, `BTreeMap::range` |
| `Prefix` query complexity | `O(vocabulary size)` | §6 item 13 |
| `TagFilter` query complexity | `O(N documents)` | §6 item 13 |
| Document removal complexity | `O(terms in that document)` | §3.4 |
| `src/search.rs` total / engine / test lines | 4,117 / ~3,035 (lines 1–3035) / ~1,080 (lines 3037–4117, 14 `#[test]` fns) | `wc -l`, `grep -c '#\[test\]'` |

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Document IDs are dense `u32` integers (`DocId`, recycled via `free_ids`), not the document's own string key — postings, the numeric range tree, and vector storage are all keyed by this dense id (vector storage uses the document's *string* key plus a `\x00<chunk_idx>` suffix for chunks, not the `DocId`, since `VectorFieldIndex` is keyed by `Bytes`). `DocMeta.terms` makes per-document deletion `O(terms in that document)` (§3.4).
* **Gotcha 2**: Numeric-field queries are served by a `BTreeMap`-backed `RangeTree`, giving genuine `O(log N + K)` range queries (§4.4) — but `TagFilter` and `Prefix` are still full scans (§6 item 13); do not assume every query type is sub-linear.
* **Gotcha 3**: Every index exists in **two independent copies** — a per-shard partition that real `FT.SEARCH`/`FT.AGGREGATE` scatter/gather across, and a process-wide mirror that only `FT.INFO`/`FT._LIST` (and, defensively, a shard missing its own local copy) read from (§2.3). `FT.ADD` is the one command that writes **only** the mirror, making its documents invisible to `FT.SEARCH` once the index exists everywhere (§6 item 1) — do not use `FT.ADD` in tests that also assert `FT.SEARCH` results.
* **Gotcha 4**: KNN/`VECTOR_RANGE` now hit a real `HnswIndex`/`FlatIndex` (§2.4), not a brute-force scan over every document — but pre-filtered hybrid queries can still silently fall back to brute force over the filtered set (`KNN_ADHOC_BF_RATIO`, §4.5); if you're benchmarking filtered KNN, check which branch actually ran.
* **Gotcha 5**: `WEIGHT`, schema-level `EPSILON`, `BLOCK_SIZE`, `DIALECT`, and `TIMEOUT` are all accepted and stored but have **zero effect on behavior** (§6 items 2-5) — do not assume setting them changes anything; only per-query `$EPSILON` in a `VECTOR_RANGE` clause is live.
* **Gotcha 6**: `CASESENSITIVE` `TAG` fields cannot be correctly queried — the query parser always lowercases tag literals (§6 item 6). If you need case-sensitive tag matching today, you'll need to fix `parse_query`'s tag branch to look up the field's schema `casesensitive` flag before lowercasing.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run search.rs's own unit tests (14 tests covering BM25/CRUD, RRF, KNN+params,
#    multi-vector chunk dedup, and linear fusion)
cargo test --lib search:: -- --test-threads=1

# 4. Run the full unit suite
cargo test --lib -- --test-threads=1
```
