# Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/search.rs` (2,488 lines)
> **High-Level Design Spec**: [`docs/design/09_redisearch.md`](../design/09_redisearch.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/search.rs` | Index data structures, tokenization, query parsing/evaluation, BM25 scoring, RRF, aggregation pipeline | `InvertedIndex`, `IndexSchema`, `QueryAst`, `AggregateOptions`, `bm25_score`, `parse_query`, `execute_search`, `reciprocal_rank_fusion` |
| `src/shard.rs` | Per-shard index partition (`ShardDb.search_indices`) and its local indexing/query hooks | `ShardDb::index_document_local`, `ShardDb::init_search_index` |
| `src/router.rs` | Cross-shard scatter/gather for `FT.SEARCH`/`FT.AGGREGATE`, index create/drop broadcast | `Router::ft_search`, `Router::ft_aggregate`, `Router::create_search_index` |
| `src/resp.rs` | `FT.CREATE`/`FT.SEARCH`/`FT.AGGREGATE`/`FT.ADD` argument parsing into the structs below | `Command::FtCreate`, `Command::FtSearch`, `Command::FtAggregate` |
| `src/connection.rs` | `FT.*` command dispatch, `HSET`/`HMSET`/`JSON.SET`/delete indexing hooks | `Command::FtCreate/FtSearch/FtAggregate/FtInfo/FtDropIndex/FtExplain/FtAdd` arms |
| `src/vector.rs` | SIMD-accelerated `cosine_distance` kernel, reused (not the HNSW index) by this file's KNN path | `cosine_distance` |

---

## 2. Component Architecture & Data Structures

```
     Raw Document: HSET "doc:1" title "Rust Systems" body "Distributed io_uring"
                                    │
                                    ▼ index_document_local() (shard-local partition)
                                      + index_document_hook() (process-wide mirror)
                                      — each checked against EVERY registered index's
                                        prefixes, not just one
                         tokenize_text() + simple_stem()
                                    │
                  ┌─────────────────┴─────────────────┬───────────────┐
                  ▼                                   ▼               ▼
             Text Fields                         Tag Fields    Numeric Fields
        ["rust", "system"]  (stemmed)        {"tech","db"}        99.5
                  │                                   │               │
                  ▼                                   ▼               ▼
       inverted: HashMap<String, Vec<Posting>>   tag_fields on    numeric_trees:
       "rust"   -> [Posting{doc_id, term_freq}]  each DocMeta     HashMap<String, RangeTree>
       "system" -> [Posting{doc_id, term_freq}]
```

### 2.1 Real Data Structures (`src/search.rs`)

```rust
pub type DocId = u32;

#[derive(Debug, Clone)]
pub enum FieldType {
    Text { weight: f64, sortable: bool, nostem: bool },
    Numeric { sortable: bool },
    Tag { separator: char, casesensitive: bool },
    Vector { dim: usize, distance_metric: String, algorithm: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct SchemaField {
    pub identifier: String, // e.g. "$.title" (JSON path) or "title" (hash field)
    pub alias: String,      // the name queries/results use (via FT.CREATE ... AS <alias>)
    pub field_type: FieldType,
}

#[derive(Debug, Clone)]
pub struct IndexSchema {
    pub name: String,
    pub on_type: String,               // "HASH" or "JSON", from FT.CREATE ... ON <type>
    pub prefixes: Vec<String>,         // FT.CREATE ... PREFIX <n> <p1> <p2> ...
    pub fields: HashMap<String, FieldType>,   // identifier/alias -> type, for O(1) lookup
    pub schema_fields: Vec<SchemaField>,      // ordered, for FT.INFO/introspection
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Posting {
    pub doc_id: DocId,     // dense integer id, NOT the document's string key
    pub term_freq: u32,
}

#[derive(Debug, Clone)]
pub struct DocMeta {
    pub key: Bytes,                                  // the document's real Redis key
    pub doc_len: usize,                               // whole-document token count, not per-field
    pub fields: HashMap<String, String>,
    pub numeric_fields: HashMap<String, f64>,
    pub tag_fields: HashMap<String, HashSet<String>>,
    pub vector_fields: HashMap<String, Vec<f32>>,
    pub terms: Vec<String>,                           // every term this doc contributed — drives O(1)-directed deletion
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderedF64(pub f64);   // total-order wrapper so f64 can key a BTreeMap

pub struct RangeTree {
    pub entries: std::collections::BTreeMap<OrderedF64, Vec<DocId>>,
    pub total_entries: usize,
}

pub struct InvertedIndex {
    pub schema: Option<IndexSchema>,
    pub inverted: HashMap<String, Vec<Posting>>,      // term -> postings
    pub numeric_trees: HashMap<String, RangeTree>,    // numeric field -> balanced tree
    pub key_to_id: HashMap<Bytes, DocId>,             // document key -> dense id
    pub id_to_meta: HashMap<DocId, DocMeta>,          // dense id -> metadata
    pub next_doc_id: DocId,
    pub free_ids: Vec<DocId>,                         // recycled ids from deleted documents
    pub total_docs: usize,
    pub total_terms: usize,
}

// Per-shard partition (src/shard.rs) — the primary store queried by FT.SEARCH:
// pub search_indices: HashMap<String, InvertedIndex>   (on ShardDb, thread-confined)

// Process-wide mirror — see design doc §1.3 for why both exist:
static SEARCH_INDICES: LazyLock<RwLock<HashMap<String, Arc<RwLock<InvertedIndex>>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
```

There is **no `vector_index: Option<HnswIndex>` field anywhere** in `InvertedIndex`. Vectors are stored per-document as plain `Vec<f32>` inside `DocMeta.vector_fields`, and KNN search (§4.4) is a brute-force linear scan using `crate::vector::cosine_distance` — a SIMD kernel this file *does* call into — not an HNSW lookup. `src/vector.rs`'s `HnswIndex` graph structure (Component 08) remains a separate index used only by the standalone `VECTOR.*`-style commands; this file reuses its distance math, not its ANN graph.

### 2.2 Two Storage Copies, One Query Path — Precisely

`Router::ft_search` (`src/router.rs`) is the real entry point for `FT.SEARCH`:

```rust
pub async fn ft_search(&self, index: &str, ast: &QueryAst, opts: &SearchOptions)
    -> (usize, Vec<SearchHit>)
{
    // Query this shard's own partition first, preferring it:
    let (local_total, local_hits) = {
        let db = self.local_db.borrow();
        if let Some(idx) = db.search_indices.get(index) {
            execute_search(idx, ast, &scatter_opts)          // per-shard partition
        } else if let Some(idx_arc) = get_search_index(index) {
            execute_search(&idx_arc.read().unwrap(), ast, &scatter_opts)  // fallback: process-wide mirror
        } else { (0, Vec::new()) }
    };
    // Then genuinely scatter/gather to every OTHER shard's own partition:
    for (sid, sender) in self.senders.iter().enumerate() {
        if sid != self.shard_id {
            sender.send(ShardMessage::SearchQuery { index, ast, options, responder });
        }
    }
    // ...collect, merge-sort by score/sortby, re-paginate by offset/limit
}
```

Every remote shard answers a `ShardMessage::SearchQuery` by querying **only its own local partition** (`db.search_indices.get(&index)`, in `src/server.rs`'s shard message loop) — remote shards never consult the process-wide mirror. The mirror fallback on the initiating shard is therefore a defensive path (exercised only if that shard's own partition is missing the index, which should not happen in normal operation since `create_search_index` initializes every shard synchronously — see §5), not the common query path. `FT.INFO` (`get_search_index`, used directly from `connection.rs`) always reads the process-wide mirror, which — because every shard writes every document into it via `index_document_hook` — reports accurate whole-cluster document/term counts without a scatter/gather round trip.

---

## 3. Execution Algorithms & Code Logic

### 3.1 Tokenization — `tokenize_text` / `simple_stem`

```rust
pub fn tokenize_text(text: &str, stem: bool) -> Vec<String> {
    for raw in text.split(|c: char| !c.is_alphanumeric()) {
        let lower = raw.to_ascii_lowercase();
        if ENGLISH_STOP_WORDS.contains(lower.as_str()) { continue; }
        tokens.push(if stem { simple_stem(&lower) } else { lower });
    }
    tokens
}

pub fn simple_stem(word: &str) -> String {
    // -ing (len > 5) -> strip; -ies (len > 4) -> "y"; -es (len > 4) -> strip;
    // -ed (len > 3) -> strip; trailing -s (len > 3, not -ss) -> strip; else unchanged
}
```

`ENGLISH_STOP_WORDS` is a fixed set of roughly 120 common English words. This same pipeline runs at both index time (`add_document`) and query time (`parse_query`/`tokenize_text` calls within it), so stored terms and query terms are always normalized identically.

### 3.2 Indexing a document — `InvertedIndex::add_document`

```rust
pub fn add_document(&mut self, doc_id_str: &str, fields: HashMap<String, String>,
                     vectors: Option<HashMap<String, Vec<f32>>>) {
    if self.key_to_id.contains_key(key_bytes) { self.remove_document(doc_id_str); } // re-index = remove + re-add

    for (field_name, field_val) in &fields {
        match schema.fields.get(field_name) {
            Some(FieldType::Text { nostem, .. }) => {
                let tokens = tokenize_text(field_val, !*nostem);
                doc_len += tokens.len();
                // accumulate per-term frequency in term_positions: HashMap<String, u32>
            }
            Some(FieldType::Numeric { .. }) => { /* parse f64, stash in numeric_fields */ }
            Some(FieldType::Tag { separator, casesensitive }) => {
                // split on separator, trim, lower-case unless casesensitive, collect into a HashSet
            }
            Some(FieldType::Vector { .. }) => {} // handled separately below
            None if field_name != "$" => { /* untyped fallback: tokenize + stem as text anyway */ }
        }
    }

    let doc_id = self.free_ids.pop().unwrap_or_else(|| { let id = self.next_doc_id; self.next_doc_id += 1; id });

    for (term, freq) in term_positions {
        self.inverted.entry(term.clone()).or_default().push(Posting { doc_id, term_freq: freq });
        indexed_terms.push(term); // kept on DocMeta.terms — drives O(1)-directed deletion, see §3.3
    }

    // Vector fields: use the caller-supplied `vectors` map if present, otherwise decode
    // the field's stored string value via parse_vector_blob (§3.5) — see the note below.
    for (field_name, &num_val) in &numeric_fields {
        self.numeric_trees.entry(field_name.clone()).or_default().add(doc_id, num_val);
    }

    self.key_to_id.insert(key_bytes, doc_id);
    self.id_to_meta.insert(doc_id, DocMeta { key, doc_len, fields, numeric_fields, tag_fields, vector_fields, terms: indexed_terms });
    self.total_docs += 1;
    self.total_terms += doc_len;
}
```

Fields not declared in the schema are still indexed as text (stemmed) unless their name is the literal JSON-root marker `"$"` — an intentional fallback so an index without an exhaustive `SCHEMA` still gets reasonable text search over unrecognized fields.

### 3.3 Deleting a document — `InvertedIndex::remove_document`

```rust
pub fn remove_document(&mut self, key: &str) {
    let doc_id = self.key_to_id.remove(key.as_bytes())?;
    let meta = self.id_to_meta.remove(&doc_id)?;
    self.total_docs -= 1; self.total_terms -= meta.doc_len;
    self.free_ids.push(doc_id);                    // recycled for the next add_document

    for term in &meta.terms {                      // O(1)-directed: only touches this doc's own terms
        self.inverted.get_mut(term).retain(|p| p.doc_id != doc_id);
        // drop the term's posting-list entry entirely if now empty
    }
    for (field_name, &num_val) in &meta.numeric_fields {   // O(log N)-directed
        self.numeric_trees.get_mut(field_name).remove(doc_id, num_val);
    }
}
```

Because `DocMeta.terms` records exactly which terms a document contributed, deletion never scans the full vocabulary — it revisits only the terms (and numeric fields) that document itself indexed, which is what makes dense integer `DocId`s (§2.1) worth the added indirection over using the document's string key directly in postings.

### 3.4 BM25 Scoring — `InvertedIndex::bm25_score`

```rust
pub fn bm25_score(&self, term: &str, posting: &Posting, doc_len: usize) -> f64 {
    let n = self.inverted.get(term).map(|v| v.len()).unwrap_or(0);
    if n == 0 || self.total_docs == 0 { return 0.0; }
    let idf = ((self.total_docs as f64 - n as f64 + 0.5) / (n as f64 + 0.5) + 1.0).ln();
    if idf <= 0.0 { return 0.0001; }  // floor for very-common terms, not in the classic formula
    let (k1, b) = (1.2, 0.75);
    let avgdl = self.avg_doc_len().max(1.0);
    let freq = posting.term_freq as f64;
    let tf = (freq * (k1 + 1.0)) / (freq + k1 * (1.0 - b + b * (doc_len as f64 / avgdl)));
    idf * tf
}
```

$k_1 = 1.2$, $b = 0.75$, and the IDF smoothing term match the textbook Okapi BM25 formula exactly. The one deviation is the `idf <= 0.0` floor (returns `0.0001` instead of a zero/negative score): for very common terms in a small corpus the classic IDF can go to zero or negative, which would otherwise make that term's contribution vanish or invert the ranking; the floor keeps it a small positive tie-breaker instead. `doc_len` is the whole document's combined token count across every indexed text field, not the length of just the field the term was found in — `FieldType::Text.weight` is accepted and stored by `FT.CREATE`'s parser but is **not read anywhere in `bm25_score` or its callers** (verified: no reference to any field's `weight` in the scoring path).

### 3.5 Query Parsing — `parse_query` / `QueryAst`

```rust
pub enum QueryAst {
    Term(String), Prefix(String), Exact(String),
    FieldScope { field: String, inner: Box<QueryAst> },
    NumericRange { field: String, min: f64, max: f64 },
    TagFilter { field: String, tags: Vec<String> },
    And(Vec<QueryAst>), Or(Vec<QueryAst>), Not(Box<QueryAst>),
    KnnVector { field: String, k: usize, query_vec: Vec<f32>, param_name: String },
    MatchAll,
}
```

`parse_query` hand-parses a real subset of RediSearch query syntax: bare words (AND'd, each stemmed via `simple_stem`), `word*` prefix matches, `-term` negation, `@field:[min max]` numeric range, `@field:{tag1|tag2}` tag filters, `@field:...` scoped sub-queries, `term1 | term2` top-level OR, and a `*=>[KNN <k> @<field> $<param>]` suffix appended to a base query for hybrid vector search. `execute_search` (via the internal `evaluate_ast`) recursively walks this AST, building a `HashMap<DocId, f64>` of candidate scores per node — `And` intersects with score accumulation (short-circuits once the running set is empty), `Or` unions with accumulation, `Not` inverts against the full document-id set — then sorts by `sortby` if given, or by score descending, and paginates by `offset`/`limit`.

**`QueryAst::Exact` is defined and handled in `evaluate_ast`, but `parse_query` never constructs it** — a quoted phrase like `"rust systems"` is currently parsed the same way as an unquoted multi-word query (an `And` of the individual stemmed terms), not as a distinguished exact-phrase match. This is consistent with §1.5's design note that phrase/positional matching is out of scope for the current implementation; the `Exact` variant is effectively dead code reachable only by constructing a `QueryAst` programmatically rather than through the parser.

### 3.6 KNN vector search — parameter resolution and blob decoding

`QueryAst::KnnVector` carries a `param_name: String` captured at parse time from the query string's `$param` token. `evaluate_ast`'s `KnnVector` arm resolves the actual query vector at execution time:

```rust
let effective_vec = if !query_vec.is_empty() {
    query_vec.clone()                                    // caller built the AST with a vector directly
} else if !param_name.is_empty() {
    opts.params.get(param_name)
        .or_else(|| opts.params.get(&format!("${}", param_name)))  // tries both bare and $-prefixed keys
        .map(|bytes| parse_vector_blob(bytes)).unwrap_or_default()
} else { Vec::new() };
```

`parse_vector_blob` accepts two formats, chosen automatically by a length check rather than an explicit flag:

```rust
pub fn parse_vector_blob(bytes: &[u8]) -> Vec<f32> {
    if bytes.len().is_multiple_of(4) && !bytes.is_empty() {
        // interpret as tightly packed little-endian f32s, 4 bytes each
    } else {
        // fall back to a comma/whitespace/bracket-separated text encoding, e.g. "1.0, 0.0, 0.0"
    }
}
```

**A real, narrow ambiguity**: any byte string whose length happens to be a multiple of 4 is always decoded as packed floats, never as text — a short text-encoded vector whose byte length coincidentally lands on a multiple of 4 (e.g. `"1,-1"`, 4 ASCII bytes) would be silently misinterpreted as one packed `f32` rather than two text numbers. Real callers use one format consistently in practice, so this is a low-priority edge case, not a defect in ordinary use (see §6).

The auto-indexing path (`add_document`, §3.2) applies the same `parse_vector_blob` decoder to a schema-declared `Vector` field's stored string value when the caller did not supply a `vectors` map directly (the normal `HSET`-driven ingestion path never does), so documents indexed through ordinary write commands get their vector fields populated correctly, not left empty.

### 3.7 Vector similarity — `cosine_similarity` (thin wrapper over Component 08's kernel)

```rust
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    (1.0 - crate::vector::cosine_distance(a, b)).max(0.0)
}
```

The `KnnVector` evaluation arm scans every document with a vector present in the queried field, computes `cosine_similarity` for each via this SIMD-accelerated kernel (`src/vector.rs`, shared with Component 08's HNSW/vector-command engine), sorts descending, and takes the top `k`. This is exact-but-linear: `O(documents with that field)` per query, with no ANN index consulted anywhere in this file.

### 3.8 Reciprocal Rank Fusion — `reciprocal_rank_fusion`

```rust
pub fn reciprocal_rank_fusion(bm25_hits: &[SearchHit], vector_hits: &[SearchHit], k: f64) -> Vec<SearchHit> {
    let mut rrf_scores: HashMap<String, f64> = HashMap::new();
    for (rank, hit) in bm25_hits.iter().enumerate() {
        *rrf_scores.entry(hit.doc_id.clone()).or_default() += 1.0 / (k + (rank as f64) + 1.0);
    }
    for (rank, hit) in vector_hits.iter().enumerate() {
        *rrf_scores.entry(hit.doc_id.clone()).or_default() += 1.0 / (k + (rank as f64) + 1.0);
    }
    // ...collect, sort descending by combined score
}
```

Standard RRF (`1 / (k + rank + 1)` per list, summed across every list a document appears in), with `k` supplied by the caller rather than hardcoded — matches the textbook formula exactly.

### 3.9 Aggregation pipeline — `execute_aggregate_pipeline`

```rust
pub enum Reducer { Count { alias }, Sum { field, alias }, Avg { field, alias }, Min { field, alias }, Max { field, alias } }
pub enum AggregateStage { Group(GroupByStage), Apply(ApplyStage), Sort(SortByStage), Limit { offset, num }, Filter(String) }
```

`Router::ft_aggregate` first runs the base query through `ft_search` (with a large internal limit, `100_000`, to approximate "all matches" before the pipeline narrows them) to produce `AggregateRow`s (`Vec<(String, String)>` keyed by field name, seeded with `__key`/`__score`), then executes `options.stages` sequentially: `Filter` retains rows matching a boolean expression, `Apply` computes a new field via the arithmetic-expression evaluator (`evaluate_expr`, a small recursive-descent parser supporting `+ - * /` and field references), `Group` buckets rows by field values and applies the configured reducers, `Sort`/`Limit` order and truncate the row set. This is a **single-shard, in-process pipeline over already-merged cross-shard search results** — `ft_aggregate` does not itself perform any additional per-shard scatter/gather beyond what `ft_search` already does; the aggregation stages run once, after all shards' results have already been combined.

---

## 4. `FT.*` Command Surface Supported (verified against `src/resp.rs`/`src/connection.rs`)

| Command | Parsed Syntax |
| :--- | :--- |
| `FT.CREATE` | `<index> [ON HASH\|JSON] [PREFIX n p1 ...] SCHEMA (<id> [AS <alias>] TEXT [WEIGHT w] [SORTABLE] [NOSTEM] \| NUMERIC [SORTABLE] \| TAG [SEPARATOR c] [CASESENSITIVE] \| VECTOR <algo> [DIM n] [DISTANCE_METRIC m] ...)+` |
| `FT.SEARCH` | `<index> <query> [NOCONTENT] [LIMIT off n] [SORTBY field [ASC\|DESC]] [RETURN n f1 ...] [PARAMS n k v ...]` |
| `FT.AGGREGATE` | `<index> <query> [LOAD n @f1 ...] [GROUPBY n @f1 ... [REDUCE COUNT\|SUM\|AVG\|MIN\|MAX n args [AS alias]]...] [APPLY ...] [SORTBY ...] [LIMIT ...] [FILTER ...]` |
| `FT.INFO` | `<index>` — reads the process-wide mirror registry (§2.2) |
| `FT.DROPINDEX` | `<index> [DD]` |
| `FT.EXPLAIN` | `<index> <query>` — returns the `Debug`-formatted `QueryAst` |
| `FT.ADD` | `<index> <doc_id> <score> [FIELDS f1 v1 ...]` |

`VECTOR` field parsing tolerates and skips `TYPE`/`FLOAT32`/`M`/`EF_CONSTRUCTION`/bare-numeric tokens for compatibility with the fuller upstream `FT.CREATE ... VECTOR HNSW n ...` argument grammar, without acting on those specific sub-arguments beyond `DIM`/`DISTANCE_METRIC`. There is no `DIALECT` argument support anywhere in the parser.

---

## 5. Cross-Component Interactions

- **`src/connection.rs`**: `Command::FtCreate/FtSearch/FtAggregate/FtInfo/FtDropIndex/FtExplain/FtAdd` arms call `Router::create_search_index`/`Router::ft_search`+`parse_query`/`Router::ft_aggregate`/`Router::drop_search_index` (or the process-wide `get_search_index`/`create_search_index`/`drop_search_index` functions for `FT.INFO`); separately, `Hset`/`Hmset`/`JsonSet` (root path only) call both `db.index_document_local` (shard-local partition) and `crate::search::index_document_hook` (process-wide mirror) after a successful write, gated by `db.has_search_indices() || crate::search::has_active_search_indices()` so the check is cheap when no index exists.
- **`src/router.rs`**: `create_search_index` initializes the index on the local shard, broadcasts `ShardMessage::InitSearchIndex` to every other shard, *and* registers it in the process-wide mirror — all three copies are created together, keeping the per-shard/mirror duality (§2.2) consistent at creation time. `ft_search` is the real cross-shard scatter/gather described in §2.2.
- **`src/shard.rs`**: owns the per-shard `search_indices: HashMap<String, InvertedIndex>` and the `index_document_local`/`delete_document_local`/`index_json_document_local` methods that mutate it directly, with no locking (shard-thread-confined).
- **`src/resp.rs`**: parses all `FT.*` argument grammars listed in §4 into the real `IndexSchema`/`FieldType`/`SearchOptions`/`AggregateOptions` values shown in §2.1/§3.9, including `PARAMS`, keyed by the bare parameter name with no `$` prefix (§3.6 — `evaluate_ast` tries both bare and `$`-prefixed forms when resolving a param, so either convention on the query-string side works).
- **`src/vector.rs`** (Component 08): supplies the `cosine_distance` SIMD kernel this file's KNN path calls into (§3.7); `HnswIndex` itself remains unused by `search.rs` — the two subsystems' vector-search implementations are independent.
- **`src/json.rs`**: `JsonSet` on the root path (`$`) flattens the top-level JSON object's scalar fields into `HashMap<String, String>` before calling the indexing hooks — nested objects/arrays are stringified via `.to_string()`, not recursively flattened into dotted-path fields.

---

## 6. Future Improvements

- **Low — resolve `parse_vector_blob`'s format-detection ambiguity for short vectors (§3.6).** A byte string that happens to be a multiple of 4 bytes long is always decoded as packed little-endian floats, never as text. An explicit format hint, or a documented requirement that `PARAMS` values for vector fields always use one format, would remove the ambiguity; low priority since real callers use one format consistently.
- **Medium — either read `FieldType::Text.weight` in `bm25_score`, or reject/ignore it explicitly at `FT.CREATE` time (§3.4).** Accepting and storing a per-field weight that scoring silently ignores is worse than not accepting it at all — a user who sets `WEIGHT 5.0` on a field reasonably expects it to matter. Implementing real per-field BM25F (per-field lengths and weighted term contributions) is the "correct" fix; documenting the current no-op behavior is the interim one.
- **Medium — reduce the dual-write cost of the per-shard/process-wide mirror split (§2.2).** Every indexed write currently pays the full tokenization/posting-list cost twice. A design that derives the mirror's metadata (document/term counts) from a lightweight cross-shard count aggregation instead of a full second copy of every document would remove the doubled indexing cost while keeping `FT.INFO` accurate.
- **Low — either use per-document term data for real phrase-query support (`"exact phrase"` matching via `QueryAst::Exact`, §3.5), or remove the unreachable `Exact` variant** — currently dead code from the parser's perspective, since `parse_query` never constructs it.
- **Low — replace `simple_stem`'s handful of suffix rules with a real Porter/Snowball stemmer (§3.1)** if search quality on real English text becomes a priority — the current heuristic is a reasonable placeholder but will both over-stem and under-stem relative to a proper algorithm.

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Document IDs are dense `u32` integers (`DocId`, recycled via `free_ids`), not the document's own string key — postings, the numeric range tree, and vector storage are all keyed by this dense id, and `DocMeta.terms` is what makes per-document deletion `O(terms in that document)` rather than a full vocabulary scan (§3.3).
* **Gotcha 2**: Numeric-field queries are served by a `BTreeMap`-backed `RangeTree`, not a linear filter scan, giving genuine `O(log N + K)` range queries (§2.1, design doc §4).
* **Gotcha 3**: Every index exists in two places at once — a per-shard partition that real `FT.SEARCH`/`FT.AGGREGATE` query execution scatters/gathers across, and a process-wide mirror that only `FT.INFO`-style metadata queries (and, defensively, a shard missing its own local copy) read from (§2.2). Do not assume a single global index is what serves queries.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
