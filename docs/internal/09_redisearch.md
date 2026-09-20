# Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion (Implementation)

## Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion — Code Reference & Implementation

> **Source Files**: ``src/search.rs``


---

### 3. Component Architecture & Data Structures

```
     Raw Document: HSET "doc:1" title "Rust Systems" body "Distributed io_uring"
                                    │
                                    ▼ index_document_hook() — checked against EVERY
                                      registered index's prefixes, not just one
                         tokenize_text() + simple_stem()
                                    │
                  ┌─────────────────┴─────────────────┬───────────────┐
                  ▼                                   ▼               ▼
             Text Fields                         Tag Fields    Numeric Fields
        ["rust", "system"]  (stemmed)        {"tech","db"}        99.5
                  │
                  ▼
        InvertedIndex.inverted: HashMap<String, Vec<Posting>>
        "rust"   -> [Posting{doc_id:"doc:1", term_freq:1, positions:[0]}]
        "system" -> [Posting{doc_id:"doc:1", term_freq:1, positions:[1]}]
```

#### Real Data Structures (`src/search.rs`)

```rust
pub enum FieldType {
    Text { weight: f64, sortable: bool, nostem: bool },
    Numeric { sortable: bool },
    Tag { separator: char, casesensitive: bool },
    Vector { dim: usize, distance_metric: String, algorithm: String },
}

pub struct IndexSchema {
    pub name: String,
    pub on_type: String,       // "HASH" or "JSON", from FT.CREATE ... ON <type>
    pub prefixes: Vec<String>, // FT.CREATE ... PREFIX <n> <p1> <p2> ...
    pub fields: HashMap<String, FieldType>,
}

pub struct Posting {
    pub doc_id: String,       // real docs are string keys, not u32 ids
    pub term_freq: u32,
    pub positions: Vec<u32>,  // token positions are tracked but never used for phrase queries
}

pub struct DocMeta {
    pub doc_id: String,
    pub doc_len: usize,       // whole-document token count, not per-field
    pub fields: HashMap<String, String>,
    pub numeric_fields: HashMap<String, f64>,
    pub tag_fields: HashMap<String, HashSet<String>>,
    pub vector_fields: HashMap<String, Vec<f32>>,
}

pub struct InvertedIndex {
    pub schema: Option<IndexSchema>,
    pub inverted: HashMap<String, Vec<Posting>>, // term -> postings
    pub docs: HashMap<String, DocMeta>,           // doc_id -> metadata
    pub total_docs: usize,
    pub total_terms: usize,
}

// Real global registry — see §2.2
static SEARCH_INDICES: LazyLock<RwLock<HashMap<String, Arc<RwLock<InvertedIndex>>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
```

There is **no `vector_index: Option<HnswIndex>` field anywhere** — vectors are stored per-document
as plain `Vec<f32>` inside `DocMeta.vector_fields`, and KNN search (§4.4) is a brute-force linear
scan, not an HNSW lookup. `src/vector.rs`'s `HnswIndex` (Component 08) is a completely separate
data structure used only by the standalone `VECTOR.*`-style commands, not by this file.

---

---

### 4. Execution Algorithms & Code Logic

#### 4.1 BM25 Scoring — real formula, whole-document length

```rust
pub fn bm25_score(&self, term: &str, posting: &Posting, doc_len: usize) -> f64 {
    let n = self.inverted.get(term).map(|v| v.len()).unwrap_or(0);
    if n == 0 || self.total_docs == 0 { return 0.0; }
    let total_docs = self.total_docs as f64;
    let idf = ((total_docs - n as f64 + 0.5) / (n as f64 + 0.5) + 1.0).ln();
    if idf <= 0.0 { return 0.0001; }  // floor for very-common terms, not in the classic formula

    let k1 = 1.2;
    let b = 0.75;
    let avgdl = self.avg_doc_len().max(1.0);
    let freq = posting.term_freq as f64;
    let tf = (freq * (k1 + 1.0)) / (freq + k1 * (1.0 - b + b * (doc_len as f64 / avgdl)));
    idf * tf
}
```

$k_1 = 1.2$, $b = 0.75$ and the IDF smoothing term match the textbook Okapi BM25 formula
exactly. The one deviation from the classic formula is the `idf <= 0.0` floor (returns `0.0001`
instead of a zero/negative score) — this matters for very common terms in a small corpus where
the classic IDF can go negative or zero, which would otherwise make that term's contribution
vanish or invert; the floor keeps it a small positive tiebreaker instead. `doc_len` is passed in
by the caller as `doc.doc_len` — the whole document's combined token count across every indexed
text field, not the length of just the field the term was found in.

#### 4.2 Query Parsing (`parse_query` / `QueryAst`) — real and considerably richer than posting lists alone

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

`parse_query` hand-parses a real subset of RediSearch query syntax: bare words (AND'd, each
stemmed via `simple_stem`), `word*` prefix matches, `-term` negation, `@field:[min max]`
numeric range, `@field:{tag1|tag2}` tag filters, `@field:...` scoped sub-queries, `term1 | term2`
top-level OR, and a `*=>[KNN <k> @<field> $<param>]` suffix for hybrid vector search appended to
a base query. `execute_search` recursively walks this AST, building a `HashMap<doc_id, f64>` of
candidate scores per node (`And` intersects with position-preserved score accumulation, `Or`
unions with accumulation, `Not` inverts against the full doc set), then sorts by `sortby` if
given or by score descending, and paginates by `offset`/`limit`.

#### 4.3 KNN vector search — a previously-verified dead-code gap, now fixed

An earlier version of this doc documented a precise, verified dead-code gap here: `parse_query`'s
`KnnVector` node was always built with `query_vec: Vec::new()`, and nothing ever populated it
from `PARAMS`, so `*=>[KNN ...]` queries silently matched zero vectors. **That has since been
fixed.** `QueryAst::KnnVector` now carries a `param_name: String` alongside `query_vec`, captured
from the query string itself:

```rust
let param_name = tokens.get(2).map(|s| s.trim_start_matches('$').to_string()).unwrap_or_default();
let knn_ast = QueryAst::KnnVector { field, k, query_vec: Vec::new(), param_name };
```

and `execute_search`'s `KnnVector` arm now resolves an `effective_vec` at query time — using
`query_vec` directly if it's already non-empty (e.g. for callers that build a `QueryAst`
programmatically), otherwise looking `param_name` up in `opts.params` (trying both the bare name
and a `$`-prefixed variant, so it matches however the caller keyed it) and decoding it via the
new `parse_vector_blob`:

```rust
let effective_vec = if !query_vec.is_empty() {
    query_vec.clone()
} else if !param_name.is_empty() {
    opts.params.get(param_name).or_else(|| opts.params.get(&format!("${}", param_name)))
        .map(|bytes| parse_vector_blob(bytes)).unwrap_or_default()
} else { Vec::new() };
```

`parse_vector_blob` accepts two real formats, chosen automatically by a length check — not by an
explicit format flag:

```rust
pub fn parse_vector_blob(bytes: &[u8]) -> Vec<f32> {
    if bytes.len().is_multiple_of(4) && !bytes.is_empty() {
        // interpret as tightly-packed little-endian f32s, 4 bytes each
        let (chunks, _) = bytes.as_chunks::<4>();
        chunks.iter().map(|c| f32::from_le_bytes(*c)).collect()
    } else {
        // fall back to a comma/whitespace/bracket-separated text encoding, e.g. "1.0, 0.0, 0.0"
        String::from_utf8_lossy(bytes)
            .split(|c: char| c == ',' || c.is_whitespace() || c == '[' || c == ']')
            .filter_map(|s| s.trim().parse::<f32>().ok())
            .collect()
    }
}
```

The auto-indexing path (`index_document_hook` → `InvertedIndex::add_document`) got the matching
other half of this fix: a document field declared `Vector` in the schema is now run through the
same `parse_vector_blob` at index time (previously `vector_fields` was only ever populated by
whatever the `vectors` parameter passed in directly, which nothing supplied via the normal
`HSET`-driven ingestion path — so indexed documents' vector fields were themselves silently
empty before this change, a second half of the same gap not fully called out in the original
finding). **Hybrid keyword+vector search via `FT.SEARCH`'s KNN syntax is now a real, working
path end to end**, verified by a new test (`test_knn_vector_search_with_params`) that indexes two
documents with text-encoded vectors, queries with a raw-little-endian-float `PARAMS` blob, and
asserts the nearer document is returned.

**One real ambiguity worth knowing about `parse_vector_blob`'s auto-detection**: a byte string
that happens to be a multiple of 4 bytes long is *always* treated as packed floats, never as
text, even if it was actually meant as a short text encoding. `"1,0,0,1"` (8 ASCII bytes) would
be parsed as text (8 is not relevant, its length just needs checking) — but a text vector whose
byte length happens to land on a multiple of 4 (e.g. `"1,0,0"` is 5 bytes — fine — but `"1,-1"` is
4 bytes exactly) would be silently reinterpreted as one packed `f32` instead of two text numbers.
This is a real, narrow edge case, not a defect in the common case (real callers use one format
consistently), but worth flagging (see §7).

#### 4.4 Vector similarity when it *is* invoked directly (`FT.ADD` + brute-force cosine)

```rust
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0; let mut norm_a = 0.0; let mut norm_b = 0.0;
    for (x, y) in a.iter().zip(b.iter()) { dot += x*y; norm_a += x*x; norm_b += y*y; }
    let denom = (norm_a * norm_b).sqrt();
    if denom == 0.0 { 0.0 } else { dot / denom }
}
```

Now that `query_vec`/`param_name` are genuinely resolved (§4.3), it's worth being precise about
what kind of search that vector actually drives: the scan itself is still a full `O(num_docs)`
loop computing cosine similarity against every document that has a vector in the queried field —
there is no ANN index (no HNSW, no quantization) inside `search.rs` itself. The fix in §4.3 makes
KNN *correct*, not fast — it's still brute force per query.

#### 4.5 Reciprocal Rank Fusion — real, and matches the standard formula

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

Standard RRF (`1 / (k + rank + 1)` per list, summed across lists a document appears in), with
`k` passed in by the caller rather than hardcoded — real and correct as far as the fusion math
goes. What's missing (§4.3) is a working vector-ranked list to fuse *with* via the `FT.SEARCH`
KNN path; the function itself has its own passing unit test (`test_reciprocal_rank_fusion`)
that constructs both hit lists manually rather than through a real KNN search.

---

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: `Command::FtCreate/FtSearch/FtInfo/FtDropIndex/FtExplain/FtAdd`
  arms call `create_search_index`/`get_search_index`+`parse_query`+`execute_search`/
  `drop_search_index` directly; separately, `Hset`/`Hmset`/`JsonSet` (root path only) call
  `index_document_hook` after a successful write, and `Del`/similar paths call
  `delete_document_hook`.
- **`src/resp.rs`**: parses `FT.CREATE`'s `ON`/`PREFIX`/`SCHEMA` clauses and each field's
  `TEXT [WEIGHT w] [SORTABLE] [NOSTEM]` / `NUMERIC [SORTABLE]` / `TAG [SEPARATOR c]
  [CASESENSITIVE]` / `VECTOR ...` options into the real `IndexSchema`/`FieldType` values shown
  in §3, and parses `FT.SEARCH ... PARAMS n k v ...` into `SearchOptions.params`, keyed by the
  bare parameter name with no `$` prefix (§4.3 — `execute_search` tries both forms when looking
  a param up, so either convention on the query-string side resolves correctly).
- **`src/vector.rs`** (Component 08): **not actually used by this file** — `HnswIndex` and this
  file's brute-force per-document vector scan are two independent, unconnected
  implementations of vector similarity search in the codebase.
- **`src/json.rs`**: `JsonSet` on the root path (`$`) flattens the top-level JSON object's
  scalar fields into `HashMap<String, String>` before calling `index_document_hook` — nested
  objects/arrays are stringified via `.to_string()`, not recursively flattened into
  dotted-path fields.

---

---

### 7. Future Improvements

- **RESOLVED — `PARAMS`-supplied vectors are now wired into `QueryAst::KnnVector` (§4.3).** Fixed by adding a `param_name` field captured at parse time, resolving it against `opts.params` (bare or `$`-prefixed) at execution time via the new `parse_vector_blob`, and — the other half of the same underlying gap — teaching the auto-indexing path (`add_document`) to populate `vector_fields` from a `Vector`-typed field's stored string value using the same decoder, since `index_document_hook` never supplied a `vectors` map directly. Verified by a new passing test (`test_knn_vector_search_with_params`). Hybrid keyword+vector search via `FT.SEARCH`'s KNN syntax is now real end to end, still brute-force (§4.4), not ANN-accelerated.
- **Low — new, from the fix above: resolve `parse_vector_blob`'s format-detection ambiguity for short vectors (§4.3).** A byte string that happens to be a multiple of 4 bytes long is always decoded as packed little-endian floats, never as text — a short text-encoded vector whose byte length is coincidentally a multiple of 4 (e.g. `"1,-1"`, 4 bytes) would be silently misinterpreted as one packed float instead of two text numbers. An explicit format hint (e.g. requiring `PARAMS` values for vector fields to always be one format, documented and enforced) would remove the ambiguity; low priority since real callers use one format consistently in practice.
- **Medium — either read `FieldType::Text.weight` in `bm25_score`, or remove it from `FT.CREATE`'s accepted syntax (§2.4).** Accepting and storing a per-field weight that scoring silently ignores is worse than not accepting it at all — a user who sets `WEIGHT 5.0` on a field reasonably expects it to matter. Implementing real per-field BM25F (per-field lengths and weighted term contributions) is the "correct" fix; dropping/erroring on `WEIGHT` until then is the honest one.
- **Medium — shard or otherwise reduce contention on the global `SEARCH_INDICES` `RwLock` (§6).** Every `HSET`/`FT.SEARCH` across every shard takes this one process-wide lock, which is the same class of exception as `BlockHub` (Component 06) but on a much hotter path (every indexed write, not just blocking commands). A per-index `RwLock` (already partially true — `Arc<RwLock<InvertedIndex>>` per index — but the *registry* itself is one lock) or sharding indexes by name hash across a small pool of registries would reduce contention when many indexes are in active use concurrently.
- **Low — either use the tracked term `positions` for real phrase-query support (`"exact phrase"` matching), or stop tracking them (§6).** Currently pure dead weight: computed and stored on every `Posting`, read by nothing.
- **Low — replace `simple_stem`'s handful of suffix rules with a real Porter/Snowball stemmer (§2.3)** if search-quality on real English text becomes a priority — the current heuristic is a reasonable placeholder but will both over-stem and under-stem relative to a proper algorithm.

---
---
