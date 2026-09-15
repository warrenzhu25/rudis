# Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion (`src/search.rs`)

## 1. Architectural Purpose & Scope

`src/search.rs` provides an in-memory full-text search and indexing engine compatible with a
subset of RediSearch (`FT.CREATE`, `FT.SEARCH`, `FT.INFO`, `FT.DROPINDEX`, `FT.EXPLAIN`,
`FT.ADD`). It supports multi-field schema definitions (`TEXT`, `TAG`, `NUMERIC`, `VECTOR`), a
hand-written inverted-index posting-list structure, real Okapi BM25 relevance scoring, a small
RediSearch-like query-string parser (`parse_query`/`QueryAst`), and Reciprocal Rank Fusion for
merging two ranked result lists. Auto-indexing is wired into `HSET`/`HMSET`/`JSON.SET` (root
path only) in `src/connection.rs`.

---

## 2. Key Invariants & Concurrency Constraints

1. **Automatic Document Ingestion, but only for `HSET`/`HMSET`/`JSON.SET`**: every `HSET`,
   `HMSET`, and `JSON.SET key $ ...` call in `connection.rs` calls
   `crate::search::index_document_hook(key, str_fields)` after the write succeeds, converting
   whatever field values it has into `HashMap<String, String>` and re-indexing that document
   against every index whose prefix matches. Other write paths (`SET`, `LPUSH`, ...) do **not**
   trigger re-indexing.
2. **The index registry is a real, global, cross-shard-shared data structure — not
   thread-local.** Despite this looking like a per-shard subsystem, `SEARCH_INDICES` is a
   single process-wide `static LazyLock<RwLock<HashMap<String, Arc<RwLock<InvertedIndex>>>>>`.
   Every shard thread that handles an `HSET`/`FT.SEARCH`/etc. call takes a real
   `std::sync::RwLock` read or write lock on it. This is a genuine, deliberate exception to
   the shared-nothing/zero-lock architecture — the same category of exception as `BlockHub`
   (Component 06) — needed because a search index has to see writes from every shard, not just
   the one that happens to own a given key's slot.
3. **Deterministic Tokenization with a real (small) stop-word list and a heuristic stemmer**:
   `tokenize_text` splits on non-alphanumeric characters, lowercases, drops any token in the
   ~170-word `ENGLISH_STOP_WORDS` set, and optionally passes survivors through `simple_stem` — a
   handful of suffix-stripping rules (`-ing`, `-ies`→`-y`, `-es`, `-ed`, trailing `-s`), not a
   real Porter/Snowball stemmer.
4. **Real BM25, but with a single whole-document length, not one length per field.** `DocMeta`
   stores one `doc_len: usize` — the total token count across *all* indexed text fields of a
   document combined — and `InvertedIndex::avg_doc_len()` is `total_terms / total_docs` across
   the whole index, not per field. This is architecturally simpler than genuine multi-field
   BM25F (which needs per-field lengths/weights) despite `FieldType::Text` carrying a `weight`
   field — that `weight` is accepted by `FT.CREATE`'s parser but **`bm25_score` never reads
   it** (verified: no reference to any field's `weight` anywhere in the scoring function).

---

## 3. Component Architecture & Data Structures

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

### Real Data Structures (`src/search.rs`)

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

## 4. Execution Algorithms & Code Logic

### 4.1 BM25 Scoring — real formula, whole-document length

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

### 4.2 Query Parsing (`parse_query` / `QueryAst`) — real and considerably richer than posting lists alone

```rust
pub enum QueryAst {
    Term(String), Prefix(String), Exact(String),
    FieldScope { field: String, inner: Box<QueryAst> },
    NumericRange { field: String, min: f64, max: f64 },
    TagFilter { field: String, tags: Vec<String> },
    And(Vec<QueryAst>), Or(Vec<QueryAst>), Not(Box<QueryAst>),
    KnnVector { field: String, k: usize, query_vec: Vec<f32> },
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

### 4.3 A verified, real dead-code gap: KNN vector search never actually receives a vector

`FT.SEARCH ... PARAMS n $param BLOB ...` parses those params into
`SearchOptions.params: HashMap<String, Vec<u8>>` (`resp.rs:6826`, `options.params.insert(k, v)`),
and `parse_query`'s KNN branch builds `QueryAst::KnnVector { query_vec: Vec::new(), ... }` with
the comment `// Populated via PARAMS`. **Nothing ever populates it.** Grepping the whole file:
`query_vec` is read exactly once, in `execute_search`'s `KnnVector` arm:

```rust
QueryAst::KnnVector { field, k, query_vec } => {
    let mut vector_dists = Vec::new();
    for (doc_id, doc) in &index.docs {
        if let Some(doc_vec) = doc.vector_fields.get(field) {
            if !query_vec.is_empty() && query_vec.len() == doc_vec.len() {
                let sim = cosine_similarity(query_vec, doc_vec);
                vector_dists.push((doc_id.clone(), sim));
            }
        }
    }
    ...
}
```

Since `query_vec` is always the `Vec::new()` built at parse time, `!query_vec.is_empty()` is
always `false`, so `vector_dists` stays empty and a `*=>[KNN ...]` query always returns zero
vector matches (the base/lexical part of the query, if any, via the surrounding `And`, still
works normally). **Hybrid keyword+vector search via `FT.SEARCH`'s KNN syntax is parseable but
functionally a no-op today** — this is a precise, verified gap, not a design choice.

### 4.4 Vector similarity when it *is* invoked directly (`FT.ADD` + brute-force cosine)

```rust
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0; let mut norm_a = 0.0; let mut norm_b = 0.0;
    for (x, y) in a.iter().zip(b.iter()) { dot += x*y; norm_a += x*x; norm_b += y*y; }
    let denom = (norm_a * norm_b).sqrt();
    if denom == 0.0 { 0.0 } else { dot / denom }
}
```

Even if `query_vec` were populated, the search itself (§4.3) is a full `O(num_docs)` scan
computing cosine similarity against every document that has a vector in the queried field —
there is no ANN index (no HNSW, no quantization) inside `search.rs` itself.

### 4.5 Reciprocal Rank Fusion — real, and matches the standard formula

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

## 5. Cross-Component Interactions

- **`src/connection.rs`**: `Command::FtCreate/FtSearch/FtInfo/FtDropIndex/FtExplain/FtAdd`
  arms call `create_search_index`/`get_search_index`+`parse_query`+`execute_search`/
  `drop_search_index` directly; separately, `Hset`/`Hmset`/`JsonSet` (root path only) call
  `index_document_hook` after a successful write, and `Del`/similar paths call
  `delete_document_hook`.
- **`src/resp.rs`**: parses `FT.CREATE`'s `ON`/`PREFIX`/`SCHEMA` clauses and each field's
  `TEXT [WEIGHT w] [SORTABLE] [NOSTEM]` / `NUMERIC [SORTABLE]` / `TAG [SEPARATOR c]
  [CASESENSITIVE]` / `VECTOR ...` options into the real `IndexSchema`/`FieldType` values shown
  in §3, and parses `FT.SEARCH ... PARAMS n k v ...` into `SearchOptions.params` (§4.3).
- **`src/vector.rs`** (Component 08): **not actually used by this file** — `HnswIndex` and this
  file's brute-force per-document vector scan are two independent, unconnected
  implementations of vector similarity search in the codebase.
- **`src/json.rs`**: `JsonSet` on the root path (`$`) flattens the top-level JSON object's
  scalar fields into `HashMap<String, String>` before calling `index_document_hook` — nested
  objects/arrays are stringified via `.to_string()`, not recursively flattened into
  dotted-path fields.

---

## 6. Performance Characteristics

- **Global `RwLock` contention, not per-shard isolation** (§2.2): every indexed write and every
  `FT.SEARCH` call takes a real lock on the process-wide index (a write lock for indexing, a
  read lock for search) — under concurrent writers across many shards to prefixed keys, this is
  a real, shared contention point unlike the rest of the storage engine.
- **`And`/`Or`/`Not` evaluation re-runs `execute_search` recursively per sub-clause** with
  `limit: usize::MAX`, materializing a full intermediate `Vec<SearchHit>`/`HashMap` at every AST
  node rather than streaming or short-circuiting — fine for the small corpora this has been
  exercised against, not optimized for deep or wide boolean queries.
- **No compression**: posting lists are plain `Vec<Posting>` (doc_id `String` + `u32` term
  frequency + `Vec<u32>` positions per entry) — no delta-encoding, no compression, and the
  tracked term `positions` are never actually read by anything (no phrase-query support uses
  them).
