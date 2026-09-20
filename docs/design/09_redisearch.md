# Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion (Design)

> **Source Files**: `src/search.rs`


---

### 1. Architectural Purpose & Scope

`src/search.rs` provides an in-memory full-text search and indexing engine compatible with a
subset of RediSearch (`FT.CREATE`, `FT.SEARCH`, `FT.INFO`, `FT.DROPINDEX`, `FT.EXPLAIN`,
`FT.ADD`). It supports multi-field schema definitions (`TEXT`, `TAG`, `NUMERIC`, `VECTOR`), a
hand-written inverted-index posting-list structure, real Okapi BM25 relevance scoring, a small
RediSearch-like query-string parser (`parse_query`/`QueryAst`), and Reciprocal Rank Fusion for
merging two ranked result lists. Auto-indexing is wired into `HSET`/`HMSET`/`JSON.SET` (root
path only) in `src/connection.rs`.

---

### 2. Key Invariants & Concurrency Constraints

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

### 3. Performance Characteristics

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

---
