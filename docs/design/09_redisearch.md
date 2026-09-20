# Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/search.rs`
> **Implementation Reference**: [`docs/internal/09_redisearch.md`](../internal/09_redisearch.md)
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem

Full-text and secondary-index search over Redis-shaped data (hashes and JSON documents) traditionally requires a dynamic loadable module — RediSearch itself, in upstream Redis. That is an operational dependency Rudis's single-binary, thread-per-core design is built to avoid: every other data type (strings, JSON, geospatial, probabilistic structures) is native to the core binary, and search should be no exception.

### 1.2 The Rudis Solution

`src/search.rs` implements a native, in-process search engine supporting multi-field indexes over `TEXT` (Okapi BM25-ranked), `TAG` (exact-match set membership), `NUMERIC` (balanced-tree range queries), and `VECTOR` (brute-force cosine-similarity KNN) fields, addressable through the familiar `FT.CREATE`/`FT.SEARCH`/`FT.AGGREGATE` command family. Query results from lexical (BM25) and vector (KNN) retrieval can be combined via **Reciprocal Rank Fusion (RRF)**, giving hybrid keyword-plus-vector search without requiring a separate fusion step on the client.

### 1.3 Indexing Model — Per-Shard Partitioned, Not a Bolt-On Global Store

The current implementation stores each index **twice**, deliberately, for two different consumption paths:

- A **per-shard partition** (`ShardDb.search_indices: HashMap<String, InvertedIndex>`), owned by whichever shard's slot the indexed key hashes to, populated only with documents that shard actually processes write commands for. This is the mechanism `FT.SEARCH`/`FT.AGGREGATE` query execution actually uses: `Router::ft_search` queries the local shard's partition first, then performs a genuine cross-shard scatter/gather — sending the parsed query AST to every other shard via `ShardMessage::SearchQuery`, collecting each shard's partial result set, and merging/re-sorting the combined hits before paginating. This keeps indexing on the data path where the rest of Rudis's shared-nothing design puts it: each shard mutates only its own memory when a locally-owned key is written.
- A **process-wide mirror** (`SEARCH_INDICES: LazyLock<RwLock<HashMap<String, Arc<RwLock<InvertedIndex>>>>>`), which every shard also writes into on every indexed document (`index_document_hook`), giving it a complete, single-lock-protected copy of every document across all shards. This is what `FT.INFO` and other whole-index metadata queries (`get_search_index`) read from, and it is the fallback query path used only if a shard's own local partition is missing an index that the process-wide registry has (a defensive path, not the common case, since `FT.CREATE` initializes the index on every shard synchronously).

**Why maintain both?** The per-shard partition gives genuine parallelism for query *execution* (N shards search their own subset concurrently) with data-path locality matching the rest of the engine. The process-wide mirror gives O(1) access to whole-index statistics (`FT.INFO`'s document/term counts) without a cross-shard round trip, at the cost of doubling per-document indexing work and memory. This is a real, load-bearing trade-off, not an oversight — see §4 for the concurrency cost it imposes.

### 1.4 Why Reciprocal Rank Fusion, Not a Unified Scoring Function

Combining a BM25-ranked lexical result list and a cosine-similarity-ranked vector result list into one ordering is a well-known hard problem: the two scores live on entirely different, incomparable scales (BM25 scores are unbounded and corpus-dependent; cosine similarity is bounded in `[-1, 1]`), so naively summing or weighting them requires per-corpus calibration that a general-purpose engine cannot assume. RRF sidesteps the scale-comparability problem entirely by discarding the raw scores and fusing on **rank position** instead: a document's fused score is the sum, across every list it appears in, of `1 / (k + rank + 1)`. This is simple, parameter-light (a single constant `k`, controlling how much weight decays with rank), well-studied in the information-retrieval literature, and — critically — trivial to implement correctly and verify, in contrast to a learned or hand-tuned scoring blend.

### 1.5 Deliberate Trade-offs vs. a Full Inverted-Index Engine

Rudis's search engine intentionally omits several features found in mature inverted-index systems (Lucene-class engines, or RediSearch itself), in favor of a smaller, easier-to-verify implementation:

- **Whole-document BM25, not per-field BM25F.** `DocMeta` stores a single combined token count (`doc_len`) across every indexed text field of a document, and `InvertedIndex::avg_doc_len()` averages over the whole corpus, not per field. Genuine BM25F (per-field lengths and per-field weighted contributions) is a materially larger implementation and was not judged worth the complexity for the corpus sizes this engine targets — see the internal doc for the specific consequence this has for `FT.CREATE`'s `WEIGHT` option.
- **No positional/phrase indexing in query evaluation.** Term positions are not persisted or consulted during scoring, so exact-phrase queries are not distinguished from a bag-of-words AND of the same terms — again, a complexity/precision trade-off, not an accidental gap.
- **Brute-force vector search, not an ANN index.** KNN queries scan every document holding a vector in the queried field and compute exact cosine similarity, rather than using an approximate nearest-neighbor index. `src/vector.rs`'s HNSW implementation (Component 08) is a separate subsystem serving standalone vector-index commands; `search.rs` reuses only its low-level SIMD-accelerated distance kernel (`cosine_distance`), not its HNSW graph. Brute-force KNN gives exact results and a simple, easily-audited implementation at the cost of `O(indexed documents)` work per KNN query — appropriate while corpora are small to moderate, and a natural place to introduce ANN acceleration later without changing the query-language surface.
- **A lightweight heuristic stemmer and stop-word list, not a full Porter/Snowball implementation.** Tokenization applies a handful of suffix-stripping rules — good enough to materially improve recall on common English morphology, not a substitute for a linguistically complete stemmer.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model

A multi-field full-text and secondary index over hash or JSON documents. `TEXT` fields are BM25-ranked; `NUMERIC` fields are served by a balanced range tree for `O(log N + K)` range queries; `TAG` fields are exact-match set filters; `VECTOR` fields support brute-force KNN. `FT.AGGREGATE` executes a multi-stage pipeline (`GROUPBY`/`REDUCE`, `APPLY`, `SORTBY`, `LIMIT`, `FILTER`) with a small arithmetic-expression evaluator for computed fields.

### 2.2 Design Rationale (The "Why")

**Automatic ingestion, restricted to the write paths that make sense for it.** Every `HSET`, `HMSET`, and `JSON.SET key $ ...` call re-indexes the affected document against every index whose configured key prefix matches, immediately after the write succeeds — there is no separate, deferred indexing step or background reconciliation job, so a document is queryable as soon as the write that created or modified it returns. This is deliberately scoped to the write commands that produce a natural field/value document (`HSET` family, and the JSON root path); other write paths (`SET`, list/set operations, etc.) do not carry structured field data and are not auto-indexed.

**Deterministic tokenization keeps query and index-time processing identical.** Because the same `tokenize_text`/`simple_stem` pipeline runs both when a document is indexed and when a query term is parsed, there is never a mismatch between how a stored term and a query term are normalized — an important correctness property for any inverted-index system, achieved here without needing a shared analyzer configuration object.

**Dense integer document IDs, not the document's own string key, back every posting list.** `InvertedIndex` maps each document's string key to a dense `u32` (`DocId`) via `key_to_id`, reclaiming IDs from a free list on deletion (`free_ids`). Postings, the numeric range tree, and vector storage are all keyed by this dense integer rather than the document's string key. This keeps posting-list entries small and — because each `DocMeta` also records exactly which terms it contributed — makes document deletion an `O(terms in that document)` operation directed by the document's own term list, rather than a scan of the entire vocabulary.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)

1. **Automatic document ingestion, but only for `HSET`/`HMSET`/`JSON.SET` (root path).** `connection.rs` calls both the shard-local indexer (`ShardDb::index_document_local`) and the process-wide indexer (`crate::search::index_document_hook`) after a successful write, converting field values to `HashMap<String, String>` and re-indexing against every index whose prefix matches. Other write commands do not trigger re-indexing.
2. **Indexes are genuinely partitioned per shard for query execution, and additionally mirrored into one process-wide registry for metadata queries (§1.3).** Both are real, concurrently-written data structures: the per-shard map lives in shard-local, thread-confined memory (no lock needed within a shard); the process-wide registry is a real `std::sync::RwLock` taken by every shard on every indexed write (write lock) and every `FT.SEARCH`/`FT.INFO` call that falls back to it (read lock) — a genuine, deliberate exception to the shared-nothing/zero-lock architecture, in the same category as `BlockHub` (Component 06).
3. **Deterministic tokenization with a real (small) stop-word list and a heuristic stemmer.** `tokenize_text` splits on non-alphanumeric characters, lowercases, drops any token in a roughly 120-word `ENGLISH_STOP_WORDS` set, and optionally passes survivors through `simple_stem` — a handful of suffix-stripping rules, not a full Porter/Snowball stemmer.
4. **Real BM25, but with a single whole-document length, not one length per field** (§1.5). `FieldType::Text` accepts a per-field `WEIGHT` at `FT.CREATE` time, but the scoring function does not currently read it — see the internal doc for the exact call chain that confirms this.
5. **Cross-shard search results are merged and re-sorted after scatter/gather, then re-paginated once.** `Router::ft_search` requests `offset + limit` results from every shard (not just `limit`), so that a global top-K by score or sort-field remains correct after merging partial per-shard result sets; it does not stream or short-circuit — every reachable shard's partition is queried on every `FT.SEARCH` call.

---

## 3. High-Level Architecture & Workflow Diagram

```
Client: HSET doc:1 title "Rust Systems" body "Distributed io_uring" price 99.5
                               │
              (owning shard, per key_slot)
                               │
              ┌────────────────┴────────────────┐
              ▼                                  ▼
   ShardDb.search_indices (this      SEARCH_INDICES (global mirror,
   shard's partition only)            every shard's documents)
              │                                  │
              └──────────────┬───────────────────┘
                              ▼
                    tokenize_text + simple_stem
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
   Text terms            Tag sets            Numeric values
   → posting lists   → set-membership        → RangeTree
                          filter               (O(log N + K))

Query: FT.SEARCH idx "@price:[50 150] rust"
                               │
                    parse_query → QueryAst
                               │
              Router::ft_search: local shard partition
                    + scatter/gather to every other shard
                               │
                     merge, sort, paginate
                               │
                    Okapi BM25 ranking (lexical)
                               │
        optional: fuse with KNN vector-ranked list via RRF
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **Dual-write indexing overhead.** Every indexed write does the tokenization/posting-list work twice: once into the shard-local partition, once into the process-wide mirrored registry (§1.3). This roughly doubles indexing CPU and memory cost relative to a single-copy design, in exchange for lock-free, thread-per-core query execution on the per-shard copies and O(1) metadata queries on the mirror.
- **Global `RwLock` contention on the mirror registry, not on the per-shard query path.** The process-wide `SEARCH_INDICES` registry takes a real lock on every indexed write (across every shard) and on `FT.INFO`/registry-lookup calls — a genuine, shared contention point, unlike the rest of the storage engine. Per-shard `FT.SEARCH` execution against the local partition does not contend on this lock.
- **`And`/`Or`/`Not` query evaluation materializes an intermediate `HashMap<DocId, f64>` per AST node** rather than streaming or using sorted-posting-list intersection — adequate for the small-to-moderate corpora this engine currently targets, not optimized for deep or wide boolean queries over very large posting lists.
- **Numeric range queries are genuinely sub-linear.** `RangeTree` is a `BTreeMap<OrderedF64, Vec<DocId>>`, giving `O(log N + K)` range queries (`N` = distinct values, `K` = matches) rather than a linear scan of every document's numeric field — this specific claim is verified against the current implementation.
- **Vector KNN is exact but linear.** Every KNN query scans every document holding a vector in the queried field and computes an exact cosine similarity via a SIMD-accelerated kernel shared with Component 08 — `O(indexed documents with that field)` per query, not sub-linear. No ANN index (HNSW, quantization) is built or consulted inside `search.rs` itself.
- **No posting-list compression.** Posting lists are plain `Vec<Posting>` (`doc_id: u32` + `term_freq: u32` per entry, no positions) — no delta-encoding or compression is applied.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/09_redisearch.md`**](../internal/09_redisearch.md): Low-level implementation and code reference.
* **Source Files**: `src/search.rs`
