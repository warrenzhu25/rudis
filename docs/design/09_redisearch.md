# Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/search.rs`  
> **Implementation Reference**: [`docs/internal/09_redisearch.md`](../internal/09_redisearch.md)  
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
Traditional in-memory datastores encounter severe scalability barriers on modern multi-core, high-throughput cloud hardware. Single-threaded architectures (such as Redis) saturate a single CPU core while leaving the remaining 95%+ of server cores idle. Multi-threaded mutex architectures (such as Memcached) suffer from heavy spinlock contention, CPU cache line bouncing, and global memory allocator lock bottlenecks.

### 1.2 The Rudis Solution
Rudis implements the **Thread-Per-Core (Shared-Nothing)** architectural paradigm natively on Linux `io_uring` via Monoio. Each physical CPU core owns its own isolated event loop, its own thread-local memory database, and its own kernel `SO_REUSEPORT` listener. Operations on local keys execute in nanoseconds with zero locks, zero atomic operations, and zero cross-core cache invalidations.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model
Multi-field full-text and secondary index. Supports TEXT (Okapi BM25), TAG, and NUMERIC fields with a balanced RangeTree. FT.AGGREGATE executes multi-stage pipelines with reducers and arithmetic expressions.

### 2.2 Design Rationale (The "Why")
Standard search modules in Redis require dynamic C modules. Rudis natively integrates full-text search, sub-millisecond RangeTree numeric queries (O(log N + K)), and multi-shard scatter-gather aggregation.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
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

## 3. High-Level Architecture & Workflow Diagram

```
Query: FT.SEARCH idx "@price:[10 50] @brand:{apple}"
                               │
               ┌───────────────┴───────────────┐
               ▼                               ▼
       RangeTree: [10 50]              Tag Inverted Index
       O(log N + K) B-Tree             Bitmap Intersection
               │                               │
               └───────────────┬───────────────┘
                               ▼
               Okapi BM25 Ranking & Aggregation Pipeline
```

---

## 4. Performance Guarantees & Theoretical Complexity

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

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/09_redisearch.md`**](../internal/09_redisearch.md): Low-level implementation and code reference.
* **Source Files**: `src/search.rs`
