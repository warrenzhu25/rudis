# Component 16: JSON Document Store & JSONPath Engine (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/json.rs`  
> **Implementation Reference**: [`docs/internal/16_json_store.md`](../internal/16_json_store.md)  
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
Native RFC 8259 document store. Supports recursive JSONPath selectors ($..*, [*], array slices) and in-place atomic mutations without full document deserialization.

### 2.2 Design Rationale (The "Why")
External JSON modules in Redis require dynamic C loading. Rudis natively supports JSON.SET, JSON.GET, and sub-path mutations with zero proxy latency.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **A real, but partial, JSONPath implementation.** `parse_json_path` hand-parses `$`, bare
   `.field` traversal, `[idx]` (including negative indices), `[*]` wildcards, `[start:end]`
   slices (including negative/omitted bounds), and `["quoted"]`/`['quoted']` field names. There
   is **no recursive descent (`$..field`) and no filter-expression syntax (`?(@.price < 10)`)**
   — both real RedisJSON/JSONPath features. A path using either silently fails to match
   anything (parses as a literal field name containing those characters) rather than erroring.
2. **Whole-document storage, no incremental structure.** `JsonStore.docs: HashMap<Bytes,
   Value>` stores one complete `serde_json::Value` tree per key. A `JSON.SET`/`NUMINCRBY`/etc.
   on a deeply nested path still has to parse the target's own sub-value in place (via
   `query_json_path_mut`, no full-document re-parse), but `JSON.GET` always calls
   `serde_json::to_string` fresh on whatever subtree matched — there's no cached serialized
   form, and a `JSON.GET key $` on a huge document re-serializes the entire thing every call.
3. **`query_json_path`/`query_json_path_mut` are structurally identical, hand-duplicated for
   `&`/`&mut`.** Every match arm in the immutable traversal (§4.2) has a corresponding
   `_mut` arm doing the identical navigation logic against `.get`/`.get_mut`,
   `.values()`/`.values_mut()`, `&arr[i]`/`&mut arr[i]`. This is a real, verified
   duplication (not a design choice with a stated rationale) — a bugfix to one traversal
   rule (e.g. how negative slice bounds clamp) has to be applied to both copies by hand.
4. **Auto-vivification on `SET`, not on read.** `set_json_path` creates intermediate
   `Object`/`Array` containers as needed when writing to a path whose parents don't exist yet
   (§4.3) — real Redis JSON has the same behavior. `NX`/`XX` are checked once, up front,
   against whether the *target* path already resolves to something, before any mutation.

---

## 3. High-Level Architecture & Workflow Diagram

```
Client ──► JSON.NUMINCRBY user:1 $.stats.views 1 ──► In-Place Mutation in ShardDb
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **`JSON.GET` cost scales with matched-subtree size, not query specificity** — every call
  does a fresh `serde_json::to_string` of whatever `query_json_path` returned, with no
  memoization; repeatedly reading the same small field from a large sibling-heavy document is
  cheap, but repeatedly reading `$` on a large document is not.
- **Path traversal is O(document breadth) per segment, not indexed** — `Field` lookups on an
  `Object` are O(1) (backed by `serde_json`'s own map), but `Wildcard`/`Slice` segments
  necessarily visit every child at that level; there's no precomputed path index.
- **`JSON.MGET`'s sequential fan-out (§4.5) is the single biggest addressable cost** on
  multi-key JSON reads spread across shards — see Future Improvements.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/16_json_store.md`**](../internal/16_json_store.md): Low-level implementation and code reference.
* **Source Files**: `src/json.rs`
