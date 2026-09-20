# Component 05: Storage Engine & Compact Encodings (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/table.rs`  
> **Implementation Reference**: [`docs/internal/05_storage_engine.md`](../internal/05_storage_engine.md)  
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
RudisTable is a custom in-memory hash table designed for 64-byte CPU cache lines with 1-byte SIMD group probing. Each entry contains the key, value, and optional TTL in a single cache-conscious 88-byte RudisEntry.

### 2.2 Design Rationale (The "Why")
Standard hash tables (dict.c or hashbrown) suffer from pointer chasing and decoupled TTL tables requiring multiple lookups. RudisTable packs key, value, and expiration into an aligned 88-byte slot with SIMD probe acceleration.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **Thread-Isolation**: Each `RudisTable` belongs to a single shard thread. It contains
   **no mutexes, atomic operations for its own data, or lock-free concurrency wrappers**
   (the two global counters described in §5 are process-wide atomics, but they're simple
   monotonic stats, not synchronization for the table itself).
2. **Inlined Metadata & Expiration**: `expire_at: Option<Instant>` lives directly inside
   `RudisEntry`. Checking key validity never requires a secondary hash lookup.
3. **Adaptive Small/Full Promotion — but not for every type**: `Hash`, `Set`, and `ZSet`
   values each start in a compact linear form and promote to an indexed form once they
   cross a size threshold (§4). Hash's thresholds are runtime-configurable via global
   atomics; Set's and ZSet's are hardcoded constants. `List` and `Stream` have no compact
   form at all — they use the same representation regardless of size.
4. **Passive Inline Eviction, with an escape hatch**: any operation encountering an expired
   key immediately frees it and returns `None`/empty, unless the process-wide
   `crate::connection::ALLOW_ACCESS_EXPIRED` atomic flag is set, in which case expiration
   checks are skipped entirely (see §5.1).
5. **Cold-storage awareness**: a value can be partially or fully moved to NVMe by
   `src/tiering.rs`. `table.rs` doesn't perform that I/O itself, but `RudisValue::Tiered`
   and `RudisValue::Cooled` exist specifically so the table can represent "this key's value
   lives on disk" or "this key's value lives on disk *and* is still cached in RAM" (§5.3).

---

## 3. High-Level Architecture & Workflow Diagram

```
RudisEntry (88 Bytes Total)
  ┌─────────────────────────┬─────────────────────────┬─────────────────────────┐
  │      key: Bytes         │   val: RudisValue       │  expire_at: Option<Inst>│
  │       (24 Bytes)        │       (40 Bytes)        │       (24 Bytes)        │
  └─────────────────────────┴─────────────────────────┴─────────────────────────┘
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **SIMD group probing is unchanged**: still one 128-bit load and compare per 16-slot group,
  triangular-step probing to avoid primary clustering.
- **Zero-allocation `INCR`/`DECR`** when the value is already `Int`-encoded — mutates the
  `i64` in place instead of round-tripping through string formatting.
- **Hand-written integer/byte conversions** (`parse_i64_bytes`, `format_i64`) avoid
  `std::str`/`format!` overhead on the hottest string-command paths.
- **Resize is still monolithic**: `RudisFlatTable::resize` still rehashes the entire table
  into a fresh allocation at 7/8 load factor — the segmented/incremental resize from the
  original design's roadmap was never built.
- **Small-form promotions trade O(n) linear scans for cache-friendly `Vec` access** below
  their thresholds — real for `Hash`/`Set`/`ZSet`, applied inconsistently (Hash's threshold
  is live-configurable; Set's and ZSet's are compile-time constants).
- **`used_memory` is an estimate, not exact accounting** — it's derived from
  `RudisValue::approx_bytes()` (a fixed per-variant heuristic, e.g. `+16`/`+32` bytes of
  assumed overhead per element) plus a flat `+64` bytes per entry, not a precise allocator
  measurement.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/05_storage_engine.md`**](../internal/05_storage_engine.md): Low-level implementation and code reference.
* **Source Files**: `src/table.rs`
