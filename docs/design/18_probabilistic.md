# Component 18: Probabilistic Data Structures (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/probabilistic.rs`  
> **Implementation Reference**: [`docs/internal/18_probabilistic.md`](../internal/18_probabilistic.md)  
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
Constant-memory probabilistic data structures: Bloom Filters (membership testing), Cuckoo Filters (membership with deletion), Count-Min Sketch (frequency estimation), and Top-K (Space-Saving heavy hitters).

### 2.2 Design Rationale (The "Why")
Tracking unique users or heavy hitters over billions of events in exact hash sets exhausts gigabytes of memory. Probabilistic structures provide bounded-error answers in kilobytes of RAM.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **A single custom hash function underlies all four structures.** `fnv1a_hash` (a
   seeded 64-bit FNV-1a) and `double_hash` (two independent FNV-1a calls with different fixed
   seeds, used for Kirsch-Mitzenmacher double-hashing) are shared by the Bloom filter, Cuckoo
   filter, and Count-Min Sketch — there is no per-structure hash family, and no cryptographic
   hash anywhere in this file (not a concern for these structures' intended use, unlike an
   auth-adjacent context).
2. **Bloom filter sizing follows the standard formulas, computed once at creation.**
   `BloomFilter::new(capacity, error_rate)` derives bit-array size via
   $m = \lceil -n \ln(p) / (\ln 2)^2 \rceil$ and hash count via $k = \text{round}((m/n)\ln 2)$,
   clamped to `[1, 30]` hashes — real, textbook Bloom filter parameter derivation, not
   hardcoded constants.
3. **The Cuckoo filter is a real, complete implementation including eviction ("cuckoo
   kicks").** `add` tries both candidate buckets first, and only falls back to the
   randomized-kick relocation loop (`MAX_KICKS = 500`) if both are full — a genuine cuckoo
   hashing insert, not a simplified always-fails-when-full variant. `delete` is also real
   (removes a matching fingerprint from either candidate bucket), which is one of the
   Cuckoo filter's actual advantages over a Bloom filter (Bloom filters can't support
   deletion at all without a counting variant, which isn't implemented here).
4. **The Top-K tracker is a real Space-Saving algorithm, not an exact top-K.** Once at
   capacity, `TopK::add` evicts the *minimum-count* tracked item and gives the new item that
   evicted item's count plus the increment — the standard Space-Saving guarantee (every
   tracked count is an overestimate, bounded by the true frequency of whatever was evicted
   last), not an exact frequency count.
5. **No structure ever shrinks or is auto-resized.** A Bloom/Cuckoo filter's bit array or
   bucket count is fixed at creation time (`BF.RESERVE`/`CF.RESERVE`'s capacity argument); a
   Count-Min Sketch's width/depth are likewise fixed at `CMS.INITBYDIM`/`INITBYPROB` time.
   There is no `BF.INSERT ... EXPANSION` auto-scaling behavior — once a filter created with a
   given capacity is over-inserted, its false-positive rate silently degrades rather than the
   structure growing.
6. **All four structures survive a restart.** `ShardDb::save_extended_rdb_chunk` serializes
   every `Bloom`/`Cuckoo`/`CMS`/`TopK` entry in `ProbabilisticStore` into the RDB chunk stream
   alongside ordinary keys (internal document §4), and the matching load path reconstructs each
   structure verbatim (bit array, bucket table, sketch table, or item map) on startup — a
   Bloom/Cuckoo/CMS/Top-K key behaves like any other durable key with respect to `SAVE`/RDB
   load, not like an ephemeral, restart-losing cache.

---

## 3. High-Level Architecture & Workflow Diagram

```
Item ──► Seeded 64-bit FNV-1a (h1, h2) ──► Kirsch-Mitzenmacher derived indices
                                             (h1 + i·h2 mod m) ──► Bloom / Cuckoo / CMS / Top-K
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **Bloom/Cuckoo `add`/`contains` are O(num_hashes) / O(1)** respectively — a Bloom filter
  check costs up to 30 bit-array probes (bounded, per §2.2's clamp), a Cuckoo filter check is
  two fixed-size (4-slot) bucket scans regardless of fill level.
- **Cuckoo insertion degrades under high load factor** — the `MAX_KICKS = 500` eviction chain
  only triggers once both candidate buckets are full, and a filter approaching its rated
  capacity will trigger it increasingly often before either succeeding or returning
  `"ERR Cuckoo filter is full"` — a real, expected cuckoo-hashing characteristic, not a bug.
- **Count-Min Sketch `incr_by`/`query` are O(depth)**, independent of how many distinct items
  have been tracked — the whole point of a fixed-size sketch over an exact per-item counter map.
- **Top-K's eviction is O(k) per new distinct item once at capacity** (§4.4) — negligible at
  small `k`, would matter if `TOPK.RESERVE` were ever used with a very large `k`.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/18_probabilistic.md`**](../internal/18_probabilistic.md): Low-level implementation and code reference.
* **Source Files**: `src/probabilistic.rs`
