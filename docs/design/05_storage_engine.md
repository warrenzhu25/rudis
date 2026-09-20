# Component 05: Storage Engine & Compact Encodings (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/table.rs`
> **Implementation Reference**: [`docs/internal/05_storage_engine.md`](../internal/05_storage_engine.md)
> **Bucket-Layout Deep-Dive**: [`docs/design/rudis_table.md`](rudis_table.md)
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem

Traditional in-memory datastores hit scalability limits on modern multi-core hardware in one of two
directions. Single-threaded architectures (Redis) saturate one CPU core while leaving the remaining cores
idle. Multi-threaded, lock-based architectures (Memcached, or a naive `Mutex<HashMap<..>>` design) avoid
that specific limit but pay for it with spinlock contention, cross-core cache-line bouncing on shared
buckets, and global-allocator lock contention under concurrent inserts.

A second, independent problem is specific to how key-value stores usually represent a key's metadata:
Redis's `dict.c` keeps values in one hash table and expirations in a separate one (`expires`), so a read
that must also check TTL, or a write that must also check a cluster slot, ends up performing several
independent hash lookups against logically separate structures per operation.

### 1.2 The Rudis Solution

Rudis implements the **thread-per-core (shared-nothing)** architectural paradigm on Linux `io_uring` via
Monoio (see [`docs/design/01_reactor_runtime.md`](01_reactor_runtime.md) and
[`docs/design/04_sharding_mesh.md`](04_sharding_mesh.md)). Each physical CPU core owns its own event loop,
its own thread-local storage engine instance, and its own kernel `SO_REUSEPORT` listener. Because no other
thread ever touches a given shard's data, operations on local keys require zero locks, zero atomic
read-modify-write operations, and zero cross-core cache invalidation for the table's own state.

Within each shard, `RudisTable` (`src/table.rs`) addresses the second problem by inlining a key's value
*and* its expiration timestamp into a single slot of a custom open-addressed hash table, so a `GET` that
must also honor TTL resolves within one probe sequence instead of two lookups against separate structures.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model

`RudisTable` is a custom, thread-local, open-addressed hash table in the SwissTable/`hashbrown` tradition:
a flat array of 1-byte fingerprints probed 16 at a time with SIMD comparisons, running parallel to a flat
array of slots. Each occupied slot holds one `RudisEntry` — key, value, and optional expiration — as a
single 88-byte, 8-byte-aligned unit. There is no separate expirations table, no separate cluster-slot
reverse index keyed by key, and (for `Hash`, `Set`, and `ZSet` values) a small/full adaptive representation
that keeps common small collections cache-friendly without paying a `HashMap`'s per-entry overhead until a
collection actually grows large enough to need it.

### 2.2 Design Rationale (The "Why")

**Why not `hashbrown::HashMap` directly?** `hashbrown::HashMap` already gives SIMD group probing and
strong single-thread throughput — it was Rudis's original table implementation. What it does not give is a
place to inline a per-value TTL or cluster-slot membership: those require either wrapping every value in a
struct (paying an extra hash lookup's worth of indirection anyway, since `HashMap<K, V>` still boxes `V`
separately from its internal control bytes in the general case) or maintaining companion tables that a hot
path must keep in sync and query independently. `RudisTable` folds those fields directly into the same
slot the SIMD probe already lands on, at the cost of owning and maintaining the open-addressing logic
itself rather than delegating to a general-purpose library.

**Why fingerprint-and-probe over chaining?** Redis's `dict.c` uses chained buckets (`dictEntry` linked
lists), which cost a pointer dereference — and likely a cache miss — per hop down a chain. Open addressing
with an 8-bit-per-slot fingerprint array lets a lookup rule out up to 16 candidate slots per iteration with
one vector compare, touching the (much larger) entry payload only for the handful of slots whose
fingerprint actually matches. This trades chaining's simplicity and slow, unbounded-length probes for a
bounded, vectorizable probe sequence at the cost of needing tombstones (rather than simple unlinking) to
handle deletion in an open-addressed scheme — see the tombstone-triggered resize discussed in the
implementation reference.

**Why triangular group-stepping instead of linear probing?** Plain linear probing (checking consecutive
groups) is known to cause *primary clustering*: once a run of nearby slots fills up, every subsequent
insertion that hashes nearby extends the same run, making the table pathologically slow to probe under
skewed load. Growing the step size by one `GROUP_SIZE` on every iteration (the same triangular-number
probing sequence `hashbrown` itself uses) scatters successive probe groups further apart as a chain grows
longer, keeping average probe length low without needing a second, independent hash function the way
double hashing would.

**Why box the collection-carrying `RudisValue` variants?** An unboxed enum's size is the size of its
largest variant. A raw `HashMap`, `VecDeque`, or similar collection header carries enough internal state
(and, depending on the collection, more than a pointer's worth) that leaving `Hash`/`Set`/`ZSet`/`Stream`
unboxed would inflate *every* `RudisEntry` — including every `String` and `Int` entry, which are typically
the majority of keys in a real workload. Boxing those four variants caps `RudisValue` at 40 bytes
regardless of how large the underlying collection grows, keeping `Vec<Option<RudisEntry>>` itself dense
even on workloads dominated by scalar values.

**Why adaptive small/full representations for `Hash`/`Set`/`ZSet` but not `List`/`Stream`?** A `Vec`
searched linearly is faster than a `HashMap`/`BTreeMap` lookup for genuinely small collections — no
hashing, no pointer chasing, excellent cache locality — and only loses once linear scan cost exceeds the
fixed overhead of the indexed structure. `List` values are already `VecDeque`, which is the fast structure
Rudis would promote *to* for a hash/set/zset; there is no faster "small" form to promote from. `Stream`
values are ordered by `StreamId` and queried by ID range (`XRANGE`), a pattern `BTreeMap` already serves
well at any size, so there is no analogous small/full split to make there either.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)

1. **Thread isolation.** Each `RudisTable` belongs to exactly one shard thread. It contains no mutexes, no
   read-write locks, and no atomic operations protecting its own data. (Two process-wide `AtomicU64`
   counters — `EXPIRED_KEYS`/`EVICTED_KEYS` — exist for `INFO` statistics; they are monotonic counters, not
   synchronization primitives for the table itself.)
2. **Inlined metadata and expiration.** `expire_at: Option<Instant>` lives directly inside `RudisEntry`.
   Checking key validity never requires a secondary table lookup, and a table with zero TTL'd keys can skip
   expiration checking with a single integer comparison against an internal live count, with no clock read
   at all — see the implementation reference for the exact mechanism.
3. **Adaptive small/full promotion — but not for every type.** `Hash`, `Set`, and `ZSet` values each start
   in a compact linear form and promote to an indexed form once they cross a size threshold. `Hash`'s
   thresholds are runtime-configurable process-wide atomics (mirroring Redis's
   `hash-max-listpack-entries`/`-value` configuration surface); `Set`'s and `ZSet`'s thresholds are
   hardcoded constants. `List` and `Stream` have no compact form at all — they use one representation
   regardless of size, for the reasons in §2.2.
4. **Passive inline eviction, with an escape hatch.** Any operation encountering an expired key immediately
   frees it and returns `None`/empty, unless a process-wide debug flag is set, in which case expiration
   checks are skipped entirely so inspection tooling can still see logically-expired keys.
5. **Cold-storage awareness.** A value can be partially or fully moved to NVMe by `src/tiering.rs`
   (see [`docs/design/07_nvme_tiering.md`](07_nvme_tiering.md)). `table.rs` performs no disk I/O itself,
   but its value representation includes explicit "this value lives on disk" and "this value lives on disk
   *and* is still cached in RAM" states, so readers never need tiering-aware branching logic of their own —
   the table unwraps the cached state transparently.
6. **Allocation reuse for small collections.** Rather than returning every `Vec`/`VecDeque` behind a
   deleted or overwritten small `List`/`Hash`/`Set`/`ZSet` to the global allocator, the table recycles them
   through a thread-local free-list pool, sized to the same small-collection footprint those values already
   have. This exists because high-QPS workloads that repeatedly `LPUSH`/`LPOP`, `HSET`/`HDEL`, `SADD`/
   `SREM`, or `ZADD`/`ZREM` the same small collection shape would otherwise churn the allocator on every
   operation; recycling turns that into a pool pop/push instead.

---

## 3. High-Level Architecture & Memory Layout

```
RudisEntry (88 bytes total, 8-byte aligned)
  ┌─────────────────────────┬─────────────────────────┬─────────────────────────┐
  │      key: Bytes         │   val: RudisValue        │  expire_at: Option<Inst>│
  │       (32 bytes)        │       (40 bytes)         │       (16 bytes)        │
  └─────────────────────────┴─────────────────────────┴─────────────────────────┘
```

`RudisValue` itself is held to 40 bytes by boxing every collection-carrying variant (`Hash`, `Set`, `ZSet`,
`Stream`); the unboxed variants (`String`, `Int`, `SmallHash`, `List`, `HyperLogLog`, `Tiered`, `Cooled`)
are each small enough to fit that budget without indirection. 88 bytes is intentionally larger than a
single 64-byte L1 cache line, and the entry array carries no cache-line alignment guarantee — the
performance win comes from the *fingerprint* array being scanned 16-at-a-time with SIMD before any entry
payload is touched at all, not from every entry individually fitting one cache line. See
[`docs/design/rudis_table.md`](rudis_table.md) for the full bucket-layout rationale, including where an
earlier design draft's "64-byte aligned bucket" framing diverges from what is actually implemented.

---

## 4. Performance Guarantees & Theoretical Complexity

- **SIMD group probing**: one 128-bit vector load and compare per 16-slot probe group on `x86_64`, with
  triangular-step probing between groups to avoid primary clustering (§2.2). Non-`x86_64` targets fall back
  to a portable scalar comparison loop over the same 16-byte groups — there is currently no vectorized path
  for ARM/NEON; see the implementation reference's Future Improvements for this gap.
- **Zero-allocation `INCR`/`DECR`** when the value is already `Int`-encoded — mutates the `i64` in place
  instead of round-tripping through string formatting.
- **Hand-written integer/byte conversions** (`parse_i64_bytes`, `format_i64`) avoid `std::str`/`format!`
  overhead on the hottest string-command paths, including static-byte fast paths for the most common small
  integers.
- **Amortized O(1) average-case lookup/insert/delete**, as for any open-addressed table at a bounded
  maximum load factor (7/8 here) with a well-distributed hash function; worst case remains O(n) under
  pathological hash collisions, which FxHash does not defend against (see §2.2's rationale on why that
  trade-off is acceptable for a thread-local table with no untrusted concurrent input).
- **Resize is monolithic, not incremental.** `RudisFlatTable::resize` rehashes the entire table into a
  fresh allocation whenever its load factor threshold is crossed — the segmented/incremental resize
  described as the target design in [`docs/design/rudis_table.md`](rudis_table.md) §2 Pillar 3 was never
  built. One partial mitigation exists: if a table's population has fallen well below its capacity through
  deletions (i.e. the table is dominated by tombstones rather than live entries), the triggering resize
  rehashes at the *same* capacity purely to clear tombstones, rather than growing memory unnecessarily —
  but it is still a synchronous, whole-table rehash either way. This remains the highest-priority structural
  gap versus the original design intent if p99.9 write latency at large key counts ever becomes a measured
  problem; it has not been benchmarked as part of this documentation pass, and no specific latency numbers
  are claimed here.
- **Small-form promotions trade O(n) linear scans for cache-friendly `Vec` access** below their thresholds
  — genuinely present for `Hash`/`Set`/`ZSet`, applied inconsistently in one respect: `Hash`'s threshold is
  live-configurable via process-wide atomics, while `Set`'s and `ZSet`'s are compile-time constants.
- **`ZRANK`/`ZREVRANK` are O(n), not O(log n), in both the small and full `ZSet` representations.** The full
  representation's ordered index is a `BTreeSet`, which has no built-in random-access rank operation in
  Rust's standard library, so both representations resolve rank via a linear scan today. There is no
  augmented/spanned skiplist anywhere in this subsystem providing sub-linear rank.
- **`used_memory` is an estimate, not exact accounting.** It is derived from a fixed per-variant heuristic
  (e.g. a flat `+16`/`+32` bytes of assumed overhead per hash entry, or a flat `count * constant` for
  `Set`/`ZSet` that does not scale with actual member size) plus a flat `+64` bytes of assumed overhead per
  entry — not a precise allocator measurement. It exists to give `src/tiering.rs` a cheap, always-available
  signal for memory-pressure decisions without walking the table.
- **Maxmemory `*-lru` eviction policies currently behave like `*-random`.** No access-recency signal is
  tracked anywhere in `RudisEntry` or the flat table, so `allkeys-lru`/`volatile-lru` sampling picks the
  first occupied slot it finds rather than a genuinely least-recently-used one. `*-ttl` policies do get a
  real nearest-expiry comparison across the sample.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step algorithm walkthroughs, and
code-level technical debt:

* [**`docs/internal/05_storage_engine.md`**](../internal/05_storage_engine.md): low-level implementation
  and code reference (struct/enum field listings, probing/resize algorithms, expiration and tiering
  mechanics, the small-collection arena, and the full prioritized list of open work).
* [**`docs/design/rudis_table.md`**](rudis_table.md): the bucket-layout design deep-dive — what the
  original SwissTable/DASH-inspired design intended, corrected against what is actually implemented today.
* **Source File**: `src/table.rs`
