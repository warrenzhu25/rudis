# Component 05: Storage Engine & Compact Encodings (Design)

> **Source Files**: `src/table.rs`


---

### 1. Architectural Purpose & Scope

`src/table.rs` is Rudis's core in-memory associative storage engine. It provides the
dictionary implementation (`RudisTable`), the definitions and per-command logic for every
`RudisValue` data type (strings, hashes, lists, sets, sorted sets, streams, bitmaps-as-strings,
HyperLogLog), active/passive key expiration, and the bookkeeping hooks that let
`src/tiering.rs` move cold values out to NVMe storage and back.

The dictionary itself is still the custom SIMD flat hash table (`RudisFlatTable`) originally
designed for this project — it has **not** been replaced by `hashbrown` or any Listpack/
Intset/skiplist-based structure. What has grown substantially since the original design is
everything built on top of it: `RudisValue` now has 11 variants instead of 2, several of
which have their own adaptive small/full representations, and `RudisTable` now tracks live
memory usage and NVMe-tiering state per key.

---

### 2. Key Invariants & Concurrency Constraints

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

### 3. Performance Characteristics

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
