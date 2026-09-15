# Component 05: Storage Engine & Compact Encodings (`src/table.rs`)

## 1. Architectural Purpose & Scope

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

## 2. Key Invariants & Concurrency Constraints

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

## 3. Data Structures & Memory Layouts

### 3.1 The SIMD flat table (unchanged core mechanism)

```rust
pub struct RudisFlatTable {
    ctrl: Vec<u8>,
    pub slots: Vec<Option<RudisEntry>>,
    pub capacity: usize,
    mask: usize,
    items: usize,
    growth_left: usize,
    pub slot_counts: Box<[u32; 16384]>,
}
```

Lookup, insert, and delete still work exactly as originally designed: a 7-bit fingerprint
per slot in a separate `ctrl` byte array, 16-slot SIMD group loads (`_mm_cmpeq_epi8` /
`_mm_movemask_epi8`), triangular-step probing, tombstone (`DELETED`) deletion, and a
monolithic doubling `resize()` at 7/8 load factor. None of that has changed. What's new on
this struct is `slot_counts` — see §6.

### 3.2 `RudisEntry` and the real `RudisValue` enum

```rust
#[derive(Clone, Debug)]
pub struct RudisEntry {
    pub key: Bytes,
    pub val: RudisValue,
    pub expire_at: Option<Instant>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RudisValue {
    String(Bytes),
    Int(i64),
    SmallHash(Vec<(Bytes, Bytes)>),
    Hash(HashMap<Bytes, Bytes>),
    List(std::collections::VecDeque<Bytes>),
    Set(RudisSet),
    ZSet(RudisZSet),
    HyperLogLog(Box<[u8; 16384]>),
    Stream(RudisStream),
    Tiered(TieredPointer),
    Cooled {
        ptr: TieredPointer,
        val: Box<RudisValue>,
    },
}
```

There is no `Bitmap` variant — `SETBIT`/`GETBIT`/bitmap commands operate directly on the
bytes of a `RudisValue::String`. There is no `Json` variant in this enum either (JSON
values are handled by `src/json.rs` as a separate concern, outside `table.rs`'s scope).

### 3.3 `RudisTable` itself

```rust
pub struct RudisTable {
    table: RudisFlatTable,
    sample_cursor: usize,
    spill_cursor: usize,
    pub used_memory: usize,
}
```

Two cursors, not one: `sample_cursor` drives active TTL expiration (unchanged, §5.2) and
`spill_cursor` independently drives NVMe-tiering candidate sampling (new, §5.3).
`used_memory` is a running estimate of live value bytes, updated on every insert, mutation,
delete, and expiration — it did not exist in the original design and exists to give
`src/tiering.rs` a cheap signal for memory-pressure decisions without walking the table.

---

## 4. Compact Encodings Deep-Dive

There is **no Listpack, Intset, or pointer-based skiplist anywhere in this file.** Three of
the eleven `RudisValue` variants have a genuine small/full adaptive representation; the
rest are always one fixed Rust type.

### 4.1 Strings: `String(Bytes)` vs. `Int(i64)`

`set` parses every incoming value as a 64-bit integer first; if it parses cleanly, the
entry is stored as `RudisValue::Int`, not `String`:

```rust
let val = if let Some(int_val) = Self::parse_i64_bytes(&value) {
    RudisValue::Int(int_val)
} else {
    RudisValue::String(value)
};
```

`incr_by` then mutates an existing `Int` in place (`*n = nv`) with zero allocation, only
falling back to parsing bytes if the value is still a plain `String`. `parse_i64_bytes` and
`format_i64` are hand-written ASCII digit loops (no `format!`/`str::parse`) used everywhere
an integer needs to become bytes or vice versa.

### 4.2 Hashes: `SmallHash(Vec<(Bytes, Bytes)>)` vs. `Hash(HashMap<Bytes, Bytes>)`

```rust
let max_entries = crate::connection::HASH_MAX_ENTRIES.load(Ordering::Relaxed);
let max_value = crate::connection::HASH_MAX_VALUE.load(Ordering::Relaxed);
...
RudisValue::SmallHash(pairs) => {
    // ... linear-scan insert/update into `pairs` ...
    if pairs.len() > max_entries || pairs.iter().any(|(k, v)| k.len() > max_value || v.len() > max_value) {
        let map: HashMap<Bytes, Bytes> = pairs.drain(..).collect();
        entry.val = RudisValue::Hash(map);
    }
}
```

`SmallHash` is a plain `Vec` of pairs searched linearly on every read/write — not a
byte-packed buffer. The promotion thresholds are **runtime-configurable** process-wide
atomics (`HASH_MAX_ENTRIES`, `HASH_MAX_VALUE`, owned by `connection.rs`), conceptually
mirroring Redis's `hash-max-listpack-entries`/`-value` config even though the small form
here is a `Vec`, not a listpack.

### 4.3 Sets: `RudisSet::Small(Vec<Bytes>)` vs. `Full(HashSet<Bytes>)`

```rust
const SMALL_SET_LIMIT: usize = 64;

pub fn insert(&mut self, member: Bytes) -> bool {
    match self {
        RudisSet::Small(v) => {
            // linear contains-check, then push
            if v.len() > SMALL_SET_LIMIT {
                let mut set = hashbrown::HashSet::with_capacity(v.len());
                for m in v.drain(..) { set.insert(m); }
                *self = RudisSet::Full(set);
            }
            true
        }
        RudisSet::Full(s) => s.insert(member),
    }
}
```

Unlike `Hash`, the set threshold is a **hardcoded constant** (64), not configurable.
Deletion from the small form uses `swap_remove` (O(1), reorders the vec) rather than
shifting. A custom `RudisSetIter` enum unifies iteration over both representations behind
one type so callers (`SMEMBERS`, `SINTER`, etc.) don't need to match on the variant.

### 4.4 Sorted sets: `RudisZSet::Small(Vec<(OrderedScore, Bytes)>)` vs. `Full { dict, tree }`

```rust
const SMALL_ZSET_LIMIT: usize = 64;

pub enum RudisZSet {
    Small(Vec<(OrderedScore, Bytes)>),
    Full {
        dict: hashbrown::HashMap<Bytes, f64>,
        tree: std::collections::BTreeSet<(OrderedScore, Bytes)>,
    },
}
```

The small form is a **sorted** `Vec`: `insert` finds the insertion point with
`binary_search` (O(log n) to *find* the slot, but `Vec::insert` still shifts elements, so
insertion itself is O(n)). The full form pairs a `HashMap<Bytes, f64>` (O(1) score lookup)
with a `BTreeSet<(OrderedScore, Bytes)>` ordered by `(score, member)` for range queries.

**Correction worth being explicit about**: `rank()` is **not** O(log n) in either
representation. Even in the `Full` form it's a linear scan:

```rust
RudisZSet::Full { dict, tree } => {
    if !dict.contains_key(member) { return None; }
    if rev { tree.iter().rev().position(|(_, m)| m.as_ref() == member) }
    else    { tree.iter().position(|(_, m)| m.as_ref() == member) }
}
```

`BTreeSet` has no random-access rank operation in `std`, so `ZRANK`/`ZREVRANK` pay for an
O(n) walk regardless of representation. There is no augmented/spanned skiplist anywhere in
this file providing O(log n) rank.

### 4.5 Lists: always `VecDeque<Bytes>`

`RudisValue::List(std::collections::VecDeque<Bytes>)` — one representation regardless of
length. `LPUSH`/`RPUSH`/`LPOP`/`RPOP` are `VecDeque::push_front`/`push_back`/`pop_front`/
`pop_back`; there is no compact/large-list distinction.

### 4.6 Streams: `BTreeMap<StreamId, Vec<(Bytes, Bytes)>>`

```rust
pub struct RudisStream {
    pub entries: std::collections::BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    pub last_id: StreamId,
    pub groups: HashMap<Bytes, StreamGroup>,
}
```

`StreamId { ms: u64, seq: u64 }` derives `Ord`, so entries are naturally ordered by ID via
the `BTreeMap`, which is what makes `XRANGE`-style ID-range queries efficient without any
custom indexing. Each `StreamGroup` tracks its own `last_delivered_id`, a map of
`StreamConsumer`s, and a pending-entries-list (`pel: BTreeMap<StreamId, StreamPelEntry>`)
recording per-entry delivery time and count for `XACK`/`XCLAIM`/`XAUTOCLAIM`.

### 4.7 HyperLogLog: always a dense `Box<[u8; 16384]>`

`RudisValue::HyperLogLog(Box<[u8; 16384]>)` is a fixed 16,384-register dense array — the
same dense representation real Redis's HLL falls back to for large cardinalities, used
unconditionally here. There is no sparse encoding for small cardinalities.

---

## 5. Expiration & Memory Management

### 5.1 Passive expiration on read — now with a global bypass and stat counter

```rust
fn check_expired_slot(&mut self, slot_idx: usize) -> bool {
    if crate::connection::ALLOW_ACCESS_EXPIRED.load(Ordering::Relaxed) {
        return false;
    }
    let is_exp = /* compare Instant::now() against entry.expire_at */;
    if is_exp {
        if let Some(removed) = self.table.remove(slot_idx) {
            let freed = removed.key.len() + removed.val.approx_bytes() + 64;
            self.used_memory = self.used_memory.saturating_sub(freed);
            inc_expired_keys();
        }
        true
    } else {
        false
    }
}
```

Same core mechanism as before (check inline, evict on the spot, zero secondary lookups),
plus two additions: `ALLOW_ACCESS_EXPIRED` is a process-wide atomic that, when set, disables
expiration checks everywhere in the table (used for debug/inspection paths that need to see
logically-expired keys); and every real eviction now updates `used_memory` and bumps a
process-wide `EXPIRED_KEYS` atomic counter (`inc_expired_keys`/`get_expired_keys`) presumably
surfaced through `INFO`.

### 5.2 Active expiration sampling — unchanged

```rust
pub fn active_expire_cycle(&mut self) -> usize {
    let cap = self.table.capacity();
    if cap == 0 || self.table.len() == 0 { return 0; }
    let mut expired_count = 0;
    let mut checked = 0;
    while checked < 20 {
        let idx = self.sample_cursor % cap;
        self.sample_cursor = (self.sample_cursor + 1) % cap;
        if self.check_expired_slot(idx) { expired_count += 1; }
        checked += 1;
    }
    expired_count
}
```

Identical in shape to the original design: bounded 20-slots-per-call sampling via a
persistent cursor, called periodically from the server's event loop.

### 5.3 NVMe tiering hooks (new since the original design)

`table.rs` doesn't talk to disk itself, but it exposes exactly the primitives
`src/tiering.rs` needs to move values in and out of RAM:

```rust
pub fn get_hot_keys_for_spill(&mut self, limit: usize) -> Vec<Bytes> {
    // round-robins spill_cursor across all slots, collecting keys whose
    // value is NOT already RudisValue::Tiered/Cooled, up to `limit`
}

pub fn decommit_all_cooled(&mut self) -> (usize, u64) {
    // for every RudisValue::Cooled{ptr, val}, drops `val` and replaces it
    // with RudisValue::Tiered(ptr) — freeing the RAM copy but keeping the
    // on-disk pointer, updating used_memory as it goes
}
```

Reading the `RudisValue` enum (§3.2) again in this light: `Tiered(TieredPointer)` means
"this value lives only on disk, referenced by `{file_id, offset, length, value_type}`";
`Cooled { ptr, val }` means "this value has been written to disk *and* is still cached in
RAM" — a transitional state that `get`/`get_entry` transparently unwrap so reads never need
to know which state a value is in:

```rust
let val_ref = match &entry.val {
    RudisValue::Cooled { val, .. } => val.as_ref(),
    other => other,
};
```

`spill_cursor` (separate from expiration's `sample_cursor`) tracks where the next spill scan
should resume, so repeated spill passes sweep the whole table rather than re-scanning from
the start every time.

---

## 6. Cluster Slot Indexing (architecture change from the original design)

The original design maintained a full reverse index, `slot_to_keys: HashMap<u16,
HashSet<Bytes>>`, so `CLUSTER COUNTKEYSINSLOT`/`GETKEYSINSLOT` could answer in O(keys in
that slot). **That reverse index is gone.** It's been replaced with a fixed-size count-only
array living inside `RudisFlatTable` itself:

```rust
pub slot_counts: Box<[u32; 16384]>,
```

incremented/decremented directly in `RudisFlatTable::insert`/`remove`/`resize`/`clear`.
This makes `count_keys_in_slot` O(1) **only in the common case that the slot is empty**:

```rust
pub fn count_keys_in_slot(&mut self, slot: u16) -> usize {
    if self.table.slot_counts[slot as usize] == 0 {
        return 0;
    }
    // otherwise: linear scan every slot in the table, filtering by
    // crate::router::key_slot(&entry.key) == slot
    ...
}
```

For a **non-empty** slot, both `count_keys_in_slot` and `get_keys_in_slot` now fall back to
an O(table capacity) linear scan over every slot in the table, checking each live entry's
computed cluster slot. This is a real regression versus the original reverse-index design
for any workload that calls these commands against a populated cluster slot — traded, most
likely, for removing the double-bookkeeping cost the old `slot_to_keys` paid on every single
insert/delete (cloning each key into a second `HashSet`). Both scanning functions still do
opportunistic lazy expiration of any expired keys they encounter along the way, same as
before.

---

## 7. Performance Characteristics

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
