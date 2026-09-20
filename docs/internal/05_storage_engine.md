# Component 05: Storage Engine & Compact Encodings (Implementation Deep-Dive & Code Reference)

> **Source File**: `src/table.rs` (11,712 lines)
> **High-Level Design Spec**: [`docs/design/05_storage_engine.md`](../design/05_storage_engine.md)
> **Bucket-Layout Design Deep-Dive**: [`docs/design/rudis_table.md`](../design/rudis_table.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

All struct/enum definitions, constants, and line numbers below were checked against `src/table.rs` as it
exists today. Where an earlier revision of this document described something that no longer matches the
source (a stale field, a changed enum variant, a missing fast path), it has been corrected and the
correction is called out explicitly rather than silently overwritten, so regressions are easier to spot in
review.

---

## 1. Source Module Map & Responsibilities

| File | Role |
| :--- | :--- |
| `src/table.rs` | The entire thread-local storage engine: the open-addressed SIMD hash table (`RudisFlatTable`), the per-shard façade over it (`RudisTable`), every `RudisValue` compact/full encoding pair, expiration bookkeeping, NVMe-tiering hooks, cluster-slot counting, and the small-collection allocation arena. |

`table.rs` has no submodules; everything below is one flat file, organized here by concern rather than by
source order.

---

## 2. Structural Overview

```
RudisTable                              (src/table.rs:1645)
├── table: RudisFlatTable                (src/table.rs:1238)
│   ├── ctrl: Vec<u8>                     control-byte array, len = capacity + GROUP_SIZE
│   ├── slots: Vec<Option<RudisEntry>>    parallel slot array, len = capacity
│   ├── capacity, mask, items, growth_left
│   └── slot_counts: Box<[u32; 16384]>    per-cluster-slot live-key counters
├── sample_cursor: usize                  active-expiration round-robin cursor
├── spill_cursor: usize                   NVMe-tiering spill-candidate round-robin cursor
├── used_memory: usize                    running estimate of live value bytes
├── arena: SmallCollectionArena           recycled Vec/VecDeque pool for small collections
└── num_expires: usize                    count of live entries currently carrying a TTL
```

`RudisTable` is the type embedded directly in `ShardDb` (`pub table: crate::table::RudisTable`,
`src/shard.rs:578`) — one instance per shard, owned exclusively by that shard's thread. There is exactly
one `RudisFlatTable` inside each `RudisTable`; the two extra cursors, the arena, and `num_expires` are
bookkeeping layered on top of the raw table by `RudisTable`'s own methods.

---

## 3. Data Structures & Memory Layouts

### 3.1 `RudisFlatTable` — the SIMD-probed open-addressed table

```rust
// src/table.rs:1238-1246
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

`ctrl` and `slots` are parallel: `ctrl[i]` describes the state of `slots[i]` for every `i` in
`0..capacity`, plus `GROUP_SIZE` (16) extra mirrored bytes at the tail of `ctrl` so a 16-byte probe group
starting near the end of the array can be read as one contiguous unaligned load without wraparound logic
(`set_ctrl`, `src/table.rs:1270–1276`, keeps the mirror in sync on every write). See
[`docs/design/rudis_table.md`](../design/rudis_table.md) §3 for why this is *not* a per-bucket
cache-line-aligned struct despite earlier design drafts describing it that way.

Constants (`src/table.rs:8–10`):

```rust
pub const GROUP_SIZE: usize = 16;
pub const EMPTY: u8 = 0xFF;
pub const DELETED: u8 = 0xFE;
```

Fingerprint and hash (`src/table.rs:1151–1159`):

```rust
#[inline(always)]
pub fn hash_key(key: &[u8]) -> u64 {
    fxhash::hash64(key)   // FxHash — see docs/design/rudis_table.md §4 for rationale
}

#[inline(always)]
pub fn fingerprint(hash: u64) -> u8 {
    (hash >> 57) as u8 & 0x7F   // top 7 bits of the 64-bit hash
}
```

SIMD group comparison (`src/table.rs:1161–1234`): on `x86_64`, `probe_group_match_or_empty` and
`probe_group_match_del_empty` use `_mm_loadu_si128` (unaligned 128-bit load) + `_mm_cmpeq_epi8` +
`_mm_movemask_epi8` to compare all 16 control bytes against a target fingerprint (and, in the
`_del_empty` variant, `DELETED` and `EMPTY` too) in one pair of vector instructions, yielding 16-bit match
bitmasks. On any other target architecture, both functions fall back to a **portable scalar loop** over
the same 16 bytes (`#[cfg(not(target_arch = "x86_64"))]`, `src/table.rs:1198–1234`) — there is currently
no NEON or other non-x86 SIMD implementation.

### 3.2 `RudisEntry` — the inlined key/value/TTL slot

```rust
// src/table.rs:1145-1149
pub struct RudisEntry {
    pub key: Bytes,                  // 32 bytes
    pub val: RudisValue,             // 40 bytes
    pub expire_at: Option<Instant>,  // 16 bytes
}
```

Measured sizes (via `std::mem::size_of`, matching the current `bytes = "1.12.1"` / standard-library
layout): `Bytes` is 32 bytes, `RudisValue` is 40 bytes, `Option<Instant>` is 16 bytes, giving
`size_of::<RudisEntry>() == 88`, `align_of::<RudisEntry>() == 8`. (An earlier revision of this document
attributed the 88 bytes to a 24+40+24 split; the correct split is 32+40+16 — the total was coincidentally
right, the per-field breakdown was not.)

### 3.3 The real `RudisValue` enum

```rust
// src/table.rs:1108-1124
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RudisValue {
    String(Bytes),
    Int(i64),
    SmallHash(Vec<(Bytes, Bytes)>),
    Hash(Box<RudisHashMap>),                          // RudisHashMap = HashMap<Bytes, Bytes, FxBuildHasher>
    List(std::collections::VecDeque<Bytes>),
    Set(Box<RudisSet>),
    ZSet(Box<RudisZSet>),
    HyperLogLog(Box<[u8; 16384]>),
    Stream(Box<RudisStream>),
    Tiered(TieredPointer),
    Cooled {
        ptr: TieredPointer,
        val: Box<RudisValue>,
    },
}
```

**Correction**: an earlier revision of this document showed `Hash(HashMap<Bytes, Bytes>)`,
`Set(RudisSet)`, `ZSet(RudisZSet)`, and `Stream(RudisStream)` — unboxed. As of the current source, all
four collection-carrying variants are boxed (`Box<RudisHashMap>`, `Box<RudisSet>`, `Box<RudisZSet>`,
`Box<RudisStream>`). Boxing is precisely what keeps `size_of::<RudisValue>()` at 40 bytes instead of
ballooning to the size of the largest possible collection header — see
[`docs/design/rudis_table.md`](../design/rudis_table.md) §2 Pillar 2.

There is no `Bitmap` variant — `SETBIT`/`GETBIT` and the other bitmap commands operate directly on the
bytes of a `RudisValue::String`. There is no `Json` variant either; JSON values are handled entirely by
`src/json.rs`, outside `table.rs`'s scope.

`approx_bytes()` (`src/table.rs:1126–1141`) is the per-variant memory heuristic used throughout this file
to keep `RudisTable::used_memory` roughly current without an exact allocator query:

| Variant | Heuristic |
| :--- | :--- |
| `String` | `len()` |
| `Int` | `8` |
| `SmallHash` | `Σ (k.len() + v.len() + 16)` per pair |
| `Hash` | `Σ (k.len() + v.len() + 32)` per pair |
| `List` | `Σ (b.len() + 16)` per element |
| `Set` | `len() * 32` — a flat per-member estimate that ignores actual member byte length |
| `ZSet` | `len() * 48` — likewise flat, ignores member byte length |
| `HyperLogLog` | `16384` (fixed) |
| `Stream` | `len() * 64` |
| `Tiered` | `24` |
| `Cooled { val, .. }` | `24 + val.approx_bytes()` |

Note `Set` and `ZSet` are the two variants whose heuristic does **not** scale with actual member size —
worth knowing if `used_memory` looks off for sets/sorted sets holding unusually large members.

### 3.4 `RudisTable` — the per-shard façade

```rust
// src/table.rs:1645-1652
pub struct RudisTable {
    table: RudisFlatTable,
    sample_cursor: usize,
    spill_cursor: usize,
    pub used_memory: usize,
    pub arena: crate::allocator::SmallCollectionArena,
    pub num_expires: usize,
}
```

**Correction**: an earlier revision omitted `arena` and `num_expires` — both are current fields.
`sample_cursor` drives active TTL expiration sampling (§6.2); `spill_cursor` independently drives NVMe
spill-candidate sampling (§7) — the two cursors advance independently so one workload's sampling cadence
never perturbs the other's. `arena` is the small-collection allocation pool (§9). `num_expires` is a
running count of entries that currently have `expire_at.is_some()`, maintained by every insert/`expire`/
`persist`/removal path; it lets hot paths (`get`, `incr_by`, `hset`, …) skip the `Instant::now()` call and
branch entirely when it is `0` — see §6.1.

---

## 4. Compact Encodings Deep-Dive

There is **no Listpack, Intset, or pointer-based skiplist anywhere in this file.** Three of the eleven
`RudisValue` variants have a genuine small/full adaptive representation; the rest are always one fixed
Rust type.

### 4.1 Strings: `String(Bytes)` vs. `Int(i64)`

`set_extended_with_hash` (`src/table.rs:2091`) parses every incoming value as a 64-bit integer first; if
it parses cleanly, the entry is stored as `RudisValue::Int`, not `String`:

```rust
let val = if let Some(int_val) = Self::parse_i64_bytes(&value) {
    RudisValue::Int(int_val)
} else {
    RudisValue::String(Bytes::copy_from_slice(&value))
};
```

`incr_by_slice_internal` (`src/table.rs:2248`) then mutates an existing `Int` in place
(`n.checked_add(delta)`) with zero allocation, only falling back to parsing bytes if the value is still a
plain `String`. `parse_i64_bytes` (`src/table.rs:2032`) and `format_i64` (`src/table.rs:1990`) are
hand-written ASCII digit loops — no `format!`/`str::parse` — used on the hottest integer-string paths, and
`format_i64` special-cases the values `0`–`10` and `-1` as `Bytes::from_static` to skip allocation
entirely for the most common small integers.

### 4.2 Hashes: `SmallHash(Vec<(Bytes, Bytes)>)` vs. `Hash(Box<RudisHashMap>)`

```rust
// hset_internal, src/table.rs:~3080-3160 (abridged)
let max_entries = crate::connection::HASH_MAX_ENTRIES.load(Ordering::Relaxed);  // default 512
let max_value = crate::connection::HASH_MAX_VALUE.load(Ordering::Relaxed);      // default 64

RudisValue::SmallHash(pairs) => {
    pairs.push((field.clone(), val.clone()));
    if pairs.len() > max_entries || field.len() > max_value || val.len() > max_value {
        let map: RudisHashMap = pairs.drain(..).collect();
        entry.val = RudisValue::Hash(Box::new(map));
    }
}
```

`SmallHash` is a plain `Vec` of pairs searched linearly on every read/write, not a byte-packed buffer. The
promotion thresholds are **runtime-configurable** process-wide atomics owned by `connection.rs`
(`HASH_MAX_ENTRIES` default `512`, `HASH_MAX_VALUE` default `64`, `src/connection.rs:791–793`) —
conceptually mirroring Redis's `hash-max-listpack-entries`/`-value`, even though the small form here is a
`Vec`, not a listpack. New hashes are constructed as `SmallHash` (acquired from the arena, §9) whenever
every field/value fits under `max_value`; otherwise they are born directly as `Hash`.

### 4.3 Sets: `RudisSet::Small(Vec<SmallSetEntry>)` vs. `Full(HashSet<Bytes, FxBuildHasher>)`

```rust
// src/table.rs:662-673
const SMALL_SET_LIMIT: usize = 64;

pub struct SmallSetEntry {
    pub hash: u64,
    pub member: Bytes,
}

pub enum RudisSet {
    Small(Vec<SmallSetEntry>),
    Full(hashbrown::HashSet<Bytes, FxBuildHasher>),
}
```

**Correction**: an earlier revision of this document showed `Small(Vec<Bytes>)`. The actual small form is
`Vec<SmallSetEntry>`, where each entry precomputes and caches `fxhash::hash64(member)` alongside the
member bytes — `contains`/`insert`/`remove` compare the cached `u64` hash before falling back to a byte
comparison (`src/table.rs:757–784`), so a miss on a large member is rejected by an integer comparison
without touching the member bytes at all.

```rust
// src/table.rs:786-815 (abridged)
pub fn insert(&mut self, member: Bytes) -> bool {
    match self {
        RudisSet::Small(v) => {
            // linear scan comparing cached hash, then full bytes
            v.push(SmallSetEntry { hash: m_hash, member });
            if v.len() > SMALL_SET_LIMIT {
                let mut set = hashbrown::HashSet::with_capacity_and_hasher(v.len(), FxBuildHasher::default());
                for m in v.drain(..) { set.insert(m.member); }
                *self = RudisSet::Full(set);
            }
            true
        }
        RudisSet::Full(s) => s.insert(member),
    }
}
```

Unlike `Hash`, the set promotion threshold is a **hardcoded constant** (`SMALL_SET_LIMIT = 64`), not
runtime-configurable. Deletion from the small form uses `Vec::swap_remove` — O(1), but it reorders the
vector — rather than a shifting remove (`src/table.rs:854–870`). A `RudisSetIter` enum
(`src/table.rs:682–706`) unifies iteration over both representations behind one type so callers
(`SMEMBERS`, `SINTER`, …) never need to match on the variant.

### 4.4 Sorted sets: `RudisZSet::Small(Vec<(OrderedScore, Bytes)>)` vs. `Full { dict, tree }`

```rust
// src/table.rs:143, 146-152
const SMALL_ZSET_LIMIT: usize = 64;

pub enum RudisZSet {
    Small(Vec<(OrderedScore, Bytes)>),
    Full {
        dict: hashbrown::HashMap<Bytes, f64>,               // hashbrown's default hasher, NOT FxBuildHasher
        tree: std::collections::BTreeSet<(OrderedScore, Bytes)>,
    },
}
```

`OrderedScore(f64)` (`src/table.rs:12–29`) wraps a bare `f64` and implements a total order via
`f64::total_cmp`, which is what lets `(OrderedScore, Bytes)` tuples live inside a `BTreeSet`/be
`binary_search`-ed despite `f64` not being `Ord` on its own.

The small form is a **sorted** `Vec`: `insert` (`src/table.rs:197–234`) locates the insertion point with
`binary_search` — O(log n) to find the slot — but `Vec::insert` still shifts every following element, so
insertion is O(n) overall. The full form pairs a `HashMap<Bytes, f64>` (O(1) score lookup by member) with
a `BTreeSet<(OrderedScore, Bytes)>` ordered by `(score, member)` for range queries. Note the `dict` here
does not pin `FxBuildHasher` the way `RudisHashMap` and `RudisSet::Full` do — it uses whatever hasher
`hashbrown::HashMap`'s type parameter defaults to (`foldhash`, per the pinned `hashbrown = "0.17.1"`) — a
minor inconsistency with no observed correctness impact, since this hasher never touches the outer
`RudisFlatTable`'s own probing.

**`rank()` is not O(log n) in either representation** (`src/table.rs:253–273`):

```rust
RudisZSet::Full { dict, tree } => {
    if !dict.contains_key(member) { return None; }
    if rev { tree.iter().rev().position(|(_, m)| m.as_ref() == member) }
    else    { tree.iter().position(|(_, m)| m.as_ref() == member) }
}
```

`BTreeSet` has no random-access rank operation in `std`, so `ZRANK`/`ZREVRANK` pay for an O(n) walk
regardless of representation. There is no augmented/spanned skiplist anywhere in this file providing
O(log n) rank — see §10.

### 4.5 Lists: always `VecDeque<Bytes>`

`RudisValue::List(std::collections::VecDeque<Bytes>)` — one representation regardless of length.
`LPUSH`/`RPUSH`/`LPOP`/`RPOP` map directly to `VecDeque::push_front`/`push_back`/`pop_front`/`pop_back`;
there is no compact/large-list distinction, and no listpack-equivalent small form.

### 4.6 Streams: `BTreeMap<StreamId, Vec<(Bytes, Bytes)>>`

```rust
// src/table.rs:1046-1072
pub struct StreamPelEntry {
    pub consumer: Bytes,
    pub delivery_time_ms: u64,
    pub delivery_count: usize,
}

pub struct StreamConsumer {
    pub name: Bytes,
    pub seen_time_ms: u64,
    pub pel: std::collections::BTreeMap<StreamId, u64>,
}

pub struct StreamGroup {
    pub name: Bytes,
    pub last_delivered_id: StreamId,
    pub consumers: HashMap<Bytes, StreamConsumer>,
    pub pel: std::collections::BTreeMap<StreamId, StreamPelEntry>,
}

pub struct RudisStream {
    pub entries: std::collections::BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    pub last_id: StreamId,
    pub groups: HashMap<Bytes, StreamGroup>,
}
```

`StreamId { ms: u64, seq: u64 }` (`src/table.rs:926–930`) derives `Ord`, so `entries` is naturally ordered
by ID via the `BTreeMap`, which is what makes `XRANGE`-style ID-range queries efficient without any custom
indexing. Each `StreamGroup` tracks its own `last_delivered_id`, a map of `StreamConsumer`s (each with its
own per-consumer pending-entries list keyed by `StreamId`), and a group-wide `pel: BTreeMap<StreamId,
StreamPelEntry>` recording per-entry delivery time and delivery count for `XACK`/`XCLAIM`/`XAUTOCLAIM`.

### 4.7 HyperLogLog: always a dense `Box<[u8; 16384]>`

`RudisValue::HyperLogLog(Box<[u8; 16384]>)` is a fixed 16,384-register dense array — the same dense
representation real Redis's HLL falls back to at large cardinalities, used unconditionally here. There is
no sparse encoding for small cardinalities.

---

## 5. Probing, Insertion, and Rehashing Algorithms

### 5.1 Lookup (`find_entry`, `contains`, `find_entry_mut`)

See [`docs/design/rudis_table.md`](../design/rudis_table.md) §5 for the full step-by-step walkthrough
(hash → single flat index → fingerprint compare → key compare → triangular group-step). All three
functions (`src/table.rs:1280`, `1319`, `1358`) share the identical probe sequence and differ only in what
they return on a hit; none of them evaluate TTL.

### 5.2 Insert (`find_or_prepare_insert`, `insert`, `insert_prepared`)

`find_or_prepare_insert` (`src/table.rs:1410–1458`) runs the same probe sequence as lookup, but tracks two
extra things as it goes: the first tombstone (`DELETED`) slot encountered (so a fresh key can reuse it
instead of extending the probe further), and, on a full miss, the first truly empty slot. It returns
`(Some(existing_idx), existing_idx)` on a key match, or `(None, candidate_insert_idx)` otherwise — a
tombstone slot if one was seen, else the first empty slot.

`insert` (`src/table.rs:1475–1502`) triggers a resize *before* probing whenever `growth_left == 0`, then
either replaces an existing entry in place or writes the fingerprint + entry into the candidate slot and
increments `items`. `insert_prepared` (`src/table.rs:1505–1519`) is a variant used by callers that already
computed the hash and candidate index via a prior `find_or_prepare_insert` call (e.g. `hset_internal`),
avoiding a second full probe.

Both paths also update `slot_counts` — but only when `crate::cluster::HAS_ACTIVE_CLUSTER` is currently
`true` (checked on every single insert/remove, `src/table.rs:1493, 1512, 1531, 1556`); when cluster mode
is inactive the array is left untouched (and stale/zero), which matters for §8.

### 5.3 Removal (`remove`, `remove_present`)

Both (`src/table.rs:1522–1561`) set the slot's control byte to `DELETED` (never back to `EMPTY` on a
per-removal basis) and decrement `items`. As a special case, when `items` drops to exactly `0` the entire
`ctrl` array is reset to `EMPTY` and `growth_left` is restored to `capacity * 7 / 8` — a cheap way to avoid
a probe sequence ever having to skip a long run of accumulated tombstones once the table is provably
empty. `remove_present` is an `unsafe`-accelerated variant for call sites that already know the slot is
occupied (skips the `Option` check), used on the hot `DEL` path.

### 5.4 Resize and Rehashing (`resize`, `defrag`, `active_defrag`)

```rust
// src/table.rs:1460-1473
fn resize(&mut self, new_cap: usize) {
    let mut new_table = RudisFlatTable::new(new_cap);
    for entry in self.slots.drain(..).flatten() {
        let h = hash_key(&entry.key);
        let (_, insert_idx) = new_table.find_or_prepare_insert(&entry.key, h);
        let tag = fingerprint(h);
        new_table.set_ctrl(insert_idx, tag);
        new_table.slots[insert_idx] = Some(entry);
        new_table.items += 1;
        new_table.growth_left = new_table.growth_left.saturating_sub(1);
    }
    new_table.slot_counts = self.slot_counts.clone();
    *self = new_table;
}
```

`resize` is **monolithic**: it allocates an entirely new `RudisFlatTable` and rehashes every live entry
into it in one synchronous pass before swapping it in. There is no incremental/segmented resize — this is
the single largest gap versus the original design intent (see
[`docs/design/rudis_table.md`](../design/rudis_table.md) §2 Pillar 3 and §6, and §10 below).

`resize` is triggered from `insert` (`src/table.rs:1476–1483`) whenever `growth_left == 0`, with one
non-obvious branch worth documenting precisely:

```rust
let new_cap = if self.items * 2 < self.capacity && self.capacity > GROUP_SIZE {
    self.capacity            // rehash at the SAME capacity
} else {
    self.capacity * 2         // double
};
```

If live items are fewer than half the current capacity when growth room runs out, the table is dominated
by tombstones rather than genuinely full — `resize` is called with `new_cap == self.capacity`, producing a
same-size rehash whose only effect is to purge every tombstone and restore `growth_left` to a full
`capacity * 7 / 8`, without growing memory. Otherwise capacity doubles as usual. The growth threshold
itself is the constructor's `growth_left = cap * 7 / 8` (`RudisFlatTable::new`, `src/table.rs:1265`) — a
7/8 (87.5%) maximum load factor before either a doubling or a tombstone-clearing resize fires.

`defrag` (`src/table.rs:1599–1609`) is a related, separately-triggered utility: it computes an "optimal"
capacity as `(items * 2).next_power_of_two().max(GROUP_SIZE).max(64)` and calls `resize` with that value
whenever the table is either over-allocated relative to its live item count or currently holds any
tombstone at all — i.e. it can *shrink* a table, which the growth-triggered path never does on its own.
`active_defrag` (`src/table.rs:1723–1744`) wraps `defrag` with a second pass that additionally
`Bytes::copy_from_slice`s every live `String` and `SmallHash` field/value to release any slack capacity
those buffers may be holding, then recomputes `used_memory` from scratch via `recalculate_used_memory`
(`src/table.rs:1712–1721`). `active_defrag` is reachable end-to-end from `src/connection.rs:8759` through
`ShardDb::active_defrag` (`src/shard.rs:815`) and a cross-shard `ShardMessage::ActiveDefrag` round-trip
(`src/router.rs:1626`, `src/server.rs:494`) — i.e. it is an operator-triggered maintenance path, not
something the table runs on its own initiative.

---

## 6. Expiration & Memory Management

### 6.1 Passive expiration on read — gated by `num_expires` and a global bypass

```rust
// check_expired_slot, src/table.rs:1759-1782 (abridged)
fn check_expired_slot(&mut self, slot_idx: usize) -> bool {
    if self.num_expires == 0 {
        return false;
    }
    if crate::connection::ALLOW_ACCESS_EXPIRED.load(Ordering::Relaxed) {
        return false;
    }
    let is_exp = /* compare Instant::now() against entry.expire_at */;
    if is_exp {
        self.expire_slot(slot_idx);   // removes the entry, updates used_memory, bumps EXPIRED_KEYS
        true
    } else {
        false
    }
}
```

**Correction**: an earlier revision of this document did not mention the `num_expires == 0` short-circuit.
It is checked first, before the `ALLOW_ACCESS_EXPIRED` flag or any `Instant::now()` call, and the same
guard is inlined directly into hot per-command paths that don't go through `check_expired_slot` at all —
e.g. `get_with_hash` (`src/table.rs:1855–1865`) and `incr_by_slice_internal`
(`src/table.rs:2255–2263`) both test `self.num_expires > 0` before even looking at `entry.expire_at`. On a
table (or a moment in a table's life) where no key carries a TTL, this collapses expiration checking to a
single integer comparison on every read/write, with no clock read and no branch on `ALLOW_ACCESS_EXPIRED`.

`ALLOW_ACCESS_EXPIRED` (`src/connection.rs:794`) is a process-wide `AtomicBool` that, when set, disables
expiration checks everywhere in the table regardless of `num_expires` — used for debug/inspection paths
that need to see logically-expired keys. Every real eviction updates `used_memory`
(`freed = key.len() + val.approx_bytes() + 64`) and bumps the process-wide `EXPIRED_KEYS` counter
(`inc_expired_keys`/`get_expired_keys`, `src/table.rs:1615–1623`), surfaced through `INFO`.

`num_expires` itself is maintained by every path that can add or remove a TTL: `expire`
(`src/table.rs:2310–2325`) increments it exactly when a key transitions from no-TTL to TTL; `persist`
(`src/table.rs:2327–2342`) decrements it on the reverse transition; any real eviction/removal of a
TTL-carrying entry decrements it too.

### 6.2 Active expiration sampling — unchanged in shape

```rust
// src/table.rs:7721-7738
pub fn active_expire_cycle(&mut self) -> usize {
    let cap = self.table.capacity();
    if cap == 0 || self.table.is_empty() { return 0; }
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

Bounded 20-slots-per-call sampling via a persistent cursor, called periodically from the server's event
loop. Because `check_expired_slot` now short-circuits on `num_expires == 0` (§6.1), this cycle is
essentially free on shards holding no TTL'd keys, even though it still advances `sample_cursor` through 20
slots every call.

### 6.3 Eviction under memory pressure (`try_evict_one_key`)

```rust
// src/table.rs:1786-1837 (abridged)
pub fn try_evict_one_key(&mut self, policy: &str) -> Option<usize> {
    // samples up to 10 occupied slots from sample_cursor, honoring:
    //   - "volatile-*": only considers keys with expire_at.is_some()
    //   - "*-ttl":       tracks the minimum expire_at seen (closest to expiring)
    //   - all other policies (allkeys-lru/allkeys-random/volatile-lru/volatile-random):
    //       takes the first sampled occupied slot as a stand-in — there is no LRU
    //       clock or access-recency tracking anywhere in this file, so "lru" policies
    //       currently behave identically to "random" ones here.
}
```

This is a deliberate simplification worth flagging precisely: despite accepting policy strings that
mention `lru`, `RudisTable` does not track per-key recency at all, so any `*-lru` maxmemory policy is, as
implemented in `table.rs`, indistinguishable from its `*-random` counterpart. Only `*-ttl` policies get
genuine sample-based comparison (nearest expiry wins).

---

## 7. NVMe Tiering Hooks

`table.rs` never performs disk I/O itself, but it exposes the primitives `src/tiering.rs` needs to move
values in and out of RAM, built around two `RudisValue` variants:

```rust
// src/table.rs:1101-1106, 1119-1123
pub struct TieredPointer {
    pub file_id: u32,
    pub offset: u64,
    pub length: u32,
    pub value_type: u8,
}
// RudisValue::Tiered(TieredPointer)           — value lives only on disk
// RudisValue::Cooled { ptr: TieredPointer, val: Box<RudisValue> }  — on disk AND still cached in RAM
```

`get_with_hash` and other readers unwrap `Cooled` transparently so callers never need to branch on tiering
state themselves:

```rust
// src/table.rs:1866-1869
let val_ref = match &entry.val {
    RudisValue::Cooled { val, .. } => val.as_ref(),
    other => other,
};
```

Relevant methods, all verified present at their current signatures:

- `get_hot_keys_for_spill(&mut self, limit: usize) -> Vec<Bytes>` (`src/table.rs:2656–2678`): round-robins
  `spill_cursor` across every slot, collecting keys whose value is *not* already `Tiered`/`Cooled`, up to
  `limit`, and leaves `spill_cursor` positioned to resume the sweep on the next call (or wraps to `0` if a
  full pass completes without hitting `limit`).
- `restore_tiered_value(&mut self, key: &[u8], val: RudisValue) -> bool` (`src/table.rs:2605–2620`):
  converts a `Tiered(ptr)` entry into `Cooled { ptr, val }`, adding `val`'s `approx_bytes()` back to
  `used_memory`.
- `decommit_cooled_key(&mut self, key: &[u8]) -> Option<(TieredPointer, usize)>`
  (`src/table.rs:2622–2636`): the single-key inverse — converts one `Cooled { ptr, val }` entry back to
  `Tiered(ptr)`, dropping `val` and returning the bytes freed.
- `decommit_all_cooled(&mut self) -> (usize, u64)` (`src/table.rs:2638–2654`): the same conversion applied
  to every `Cooled` entry in the table in one pass, returning `(count converted, total bytes freed)`.
- `get_value_for_spill(&mut self, key: &[u8]) -> Option<(Vec<u8>, u8)>` (`src/table.rs:2680+`): serializes
  a non-tiered value's payload and type tag for `src/tiering.rs` to write to disk; returns `None` for keys
  already `Tiered`/`Cooled` or that turn out to be expired (lazily evicted along the way via
  `check_expired_slot`).

`spill_cursor` is intentionally independent of expiration's `sample_cursor` so the two sampling passes
never perturb each other's progress through the table.

---

## 8. Cluster Slot Indexing

The reverse index once described here (`slot_to_keys: HashMap<u16, HashSet<Bytes>>`) does not exist in
the current source. It has been replaced by a fixed-size **count-only** array living inside
`RudisFlatTable` itself:

```rust
pub slot_counts: Box<[u32; 16384]>,
```

incremented/decremented directly inside `RudisFlatTable::insert` / `insert_prepared` / `remove` /
`remove_present` (`src/table.rs:1493, 1512, 1531–1534, 1556–1559`) — but, as noted in §5.2, **only when
`crate::cluster::HAS_ACTIVE_CLUSTER` is `true` at the moment of the mutation.** With cluster mode inactive,
`slot_counts` is never touched and stays at its initial all-zero state.

```rust
// count_keys_in_slot, src/table.rs:6609-6645 (abridged)
pub fn count_keys_in_slot(&mut self, slot: u16) -> usize {
    if self.table.items == 0 { return 0; }
    if crate::cluster::HAS_ACTIVE_CLUSTER.load(Ordering::Relaxed) {
        if self.table.slot_counts[slot as usize] == 0 { return 0; }
        if self.num_expires == 0 {
            return self.table.slot_counts[slot as usize] as usize;   // true O(1) path
        }
    }
    // otherwise: full O(capacity) linear scan over every slot, filtering by
    // crate::router::key_slot(&entry.key) == slot, lazily expiring any stale
    // entries encountered along the way
}
```

**Correction**: an earlier revision of this document stated the O(1) fast path applies "in the common case
that the slot is empty," and that any non-empty slot always falls back to a full scan. That is only
accurate when cluster mode is active and at least one key in the *entire table* currently carries a TTL.
The actual fast path is three-way:

1. Cluster inactive → always a full linear scan (the count array cannot be trusted; it was never updated).
2. Cluster active, `slot_counts[slot] == 0` → `O(1)`, returns `0` immediately.
3. Cluster active, `slot_counts[slot] > 0`, and **no key in the whole table has a TTL** (`num_expires ==
   0`) → `O(1)`, returns the counter directly (safe, because with `num_expires == 0` the counter cannot be
   stale due to a lazily-expired key that was never subtracted).
4. Cluster active, slot non-empty, and at least one key anywhere in the table has a TTL → full
   `O(table capacity)` linear scan, because the counter might overcount keys that have since expired but
   not yet been lazily evicted.

`get_keys_in_slot` (`src/table.rs:6647–6687`) follows the same shape (fast-path-eligible only on an empty
slot; otherwise always scans, since it needs to enumerate keys, not just count them). Both functions
opportunistically lazily-expire any expired entries they encounter mid-scan, decrementing `num_expires`
for each one removed.

This remains a real regression versus the original reverse-index design for any workload calling these
commands against a populated, non-empty cluster slot while any key anywhere in the shard has a TTL — see
§10.

---

## 9. Small-Collection Allocation Arena

`RudisTable.arena: crate::allocator::SmallCollectionArena` (`src/allocator.rs:100–107`) is a thread-local
free-list pool that recycles the backing heap allocations behind the four collection representations that
churn allocations fastest: `List` (`VecDeque<Bytes>`), `SmallHash` (`Vec<(Bytes, Bytes)>`), small `Set`
(`Vec<SmallSetEntry>`), and small `ZSet` (`Vec<(OrderedScore, Bytes)>`).

```rust
// src/allocator.rs:100-107
pub struct SmallCollectionArena {
    list_pool: Vec<VecDeque<Bytes>>,
    hash_pool: Vec<Vec<(Bytes, Bytes)>>,
    set_pool: Vec<Vec<crate::table::SmallSetEntry>>,
    zset_pool: Vec<Vec<(crate::table::OrderedScore, Bytes)>>,
    pub allocations_saved: u64,
    pub recycles_count: u64,
}
```

Each collection type has a symmetric `acquire_*(min_cap)` / `recycle_*(collection)` pair
(`src/allocator.rs:121–210`): `acquire_*` pops a pooled, already-heap-allocated collection and grows it to
`min_cap` only if needed, falling back to a fresh allocation when the pool is empty; `recycle_*` clears the
collection (dropping its elements but retaining its capacity) and pushes it back onto the pool, capped at
`MAX_ARENA_POOLED = 1024` entries per pool and a per-collection capacity ceiling of `512` elements (larger
buffers are simply dropped rather than pooled, to bound the arena's own memory footprint).

`RudisTable::recycle_value` (`src/table.rs:1680–1696`) is the single dispatch point that feeds values back
to the arena whenever an entry is overwritten, deleted, or expired:

```rust
pub fn recycle_value(&mut self, val: RudisValue) {
    match val {
        RudisValue::List(deque) => self.arena.recycle_list(deque),
        RudisValue::SmallHash(pairs) => self.arena.recycle_small_hash(pairs),
        RudisValue::Set(s) => { if let RudisSet::Small(v) = *s { self.arena.recycle_small_set(v); } }
        RudisValue::ZSet(z) => { if let RudisZSet::Small(v) = *z { self.arena.recycle_small_zset(v); } }
        _ => {}
    }
}
```

Note that `Full`-form `Set`/`ZSet` values, `Hash`, `Stream`, and every other variant are simply dropped —
only the four `Vec`/`VecDeque`-backed small forms are pooled, since those are the allocations churned most
frequently by high-QPS `LPUSH`/`HSET`/`SADD`/`ZADD` workloads. Callers acquire from the arena on the
construction side too (e.g. `hset_internal` at `src/table.rs:3146`, list push paths at
`src/table.rs:4200/4278`, set/zset insert paths at `src/table.rs:5361/5424/6835`), so a steady-state
workload that repeatedly creates and deletes small collections of the same shape can avoid touching the
global allocator almost entirely. `allocations_saved`/`recycles_count` are exposed via `pool_stats()`
(`src/allocator.rs:213–220`) for introspection/testing.

This entire subsystem — struct, dispatch, and every call site — postdates the original storage-engine
design documents and is not mentioned in either of them; it is documented here for the first time.

---

## 10. Future Improvements

- **High — implement a real segmented/incremental resize.** `RudisFlatTable::resize` (§5.4) is still a
  monolithic rehash of the entire live table whenever growth room runs out (with one partial mitigation:
  the same-capacity tombstone-clearing branch, which at least avoids growing memory unnecessarily, but
  still rehashes every entry synchronously). This is the exact tail-latency spike the original design
  intent in [`docs/design/rudis_table.md`](../design/rudis_table.md) §2 Pillar 3 was written to eliminate,
  and remains the single highest-value structural change to this file if p99.9 write latency at large key
  counts ever becomes a measured problem.
- **Medium — restore an efficient cluster-slot key index for the case that currently regresses (§8).**
  Cluster-active, non-empty-slot, `num_expires > 0` still costs `O(table capacity)` per
  `CLUSTER COUNTKEYSINSLOT`/`GETKEYSINSLOT` call. A middle ground — e.g. a small per-slot `Vec<usize>` of
  slot indices, sized only for slots actually in use, rather than a full `HashSet<Bytes>` clone of every
  key per the original reverse-index design — could recover most of the lookup speed without paying the
  original design's full per-insert cloning cost.
- **Medium — give `ZSet`/`ZRANK` a real O(log n) rank operation.** `rank()` is a linear scan even in the
  `Full` representation because `BTreeSet` has no built-in indexable-rank support (§4.4). An
  order-statistics structure (a `BTreeMap` augmented with subtree sizes, or a hand-rolled indexable
  skiplist) would make `ZRANK`/`ZREVRANK` genuinely sub-linear, which matters more as sorted sets grow past
  the 64-element small-form threshold.
- **Medium — give `try_evict_one_key`'s `*-lru` policies actual recency tracking (§6.3).** Today every
  `*-lru` maxmemory policy behaves identically to `*-random` because no access-recency clock exists
  anywhere in `RudisEntry` or `RudisFlatTable`. A compact recency signal (even a coarse clock-sweep byte
  per slot, à la CLOCK/second-chance, rather than a full LRU list) would let `allkeys-lru`/`volatile-lru`
  do what their names claim.
- **Low — add a SIMD group-probing path for non-`x86_64` targets.** `probe_group_match_or_empty` and
  `probe_group_match_del_empty` (§3.1) fall back to a 16-iteration scalar loop on every architecture other
  than `x86_64`, including ARM64 (Apple Silicon, AWS Graviton). A NEON (`vceqq_u8`/`vmaxvq_u8`) path would
  close this portability gap; none exists in the current source.
- **Low — make `Set`/`ZSet`'s small-form promotion thresholds runtime-configurable**, consistent with
  `Hash`'s already-configurable `HASH_MAX_ENTRIES`/`HASH_MAX_VALUE` (§4.2). `SMALL_SET_LIMIT`/
  `SMALL_ZSET_LIMIT` are compile-time constants (§4.3/§4.4) today, an inconsistency with no apparent reason
  beyond historical accident.
- **Low — track `used_memory` per-variant more precisely for `Set`/`ZSet`.** Both heuristics (§3.3) are
  flat `count * constant` estimates that ignore actual member byte length, unlike every other variant's
  heuristic. A closer estimate would make `src/tiering.rs`'s offload/upload threshold decisions more
  accurate without needing real allocator introspection.

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: `RudisValue` is held to 40 bytes specifically by boxing its four collection-carrying
  variants (`Hash`, `Set`, `ZSet`, `Stream`). Adding a new unboxed variant with a payload larger than the
  current 32-byte maximum (`String`/`List`/`Cooled`) will silently grow every `RudisEntry` in the table.
* **Gotcha 2**: `RudisEntry` is 88 bytes total, but not as `24B key + 40B val + 24B Option<Instant>` — the
  correct split is `32B key (Bytes) + 40B val (RudisValue) + 16B expire_at (Option<Instant>)`. Its
  alignment is 8 bytes, not 64 — it is not cache-line-aligned, and at 88 bytes it does not fit in a single
  64-byte cache line regardless.
* **Gotcha 3**: `slot_counts` is only maintained while `crate::cluster::HAS_ACTIVE_CLUSTER` is `true` at
  the time of each insert/remove. Toggling cluster mode on after keys already exist means the counters
  will under-report until the table is repopulated or rehashed; readers must not assume the array is
  authoritative in every configuration (§8).
* **Gotcha 4**: `check_expired_slot` and its inlined equivalents short-circuit entirely when
  `RudisTable::num_expires == 0`. Any code path that mutates `expire_at` directly on a `RudisEntry` without
  going through `expire()`/`persist()`/the table's own removal paths will desynchronize `num_expires` from
  reality and silently disable or wrongly enable expiration checks.
* **Gotcha 5**: Active expiration cycles sample up to 20 bucketed slots per call round-robin via
  `sample_cursor`, without any locking — this is safe only because `RudisTable` is thread-local.

### How to Verify Changes

```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
