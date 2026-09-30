# Component 05: Storage Engine & Compact Encodings (Implementation Deep-Dive & Code Reference)

> **Source File**: `src/table.rs` (16,925 lines, 349 `pub fn`)
> **High-Level Design Spec**: [`docs/design/05_storage_engine.md`](../design/05_storage_engine.md)
> **Bucket-Layout Design Deep-Dive**: [`docs/design/rudis_table.md`](../design/rudis_table.md) — **read with caution**: it
> predates the extendible-hashing rewrite documented below and describes an older flat single-array design.
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

This is a ground-up rewrite of this document. `table.rs` has grown from ~11.7K to ~16.9K lines since the last
revision, and its core hash table was rebuilt twice in that window: once to add a cooperative incremental-rehash
protocol, and again — superseding the first change almost entirely — to replace the single flat array with a
**Dragonfly-style extendible-hashing directory over fixed-size SIMD segments**. Every struct, constant, and line
number below was read directly out of the current source (commit history current as of `69fb2be`); nothing is
carried forward from memory.

---

## 1. Source Module Map & Responsibilities

| File | Role |
| :--- | :--- |
| `src/table.rs` | The entire thread-local storage engine: the SIMD-probed, extendible-hashing hash table (`RudisFlatTable` = `directory` + `Vec<RawSegment>`), the per-shard façade over it (`RudisTable`), every `RudisValue` compact/full encoding pair, key-level and hash-field-level expiration bookkeeping, NVMe-tiering hooks, cluster-slot counting, and the small-collection allocation arena. |

`table.rs` has no submodules; everything below is one flat file, organized here by concern rather than by
source order.

---

## 2. Structural Overview

```
RudisTable                                          (src/table.rs:2418)
├── table: RudisFlatTable                            (src/table.rs:1976)
│   ├── segments: Vec<RawSegment>                     one or more fixed-capacity SIMD sub-tables
│   │   └── RawSegment                                (src/table.rs:1584)
│   │       ├── ctrl: Vec<u8>                          control-byte array (main probe region)
│   │       ├── slots: Vec<Option<RudisEntry>>         main region + 4-slot overflow stash, parallel to ctrl
│   │       ├── capacity, mask, items, growth_left
│   │       ├── local_depth: u8                        extendible-hashing depth of this segment
│   │       └── stash_ctrl: [u8; 4], stash_count: u8    DashTable-style 4-slot overflow stash
│   ├── directory: Vec<u32>                            2^global_depth pointers into `segments`
│   ├── global_depth: u8, dir_mask: usize
│   ├── capacity, items
│   └── slot_counts: Box<[u32; 16384]>                 per-cluster-slot live-key counters
├── sample_cursor: usize                  shared TTL-sampling cursor (key-level AND hash-field-level)
├── spill_cursor: usize                   NVMe-tiering spill-candidate round-robin cursor
├── used_memory: usize                    running estimate of live value bytes
├── arena: SmallCollectionArena           recycled Vec/VecDeque pool for small collections
├── num_expires: usize                    count of live entries currently carrying a whole-key TTL
└── hash_field_expires: HashMap<Bytes, HashMap<Bytes, Instant>>   per-field TTLs for HEXPIRE (hash fields)
```

`RudisTable` is the type embedded directly in `ShardDb` (`pub table: crate::table::RudisTable`,
`src/shard.rs:578`) — one instance per shard, owned exclusively by that shard's thread. There is exactly
one `RudisFlatTable` inside each `RudisTable`.

**What changed structurally since the last revision of this document**: the table used to be one flat
`ctrl`/`slots` pair resized monolithically. It is now a **directory of fixed 1024-slot segments**, each
independently split or compacted. This is the single biggest change in the file and is covered in full in
§5.

---

## 3. Data Structures & Memory Layouts

### 3.1 `RawSegment` — a fixed-capacity SIMD-probed sub-table with an overflow stash

```rust
// src/table.rs:1576-1594
const SEG_SHIFT: usize = 10;
const SEG_CAP: usize = 1 << SEG_SHIFT;   // 1024 main SIMD slots per segment
const STASH_CAP: usize = 4;              // 4 DashTable-style overflow stash slots per segment
const GLOBAL_IDX_SHIFT: usize = 11;      // bits reserved for a segment-local index in a global index
const GLOBAL_IDX_MASK: usize = (1 << GLOBAL_IDX_SHIFT) - 1;

/// Fixed-size SwissTable segment with a 4-slot DashTable-style overflow stash,
/// managed by `RudisFlatTable`'s extendible hashing directory.
pub struct RawSegment {
    pub ctrl: Vec<u8>,                     // len = capacity + GROUP_SIZE (mirrored tail, see §3.4)
    pub slots: Vec<Option<RudisEntry>>,    // len = capacity + STASH_CAP
    pub capacity: usize,                   // power of two, GROUP_SIZE..=SEG_CAP
    mask: usize,                           // capacity - 1
    pub items: usize,                      // live entries, main region + stash
    pub growth_left: usize,                // inserts allowed before a split/grow is forced
    pub local_depth: u8,                   // this segment's extendible-hashing depth
    pub stash_ctrl: [u8; STASH_CAP],       // inline fingerprint array for the 4 stash slots
    pub stash_count: u8,
}
```

`RawSegment` derives neither `Clone` nor `Debug`; it is move-only, owned exclusively by the `Vec<RawSegment>`
inside `RudisFlatTable`.

A segment starts small (`GROUP_SIZE` = 16 slots) and doubles in place (§5.2, case 2) up to `SEG_CAP` = 1024
slots before the extendible-hashing directory ever gets involved — so a shard holding only a few keys never
pays for a 1024-slot segment.

`RawSegment::new` (`src/table.rs:1597–1615`):

```rust
pub fn new(capacity: usize, local_depth: u8) -> Self {
    let cap = capacity.next_power_of_two().clamp(GROUP_SIZE, SEG_CAP);
    let ctrl = vec![EMPTY; cap + GROUP_SIZE];
    let mut slots = Vec::with_capacity(cap + STASH_CAP);
    slots.resize_with(cap + STASH_CAP, || None);
    let stash_bonus = if cap == SEG_CAP { STASH_CAP } else { 0 };
    Self {
        ctrl, slots, capacity: cap, mask: cap - 1, items: 0,
        growth_left: (cap * 7) / 8 + stash_bonus,
        local_depth, stash_ctrl: [EMPTY; STASH_CAP], stash_count: 0,
    }
}
```

**Gotcha**: the 4 stash slots are *physically allocated* in `slots` for every segment regardless of size
(`cap + STASH_CAP`), but `growth_left` only credits them (`stash_bonus`) once the segment has reached full
`SEG_CAP` (1024) capacity. A sub-`SEG_CAP` segment can still technically write into its stash region via the
same index arithmetic, but `growth_left` will hit zero slightly earlier than the segment's true physical
capacity for such segments — not a correctness bug (it just triggers an in-place doubling one insert sooner
than strictly necessary), but worth knowing if you're reasoning about exact growth-left arithmetic.

**Segment memory footprint, computed from `size_of::<Option<RudisEntry>>() == 88`** (see §3.3 below, and the
`test_rudis_entry_size` unit test at `src/table.rs:14975–14986` which asserts this):

| Component | Size (full, 1024-slot segment) |
| :--- | :--- |
| `ctrl: Vec<u8>` | `1024 + GROUP_SIZE(16)` = 1,040 bytes |
| `slots: Vec<Option<RudisEntry>>` | `(1024 + STASH_CAP(4)) * 88` = 90,464 bytes |
| `stash_ctrl` | 4 bytes (inline, no heap) |
| scalar fields (`capacity`, `mask`, `items`, `growth_left`, `local_depth`) | ~33 bytes (padded) |
| **Total (heap-allocated, per full segment)** | **≈ 91,504 bytes (≈ 89.4 KiB)** |

**Correction / stale-comment flag**: the source comment directly above `SEG_CAP` (`src/table.rs:1577`) reads
`"1024 main SIMD slots per segment (~48KB, L1/L2 cache resident)"`. The actual computed size of a full
segment's backing storage is **~89 KiB, not ~48 KiB** — off by roughly 1.9x at the current 88-byte
`RudisEntry` size. This looks like a comment that was never updated after `RudisEntry`'s layout was last
touched (`e3bfe9a`, "shrink RudisValue to 40B, RudisEntry to 88B"); either the comment predates that change
or was simply never re-derived. Flagging here so it doesn't get propagated as fact.

### 3.2 The overflow stash, in detail

`STASH_CAP = 4` per segment. The stash is **not** a textbook DashTable two-choice/displacement structure —
it is a flat 4-slot side array checked by linear scan with a fingerprint pre-filter, reachable only after a
probe has already walked 2 full 16-byte SIMD groups (32 main-region slots) without finding the key or an
empty slot:

```rust
// RawSegment::find_entry, src/table.rs:1676-1729 (structure shared by find_entry_mut and
// find_or_prepare_insert_raw)
loop {
    let (match_mask, empty_mask) = probe_group_match_or_empty(...);   // SIMD compare, 16 ctrl bytes
    // ... check match_mask for a key hit, return on empty_mask != 0 ...
    step += GROUP_SIZE;                     // triangular probe step, see §5.1
    if step == 2 * GROUP_SIZE && self.stash_count > 0 {
        for s in 0..STASH_CAP {
            if self.stash_ctrl[s] == tag {
                let slot_idx = self.capacity + s;   // stash slots live at indices [capacity, capacity+4)
                // fast_slice_eq key compare, return on hit
            }
        }
    }
    if step >= self.capacity { return None; }   // bounded probe — never loops forever
    idx = (idx + step) & self.mask;
}
```

On insert (`find_or_prepare_insert_raw`, `src/table.rs:1805–1881`), if no free slot has been found in the
main region by the time `step == 2 * GROUP_SIZE` and the stash isn't full, a stash slot becomes the
candidate insert index — this is what actually bounds a segment's worst-case probe chain: any key that would
otherwise require a long tail-probe past 32 main-region slots gets shunted into the stash instead. Stash
slots are addressed as `capacity + stash_index` in the same `usize` index space as main-region slots, so
`insert_at`/`remove`/`remove_present` (`src/table.rs:1884–1962`) branch on `insert_idx < self.capacity` to
decide whether to touch `ctrl`/`set_ctrl` or `stash_ctrl`/`stash_count`.

Removing the last item from a segment (`items == 0`) resets **both** the main `ctrl` array to `EMPTY` and the
stash (`stash_ctrl = [EMPTY; STASH_CAP]`, `stash_count = 0`), and restores `growth_left` including the stash
bonus if the segment is full-sized — the same "cheap full-reset" optimization the old flat table had,
extended to the stash.

### 3.3 `RudisEntry` — the inlined key/value/TTL slot

```rust
// src/table.rs:1476-1481
#[derive(Clone, Debug)]
pub struct RudisEntry {
    pub key: Bytes,                  // 32 bytes
    pub val: RudisValue,             // 40 bytes
    pub expire_at: Option<Instant>,  // 16 bytes
}
```

Verified by the in-file unit test (`src/table.rs:14975–14986`):

```rust
assert_eq!(std::mem::size_of::<RudisValue>(), 40);
assert_eq!(std::mem::size_of::<RudisEntry>(), 88);
assert_eq!(std::mem::size_of::<Option<RudisEntry>>(), 88);   // niche-optimized, no size increase
```

`align_of::<RudisEntry>() == 8` — it is not cache-line-aligned, and at 88 bytes it never fits in a single
64-byte cache line regardless of alignment.

### 3.4 Hashing, fingerprinting, and SIMD group comparison

```rust
// src/table.rs:1483-1499
#[inline(always)]
pub fn hash_key(key: &[u8]) -> u64 {
    hash64(key)   // fxhash::hash64 — see docs/design/rudis_table.md §4 for rationale
}

#[inline(always)]
fn mix_hash(mut h: u64) -> u64 {
    h ^= h >> 32;
    h = h.wrapping_mul(0xd6e8feb86659fd93);
    h ^= h >> 32;
    h
}

#[inline(always)]
pub fn fingerprint(hash: u64) -> u8 {
    (hash >> 57) as u8 & 0x7F   // top 7 bits of the (mixed) hash
}
```

`mix_hash` is the **single-pass hash avalanche** finalizer added in `b3d3362` (a truncated MurmurHash3
`fmix64`-style xorshift/multiply/xorshift — one multiply, not the usual two). `RudisFlatTable` applies
`mix_hash` to the raw `fxhash::hash64` output before doing anything else with it — directory indexing
(`dir_index`, §5.2) and segment-local probing both operate on the *mixed* hash, and `fingerprint()` is always
computed on that mixed value too. FxHash's own multiplicative step is fast but has known weaker
high-bit mixing for directory-index selection (which reads high bits via `>> SEG_SHIFT`); the avalanche pass
exists specifically to make the directory-index bits and the fingerprint bits behave independently of each
other and of the raw key content.

SIMD group comparison (`src/table.rs:1501–1574`): on `x86_64`, `probe_group_match_or_empty` and
`probe_group_match_del_empty` use `_mm_loadu_si128` + `_mm_cmpeq_epi8` + `_mm_movemask_epi8` to compare all
16 control bytes against a target fingerprint (and, in the `_del_empty` variant, `DELETED`/`EMPTY` too) in
one pair of vector instructions, yielding 16-bit match bitmasks. On any other target architecture, both
functions fall back to a **portable scalar loop** over the same 16 bytes
(`#[cfg(not(target_arch = "x86_64"))]`, `src/table.rs:1538–1574`) — there is still no NEON or other non-x86
SIMD implementation (unchanged from the prior revision of this document; see §9's future-improvements list).

Constants: `GROUP_SIZE = 16`, `EMPTY = 0xFF`, `DELETED = 0xFE` (`src/table.rs:8–11`, unchanged).

`fast_slice_eq` (`src/table.rs:1627–1672`) is a hand-rolled key-byte comparator added in `8fbc821`: for keys
up to 32 bytes it reads two (or four) overlapping `u32`/`u64` words via unaligned pointer loads instead of
calling `<[u8]>::eq`, checking the first and last word(s) of the slice so short keys are compared with 1–2
integer comparisons instead of a byte-by-byte loop; it falls back to `a == b` above 32 bytes. Every probe
function (`find_entry`, `find_entry_mut`, `find_or_prepare_insert_raw`) uses this for the final key-equality
check after a fingerprint match.

### 3.5 `RudisFlatTable` — the extendible-hashing directory over segments

```rust
// src/table.rs:1974-1984
/// Dragonfly-style Extendible Hashing Table (`Directory` + Fixed-Size SIMD `RawSegment`s).
/// Eliminates monolithic stop-the-world resizes and `old_table` double-lookup overhead.
pub struct RudisFlatTable {
    pub segments: Vec<RawSegment>,
    pub directory: Vec<u32>,     // len == 2^global_depth; directory[i] = index into `segments`
    pub global_depth: u8,
    dir_mask: usize,             // directory.len() - 1
    pub capacity: usize,         // sum of all segments' physical capacity
    pub items: usize,
    pub slot_counts: Box<[u32; 16384]>,
}
```

A freshly created table (`RudisFlatTable::new`, `src/table.rs:1987–1999`) has exactly **one** segment and a
one-entry directory (`global_depth = 0`, `directory = vec![0]`) — the extendible-hashing machinery is
entirely dormant until that single segment fills up and needs to split.

`dir_index` (`src/table.rs:2001–2004`) picks a directory slot from the *mixed* hash:

```rust
fn dir_index(&self, mixed_hash: u64) -> usize {
    ((mixed_hash >> SEG_SHIFT) as usize) & self.dir_mask
}
```

i.e. it uses the bits of the hash *above* the 10 bits (`SEG_SHIFT`) already consumed by in-segment probing —
the same hash value locates both the segment (via the directory) and the slot within that segment (via
`(h as usize) & seg.mask`), with no overlap between the two bit ranges as long as `global_depth` stays within
the width of a `u64` (in practice `global_depth` only grows a few bits before a shard would hold many
millions of segments × 1024 slots, so this is not a practical concern).

A **global index** (the `usize` returned by `find_entry`/`find_or_prepare_insert`/etc. and stored by callers
like `hset_slice_fast`) packs a segment id and a segment-local slot index into one integer:
`global_idx = (seg_id << GLOBAL_IDX_SHIFT) | local_idx`, with `GLOBAL_IDX_SHIFT = 11` (2,048) comfortably
covering the largest possible local index (`SEG_CAP + STASH_CAP - 1` = 1,027).

---

## 4. Hashing → Directory → Segment → Slot: the Full Lookup/Insert Path

### 4.1 Lookup (`RudisFlatTable::find_entry`, `contains`, `find_entry_mut`)

```rust
// src/table.rs:2074-2085 (find_entry; contains and find_entry_mut are structurally identical)
pub fn find_entry(&self, key: &[u8], hash: u64) -> Option<(usize, &RudisEntry)> {
    if self.items == 0 { return None; }
    let h = mix_hash(hash);
    let dir_idx = self.dir_index(h);
    let seg_id = self.directory[dir_idx] as usize;   // unsafe get_unchecked in the real code
    let seg = &self.segments[seg_id];
    seg.find_entry(key, h).map(|(local_idx, entry)| ((seg_id << GLOBAL_IDX_SHIFT) | local_idx, entry))
}
```

Every lookup is: mix the caller-supplied raw `fxhash::hash64` once, index the directory (O(1), one `Vec`
index), index the segment (O(1), one `Vec` index), then run the triangular SIMD probe described in §3.2
inside that single fixed-size segment. None of `find_entry`/`contains`/`find_entry_mut` evaluate TTL — that
is layered on top by `RudisTable` (§6).

### 4.2 Insert (`find_or_prepare_insert`, `insert`, `insert_prepared`)

```rust
// src/table.rs:2117-2136
pub fn find_or_prepare_insert(&mut self, key: &[u8], hash: u64) -> (Option<usize>, usize) {
    let h = mix_hash(hash);
    let mut dir_idx = self.dir_index(h);
    let mut seg_id = self.directory[dir_idx] as usize;

    if self.segments[seg_id].growth_left == 0 {
        if let Some((local_idx, _)) = self.segments[seg_id].find_entry(key, h) {
            return (Some(global_idx), global_idx);   // key already exists — no split needed
        }
        self.split_or_grow_segment(dir_idx, seg_id, h);   // §5
        dir_idx = self.dir_index(h);
        seg_id = self.directory[dir_idx] as usize;
    }
    let (existing, local_idx) = self.segments[seg_id].find_or_prepare_insert_raw(key, h);
    // pack (seg_id, local_idx) into a global index and return
}
```

Note the ordering: a segment at `growth_left == 0` is checked for a **key match first** (an update to an
existing key never needs to trigger a split), and only a genuinely new key triggers
`split_or_grow_segment`. `insert` (`src/table.rs:2154–2171`) and `insert_prepared`
(`src/table.rs:2173–2187`, used by callers that already ran `find_or_prepare_insert` once, e.g.
`hset_slice_fast`) both also maintain `slot_counts` (§8) — but only when
`crate::cluster::HAS_ACTIVE_CLUSTER` is `true` at the moment of the mutation, checked on every single
insert/remove.

### 4.3 Removal (`remove_key`, `remove`, `remove_present`)

`src/table.rs:2189–2226`. All three unpack the global index back into `(seg_id, local_idx)` and delegate to
`RawSegment::remove`/`remove_present` (§3.2), then decrement `RudisFlatTable::items` and `slot_counts` (again
gated on `HAS_ACTIVE_CLUSTER`).

---

## 5. Extendible Hashing: Segment Splitting, and What Happened to Incremental Rehashing

This is the section that changed the most since the last revision and is the central mechanism of the
current design. Read it in full.

### 5.1 `split_or_grow_segment` — the four-way decision tree

```rust
// src/table.rs:2006-2072 (abridged, control flow preserved)
fn split_or_grow_segment(&mut self, mut dir_idx: usize, mut seg_id: usize, mixed_hash: u64) {
    while self.segments[seg_id].growth_left == 0 {
        let seg_items = self.segments[seg_id].items;
        let seg_cap = self.segments[seg_id].capacity;

        // 1. Tombstone-dominated (< 50% live): compact THIS SEGMENT ONLY in place.
        if seg_items * 2 < seg_cap {
            self.segments[seg_id].rebuild(seg_cap);   // re-probe every live entry into a fresh same-size segment
            break;
        }

        // 2. Genuinely full, but hasn't reached SEG_CAP (1024) yet: double THIS SEGMENT ONLY in place.
        if seg_cap < SEG_CAP {
            self.segments[seg_id].rebuild((seg_cap * 2).min(SEG_CAP));
            self.capacity = /* segments.len()==1 ? new_cap : segments.len() << SEG_SHIFT */;
            break;
        }

        // 3. At SEG_CAP and genuinely full: an Extendible-Hashing SPLIT.
        let d = self.segments[seg_id].local_depth;
        if d == self.global_depth {
            // Directory doesn't have enough resolution to distinguish the two new segments —
            // double the directory (append a copy of itself) and bump global_depth first.
            let len = self.directory.len();
            for i in 0..len { self.directory.push(self.directory[i]); }
            self.global_depth += 1;
            self.dir_mask = self.directory.len() - 1;
            dir_idx = self.dir_index(mixed_hash);
        }

        // Split this segment's SEG_CAP entries into two new SEG_CAP segments by bit `SEG_SHIFT + d`.
        let mut seg_zero = RawSegment::new(SEG_CAP, d + 1);
        let mut seg_one = RawSegment::new(SEG_CAP, d + 1);
        let bit_shift = SEG_SHIFT + (d as usize);
        for entry in self.segments[seg_id].slots.drain(..).flatten() {
            let h = mix_hash(hash_key(&entry.key));
            if ((h >> bit_shift) & 1) == 0 { seg_zero.insert_migrated(entry, h); }
            else                           { seg_one.insert_migrated(entry, h); }
        }
        self.segments[seg_id] = seg_zero;
        let new_seg_id = self.segments.len();
        self.segments.push(seg_one);
        self.capacity = self.segments.len() << SEG_SHIFT;

        // Retarget every directory entry that pointed at the old segment and whose extra bit is 1.
        let base = dir_idx & ((1usize << d) - 1);
        let step = 1usize << (d + 1);
        let mut i = base | (1usize << d);
        while i < self.directory.len() {
            self.directory[i] = new_seg_id as u32;
            i += step;
        }
        dir_idx = self.dir_index(mixed_hash);
        seg_id = self.directory[dir_idx] as usize;
        // loop again — the segment the caller actually wanted to insert into might itself
        // still be full if it just got unlucky and both halves of the split landed full
    }
}
```

**Four distinct outcomes, cheapest first**: (1) in-place tombstone compaction of one segment, (2) in-place
doubling of one sub-1024 segment, (3) a real extendible-hashing split of one 1024-slot segment into two
1024-slot segments (with the directory doubled first, but *only* when this segment's `local_depth` has
caught up to `global_depth` — most splits do **not** require a directory doubling, only a directory
retarget), (4) — implicit — the `while` loop simply repeats if, after any of the above, the target segment
is *still* full (rare, but possible after case 3 if the split was maximally unlucky).

**Every one of these operations touches at most one segment's worth of data** — at most 1,024–2,048 entries
(a split reads ≤1,028 entries out of the old segment and writes them into two new ones) — regardless of how
many billions of keys live in the rest of the table. This is the structural property that makes a
large-table insert's worst-case cost bounded and predictable, and it is the entire reason the mechanism
described next exists only as dead code.

### 5.2 `is_rehashing` / `rehash_step` / `finish_rehash` / `migrate_key_if_in_old` / `prepare_key_lookup` — **dead stubs**

```rust
// src/table.rs:2138-2152 (RudisFlatTable); src/table.rs:2701-2723 (RudisTable forwards to these 1:1)
pub fn is_rehashing(&self) -> bool { false }
pub fn migrate_key_if_in_old(&mut self, _key: &[u8], _hash: u64) {}
pub fn rehash_step(&mut self, _n: usize) -> bool { false }
pub fn finish_rehash(&mut self) {}
// RudisTable::prepare_key_lookup, src/table.rs:2723, is also a no-op: fn prepare_key_lookup(&mut self, _key: &[u8], _hash: u64) {}
```

This is directly relevant to the task brief for this document, which asked specifically about "progressive
incremental table rehashing" mentioned in a recent commit message. Here is the full history, reconstructed
from `git log -- src/table.rs`:

1. **`e244128`** ("progressive incremental table rehashing for latency spike elimination") added a real
   cooperative rehash protocol to the *old* flat single-array `RudisFlatTable`: an `old_table` field, a
   `rehash_step(n)` that migrated `n` buckets per call, and `migrate_key_if_in_old` to check the old array on
   a miss against the new one — the classic "rehash a little on every operation" design (similar in spirit to
   Redis's own incremental rehashing).
2. **`c956deb`** and **`62b25b5`** ("bound probe loops and enforce proactive resizing", "bound SwissTable probe
   loops, respect `sched_getaffinity`") landed fixes on top of that design shortly after.
3. **`4713691`** ("implement Dragonfly-style extendible hashing directory") **replaced the entire flat-array +
   `old_table` design** with the segmented directory structure described in §3.5/§5.1. Because a segment
   split now costs O(1,024) in the worst case regardless of total table size, there is no longer a "large
   monolithic rehash" to spread incrementally across calls — the problem the incremental rehash protocol
   existed to solve was designed away at the data-structure level instead.
4. **`b3d3362`** kept the five methods above as no-op stubs (`is_rehashing` unconditionally `false`,
   `rehash_step` unconditionally `false`/no-op) rather than deleting them.

**Verified**: `grep -rn 'is_rehashing\|rehash_step\|finish_rehash\|migrate_key_if_in_old\|prepare_key_lookup'
src/*.rs` outside `table.rs` returns **zero matches** — nothing anywhere else in the codebase calls any of
these five methods. They are pure vestigial API surface from the superseded design, currently unreachable
dead code kept (presumably) for source compatibility with external callers or tests that may not currently
exist. If you're looking for "the incremental rehashing feature," there isn't one in the sense of a
multi-call cooperative protocol — the thing that made it unnecessary is the extendible-hashing segment cap.

### 5.3 `RawSegment::rebuild` — the actual per-segment "resize"

```rust
// src/table.rs:1964-1971
pub fn rebuild(&mut self, new_cap: usize) {
    let mut next = RawSegment::new(new_cap, self.local_depth);
    for entry in self.slots.drain(..).flatten() {
        let h = mix_hash(hash_key(&entry.key));
        next.insert_migrated(entry, h);
    }
    *self = next;
}
```

This is a monolithic rehash — but monolithic *within one segment*, i.e. bounded at `SEG_CAP` (1,024) entries
maximum. There is no time-sliced/incremental version of even this — it's a synchronous loop over the
segment's own slots — but because the segment is capped, "monolithic" here means "at most ~1K entries," not
"the whole table."

### 5.4 `RudisFlatTable::defrag` — whole-table maintenance

```rust
// src/table.rs:2330-2365 (abridged)
pub fn defrag(&mut self) -> usize {
    let optimal_cap = (self.items * 2).next_power_of_two().max(64);
    if optimal_cap <= SEG_CAP && (self.segments.len() > 1 || optimal_cap < before_cap || has_del) {
        // Collapse the ENTIRE table back down to a single segment (even if it was split into many)
        // whenever the live item count would now fit in one segment.
        // ... rebuilds everything into one new RawSegment, resets directory to `vec![0]`, global_depth = 0 ...
    } else if has_del {
        // Otherwise, just rebuild any individual segment that currently holds a tombstone.
        for seg in self.segments.iter_mut() {
            if seg.ctrl.contains(&DELETED) { let cap = seg.capacity; seg.rebuild(cap); }
        }
    }
}
```

This is the one place a whole-table structural collapse can still happen — but only when a table that had
grown past one segment has since shrunk back down to fitting in one again (e.g. after a mass `DEL`). It is
reachable end-to-end via `RudisTable::active_defrag` → `ShardDb::active_defrag` (`src/shard.rs:815`) → a
cross-shard `ShardMessage::ActiveDefrag` round-trip (`src/router.rs:1626`, `src/server.rs:494`) — i.e. it is
operator-triggered maintenance (`connection.rs:8759`), not something the table runs on its own initiative.
`active_defrag` additionally `Bytes::copy_from_slice`s every live `String`/`SmallHash` field/value to release
slack capacity, then calls `recalculate_used_memory` (§7).

---

## 6. The Real `RudisValue` Enum

```rust
// src/table.rs:1416-1431
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
    Cooled { ptr: TieredPointer, val: Box<RudisValue> },
}
```

Unchanged in shape from the prior revision: all four collection-carrying variants remain boxed, which is
precisely what keeps `size_of::<RudisValue>()` at 40 bytes (verified by the unit test, §3.3) instead of
ballooning to the size of the largest possible collection header. There is no `Bitmap` variant
(`SETBIT`/`GETBIT` operate directly on `RudisValue::String` bytes) and no `Json` variant (handled entirely by
`src/json.rs`).

`approx_bytes()` (`src/table.rs:1434–1448`) — the per-variant memory heuristic behind `used_memory` — is also
unchanged:

| Variant | Heuristic |
| :--- | :--- |
| `String` | `len()` |
| `Int` | `8` |
| `SmallHash` | `Σ (k.len() + v.len() + 16)` per pair |
| `Hash` | `Σ (k.len() + v.len() + 32)` per pair |
| `List` | `Σ (b.len() + 16)` per element |
| `Set` | `len() * 32` — flat estimate, ignores actual member byte length |
| `ZSet` | `len() * 48` — likewise flat |
| `HyperLogLog` | `16384` (fixed) |
| `Stream` | `len() * 64` |
| `Tiered` | `24` |
| `Cooled { val, .. }` | `24 + val.approx_bytes()` |

---

## 7. Compact Encodings Deep-Dive

Still **no Listpack, Intset, or pointer-based skiplist anywhere in this file.** Three of the eleven
`RudisValue` variants have a genuine small/full adaptive representation; the rest are fixed Rust types. One
variant (`HyperLogLog`) has an important new nuance described in §7.5.

### 7.1 Strings: `String(Bytes)` vs. `Int(i64)`

`set_extended_with_hash` (`src/table.rs:3123`) parses every incoming value as a 64-bit integer first
(`parse_i64_bytes`, hand-written ASCII digit loop, no `str::parse`); a clean parse stores `RudisValue::Int`,
otherwise `RudisValue::String`. `format_i64` special-cases `0`–`10` and `-1` as `Bytes::from_static` to avoid
allocation for the most common small integers.

### 7.2 Hashes: `SmallHash(Vec<(Bytes, Bytes)>)` vs. `Hash(Box<RudisHashMap>)`

Thresholds are **runtime-configurable process-wide atomics** owned by `connection.rs`:

```rust
// src/connection.rs:1006-1008
pub static HASH_MAX_ENTRIES: AtomicUsize = AtomicUsize::new(512);
pub static HASH_MAX_VALUE: AtomicUsize = AtomicUsize::new(64);
```

`hset` now dispatches through `hset_slice_fast` (`src/table.rs:4746`), a perf-optimized path added in the
`fc3345b`/`68fd391` series that uses `find_entry_mut`/`find_or_prepare_insert` directly instead of a second
lookup; `hsetnx` (`src/table.rs:5026–5086`) shows the promotion check most plainly:

```rust
let should_promote = pairs.len() >= max_entries || field.len() > max_value || value.len() > max_value;
pairs.push((field, value));
if should_promote {
    let map: RudisHashMap = pairs.drain(..).collect();
    entry.val = RudisValue::Hash(Box::new(map));
}
```

`SmallHash` is a linearly-searched `Vec` of pairs, not a byte-packed buffer. New hashes are born `SmallHash`
(acquired from the arena, §11) unless a field/value already exceeds `max_value`, in which case they're born
directly as `Hash`.

### 7.3 Sets: `RudisSet::Small(Vec<SmallSetEntry>)` vs. `Full(HashSet<Bytes, FxBuildHasher>)`

```rust
// src/table.rs:662-673
const SMALL_SET_LIMIT: usize = 64;   // still a hardcoded constant, not runtime-configurable

pub struct SmallSetEntry { pub hash: u64, pub member: Bytes }

pub enum RudisSet {
    Small(Vec<SmallSetEntry>),
    Full(hashbrown::HashSet<Bytes, FxBuildHasher>),
}
```

Each small-form entry caches `fxhash::hash64(member)`; `contains`/`insert`/`remove` compare the cached `u64`
before falling back to a byte comparison, so a miss on a large member is rejected without touching the member
bytes. Crossing `SMALL_SET_LIMIT` (64 entries) converts to `Full` by draining the `Vec`. Deletion from the
small form uses `Vec::swap_remove` — O(1) but reorders the vector. `RudisSetIter` (`src/table.rs:683–706`)
unifies iteration over both representations.

### 7.4 Sorted sets: `RudisZSet::Small(Vec<(OrderedScore, Bytes)>)` vs. `Full { dict, tree }`

```rust
// src/table.rs:144, 147-153
const SMALL_ZSET_LIMIT: usize = 64;   // still hardcoded

pub enum RudisZSet {
    Small(Vec<(OrderedScore, Bytes)>),
    Full {
        dict: hashbrown::HashMap<Bytes, f64>,               // NOT FxBuildHasher — default hashbrown hasher
        tree: std::collections::BTreeSet<(OrderedScore, Bytes)>,
    },
}
```

`OrderedScore(f64)` (`src/table.rs:14–29`) wraps `f64::total_cmp` so `(OrderedScore, Bytes)` can live in a
`BTreeSet`/be `binary_search`-ed. The small form's `insert` (`src/table.rs:198–235`) locates the insertion
point with `binary_search` (O(log n)) but `Vec::insert` still shifts every following element (O(n) overall).
Crossing `SMALL_ZSET_LIMIT` converts to `Full`, pairing a `HashMap<Bytes, f64>` with a
`BTreeSet<(OrderedScore, Bytes)>`.

**`rank()` is still not O(log n) in either representation** (`src/table.rs:254–274`) — `BTreeSet` has no
built-in rank/order-statistics operation, so `ZRANK`/`ZREVRANK` pay an O(n) `.position()` walk regardless of
representation.

**`range()` by score, however, is genuinely sub-linear in the `Full` form**: `ZRangeOpts::by_score` uses
`tree.range((Included((OrderedScore(min), Bytes::new())), Unbounded)).take_while(...)`
(`src/table.rs:364–378`), i.e. an O(log n) `BTreeSet::range` seek to the start bound followed by an O(k) walk
for the `k` matching elements — `ZRANGEBYSCORE` is efficient; only `ZRANK`/`ZREVRANK` are the linear
outlier.

### 7.5 HyperLogLog: `HyperLogLog(Box<[u8; 16384]>)` is now a **legacy-only read path** — live writes use sparse `String`

This is a real behavioral change since the prior revision of this document, uncovered by tracing every
constructor of `RudisValue::HyperLogLog` in the current source.

`pfadd` (`src/table.rs:11123–11181`) — the only way to create a fresh HLL via `PFADD` — **never constructs
`RudisValue::HyperLogLog`**. A brand-new key is created via `crate::hll::hll_create_sparse_empty()` and
stored as a plain `RudisValue::String`:

```rust
// pfadd, new-key branch, src/table.rs:11160-11168
let bytes = if elements.is_empty() {
    crate::hll::hll_create_sparse_empty()
} else {
    let mut b = crate::hll::hll_create_sparse_empty();
    let _ = crate::hll::hll_add(&mut b, elements)?;
    b
};
self.set(key, Bytes::from(bytes), None);
```

`pfadd`/`pfcount`/`pfmerge` all branch on the *existing* value's variant and handle **both**
`RudisValue::String` (validated/decoded via `crate::hll::hll_validate`/`hll_decode_registers`, which
transparently understands both Redis's real sparse run-length format and the dense format — see `src/hll.rs`)
and `RudisValue::HyperLogLog` (the legacy dense box, read directly). But a `grep` for every construction site
of `RudisValue::HyperLogLog` across the entire codebase (not just reads/matches) turns up exactly one place
that builds it: **`RESTORE`/DUMP payload deserialization**, type tag `5`
(`src/table.rs:12717–12725`), for loading an old DUMP blob that was serialized back when the dense box was
the live representation. Everywhere else `RudisValue::HyperLogLog(regs)` appears in `table.rs` is a
match-arm that *reads* an already-existing dense box (`pfadd`, `pfcount`, `pfmerge`, `pfdebug_*`,
`get_with_hash`, `write_get_resp`, DUMP-serialization for an already-dense key, etc.) — none of them create
one from scratch.

**Practical upshot**: on a table with no `RESTORE`d legacy DUMP payloads, `RudisValue::HyperLogLog` never
appears at all; every live HLL is a `RudisValue::String` holding Redis's real sparse-or-dense byte encoding
(auto-promoted from sparse to dense by `crate::hll::hll_add`/`hll_count` once a register update makes the
sparse form larger than the dense form, mirroring real Redis's own HLL promotion behavior — `PFDEBUG
ENCODING`/`PFDEBUG TODENSE` at `src/table.rs:11365–11402` expose this state for introspection/testing). This
supersedes the prior revision's claim that HLLs are "always a dense `Box<[u8; 16384]>`... used
unconditionally" — that was accurate before the sparse-HLL work (`b85b6a7`) landed; it no longer is.

### 7.6 Lists: always `VecDeque<Bytes>`

Unchanged: `RudisValue::List(std::collections::VecDeque<Bytes>)`, one representation regardless of length.
`LPUSH`/`RPUSH`/`LPOP`/`RPOP` map directly to `push_front`/`push_back`/`pop_front`/`pop_back`; no
compact/large-list distinction, no listpack-equivalent small form.

### 7.7 Streams: `BTreeMap<StreamId, Vec<(Bytes, Bytes)>>` plus substantially more bookkeeping than before

```rust
// src/table.rs:928-931, 987-995, 1076-1172 (all current fields)
pub struct StreamId { pub ms: u64, pub seq: u64 }

pub enum StreamAddId { Auto, AutoSeq(u64), Explicit(StreamId) }

#[repr(u8)]
pub enum StreamTrimStrategy { KeepRef = 0, DelRef = 1, Acked = 2 }

pub enum StreamIdmpOption {
    Manual { producer: Bytes, iid: Bytes },
    Auto { producer: Bytes },
}

pub enum StreamAddResult { Added(StreamId), Duplicate(StreamId), NoMkStream }

pub struct IdmpProducer {
    pub iids: HashMap<Bytes, (StreamId, u64)>,        // internal-id -> (assigned StreamId, timestamp)
    pub order: std::collections::VecDeque<Bytes>,      // insertion order, for IDMP window eviction
}

pub struct StreamPelEntry {
    pub consumer: Bytes,
    pub delivery_time_ms: u64,
    pub delivery_count: usize,
    pub nack_seq: u64,               // NEW: XNACK sequencing
}

pub struct StreamConsumer {
    pub name: Bytes,
    pub seen_time_ms: u64,
    pub active_time_ms: Option<u64>, // NEW
    pub pel: std::collections::BTreeMap<StreamId, u64>,
}

pub struct StreamGroup {
    pub name: Bytes,
    pub last_delivered_id: StreamId,
    pub entries_read: Option<u64>,   // NEW: XINFO GROUPS `entries-read`
    pub consumers: HashMap<Bytes, StreamConsumer>,
    pub pel: std::collections::BTreeMap<StreamId, StreamPelEntry>,
    pub next_nack_seq: u64,          // NEW: monotonic XNACK sequence counter
}

pub struct RudisStream {
    pub entries: std::collections::BTreeMap<StreamId, Vec<(Bytes, Bytes)>>,
    pub last_id: StreamId,
    pub entries_added: u64,                       // NEW: lifetime add counter (survives trims)
    pub max_deleted_entry_id: StreamId,            // NEW
    pub groups: HashMap<Bytes, StreamGroup>,
    pub idmp_duration: Option<u64>,                // NEW: idempotent-producer dedup window (ms)
    pub idmp_maxsize: Option<usize>,               // NEW: per-producer dedup cache size cap
    pub idmp_producers: HashMap<Bytes, IdmpProducer>,  // NEW
    pub iids_added: u64,                           // NEW: stats counters
    pub iids_duplicates: u64,                      // NEW
    pub nodes: std::collections::VecDeque<Vec<StreamId>>,  // NEW: fake "radix tree node" chunks
}
```

`StreamId` (`Ord`-derived) keeps `entries` naturally ordered via the `BTreeMap`. New since the prior revision:

- **Idempotent producers** (`IdmpProducer`, `StreamIdmpOption`, `compute_stream_auto_iid`,
  `STREAM_IDMP_DURATION`/`STREAM_IDMP_MAXSIZE` atomics defaulting to `100`ms/`100` entries,
  `src/table.rs:1097–1131`) — an `XADD` extension letting a producer supply a manual or auto-computed
  internal id (`iid`); a duplicate `iid` within the configured time/size window returns the
  previously-assigned `StreamId` instead of creating a new entry (`StreamAddResult::Duplicate`). This is a
  Rudis-specific idempotent-write extension, not part of standard Redis stream semantics.
- **XNACK support**: `StreamPelEntry.nack_seq` plus `StreamGroup.next_nack_seq`, a monotonically increasing
  per-group counter stamped onto PEL entries so a negative-acknowledgement can reference a specific delivery
  attempt.
- **Fake radix-tree node accounting**: `RudisStream.nodes` is a `VecDeque<Vec<StreamId>>` chunked at
  `STREAM_NODE_MAX_ENTRIES` (default 100, `src/table.rs:1097–1098`) IDs per "node"
  (`rebuild_nodes`/push/remove logic around `src/table.rs:1197–1235`). It has nothing to do with lookup
  performance (the real index is still the `BTreeMap`) — it exists purely so `XINFO STREAM`'s
  `radix-tree-keys`/`radix-tree-nodes` fields (`src/table.rs:14076–14080`, `14212`) can report
  realistic-looking numbers for Redis-compatibility, mirroring the shape (if not the actual implementation)
  of real Redis's listpack-chunked stream storage.

### 7.8 HyperLogLog legacy dense format: still a fixed `Box<[u8; 16384]>` when it exists

See §7.5 — unchanged in byte layout, just no longer constructed by any live write path.

---

## 8. Expiration & Memory Management

### 8.1 Key-level TTLs: `num_expires` fast path — unchanged in spirit, still present everywhere

```rust
// RudisTable::get_with_hash, src/table.rs:2875-2899 (abridged) — the fast-path pattern repeated
// throughout every read/write command in this file
if self.num_expires > 0
    && let Some(expire_at) = entry.expire_at
    && !crate::connection::ALLOW_ACCESS_EXPIRED.load(Ordering::Relaxed)
    && Instant::now() >= expire_at
{
    self.expire_slot(idx);
    return Ok(None);
}
```

`num_expires` — a running count of entries with `expire_at.is_some()` — is checked first, before
`ALLOW_ACCESS_EXPIRED` or any `Instant::now()` call, inlined directly into dozens of hot per-command paths
(`get_with_hash`, `get_compact_with_hash`, `write_get_resp`, `write_hget_resp`, `increx`, …) rather than
funneled through one shared helper everywhere. On a table where no key carries a TTL this collapses
expiration checking to a single integer comparison with no clock read. `expire`/`persist` and every real
eviction/removal path keep it in sync (increment on no-TTL→TTL, decrement on the reverse or on removal of a
TTL-carrying key).

`ALLOW_ACCESS_EXPIRED` (`src/connection.rs:1009`) is a process-wide `AtomicBool` that, when set, disables
expiration checks everywhere regardless of `num_expires` — used for debug/inspection paths that need to see
logically-expired keys.

### 8.2 Active key-level expiration sampling — now segment-aware

```rust
// RudisTable::active_expire_cycle, src/table.rs:10437-10469 (key-level portion)
let bound = self.table.cursor_bound();
let max_scan = bound.min(512);
let mut checked = 0;
let mut slots_scanned = 0;
while checked < 20 && slots_scanned < max_scan && self.num_expires > 0 {
    let cur = self.sample_cursor % bound;
    self.sample_cursor = (self.sample_cursor + 1) % bound;
    let idx = self.table.cursor_to_global_idx(cur);
    if let Some(entry) = self.table.get_slot(idx) && entry.expire_at.is_some() {
        if self.check_expired_slot(idx) { expired_count += 1; inc_expired_keys_active(); }
        checked += 1;
    }
    slots_scanned += 1;
}
```

**Correction vs. the prior revision**: the sampling loop no longer indexes the table directly by a flat
`0..capacity` cursor — it goes through `RudisFlatTable::cursor_bound()`/`cursor_to_global_idx()`
(`src/table.rs:2295–2314`), which account for the segmented layout: with one segment, the cursor space is
just that segment's `slots.len()`; with multiple segments, it's `segments.len() * (SEG_CAP + STASH_CAP)`,
and a cursor value is mapped to a `(seg_id, local_idx)` pair via integer division/modulo by that stride. This
keeps the round-robin sweep correct across an arbitrary number of segments.

Also new: `max_scan = bound.min(512)` bounds how many *cursor positions* a single call will step through (not
just how many TTL'd keys it will expire) — on a huge, TTL-sparse table, the old unconditional 20-key quota
with no positional cap could in principle scan very deep before finding 20 TTL'd keys; this change caps the
positional cost of a single call at 512 slots regardless.

### 8.3 Hash-field-level TTLs (`HEXPIRE`/`HTTL`/`HPERSIST`/`HGETEX`/`HSETEX`) — a second, independent expiry subsystem

```rust
// RudisTable, src/table.rs:2425
pub hash_field_expires: hashbrown::HashMap<Bytes, hashbrown::HashMap<Bytes, Instant>>,
```

This is a completely separate map from the main table: outer key is the hash's Redis key, inner map is
`field -> Instant` for fields that carry a field-level TTL (`HEXPIRE key seconds FIELDS n f1 f2 ...`). It is
**not** reflected in `num_expires` at all — `num_expires` only ever counts whole-key TTLs.

Two mechanisms keep it current:

- **Passive**: `purge_expired_hash_fields(key)` (`src/table.rs:5088–5109`) is called at the top of hash
  read/write commands (`write_hget_resp`, `hexpire`, `httl`, `hgetex`, `hsetex`, `hdel`, …), each gated by
  `if !self.hash_field_expires.is_empty()` so it costs nothing on shards that never used `HEXPIRE`. It
  `retain`s the inner map, collecting expired field names, then calls `self.hdel(key, &expired)` to actually
  remove them from the hash's `RudisValue`.
- **Active**: `evict_expired_hash_fields_sample(max_keys)` (`src/table.rs:10393–10433`), called with
  `max_keys = 8` as the very first thing `active_expire_cycle` does on every invocation
  (`src/table.rs:10439–10441`), before the key-level sampling loop in §8.2 even runs.

**Gotcha — shared cursor, different moduli**: `evict_expired_hash_fields_sample` samples its starting point
via `self.sample_cursor % total` where `total = self.hash_field_expires.len()` — the **same**
`RudisTable::sample_cursor` field the key-level expiration loop in §8.2 advances via `% bound` (the main
table's cursor-space size). `evict_expired_hash_fields_sample` itself never advances `sample_cursor`; it only
reads whatever position the *previous* call's key-level sweep left it at, then reduces that value modulo a
completely different number. In practice this means hash-field sampling does not get a clean, dedicated
round-robin traversal of `hash_field_expires` — its effective starting point on any given call is whatever
pseudo-random residue the key-level sweep's cursor happens to leave behind, modulo the (usually much smaller)
`hash_field_expires.len()`. Not a correctness bug (eventually every key does get sampled, and the passive
path backstops anything missed), but worth knowing if `HEXPIRE`'s active-eviction latency ever needs to be
reasoned about precisely.

### 8.4 Eviction under memory pressure (`try_evict_one_key`) — still no real LRU

```rust
// RudisTable::try_evict_one_key, src/table.rs:2802-2857 (abridged)
pub fn try_evict_one_key(&mut self, policy: &str) -> Option<usize> {
    let bound = self.table.cursor_bound();
    let is_volatile = policy.to_lowercase().starts_with("volatile");
    let mut checked = 0;
    let mut attempts = 0;
    while checked < 10 && attempts < bound {
        let cur = self.sample_cursor % bound;
        self.sample_cursor = (self.sample_cursor + 1) % bound;
        attempts += 1;
        let idx = self.table.cursor_to_global_idx(cur);
        if let Some(entry) = self.table.get_slot(idx) {
            if is_volatile && entry.expire_at.is_none() { continue; }
            if policy.to_lowercase().contains("ttl") {
                // track the minimum expire_at seen so far -> "nearest to expiring wins"
            } else {
                best_slot = Some(idx);   // first sampled occupied slot wins — this is what "*-lru" resolves to
                checked += 1;
            }
        }
    }
    // remove `best_slot`, update used_memory, inc_evicted_keys(), return bytes freed
}
```

Re-verified against the current source, and the conclusion from the prior revision still holds exactly:
**this samples up to 10 occupied slots via the shared `sample_cursor`/`cursor_bound`/`cursor_to_global_idx`
machinery, and only `*-ttl` policies get genuine comparison (nearest expiry wins).** `allkeys-lru` and
`volatile-lru` take the *first* sampled occupied (and, for `volatile-*`, TTL-carrying) slot — there is still
no access-recency clock, second-chance bit, or any other recency signal anywhere in `RudisEntry` or
`RawSegment`/`RudisFlatTable`. As implemented, `allkeys-lru` is indistinguishable from `allkeys-random`, and
`volatile-lru` from `volatile-random`. `try_evict_one_key` also now shares the very same `sample_cursor` as
both expiration sweeps in §8.2/§8.3 — three independent consumers (key-TTL sampling, hash-field-TTL sampling,
and eviction sampling) all advance and read the one `RudisTable::sample_cursor` field, each against its own
modulus. This is a change from the prior revision, which described `sample_cursor` as dedicated to
active-expiration only (and a separate `spill_cursor` for tiering, which remains genuinely independent — see
§10).

---

## 9. MGET/MSET Direct Buffer Serialization

The commit that introduced this (`b3d3362`) mostly touched `connection.rs`/`router.rs`/`mailbox.rs`, not
`table.rs` — the bulk of the batching/response-gathering machinery (`write_resp_bulk_bytesmut`,
`write_resp_array_header_bytesmut` in `connection.rs`, `Router::begin_mget_resp`/`finish_mget_resp`/
`begin_mset`/`finish_mset` in `router.rs`, and the scatter/gather descriptors in `mailbox.rs`) lives outside
this file's scope — see `docs/internal/04_sharding_mesh.md` for that side. `table.rs`'s contribution is the
primitive the fast paths call directly:

- **Single pre-computed hash, reused for routing and lookup.** `Router::begin_mget_resp`
  (`src/router.rs:1274`) computes `target_shard_and_hash(key)` once per key to decide local-vs-remote
  routing, then — for keys that land on the current shard — passes that *same* hash straight into
  `RudisTable::get_with_hash(key, key_hash)` (`src/router.rs:1328`) instead of calling plain `get(key)`,
  which would recompute `hash_key` a second time.
- **A family of `write_*_resp` methods that serialize straight into the caller's output `Vec<u8>`,
  bypassing an intermediate `Bytes`/`Vec<Bytes>` allocation entirely**: `write_get_resp`
  (`src/table.rs:2946`), `write_hget_resp` (`5850`), `write_lpop_resp`/`write_lpop_resp_with_hash` (`6778`,
  `6857`), `write_rpop_resp`/`write_rpop_resp_with_hash` (`6868`, `6947`), `write_lrange_resp` (`7304`),
  `write_sismember_resp` (`8189`), `write_zrange_resp` (`9823`). Each does the TTL check and
  `RudisValue` match internally, then calls `crate::connection::write_resp_bulk`/`write_resp_null` directly
  on the passed-in buffer rather than returning an owned value for the caller to re-serialize.
- The genuinely-local (`num_shards <= 1`, or all keys land on the current shard) MGET fast path in
  `router.rs` reserves the output buffer once (`total_keys * 32 + 16`) and writes the RESP array header plus
  every element directly, with **zero channel operations and zero intermediate per-key allocation** — this
  is the "direct buffer serialization" the commit message refers to, and `get_with_hash` (not a
  batch-oriented table.rs API) is the primitive it's built on.

There is no dedicated "batch get" or "batch set" method inside `RudisTable` itself — MGET/MSET batching is
entirely a `router.rs`/`connection.rs`-level concern layered on top of table.rs's ordinary single-key,
hash-reusing primitives.

---

## 10. NVMe Tiering Hooks — unchanged behavior, adapted to the segmented layout

Built around two `RudisValue` variants (`src/table.rs:1401–1430`):

```rust
pub struct TieredPointer { pub file_id: u32, pub offset: u64, pub length: u32, pub value_type: u8 }
// RudisValue::Tiered(TieredPointer)                                — value lives only on disk
// RudisValue::Cooled { ptr: TieredPointer, val: Box<RudisValue> }  — on disk AND still cached in RAM
```

`get_with_hash` and friends unwrap `Cooled` transparently (`match &entry.val { Cooled { val, .. } =>
val.as_ref(), other => other }`). Relevant methods, current line numbers:

- `get_hot_keys_for_spill(&mut self, limit)` (`src/table.rs:3765–3788`): round-robins `spill_cursor` (now
  over `cursor_bound()`/`cursor_to_global_idx()`, same segment-aware adaptation as §8.2's sampling) collecting
  keys whose value isn't already `Tiered`/`Cooled`, up to `limit`.
- `restore_tiered_value` (`3716–3731`): `Tiered(ptr)` → `Cooled { ptr, val }`, adds `val.approx_bytes()` to
  `used_memory`.
- `decommit_cooled_key` (`3734–3747`): single-key inverse, `Cooled` → `Tiered`, frees bytes.
- `decommit_all_cooled` (`3749–3763`): the same conversion applied table-wide in one pass.
- `get_value_for_spill` (`3791+`): serializes a non-tiered value's payload/type tag for `src/tiering.rs`,
  lazily expiring the key along the way if it turns out stale.

`spill_cursor` remains a field distinct from `sample_cursor` (unlike the three-way sharing of `sample_cursor`
described in §8.4) — tiering sampling still never perturbs expiration/eviction sampling cadence, or
vice versa.

---

## 11. Cluster Slot Indexing

Unchanged in design from the prior revision — a count-only fixed array, not a reverse key index:

```rust
pub slot_counts: Box<[u32; 16384]>,   // one field on RudisFlatTable, NOT per-segment
```

Incremented/decremented inside `RudisFlatTable::insert`/`insert_prepared`/`remove`/`remove_present`, but
**only when `crate::cluster::HAS_ACTIVE_CLUSTER` is `true` at the moment of the mutation** — with cluster
mode inactive the array is never touched and stays stale/zero.

```rust
// count_keys_in_slot, src/table.rs:9280-9316 (current)
pub fn count_keys_in_slot(&mut self, slot: u16) -> usize {
    if self.table.items == 0 { return 0; }
    if crate::cluster::HAS_ACTIVE_CLUSTER.load(Ordering::Relaxed) {
        if self.table.slot_counts[slot as usize] == 0 { return 0; }
        if self.num_expires == 0 { return self.table.slot_counts[slot as usize] as usize; }
    }
    // full linear scan via self.table.enumerate_slots(), filtering by crate::router::key_slot(&entry.key) == slot,
    // lazily expiring any stale entries encountered along the way
}
```

Same three-way behavior as before: (1) cluster inactive → always a full scan; (2) cluster active, counter is
`0` → O(1) true zero; (3) cluster active, counter `>0`, and **no key anywhere in the table has a TTL**
(`num_expires == 0`) → O(1), counter trusted directly; (4) cluster active, non-empty slot, and *any* key
anywhere in the shard has a TTL → full `O(table capacity)` scan, because the counter might overcount
already-logically-expired-but-not-yet-evicted keys. The only mechanical change is that the fallback scan now
goes through `enumerate_slots()` (`src/table.rs:2258–2267`), a segment-aware iterator
(`segments.iter().enumerate().flat_map(...)`) instead of indexing a flat array directly. `get_keys_in_slot`
(`src/table.rs:9318–9358`) follows the identical shape. This remains a real regression versus a reverse-index
design for cluster-active, non-empty-slot, `num_expires > 0` workloads (see §13).

---

## 12. Memory Accounting

`RudisTable::used_memory` is a running estimate, not an exact allocator query. `RudisTable::new`
(`src/table.rs:2660–2672`) seeds it with the initial table's structural overhead:

```rust
let base_mem = 64 * std::mem::size_of::<Option<RudisEntry>>() + 64 + GROUP_SIZE + 16384 * 4;
//            = 64 * 88                                        + 64 + 16         + 65,536
//            = 5,632                                          + 80             + 65,536  = 71,248 bytes
```

(the `16384 * 4` term is the 64 KiB `slot_counts` array, allocated unconditionally even when cluster mode is
never turned on).

Every mutation updates `used_memory` incrementally (`+= entry_mem` on insert, `-= freed` on remove/expire/
evict, where `freed = key.len() + val.approx_bytes() + 64`). `recalculate_used_memory`
(`src/table.rs:2725–2734`) recomputes it from scratch — `capacity * size_of::<Option<RudisEntry>>() +
ctrl_bytes() + 16384*4`, plus every live entry's `key.len() + approx_bytes() + 64` — used by `active_defrag`
and available for drift-correction. `RudisFlatTable::ctrl_bytes()` (`src/table.rs:2274–2278`) now sums every
segment's `ctrl.len()` plus `directory.len() * size_of::<u32>()`, reflecting the segmented layout.

Set/ZSet remain the two variants whose `approx_bytes()` heuristic doesn't scale with actual member byte
length (flat `count * constant`) — unchanged caveat from the prior revision.

---

## 13. Small-Collection Allocation Arena — unchanged

`RudisTable.arena: crate::allocator::SmallCollectionArena` (`src/allocator.rs:100–107`) recycles the backing
allocations behind `List` (`VecDeque<Bytes>`), `SmallHash` (`Vec<(Bytes, Bytes)>`), small `Set`
(`Vec<SmallSetEntry>`), and small `ZSet` (`Vec<(OrderedScore, Bytes)>`) — the four representations that churn
allocations fastest. Verified still current: `MAX_ARENA_POOLED = 1024` entries per pool
(`src/allocator.rs:94`), a per-collection capacity ceiling of `512` elements before a buffer is dropped rather
than pooled (`src/allocator.rs:137,159,181,206`). `RudisTable::recycle_value` (`src/table.rs:2675–2694`)
dispatches values back to the arena on overwrite/delete/expire; `Full`-form `Set`/`ZSet`, `Hash`, `Stream`,
and every other variant are simply dropped, not pooled.

---

## 14. Concrete Numbers — Summary Table

| Quantity | Value | Source |
| :--- | :--- | :--- |
| `size_of::<RudisValue>()` | 40 bytes | unit test, `table.rs:14983` |
| `size_of::<RudisEntry>()` / `Option<RudisEntry>` | 88 bytes (both — niche-optimized) | unit test, `table.rs:14984-14985` |
| `align_of::<RudisEntry>()` | 8 bytes | struct field types |
| `GROUP_SIZE` (SIMD probe group) | 16 | `table.rs:8` |
| `SEG_CAP` (max single-segment capacity) | 1,024 | `table.rs:1577` |
| `STASH_CAP` (overflow stash slots/segment) | 4 | `table.rs:1578` |
| `GLOBAL_IDX_SHIFT` | 11 bits (max local index 2,047) | `table.rs:1579` |
| Max load factor before split/grow | 7/8 (87.5%), `+4` stash slots at full `SEG_CAP` | `RawSegment::new` |
| Full-segment heap footprint | ≈ 91,504 bytes (≈ 89.4 KiB) — **not** the ~48 KiB the source comment claims | computed, §3.1 |
| `HASH_MAX_ENTRIES` default | 512 (runtime-configurable) | `connection.rs:1006` |
| `HASH_MAX_VALUE` default | 64 bytes (runtime-configurable) | `connection.rs:1008` |
| `SMALL_SET_LIMIT` | 64 (hardcoded) | `table.rs:662` |
| `SMALL_ZSET_LIMIT` | 64 (hardcoded) | `table.rs:144` |
| `STREAM_NODE_MAX_ENTRIES` default | 100 | `table.rs:1097` |
| `STREAM_IDMP_DURATION` default | 100 ms | `table.rs:1099` |
| `STREAM_IDMP_MAXSIZE` default | 100 entries | `table.rs:1101` |
| Active expiration sample size | 20 TTL'd keys, capped at 512 scanned cursor positions, per call | `table.rs:10437-10469` |
| Hash-field active expiration sample size | 8 keys per call | `table.rs:10393` |
| Eviction sample size | 10 occupied slots per call | `table.rs:2802-2857` |
| `MAX_ARENA_POOLED` | 1,024 pooled collections per pool | `allocator.rs:94` |
| Arena per-collection capacity ceiling | 512 elements | `allocator.rs:137` et al. |
| `slot_counts` array | `Box<[u32; 16384]>` = 64 KiB, one per table (not per segment) | `table.rs:1983` |
| `RudisTable::new()` base memory | 71,248 bytes | computed, §12 |

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: `RudisValue` is held to 40 bytes by boxing its four collection-carrying variants (`Hash`,
  `Set`, `ZSet`, `Stream`). A new unboxed variant with a payload larger than the current 32-byte maximum
  (`String`/`List`/`Cooled`) silently grows every `RudisEntry`.
* **Gotcha 2**: `RudisEntry` is 88 bytes as `32B key (Bytes) + 40B val (RudisValue) + 16B expire_at
  (Option<Instant>)`, alignment 8 — not cache-line-aligned, and wouldn't fit one 64-byte line even if it were.
* **Gotcha 3**: the comment `"1024 main SIMD slots per segment (~48KB, L1/L2 cache resident)"`
  (`table.rs:1577`) is stale/inaccurate — a full segment's backing storage is ≈89 KiB at the current 88-byte
  `RudisEntry` size (§3.1). Don't cite it as fact.
* **Gotcha 4**: `is_rehashing`/`rehash_step`/`finish_rehash`/`migrate_key_if_in_old`/`prepare_key_lookup` are
  all unconditional no-op stubs with zero external callers anywhere in the codebase (§5.2). The incremental
  rehashing feature they implemented was superseded by the extendible-hashing segment design and is
  effectively dead code today.
* **Gotcha 5**: `RudisValue::HyperLogLog` is only ever *constructed* by `RESTORE`-ing a legacy DUMP payload
  (type tag `5`). Every live `PFADD` writes a `RudisValue::String` holding Redis's real sparse/dense HLL byte
  format (`src/hll.rs`). Don't assume a table has any `HyperLogLog`-variant entries just because it has HLL
  keys (§7.5).
* **Gotcha 6**: `hash_field_expires` (HEXPIRE) is entirely independent of `num_expires` — field-level TTLs
  never affect `num_expires`, and whole-key TTL handling never looks at `hash_field_expires`. They're
  separate maps with separate fast-path guards (`num_expires > 0` vs. `!hash_field_expires.is_empty()`).
* **Gotcha 7**: `RudisTable::sample_cursor` is now shared by **three** independent samplers —
  active key-TTL expiration (§8.2), active hash-field-TTL expiration (§8.3), and `try_evict_one_key` (§8.4) —
  each reading/advancing it modulo its own, different bound. `spill_cursor` (NVMe tiering, §10) remains the
  one genuinely independent cursor.
* **Gotcha 8**: `slot_counts` is only maintained while `crate::cluster::HAS_ACTIVE_CLUSTER` is `true` at the
  time of each insert/remove. Toggling cluster mode on after keys already exist leaves the counters
  under-reporting until the table is repopulated (§11).
* **Gotcha 9**: any code path that mutates `expire_at` directly on a `RudisEntry` without going through
  `expire()`/`persist()`/the table's own removal paths desynchronizes `num_expires` from reality.
* **Gotcha 10**: a `RawSegment` physically allocates its 4 stash slots regardless of the segment's current
  size, but `growth_left` only credits them once the segment reaches full `SEG_CAP` — see §3.1.

### How to Verify Changes

```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests (table.rs has its own #[cfg(test)] mod at the bottom of the file, including
#    test_rudis_entry_size and test_extendible_hashing_segment_splits)
cargo test --lib -- --test-threads=1
```
