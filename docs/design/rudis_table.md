# Design Document: `RudisTable` — Custom In-Memory Storage Engine

> **Subsystem Scope**: `src/table.rs`
> **Companion Docs**: [`docs/design/05_storage_engine.md`](05_storage_engine.md) (architectural overview) ·
> [`docs/internal/05_storage_engine.md`](../internal/05_storage_engine.md) (struct layouts, algorithms, line references)

> **Document status**: This is the original bucket-layout design spec for `RudisTable`, corrected against
> the implementation as of the current `src/table.rs`. Every claim below is labeled **Implemented**
> (verified in source) or **Future Work / Unrealized** (part of the original design intent that was never
> built, or was superseded by a simpler mechanism). Do not treat unlabeled prose as a guarantee about the
> running system — read the label.

---

## 1. Background & Motivation

In-memory data stores depend fundamentally on the core associative hash table beneath every key.
Three points of reference motivated a custom implementation for Rudis:

- **Redis (`dict.c`)**: uses separate-chaining with linked lists (`dictEntry`). It supports progressive
  incremental rehashing (`dictRehash`) — moving a bounded number of buckets per operation instead of
  stopping the world — but pays for it with pointer chasing, poor cache locality, and per-entry memory
  overhead from heap-allocated list nodes.
- **Dragonfly (`DashTable`)**: a table inspired by the VLDB 2020 paper *"DASH: Scalable and
  Write-Efficient In-Memory Hashing"*. It organizes entries into cache-line-sized buckets carrying 1-byte
  hash fingerprints, and splits its directory incrementally to avoid latency spikes. Its production
  implementation also carries concurrency machinery (versioning headers, fiber-aware locks) that Rudis
  does not need, because Rudis never shares a table across threads.
- **A SwissTable-style baseline** (the kind of open-addressed, SIMD-probed table `hashbrown::HashMap`
  implements): high single-thread lookup throughput via 16-way parallel fingerprint comparison, but as a
  general-purpose `HashMap` it does not inline a value's TTL or its cluster slot, so a full `GET`/`EXPIRE`
  path over such a table means multiple independent hash lookups — one for the value, one for the
  expiration timestamp, one for slot bookkeeping — against logically separate tables.

`RudisTable` (`src/table.rs`) is Rudis's answer: a custom, thread-local, open-addressed hash table that
inlines the key, the value, and the expiration timestamp into one slot, tailored to the thread-per-core,
shared-nothing, `io_uring` architecture described in
[`docs/design/01_reactor_runtime.md`](01_reactor_runtime.md). It borrows SwissTable's SIMD
group-probing idea and DASH's fingerprint-in-metadata idea; it does **not** implement DASH's segmented,
incrementally-splitting directory — see §2, Pillar 3, and §6.

---

## 2. Design Pillars: Intent vs. Implementation

### Pillar 1 — Fingerprint Control Bytes with SIMD Group Probing — **Implemented**

Every table slot has a parallel 1-byte control entry, drawn from three possible states:

| Value | Meaning |
| :--- | :--- |
| `0xFF` (`EMPTY`) | Slot has never been occupied since the last full clear/resize; probing stops here. |
| `0xFE` (`DELETED`) | Tombstone left by a removed entry; probing continues past it. |
| `0x00`–`0x7F` | 7-bit hash fingerprint of the occupied slot's key. |

Sixteen consecutive control bytes (`GROUP_SIZE = 16`, `src/table.rs:8`) form a probe **group**. A lookup
loads a group with one 128-bit vector instruction and compares it against a broadcast target fingerprint
and a broadcast `EMPTY` value in the same instruction pair, producing two 16-bit match masks in a single
pair of vector ops rather than sixteen scalar byte comparisons.

**Verified against source**: on `x86_64` this is `_mm_loadu_si128` + `_mm_cmpeq_epi8` +
`_mm_movemask_epi8` (`probe_group_match_or_empty` / `probe_group_match_del_empty`,
`src/table.rs:1163–1234`). Note the load is **unaligned** (`_mm_loadu_si128`, not `_mm_load_si128`) — the
implementation never assumes or enforces any particular alignment of the control-byte array.

On any non-`x86_64` target, `table.rs` falls back to a **portable scalar loop** that inspects the same 16
bytes one at a time (`src/table.rs:1198–1234`). There is no NEON (`vceqq_u8`) or other ARM SIMD path
today — an earlier draft of this document claimed one; it does not exist in the current source. Group
probing is still logically 16-wide on ARM, just not vector-accelerated. This is called out again as a gap
in §6.

### Pillar 2 — Inlined Entry and TTL Representation — **Implemented**

Instead of a separate `expirations` table, every occupied slot stores one `RudisEntry`:

```rust
pub struct RudisEntry {
    pub key: Bytes,                 // 32 bytes
    pub val: RudisValue,            // 40 bytes
    pub expire_at: Option<Instant>, // 16 bytes
}
```

`size_of::<RudisEntry>() == 88`, `align_of::<RudisEntry>() == 8` (measured against the current
definitions of `Bytes`, `RudisValue`, and `Option<Instant>` on this target). `RudisValue` itself is kept
to 40 bytes specifically by boxing its variable-size collection variants (`Hash`, `Set`, `ZSet`, `Stream`
are all `Box<...>`; see [`docs/internal/05_storage_engine.md`](../internal/05_storage_engine.md) §3.2 for
the full enum). 88 bytes is deliberately **larger** than one 64-byte cache line, not equal to it — the
original framing of "one cache line per entry" does not hold exactly, though a probe group's worth of
control bytes (16 bytes) is small enough to sit well within one line by itself.

`GET`, `TTL`, `EXPIRE`, and passive-expiration checks all resolve against the same `RudisEntry` found by
one probe — there is no second table to consult for a key's TTL.

### Pillar 3 — Segmented, Incrementally-Splitting Directory — **Future Work / Unrealized**

The original intent (stated in earlier drafts of this document) was DASH-style extendible hashing: a
top-level directory of independent segments, where crossing a load-factor threshold splits **one**
segment into two rather than rehashing the whole table, bounding worst-case pause time per operation.

**This was never built.** `RudisFlatTable` (`src/table.rs:1238`) is a single flat structure — one
`Vec<u8>` of control bytes and one parallel `Vec<Option<RudisEntry>>` of slots, both sized to the same
power-of-two capacity. There is no directory, no segments, and no incremental split. Growing the table
means rehashing every live entry into a freshly allocated, larger `RudisFlatTable` in one synchronous pass
(`RudisFlatTable::resize`, `src/table.rs:1460–1473`) — see the internal doc's algorithm walkthrough and
its Future Improvements section for the concrete trigger conditions and current cost. This is the single
largest gap between this document's original design goal and what actually ships. Any p99.9 write-latency
sensitivity to large monolithic rehashes should be assumed present in the current implementation.

### Pillar 4 — Pure Thread-Local, Zero Synchronization — **Implemented**

Because Rudis enforces thread-per-core ownership, exactly one OS thread ever touches a given
`RudisTable`. Verified in `src/table.rs`:

- No `Mutex`, `RwLock`, or atomic CAS loop guards any table field (`ctrl`, `slots`, `capacity`, `mask`,
  `items`, `growth_left`, or `RudisTable`'s own fields).
- No versioning header or generation counter exists in the control-byte metadata.

Two caveats, both intentional and neither a synchronization mechanism for the table's own data:
`EXPIRED_KEYS` / `EVICTED_KEYS` (`src/table.rs:1612–1613`) are process-wide `AtomicU64` counters used
purely for `INFO` statistics, and reads of `crate::cluster::HAS_ACTIVE_CLUSTER` /
`crate::connection::ALLOW_ACCESS_EXPIRED` (both process-wide flags owned by other modules) gate optional
behavior inside table methods without protecting any table-internal state.

---

## 3. Data Layout — Corrected

### 3.1 What the original design proposed

The original goal — following SwissTable and DASH — was a table organized into discrete buckets, each
independently aligned to a 64-byte cache line, with a bucket's control header and its slot payloads
co-located in that one aligned allocation:

```text
+-------------------------------------------------------------------------+
|                    Aspirational Bucket Layout (NOT BUILT)               |
+-------------------------------------------------------------------------+
| Bucket 0: [16B Control Bytes][Slot 0 .. Slot N Payload], 64B-aligned    |
| Bucket 1: [16B Control Bytes][Slot 0 .. Slot N Payload], 64B-aligned    |
| ...                                                                     |
+-------------------------------------------------------------------------+
```

### 3.2 What `src/table.rs` actually implements

There is no bucket struct and no `#[repr(align(64))]` anywhere in `table.rs`. The real layout is a
classic SwissTable-family **structure-of-arrays**: one flat control-byte array running parallel to one
flat slot array, both indexed by the same open-addressed slot index, with no per-group or per-bucket
alignment guarantee:

```text
ctrl:  Vec<u8>                     length = capacity + GROUP_SIZE
       [c0][c1][c2]...[c(N-1)][c0..c15 mirrored]   <- GROUP_SIZE-byte wraparound copy at the tail

slots: Vec<Option<RudisEntry>>     length = capacity
       [Some(entry) | None][Some(entry) | None]...
```

`ctrl` carries `GROUP_SIZE` extra mirrored bytes at its tail (`RudisFlatTable::new`,
`src/table.rs:1250–1252`; kept in sync by `set_ctrl`, `src/table.rs:1270–1276`) so a probe group starting
near the end of the array can still be read as one contiguous 16-byte load without special-casing the
wraparound — a standard SwissTable trick, not a DASH bucket boundary. A "group" is therefore sixteen
logically (not physically-bucket) consecutive control bytes at an arbitrary offset; it is read with an
**unaligned** SIMD load, so 64-byte cache-line alignment is neither required nor provided.

`slot_counts: Box<[u32; 16384]>` is a third parallel array — a per-cluster-slot live-key counter,
unrelated to the bucket concept above. See [`docs/internal/05_storage_engine.md`](../internal/05_storage_engine.md)
§8 for its semantics and known limitation.

---

## 4. Control Byte States and Fingerprinting — Verified

```rust
// src/table.rs:8-10, 1157-1159
pub const GROUP_SIZE: usize = 16;
pub const EMPTY: u8 = 0xFF;
pub const DELETED: u8 = 0xFE;

#[inline(always)]
pub fn fingerprint(hash: u64) -> u8 {
    (hash >> 57) as u8 & 0x7F
}
```

The 64-bit hash itself comes from `fxhash::hash64` (`hash_key`, `src/table.rs:1152–1154`) — FxHash, not
the "foldhash or fxhash" wording of an earlier draft. FxHash is a fast, non-cryptographic multiply-xor
hash; it is an appropriate choice here specifically because `RudisTable` is thread-local and never
exposed to untrusted concurrent access patterns that hash-flooding attacks rely on, so the DoS-resistance
that a cryptographically stronger hash would buy is not a design requirement.

(Note: `RudisHashMap` — the backing map for the `Hash` variant — and `RudisSet::Full` both also pin
`fxhash::FxBuildHasher` explicitly. `RudisZSet::Full`'s internal `dict: HashMap<Bytes, f64>` does **not**
override its hasher and so uses `hashbrown`'s own default (`foldhash`, as of the `hashbrown = "0.17"`
dependency pinned in `Cargo.toml`) — a minor, apparently unintentional inconsistency, not a correctness
issue, since that hasher is private to the `ZSet`'s internal dictionary and never interacts with the outer
table's own fingerprint/index computation.)

---

## 5. Lookup Algorithm — Verified Against `RudisFlatTable::find_entry`

The actual algorithm (`src/table.rs:1280–1315`), corrected from the original two-level "segment index /
bucket index" sketch to the real single-level flat-index scheme:

1. Compute `h = fxhash::hash64(key)` — one 64-bit hash, no segment split.
2. `idx = (h as usize) & self.mask` — a single index into the flat `ctrl`/`slots` arrays (`mask =
   capacity - 1`; capacity is always a power of two).
3. `tag = fingerprint(h)` — top 7 bits of `h`.
4. Load the 16 control bytes at `idx` with one unaligned 128-bit SIMD load; compare against a
   broadcast `tag` and, in the same pair of intrinsics, against broadcast `EMPTY`, yielding
   `match_mask` and `empty_mask`.
5. For each set bit in `match_mask` (lowest first, via `trailing_zeros`), compare the candidate slot's
   full key bytes (length check, then byte-for-byte). Return on the first exact match.
6. If `empty_mask != 0`, the group contains at least one never-used slot, which terminates the probe
   sequence — the key is not present.
7. Otherwise, advance to the next group: `step += GROUP_SIZE; idx = (idx + step) & self.mask`. Because
   `step` accumulates by `GROUP_SIZE` on every iteration (16, 48, 112, 208, …), the sequence of group
   offsets grows **triangularly** rather than linearly. This is the same probing shape `hashbrown` uses
   internally, and it exists to avoid the primary clustering that plain linear group-stepping would cause
   under skewed hash distributions.

Two corrections versus an earlier draft of this document:

- **TTL evaluation is not inside the low-level lookup.** `find_entry`/`find_entry_mut`/`contains` return
  only whether a live slot exists with a matching key; they do not read `expire_at`, mark tombstones, or
  free anything. Expiration is evaluated one layer up, in `RudisTable` methods such as `get_with_hash`
  (`src/table.rs:1855–1874`), which call `check_expired_slot` only after a successful `find_entry`, and
  only when the table's `num_expires` counter (see
  [`docs/internal/05_storage_engine.md`](../internal/05_storage_engine.md) §6) indicates at least one key
  in the table currently carries a TTL. This separation keeps the hot, allocation-free lookup path free of
  a `Instant::now()` call and a branch on every probe, not just every hit.
- **Insertion uses a distinct combined probe**, `find_or_prepare_insert` (`src/table.rs:1410–1458`), which
  simultaneously tracks the first tombstone seen along the probe chain so a fresh insert can reuse it
  instead of extending the probe sequence further — an optimization with no equivalent in the simplified
  lookup-only sketch above.

---

## 6. Implementation Status Summary

| Original Phase | Status | Notes |
| :--- | :--- | :--- |
| **Phase 1** — Flat SIMD bucket engine, 16-slot control groups, inlined `RudisEntry` | **Done** | Implemented as `RudisFlatTable` + `RudisEntry`, matching §2 Pillars 1–2 above. |
| **Phase 2** — Segmented directory, incremental splitting, bounded pause time | **Not started** | See §2 Pillar 3. `resize()` remains a single monolithic rehash. Tracked as the highest-priority structural gap in [`docs/internal/05_storage_engine.md`](../internal/05_storage_engine.md) §10 (Future Improvements). |
| **Phase 3** — Integration into the shard's storage (`ShardDb.table: RudisTable`, `src/shard.rs:578`) | **Done, and exceeded scope** | `RudisTable` is the live storage engine for every shard. Beyond the original plan, it has since grown: NVMe cold-tiering support (`RudisValue::Tiered`/`Cooled`, `get_hot_keys_for_spill`, `decommit_all_cooled` — see the internal doc §7), a cluster-slot live-count index (`slot_counts`), a per-table `num_expires` fast-path counter that skips `Instant::now()` entirely on tables with no TTL'd keys, and a thread-local small-collection allocation arena (`SmallCollectionArena`) that recycles the backing `Vec`/`VecDeque` allocations behind `List`, `SmallHash`, small `Set`, and small `ZSet` values across `DEL`/expiry/overwrite instead of returning them to the global allocator. None of these were part of the original three-phase plan; all are documented in [`docs/internal/05_storage_engine.md`](../internal/05_storage_engine.md). |

For remaining known gaps and their relative priority — the unrealized Phase 2 directory, the
`ZRANK`/`ZREVRANK` linear-scan cost, the cluster-slot count-only regression, and the ARM SIMD gap noted in
§2 Pillar 1 — see [`docs/internal/05_storage_engine.md`](../internal/05_storage_engine.md) §10, Future
Improvements, which is the authoritative, single list of open work for this subsystem. This document does
not duplicate it further to avoid the two lists drifting out of sync again.
