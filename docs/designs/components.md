# Rudis Component Design & Learning Guide

This is the single reference for how every core module of `rudis` works and — more
importantly — **why** it was built that way. It merges what used to be five separate
per-component docs into one file so the whole system can be read start to finish, or
jumped into by part.

Each part below is written as a learning guide against the **actual shipped code**, not
a proposal — it explains rationale, walks through real code, and calls out known gaps and
limitations explicitly rather than leaving them implicit. The one exception is Part 1's
§1-§4, which is the *original, still-aspirational* storage-engine design (kept verbatim
as the roadmap for future work) — Part 1's §5 onward documents what actually shipped
against that plan.

See `agent.md`'s "Core Architectural Invariants" for the non-negotiable rules these
modules exist to satisfy, and `docs/benchmarks/` for the performance numbers referenced
throughout.

> **Relationship to `docs/components/`**: that directory covers 15 subsystems, including
> 10 (blocking ops, tiering, vector search, RediSearch, kernel bypass, cluster gossip, CRDT
> replication, Lua scripting, persistence, security/TLS) this doc doesn't touch at all. Its
> files 01-05 cover the same five subsystems as the five parts below, but as of this writing
> **do not match the current source** (verified by spot-checking each against `src/` — see
> the warning banners on those files) — this doc is the one checked against real code for
> those five areas. This doc also predates several major additions to `RudisValue`
> (`List`, `Set`, `ZSet`, `Stream`, `Tiered`/`Cooled` for NVMe tiering) — Part 1 documents the
> SIMD table engine underneath all of them accurately, but doesn't cover those newer value
> types' own internals.

**Contents**
- Part 1: Storage Engine (`RudisTable`) — `src/table.rs`, `src/shard.rs`
- Part 2: Server Runtime and Entrypoint — `src/server.rs`, `src/main.rs`
- Part 3: Connection Handling and Pipeline Squashing — `src/connection.rs`
- Part 4: Router and Cross-Shard Mesh — `src/router.rs`
- Part 5: RESP Parsing Engine — `src/resp.rs`

---
---

## Part 1: Storage Engine (RudisTable)

> **Status at a glance** (updated as of the `feat: implement custom RudisTable storage engine`
> commit). §1-§4 below are the *original design* as first proposed, kept as-is for
> historical context and as the roadmap for future work. §5 onward is a **learning
> guide to what actually shipped**, written against the real code in `src/table.rs`, and calls
> out anywhere the shipped implementation currently differs from the original plan.
>
> | Piece of the original design | Status |
> | :--- | :--- |
> | Pillar 1 — SIMD control-byte probing | ✅ Implemented (as a *SwissTable-style* layout, see §5.4 — not literally as drawn in §3) |
> | Pillar 2 — Inlined `RudisEntry` (value + TTL in one slot) | ✅ Implemented |
> | Pillar 3 — Segmented directory / incremental rehashing | 🔲 Not implemented yet. Current table still does a **monolithic doubling resize**, i.e. the exact hashbrown limitation called out in §1 still applies today. See §5.7. |
> | Pillar 4 — Zero synchronization, thread-local only | ✅ Implemented (unchanged — inherited for free from the shared-nothing shard model) |
> | `RudisValue::Hash(FlatHash)` | 🔲 Not built. Hash fields currently use a plain `hashbrown::HashMap<Bytes, Bytes>` nested inside the entry. `FlatHash` is still on the roadmap. See §5.8. |
> | Phase 1 (flat SIMD bucket engine) | ✅ Done |
> | Phase 2 (segmented directory) | 🔲 Not started |
> | Phase 3 (integration into `ShardDb`) | ✅ Done — `ShardDb` in `src/shard.rs` now wraps `RudisTable` exclusively |
>
> Nothing in §1-§4 has been edited to reflect this — they remain the target design to
> come back to. If you're trying to understand *what the code does right now*, skip to §5.

### 1. Background & Motivation

In-memory data stores like Redis and Dragonfly depend fundamentally on their core associative hash table implementation:
- **Redis (`dict.c`)**: Uses chaining with linked lists (`dictEntry`). While it supports progressive incremental rehashing (`dictRehash`), it suffers from heavy pointer chasing, cache misses, and significant memory overhead (24–32 bytes per entry).
- **Dragonfly (`DashTable`)**: Uses a custom hash table inspired by the VLDB 2020 paper *"DASH: Scalable and Write-Efficient In-Memory Hashing"*. It organizes entries into 64-byte cache-line-sized buckets with 1-byte hash fingerprints, using segmented directory splits to eliminate latency spikes. However, its implementation carries complex concurrency locks and versioning headers designed for fiber-level preemption and migration.
- **Rudis (Current)**: Relies on `hashbrown::HashMap` (Google SwissTable). While it provides high single-thread performance (940k Ops/sec) and SIMD SSE2/NEON group probing, it has two key limitations:
  1. **Monolithic Reallocation**: Resizes the entire backing table at once when load factor exceeds 87.5%, introducing tail-latency (p999) spikes on large key spaces.
  2. **Decoupled TTL & Multi-Map Overhead**: Values (`entries`), TTL timestamps (`expirations`), and cluster slots (`slot_to_keys`) are stored in distinct hash tables. A read or write operation frequently requires multiple independent hash lookups.

`RudisTable` is a custom storage engine tailored specifically for the thread-per-core, shared-nothing, `io_uring` architecture of `rudis`.

---

### 2. Core Architectural Pillars

#### Pillar 1: Cache-Line Aligned SIMD Bucket Groups
Following the principles of SwissTable and DASH:
- The table is organized into discrete **Buckets** aligned to 64-byte boundaries (the standard CPU L1 cache line size).
- Each bucket contains a **Metadata Control Header** consisting of 14–16 one-byte slots:
  - `0xFF`: Empty slot
  - `0xFE`: Deleted slot (Tombstone)
  - `0x00..=0x7F`: 7-bit hash fingerprint (top 7 bits of hash)
- **SIMD Probing**: Using 128-bit vector instructions (`_mm_cmpeq_epi8` / `movemask` on x86_64, or NEON `vceqq_u8` on ARM), a lookup evaluates all 16 slots in parallel in a single CPU clock cycle before dereferencing any key or value payload.

#### Pillar 2: Inlined Entry & TTL Representation
Instead of maintaining separate tables for values and expirations:
- Each slot stores a unified `RudisEntry`:
  ```rust
  pub enum RudisValue {
      String(Bytes),
      Hash(FlatHash),
  }

  pub struct RudisEntry {
      pub key: Bytes,
      pub val: RudisValue,
      pub expire_at: Option<Instant>, // or compact relative timestamp
  }
  ```
- **Single-Probe Resolution**:
  - `GET`, `SET`, `TTL`, `EXPIRE`, and passive expiration checks are evaluated within the same cache line.
  - Expired keys encountered during normal probes are passively evicted inline with zero secondary table lookups.

#### Pillar 3: Segmented Incremental Rehashing (Zero Latency Spikes)
To eliminate monolithic resize pauses:
- The table utilizes **Segmented Directory Resizing**:
  - A top-level directory points to independent **Segments** (contiguous arrays of buckets).
  - When an individual segment exceeds its target load factor (e.g., 85%), only that segment is split and rehashed into two new segments.
  - The top-level directory updates its pointers using standard extendible hashing prefix masks.
  - Maximum pause time per operation remains strictly bounded ($O(1)$ constant time per split), preserving sub-millisecond p99.9 latency under high ingestion rates.

#### Pillar 4: Pure Thread-Local / Shared-Nothing
- Because Rudis strictly enforces thread-per-core isolation via Monoio, every `RudisTable` instance belongs exclusively to one thread on its dedicated CPU core.
- **Zero Synchronization**:
  - NO mutexes
  - NO read-write locks
  - NO atomic CAS loops
  - NO versioning headers in bucket metadata

---

### 3. Detailed Data Layout

```text
+-------------------------------------------------------------------------+
|                               Segment                                   |
+-------------------------------------------------------------------------+
| Bucket 0: [16B Control Bytes] [Slot 0 .. Slot 13 Payload Pointers/Data] |
| Bucket 1: [16B Control Bytes] [Slot 0 .. Slot 13 Payload Pointers/Data] |
| Bucket 2: [16B Control Bytes] [Slot 0 .. Slot 13 Payload Pointers/Data] |
| ...                                                                     |
+-------------------------------------------------------------------------+
```

#### 3.1 Control Byte States
```rust
const EMPTY: u8 = 0xFF;
const DELETED: u8 = 0xFE;

#[inline(always)]
fn fingerprint(hash: u64) -> u8 {
    ((hash >> 57) & 0x7F) as u8
}
```

#### 3.2 Lookup Algorithm
1. Compute 64-bit hash $H = \text{hash}(key)$ using high-speed `foldhash` or `fxhash`.
2. Extract segment index and bucket index:
   $$\text{seg\_idx} = (H \gg 32) \ \& \ \text{segment\_mask}$$
   $$\text{bucket\_idx} = H \ \& \ \text{bucket\_mask}$$
3. Load 16-byte control array into 128-bit SIMD register.
4. Compare against broadcasted target fingerprint $F = \text{fingerprint}(H)$.
5. Extract match bitmask. For each matched bit:
   - Check if key in slot matches target key.
   - If match found, check `expire_at`:
     - If expired, mark slot as `DELETED`, drop entry, return `None`.
     - If valid, return reference to value.
6. If empty slot is encountered during probe chain, terminate search (key does not exist).

---

### 4. Phased Implementation Plan

1. **Phase 1: Flat SIMD Bucket Engine (`RudisBucketTable`)**:
   - Implement the 64-byte bucket layout with 16-slot SIMD control bytes and unified inlined entry (`key`, `val`, `expire_at`).
   - Benchmark vs `hashbrown` on fixed sizes to verify equal or higher raw probe speed.

2. **Phase 2: Segmented Directory & Incremental Splitting**:
   - Implement directory-based extendible hashing over segment arrays.
   - Verify bounded pause times during continuous heavy insertions.

3. **Phase 3: Integration into `ShardDb`**:
   - Replace separate `entries` and `expirations` in `ShardDb` with `RudisTable`.
   - Update passive expiration and active sampling cycle to leverage inlined timestamps.
   - Validate against full test suite (`cargo test`) and ensure zero benchmark regressions.

---

### 5. Learning Guide: How `RudisTable` Actually Works Today

This section explains the code that shipped (`src/table.rs`), not the plan. It's written so
that someone who has never seen a SwissTable-style hash map before can follow along, and so
that every non-obvious choice has a stated reason. Read it side-by-side with `src/table.rs`.

#### 5.1 Glossary

| Term | Meaning |
| :--- | :--- |
| **Control byte** | One byte per slot that says what's in that slot *without touching the slot itself*: `0xFF` empty, `0xFE` tombstone/deleted, otherwise a 7-bit hash fingerprint. |
| **Fingerprint** | 7 bits derived from the key's hash (`hash >> 57`), stored in the control byte. Lets you reject almost all non-matching slots without ever comparing the actual key bytes. |
| **Group** | 16 contiguous control bytes (`GROUP_SIZE = 16`), matching a 128-bit SIMD register. One SIMD instruction checks all 16 at once. |
| **Tombstone** | A control byte marking "a key used to live here but was deleted." Needed so that probing past a deleted slot still finds keys that were inserted *after* it in the same probe chain. |
| **Load factor** | `items / capacity`. Rudis resizes at 7/8 (87.5%), the same threshold hashbrown uses. |
| **Probe sequence** | The order of groups visited when the first group is full. See §5.5. |

#### 5.2 Two structs, two jobs

`table.rs` splits the engine into two layers, which is worth noticing because it mirrors the
separation of concerns in the rest of the codebase (transport vs. routing vs. storage):

- **`RudisFlatTable`** — the raw open-addressing hash table. Knows nothing about Redis
  semantics, TTL, or cluster slots. Just `find`, `insert`, `remove` over `RudisEntry`.
- **`RudisTable`** — the Redis-facing storage engine. Owns one `RudisFlatTable` plus two things
  the raw table has no business knowing about:
  - `slot_to_keys: HashMap<u16, HashSet<Bytes>>` — a reverse index from cluster slot → keys,
    used only by `CLUSTER COUNTKEYSINSLOT` / `CLUSTER GETKEYSINSLOT` (see §5.9).
  - `sample_cursor: usize` — round-robin position for the active-expiration cycle (see §5.10).

Keeping these separate means the SIMD probing logic can be unit-tested and reasoned about
independently of Redis command semantics — the `test_flat_table_crud_and_growth` test in
`table.rs` never has to think about TTLs or WRONGTYPE errors.

#### 5.3 Why `fxhash` for the hash function

`hash_key` is `fxhash::hash64`, a fast multiplicative/rotate hash (the same family used inside
`rustc` itself), not a cryptographic hash like SipHash (which `std::collections::HashMap` uses
by default). This is a deliberate speed-over-safety trade-off:

- **Why it's fine here**: every shard is thread-local and only ever sees keys from clients on
  that connection; there's no untrusted multi-tenant hashing surface where an attacker could
  engineer hash collisions to degrade a *shared* table the way they could against a public web
  service's session store.
- **The trade-off, named explicitly**: if `rudis` is ever exposed as a public-facing cache with
  adversarial keys, `fxhash` becomes a hash-flooding DoS vector (attacker sends keys that all
  collide, turning O(1) lookups into O(n) probes). This hasn't been revisited — it's an
  accepted risk for the current use case, not an oversight.

#### 5.4 The actual data layout (not what §3 draws)

The design's §3 diagram shows control bytes and slot payload interleaved *inside* one
64-byte bucket. That's not what got built. The real layout is the classic SwissTable shape:
**two parallel, separately-allocated arrays**, not one array of combined buckets:

```text
ctrl:  [ FF FE 3A 01 FF FF 7C FF | FF FF FF 12 FF FE FF FF | ... ]  (1 byte per slot)
slots: [ -  -  E2 E7 -  -  E5 -  | -  -  -  E9 -  -  -  -  | ... ]  (Option<RudisEntry> per slot)
                ▲slot 2                    ▲slot 11
```

`ctrl` is `Vec<u8>` and `slots` is `Vec<Option<RudisEntry>>`, indexed by the same `slot_idx`.
This is simpler to implement correctly than truly-interleaved 64-byte buckets and is exactly
what `hashbrown` itself does — the "cache-line aligned bucket" framing in Pillar 1 describes
the *probing unit* (16 control bytes read in one SIMD load), not literal memory interleaving
of control and payload. If Phase 1's true bucket-interleaved layout is revisited later, this is
the section to update.

**One extra wrinkle**: `ctrl` is actually allocated as `capacity + GROUP_SIZE` bytes, and the
first `GROUP_SIZE` bytes are mirrored at the tail (`ctrl.copy_within(0..GROUP_SIZE, cap)`, kept
in sync on every write via `set_ctrl`). This is why a 16-byte SIMD load starting near the end
of the table never needs a bounds check or wraparound branch — it just reads into the mirrored
region. Same trick hashbrown uses; it trades a few extra bytes of memory for removing a branch
from the hottest loop in the table.

#### 5.5 Probing: why triangular steps instead of plain linear probing

`find` and `find_or_prepare_insert` start at `idx = hash & mask` and, if the first group has no
match and no empty slot, advance with:

```rust
step += GROUP_SIZE;              // 16, 32, 48, 64, ...
idx = (idx + step) & self.mask;
```

This is **triangular-number probing at group granularity**, not simple `idx += GROUP_SIZE`
linear probing. The reason: plain linear probing suffers *primary clustering* — once two probe
chains merge, they stay merged and grow into an ever-longer wall of occupied groups. Stepping
by a triangular sequence (16, then 48, then 96, ... i.e. cumulative sums of 16) is proven to
visit every group in a power-of-two-sized table exactly once before repeating, which spreads
out collisions the same way `hashbrown`'s own probe sequence does, at zero extra cost per step
(just an addition).

**Worked trace** — `GET foo` where `foo` hashes such that `idx = 32` and capacity is 128:
1. Compute `h = fxhash::hash64(b"foo")`, `tag = fingerprint(h)`, `idx = h & 127`.
2. SIMD-load `ctrl[32..48]`, compare all 16 bytes against `tag` in one instruction.
3. For each matching bit, check `slots[idx+offset].key == "foo"` (the actual byte comparison —
   fingerprints can collide, this is the tie-breaker).
4. If no byte match and no `EMPTY` byte was seen in this group, the key *might* still be further
   down the chain (a full group means insertion could have overflowed past it), so advance:
   `step = 16`, `idx = (32 + 16) & 127 = 48`, and repeat.
5. If an `EMPTY` control byte is ever seen in a group, probing stops — an empty slot proves the
   key was never inserted (insertion always fills the first empty/tombstone slot it finds, so a
   later insert could never have "jumped over" an empty one).

#### 5.6 Deletion: tombstones, not backward-shift

`remove` sets the control byte to `DELETED` (`0xFE`) and clears the slot — it does **not** shift
later entries in the probe chain backward to close the gap (the alternative used by some
open-addressing tables, notably Python's dict predecessor designs). Trade-off:

- **Why tombstones**: `remove` is O(1) — one control-byte write, no chain walking. Robin-Hood /
  backward-shift deletion is also O(1) amortized but requires walking forward until an empty
  slot or a slot with zero probe distance, which is more branching in code that runs on every
  Redis `DEL`/hash-field-empty/TTL-expiry path.
- **The cost, named explicitly**: `DELETED` slots still count as "occupied" for probe-stopping
  purposes (only `EMPTY` stops a probe — see §5.5 step 5), so repeated delete/insert churn on
  the same keys can lengthen probe chains over time. The *only* thing that reclaims tombstones
  is a full resize (`resize` in `insert`, triggered by `growth_left == 0`) — there is no
  standalone "compact this table" operation. `find_or_prepare_insert` does reuse the first
  tombstone it walks past as the insertion point when the key isn't found, which limits how bad
  this gets in practice, but it's not a hard bound.
- **A subtlety worth noticing**: `growth_left` is decremented on insert but is **not**
  incremented back on `remove`. That's intentional, not a bug — `growth_left` tracks how many
  never-before-used (`EMPTY`) slots remain, and deleting a key turns an occupied slot into a
  tombstone, not back into `EMPTY`. So a workload that inserts and deletes the same key
  repeatedly will still eventually force a resize purely to reclaim tombstone slots, even though
  `len()` never grows. This matches hashbrown's own behavior.

#### 5.7 Growth policy — and the limitation §1 warned about

`RudisFlatTable::new` sets `growth_left = capacity * 7 / 8` (resize at 87.5% load factor, same
threshold as hashbrown). When `growth_left` hits zero, `insert` calls `resize(capacity * 2)`,
which allocates a **brand-new full-size table and rehashes every live entry into it** before
swapping it in (`*self = new_table`).

This means: **the exact "Monolithic Reallocation" limitation of `hashbrown` that §1 uses to
motivate `RudisTable` in the first place has not actually been fixed yet.** Pillar 3
(segmented directory resizing) is what was supposed to fix it, and it's still unimplemented
(🔲 in the status table at the top). Today, a single `SET` that happens to cross the 87.5%
threshold on a large table pays for rehashing the *entire* table inline, which is exactly the
tail-latency spike the design set out to avoid. This is the single biggest gap between "what
motivated this design" and "what's shipped" — treat Pillar 3 / Phase 2 as the highest-value
remaining work if p99.9 latency during table growth becomes a problem in benchmarks.

#### 5.8 `RudisValue::Hash` today: plain `HashMap`, not `FlatHash`

The design's Pillar 2 sketch used `Hash(FlatHash)` — implying hash-type Redis values (`HSET`
etc.) would get their own cache-friendly flat representation. What shipped is simpler:

```rust
pub enum RudisValue {
    String(Bytes),
    Hash(HashMap<Bytes, Bytes>),   // hashbrown::HashMap, re-exported via `use hashbrown::HashMap`
}
```

Each hash-type key stores an ordinary nested `hashbrown::HashMap`. This was a reasonable scope
cut for Phase 1: the top-level `RudisFlatTable` (keyed on the Redis key) is where the
p50/p99 latency of every command lives, since every `GET`/`SET`/`HGET`/etc. does exactly one
top-level probe; a bespoke `FlatHash` for the *fields within* one hash value only pays off for
workloads with very large individual hashes (many thousands of fields), which isn't the
benchmarked workload (`docs/benchmarks/`). `FlatHash` stays a real option if that changes.

#### 5.9 The `slot_to_keys` reverse index — why it exists at all

Redis Cluster's `CLUSTER COUNTKEYSINSLOT` / `CLUSTER GETKEYSINSLOT` need "which keys hash to
cluster slot N," which the primary table (keyed by key bytes, not by slot) can't answer without
a full scan. `RudisTable` maintains a second, independent index for this:

```rust
slot_to_keys: HashMap<u16, hashbrown::HashSet<Bytes>>
```

Every `set`, `hset` (on new key), `incr_by` (on new key), etc. inserts into `slot_to_keys` in
addition to the main table; every `del`, `hdel`-to-empty, etc. removes from it too. This is a
real memory and CPU cost paid on every write (double bookkeeping — the key `Bytes` is cloned
into the `HashSet`, since `Bytes` is cheaply-cloneable ref-counted storage) purely to make the
cluster-slot commands O(keys in slot) instead of O(all keys). `count_keys_in_slot` and
`get_keys_in_slot` also do **lazy expiration** inline — they walk the slot's key set, evict any
that have passed their `expire_at`, and only then report/return the survivors. This means those
two commands double as a (very small, single-slot) passive-expiration sweep.

#### 5.10 Active expiration cycle — bounded work per tick

`active_expire_cycle` samples **20 slots per call**, starting from `sample_cursor` and wrapping
modulo `table.capacity()`, checking each for expiry and evicting if needed:

```rust
while checked < 20 {
    let idx = self.sample_cursor % cap;
    self.sample_cursor = (self.sample_cursor + 1) % cap;
    if self.check_expired_slot(idx) { expired_count += 1; }
    checked += 1;
}
```

This is the same shape as real Redis's active-expire cycle (`activeExpireCycleTryExpire`):
bounded, constant work per invocation regardless of table size, called periodically from the
server's event loop (see Part 2 §3.4) rather than scanning everything at once. The cursor
persisting across calls means the whole table gets swept over many ticks without ever doing an
O(n) pass in a single call — important on a thread-per-core server where blocking the event
loop for a full-table scan would stall every connection on that core.

#### 5.11 Worked example: tracing `SET foo bar EX 10` then `GET foo`

1. Router hashes `foo` via CRC16 (`router::key_slot`, Part 4 §2) to pick the owning shard/core;
   assume it's local, so the shard's `ShardDb::set` is called directly.
2. `ShardDb::set` → `RudisTable::set(key, value, Some(Duration::from_secs(10)))`.
3. `RudisTable::set` computes `slot = key_slot(&key)` and inserts `key.clone()` into
   `slot_to_keys[slot]` (§5.9), sets `expire_at = Some(Instant::now() + 10s)`, builds a
   `RudisEntry { key, val: String(value), expire_at }`, and calls `table.insert(entry)`.
4. `RudisFlatTable::insert` checks `growth_left`; if nonzero, calls `find_or_prepare_insert`
   (§5.5 walk), writes the fingerprint into `ctrl[insert_idx]` (mirrored if near the tail,
   §5.4), and stores the entry in `slots[insert_idx]`.
5. On `GET foo`: `RudisTable::get` hashes the key, `table.find` walks the same probe sequence,
   `check_expired_slot` compares `Instant::now()` against `expire_at` *before* returning the
   value (passive expiration — zero extra table lookups, per Pillar 2's original goal), and only
   then clones the `Bytes` value out to the caller.

#### 5.12 Comparison: where `RudisTable` sits today

| | Redis `dict.c` | Dragonfly `DashTable` | `hashbrown` (Rudis's old engine) | **`RudisTable` (shipped)** | Pillar 3 target (unshipped) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| Layout | Chained linked lists | 64B buckets, versioned, segmented | SIMD SwissTable, monolithic | SIMD SwissTable, monolithic | SIMD SwissTable, **segmented** |
| Resize | Incremental (`dictRehash`) | Segment-local split | Monolithic, all-at-once | Monolithic, all-at-once | Segment-local split |
| TTL storage | Separate expire dict | N/A (Dragonfly stores inline) | Separate `expirations` map | **Inlined in `RudisEntry`** | Inlined |
| Concurrency | Global lock (per DB) | Fiber-aware versioning/locks | Thread-local (Rudis usage) | Thread-local, zero sync | Thread-local, zero sync |
| Memory/entry | 24-32B overhead | Lower via fingerprints | ~1 byte ctrl + hashbrown overhead | 1 byte ctrl + `Option` tag | 1 byte ctrl + `Option` tag |

The one row that matters most for the original motivation (§1) is **Resize**: `RudisTable`
has not yet moved off "monolithic," which is the whole reason this design exists.
Everything else in the table is already an improvement over the old `hashbrown`-based engine.

#### 5.13 Known limitations / what's left (ties back to §4's roadmap)

- **Monolithic resize is still live** (§5.7) — Phase 2 / Pillar 3 is the fix; not started.
- **No tombstone compaction outside of resize** (§5.6) — delete-heavy, insert-heavy churn on a
  stable key set can degrade probe length between resizes.
- **`fxhash` has no DoS hardening** (§5.3) — fine for the current trusted/thread-local usage
  model, would need revisiting before exposing `rudis` to untrusted multi-tenant traffic.
- **`slot_to_keys` duplicates every key's bytes** into a second `HashSet` (§5.9) — cheap because
  `Bytes` clones are ref-count bumps, not copies, but it's still a second data structure to keep
  correct on every insert/delete path; covered end-to-end by the `CLUSTER COUNTKEYSINSLOT` /
  `GETKEYSINSLOT` case in `tests/test_server_e2e.rs`, but not by a focused unit test in
  `table.rs` itself.
- **`RudisValue::Hash` is a plain nested `HashMap`, not `FlatHash`** (§5.8) — fine until hash
  values with very large field counts show up in benchmarks.

---
---

## Part 2: Server Runtime and Entrypoint

> Covers `src/main.rs` (entrypoint) and `src/server.rs` (per-shard worker). This is a
> **learning guide to the shipped code**, not a forward-looking proposal like Part 1's
> original design section — everything described here is implemented and running today.
> See `agent.md`'s "Core Architectural Invariants" for the non-negotiable rules this
> module exists to satisfy, and Part 4 / Part 3 for what happens once a connection is
> accepted.

### 1. What problem this module solves

A single-threaded event loop (classic Redis) or a lock-based multi-threaded server (most
"thread-per-connection" designs) both hit the same wall past a handful of cores: either
you're not using the other cores at all, or every core pays cache-coherency traffic to
touch a shared data structure protected by a mutex or atomic. `rudis` avoids both by
giving each core its own **fully independent copy of the server** — its own listener, its
own `io_uring` instance, its own key-value store — and using the kernel and a message-passing
mesh to make N independent single-threaded servers behave like one logical Redis instance.
`src/main.rs` and `src/server.rs` are where that per-core independence is actually assembled.

### 2. `main.rs`: sizing the fleet and building the mesh, before any thread exists

```rust
let num_shards = args.threads.unwrap_or_else(|| num_cores.min(8));
```

**Why default to `min(cores, 8)` instead of "one shard per core"?** Two reasons, both
about avoiding surprises on shared/cloud hosts rather than dedicated benchmark boxes:
- On a large shared machine (say 64 or 128 cores), silently spawning 128 `io_uring`
  instances and 128 TCP listeners by default is a lot of kernel resource commitment for
  a service that was just started with no flags.
- `core_affinity::get_core_ids()` can return more IDs than are actually usable/isolated
  for this process (e.g. inside a container with a cgroup cpuset narrower than the host);
  capping the default keeps behavior predictable, while `--threads N` still lets an
  operator opt into full-machine scaling explicitly for the benchmark configurations
  documented under `docs/benchmarks/`.

**Why build the cross-shard channel mesh in `main` and hand it to each thread, instead of
having each shard worker create its own channels?** Every shard needs a `Sender` to
*every other* shard (to forward a command for a key it doesn't own) but only needs to
`Receiver` from its *own* inbox. That N×N sender fan-out has to be constructed once,
centrally, before any thread starts — there's no way for shard 3 to hand shard 7 a
sender to shard 7's own receiver after the fact without a second discovery mechanism.
`main.rs` builds `num_shards` independent `flume::unbounded` channels up front:

```rust
for _ in 0..num_shards {
    let (tx, rx) = flume::unbounded::<ShardMessage>();
    senders.push(tx);
    receivers.push(rx);
}
```

then gives **every** shard worker a clone of the full `senders` vector (so shard *i* can
reach any shard *j* by indexing `senders[j]`) but only **its own** `rx` (so it never
polls a mailbox that isn't its own). This is the entire cross-shard mesh — there is no
broker, no central dispatcher thread, just N mailboxes and N full address books.

**Why `flume::unbounded` and not a bounded channel?** An unbounded channel can never
block a `send`, which matters because sending is done from inside an async task that
also owns the sending shard's local database borrow in some paths — a bounded channel
that filled up would risk a worker stalling while holding a `RefCell` borrow, which is a
correctness hazard (panic on re-entrant borrow) as much as a performance one. The
trade-off, made deliberately: an unbounded channel means a slow/stuck peer shard's inbox
can grow without limit under sustained overload. There's no backpressure mechanism today —
this is a known gap, not an oversight (see §6).

**Why pin thread *i* to `core_ids[i]`, falling back to no pinning if there are more
shards than cores?** `core_ids[shard_id]` is a 1:1 assignment chosen for simplicity and
determinism (shard 0 always lands on core 0, useful when reading `taskset`-pinned
benchmark output). If `--threads` is set higher than the physical core count, the extra
threads simply run unpinned rather than erroring out — oversubscription is allowed, it
just stops getting the thread-per-core cache-locality guarantee for the excess threads.

### 3. `server.rs::run_shard_worker`: what happens inside one shard's thread

Each spawned OS thread runs this function once, forever. It does five things, in this
order, and the order matters:

#### 3.1 Pin to core, then build the runtime

```rust
if let Some(core) = core_id {
    core_affinity::set_for_current(core);
}
let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
    .enable_timer()
    .build()
```

Pinning happens **before** the `io_uring` instance is created. This ordering matters:
`io_uring`'s kernel-side polling and completion delivery has NUMA/cache locality to the
CPU that created it, so creating the ring only after the thread is already pinned to its
final core avoids the ring being set up on the wrong node and then migrated.

#### 3.2 One socket per shard, all bound to the same port via `SO_REUSEPORT`

```rust
socket.set_reuse_port(true)...
socket.bind(&addr.into())...
socket.listen(4096)...
```

**Why does every shard bind its own listener to the identical port instead of one shard
accepting and handing connections off?** A single "acceptor" thread handing connections
to workers would need a queue and a wakeup mechanism between accept and handoff — exactly
the kind of cross-thread coordination this architecture is designed to avoid. `SO_REUSEPORT`
pushes that fan-out into the kernel: the kernel load-balances incoming SYNs across all
sockets bound to the port (by a hash of the connection 4-tuple), so each shard's `accept()`
just pulls connections the kernel already decided belong to it — zero user-space
coordination, and it's why `agent.md` calls this out as one of the four architectural
invariants.

**Why manually set 512KB send/recv buffers** (`set_recv_buffer_size` / `set_send_buffer_size`)
**instead of leaving the OS default?** This is the socket-tuning half of the write-batching
work recorded in `docs/benchmarks/write_batching.md` — larger kernel buffers reduce the
number of `io_uring` completion round-trips needed to drain a large pipelined write, which
matters directly for the "batch all accumulated responses in one io_uring write" behavior in
`connection.rs`. Both calls are `let _ =` (best-effort) because a kernel that refuses to grow
the buffer (e.g. capped by `net.core.rmem_max`) shouldn't be fatal — it's a performance tuning
knob, not a correctness requirement.

#### 3.3 Thread-local `ShardDb`, wrapped in `Rc<RefCell<_>>`, not `Arc<Mutex<_>>`

```rust
let local_db = Rc::new(RefCell::new(ShardDb::new()));
```

This single line is the concrete embodiment of `agent.md`'s "Zero Mutexes / Zero Locks in
Data Path" invariant. `Rc`/`RefCell` (not `Arc`/`Mutex`) are chosen deliberately: they are
**not** `Send`, so the Rust compiler itself refuses to compile any code that would let a
`ShardDb` handle escape to another OS thread. The lack of thread-safety here isn't a gap —
it's the enforcement mechanism. If someone later tries to share a `ShardDb` across shards
directly (instead of going through the `ShardMessage` mesh), the code won't compile, which is
a much stronger guardrail than a code-review rule against introducing locks.

The same reasoning applies to `client_registry: Rc<RefCell<HashMap<u64, ClientInfo>>>` —
`CLIENT LIST`/`CLIENT SETNAME` bookkeeping is per-shard local state for the same reason the
data store is: every connection that could touch a given `ClientInfo` is already pinned to
one shard by `SO_REUSEPORT`.

#### 3.4 The active-expiration background task — a second, cooperating async task, not a separate thread

```rust
monoio::spawn(async move {
    loop {
        monoio::time::sleep(Duration::from_millis(100)).await;
        active_db.borrow_mut().active_expire_cycle();
    }
});
```

**Why a `monoio::spawn`ed task sharing the same thread instead of a dedicated background
thread?** A separate OS thread would need to synchronize with the connection-handling
threads to touch `ShardDb` — back to locks. Spawning it as another task *on the same
single-threaded runtime* means it shares the exact same `Rc<RefCell<ShardDb>>` with zero
synchronization, at the cost of it only running between `.await` points of other tasks on
that core (i.e., it's cooperatively scheduled, not preemptive — a pathologically
long-running synchronous command handler could delay it). 100ms was chosen to match
`RudisTable::active_expire_cycle`'s bounded 20-slot-per-tick sampling (Part 1 §5.10):
frequent enough that expired keys don't linger long, cheap enough (bounded work per tick)
that it never shows up as a latency spike.

#### 3.5 The cross-shard receiver loop — where remote requests actually get executed

```rust
monoio::spawn(async move {
    while let Ok(msg) = rx.recv_async().await {
        match msg { ShardMessage::Get { .. } => ..., ... }
    }
});
```

This is the *server side* of the mesh whose *client side* lives in `Router` (Part 4).
Every `ShardMessage` variant maps 1:1 to a `ShardDb` method, with one exception worth
calling out because it's the highest-leverage part of this whole file:

```rust
ShardMessage::Batch { items, responder } => {
    let mut db = cross_shard_db.borrow_mut();
    let mut results = Vec::with_capacity(items.len());
    for (idx, cmd) in items {
        let mut out = Vec::new();
        let _ = execute_local_command(&cmd, &mut db, &mut out);
        results.push((idx, out));
    }
    let _ = responder.send(results);
}
```

`Batch` is what lets an entire pipeline's worth of remote-shard commands cross the core
boundary in **one channel hop**, instead of one hop per command — this is the "Parallel
Cross-Shard Pipeline Squashing" invariant from `agent.md`, and it's the single biggest
contributor to the throughput jump documented in `docs/benchmarks/pipeline_squashing.md`
(297k → 2.63M ops/sec at 16 threads). The other per-command `ShardMessage` variants
(`Get`, `Set`, `Del`, ...) still exist and are still used — see §6 for exactly when, and
why that matters.

#### 3.6 The accept loop — last, because everything it needs must already exist

```rust
let router = Rc::new(Router::new(shard_id, num_shards, port, local_db, senders));
loop {
    match listener.accept().await {
        Ok((stream, client_addr)) => {
            monoio::spawn(async move {
                handle_connection(stream, client_addr, client_id, reg, r).await;
            });
        }
        ...
    }
}
```

Each accepted connection becomes its own `monoio::spawn`ed task — cheap because `monoio`
tasks are not OS threads, just state machines driven by the same single-threaded runtime.
`client_id` is constructed as `((shard_id as u64) << 48) + counter`, encoding the owning
shard into the top bits of every client ID. **Why encode the shard into the ID rather than
using a plain global counter?** It makes client IDs collision-free across shards *without
any cross-shard coordination* — shard 3's client IDs and shard 7's client IDs can never
collide, because they live in disjoint bit ranges, and no shard ever needs to ask another
shard "what's the next free ID" the way a shared global counter would require.

### 4. Accept errors are logged and the loop continues — is that a design choice?

```rust
Err(e) => {
    eprintln!("[Shard {}] Accept error: {}", shard_id, e);
}
```

Yes, deliberately: a transient `accept()` error (e.g. `EMFILE` from a temporary
file-descriptor exhaustion spike) should not bring down an entire shard — and by
extension, one-`num_shards`-th of the server's total listening capacity — over a
recoverable condition. The loop just tries `accept()` again next iteration.

### 5. What this module deliberately does *not* do

- **No graceful shutdown path.** There is no signal handler, no drain-in-flight-requests
  logic, and no way to tell a shard worker to stop accepting and exit cleanly — the
  process only stops via `handles.join()` after threads that never intentionally return,
  which in practice means "the process is killed." This is fine for a benchmark-driven
  project; it would need addressing before any deployment that cares about zero-downtime
  restarts.
- **No dynamic shard count.** `num_shards` is fixed at process start (`--threads`) and
  never changes. Cluster resharding (`CLUSTER SETSLOT`, live slot migration between
  shards) isn't implemented — `CLUSTER SLOTS`/`NODES`/`INFO` (Part 3) report a static,
  evenly-divided slot range per shard computed from `num_shards`, but nothing ever moves
  a slot's keys between shards at runtime.
- **No connection-level backpressure.** A shard with more connections than others (an
  artifact of `SO_REUSEPORT`'s hash-based balancing, which is not perfectly even under a
  small number of connections) just runs hotter; there's no rebalancing.

### 6. A gap worth naming explicitly: not every cross-shard path uses `Batch`

Reading Part 4 alongside this section matters here: `Router::get`/`set`/`del`/`exists`/
`incr_by`/`expire`/`persist`/`ttl`/`count_keys_in_slot`/`get_keys_in_slot` each send their
own single-purpose `ShardMessage` variant (`ShardMessage::Get`, `ShardMessage::Set`, ...)
over a **freshly allocated** `flume::bounded(1)` channel, *not* the pre-allocated
per-shard `ResponderChannel` pool that `connection.rs` builds once per connection. Those
individual-command variants are only reached when a pipeline can't be squashed (Part 3
§4) or for a lone unpipelined command — the true steady-state hot path
(`execute_commands_squashed`) always uses `ShardMessage::Batch` with the pre-allocated
pool. This means `agent.md`'s "never allocate one-shot channels on the hot request path"
invariant holds for the benchmarked workload (pipelined `SET`s), but not universally
across every code path in this file — worth knowing before assuming *all* cross-shard
traffic is zero-allocation.

---
---

## Part 3: Connection Handling and Pipeline Squashing

> Covers `src/connection.rs`, the largest and most performance-critical module in the
> codebase. Learning guide to shipped code. This is where `agent.md`'s "Parallel
> Cross-Shard Pipeline Squashing" and "Zero-Allocation Steady State" invariants are
> actually implemented — read Part 2 §3.5 and Part 4 §4 first for the mesh this module
> drives.

### 1. What problem this module solves

A `memtier_benchmark` client pipelining 100 commands per round-trip sends all 100 in one
`write()`, then reads all 100 responses back in one `read()`. If a server naively awaited
each command one at a time — including any cross-shard hop it might need — a pipeline
whose 100 commands happen to be spread across 8 shards would pay for up to 100 *serialized*
cross-core round-trips before it could reply. `connection.rs` exists to turn that into: one
local database borrow per local command (no I/O at all), plus **at most one channel hop
per distinct remote shard touched, all of them running concurrently** — regardless of how
many individual commands in the pipeline target that shard. This single idea
(`execute_commands_squashed`, §5) is responsible for the jump from 705,995 to 2,634,081
ops/sec at 16 threads recorded in `docs/benchmarks/pipeline_squashing.md`.

### 2. The per-connection read loop: parse everything available, then act

```rust
let (res, returned_buf) = stream.read(read_buf).await;
...
while !buf.is_empty() {
    match parse_command(&mut buf) {
        Ok(Some(cmd)) => commands.push(cmd),
        Ok(None) => break,          // incomplete frame, wait for more bytes
        Err(err) => { ...; should_quit = true; break; }
    }
}
```

**Why drain the entire read buffer into a `Vec<Command>` before executing anything,
instead of executing each command as soon as it's parsed?** Executing eagerly would mean
the very first command in a pipeline could already be off routing to a remote shard while
the 99 behind it are still unparsed — at which point there's no opportunity left to notice
"actually, 40 of these target the same remote shard, let's send them together." Parsing
the whole readable buffer up front is what makes batch-and-fan-out possible at all: you
can't squash requests you haven't seen yet.

`read_buf` is a **rented** buffer handed to and returned from `stream.read()` — this is
`monoio`'s ownership-transfer I/O model (required because `io_uring` needs a stable
buffer address for the duration of the kernel operation, which a borrowed `&mut [u8]`
can't guarantee across an `.await` point the way an owned, moved buffer can).

### 3. One command vs. many: two different execution strategies, chosen per read

```rust
if commands.len() == 1 {
    execute_command(commands.pop().unwrap(), &router, ...).await
} else {
    execute_commands_squashed(commands, &router, &responders, ...).await
}
```

**Why fork into two entirely different functions instead of always calling the squashing
path with a length-1 list?** `execute_commands_squashed` has fixed overhead — clearing
`remote_batches`, allocating a `responses: Vec<Vec<u8>>` sized to the batch, checking
`can_squash` across the whole list — that's pure waste for the extremely common case of an
unpipelined client sending one command, waiting for the reply, sending the next (e.g. an
interactive `redis-cli` session, or any client library with pipelining disabled). Splitting
the two paths means the single-command case pays only for what it needs: one
`target_shard_of_cmd` check and, if local, a direct call — no batch bookkeeping at all.

### 4. `execute_commands_squashed`: the all-or-nothing squash decision

```rust
let mut can_squash = true;
for cmd in &commands {
    if target_shard_of_cmd(cmd, router.num_shards).is_none()
        && !matches!(cmd, Command::Ping(_) | Command::CommandDocs | Command::Quit)
    {
        can_squash = false;
        break;
    }
}
if !can_squash {
    for cmd in commands { execute_command(cmd, ...).await; }   // fully sequential fallback
    return should_close;
}
```

`target_shard_of_cmd` returns `None` for anything that isn't a single, staticallyknown-key
operation: `MGET`, `MSET`, multi-key `DEL`/`EXISTS`, `CLUSTER *`, `CLIENT *`, `INFO`. **Why
does the presence of even *one* such command in a 100-command pipeline force the *entire*
pipeline back to fully sequential, one-at-a-time execution — including the 99 other
commands that would have squashed fine on their own?** Because `execute_commands_squashed`
needs to know the complete target-shard bucketing *before* it starts dispatching, in order
to build one `ShardMessage::Batch` per destination shard; a command whose shard target
isn't knowable up front (or that has no single target at all, like `MGET` across many keys)
breaks that up-front bucketing for the *whole* list, not just itself, because the function
has no partial/mixed-mode: it's "batch everything" or "batch nothing." This is a real,
sharp edge worth internalizing: a benchmark or workload that occasionally interleaves one
`MGET` into an otherwise all-`SET` pipeline will silently lose squashing for that entire
pipeline, not just pay a small tax on the `MGET` itself. If mixed pipelines become common
in practice, the fix would be to bucket the *squashable subset* and fall back to sequential
only for the non-squashable commands, interleaving the two — not implemented today.

### 5. The squash path itself: bucket locally, dispatch once per remote shard, await all in parallel

```rust
for (idx, cmd) in commands.into_iter().enumerate() {
    if let Some(target) = target_shard_of_cmd(&cmd, router.num_shards) {
        if target == router.shard_id {
            execute_local_command(&cmd, &mut router.local_db.borrow_mut(), &mut responses[idx]);
        } else {
            remote_batches[target].push((idx, cmd));   // no I/O yet — just bucket it
        }
    } else {
        execute_local_command(&cmd, ...);               // PING/QUIT/COMMAND run locally
    }
}
let mut pending = Vec::new();
for (target_shard, items) in remote_batches.iter_mut().enumerate() {
    if !items.is_empty() {
        let (tx, rx) = &responders[target_shard];
        router.senders[target_shard].send(ShardMessage::Batch { items: std::mem::take(items), responder: tx.clone() });
        pending.push(rx);
    }
}
for rx in pending {
    if let Ok(results) = rx.recv_async().await {
        for (idx, resp) in results { responses[idx] = resp; }
    }
}
for resp in responses { out.extend_from_slice(&resp); }
```

Walking through **why** each step is shaped this way:

- **Local commands execute inline in the bucketing loop itself**, with zero `.await` —
  there's no reason to defer work that doesn't need to leave the thread.
- **Remote commands are only bucketed (`.push`), not sent, during this first pass.** Only
  after every command has been classified does the second loop actually call
  `senders[target].send(...)` — once per shard that has at least one command destined for
  it, carrying *all* of that shard's commands from this pipeline in one `Vec`. This is
  the entire "squashing": what could have been up to `commands.len()` channel sends
  becomes at most `num_shards` sends, each a batch.
- **All the sends happen before any of the awaits.** `pending` collects every `rx` first;
  the actual `recv_async().await` loop runs afterward. This is what makes the remote
  shards' work happen **concurrently** — by the time this connection task awaits the
  first response, every other targeted shard has already had its `ShardMessage::Batch`
  delivered to its inbox and can be processing it in parallel on its own core. Awaiting
  them one-by-one in send order would still be correct, but awaiting the *first* one
  wouldn't have blocked the *second* one from having already been dispatched — the
  concurrency comes from separating "dispatch all" from "await all," not from any
  particular await ordering.
- **`responses[idx]` preserves original FIFO order regardless of completion order.** Every
  command's output slot is fixed by its position in the original pipeline (`idx`) before
  any dispatching happens, so it doesn't matter whether shard 3's batch comes back before
  shard 1's — the final `for resp in responses` loop always emits replies in the client's
  original request order, which Redis's pipelining contract requires.
- **`std::mem::take(items)` empties the bucket in place** instead of cloning it, and the
  buckets themselves (`remote_batches`) are cleared and reused (not reallocated) across
  every single call via the `for batch in remote_batches.iter_mut() { batch.clear(); }` at
  the top of the function — allocated once per connection in `handle_connection`, not once
  per pipeline flush.

### 6. `responders`: the pre-allocated channel pool, and why it's sized per-shard, not per-command

```rust
let responders: Vec<ResponderChannel> = (0..router.num_shards)
    .map(|_| flume::bounded(1))
    .collect();
```

Allocated **once**, when the connection is accepted, and reused for the connection's
entire lifetime. **Why one channel per *shard* instead of one per in-flight *command*?**
Because the unit of dispatch is "one `Batch` message per remote shard per pipeline flush,"
never "one message per command" — so the maximum number of simultaneously in-flight
responder channels a connection ever needs is bounded by `num_shards`, not by pipeline
depth. A connection pipelining 1,000 commands across 8 shards still only ever needs 8
channels, reused across every read-execute-write cycle of that connection's lifetime. This
is the concrete mechanism behind `agent.md`'s "Reusable Channels" rule and its explicit
warning against one-shot `flume::bounded(1)` allocations on the hot path — here, the
allocation happens exactly once per connection, not once per request.

### 7. `execute_local_command` vs. `execute_command`: two different call surfaces for the same command set

`execute_local_command(cmd, db, out)` is synchronous, takes an already-borrowed `&mut
ShardDb`, and is called from three different places: the squash path's local-command
branch, `server.rs`'s `ShardMessage::Batch` handler (Part 2 §3.5) for *remote* requests
arriving from peer shards, and the hash-command branch of `execute_command` itself when
the target happens to be local. **Why factor this out as its own function rather than
inlining it into each caller?** Because it's the one piece of logic that has to behave
*identically* whether a command is being run because it was local to begin with, or
because it arrived over the wire as part of a remote shard's `Batch` — any divergence
between "local execution" and "remote execution" of the same command would be an
observable correctness bug (a client could get different behavior for a key depending on
which shard happened to receive its connection). Sharing one function makes that
divergence structurally impossible rather than something to keep in sync by hand.

`execute_command(cmd, router, ...)` is the `async` outer layer used only by the
single-command path and the non-squashed fallback — it's the one that knows how to *route*
(via `Router`, potentially awaiting a cross-shard hop) rather than just execute against an
already-local `db`.

### 8. `target_shard_of_cmd`: the single source of truth for "is this squashable"

```rust
pub fn target_shard_of_cmd(cmd: &Command, num_shards: usize) -> Option<usize> {
    match cmd {
        Command::Get(key) | Command::Set { key, .. } | ... => Some(target_shard(key, num_shards)),
        Command::Del(keys) | Command::Exists(keys) if keys.len() == 1 => Some(target_shard(&keys[0], num_shards)),
        _ => None,
    }
}
```

This one function is consulted by both `execute_command` (to decide local-vs-remote for
hash commands) and `execute_commands_squashed` (to decide both squashability and
bucketing) — it's the single place that defines "which commands have exactly one
statically-known target shard." **Why does single-key `DEL`/`EXISTS` get a target shard
but multi-key `DEL`/`EXISTS` doesn't**, even though both are the same `Command` variant?
Because a multi-key `DEL` might need to touch several different shards *within one
command*, which doesn't fit the "one command → one target shard" shape this function
returns — extending it to fan out one logical command across multiple shards would need
a different return type entirely (a set of targets, not one `Option<usize>`), which is
exactly the same gap identified for `MGET`/`MSET` in Part 4 §6. This function is the
textbook place that gap would need to be resolved if it's ever addressed.

### 9. Per-connection bookkeeping that rides along on every command: `ClientInfo`

```rust
if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
    c.last_active = Instant::now();
    c.last_cmd = cmd_name.to_string();
}
```

Every single-command execution updates `last_active`/`last_cmd`; the squashed path only
updates it once per *flush*, using the **last** command in the batch (`commands.last()`),
not once per command in the batch. **Why the discrepancy?** `CLIENT LIST` only ever
reports the most recent command and the most recent activity timestamp for a connection —
updating it once per flush with the last command's name is observably indistinguishable
from updating it after every command in the batch (the intermediate values are never
read by anything), so the squashed path skips (`commands.len() - 1`) redundant
`String::from_utf8`-free `to_string()` allocations and registry lookups per flush. This
is a real optimization enabled by understanding exactly what `CLIENT LIST`'s contract
actually requires, not an oversight — but it does mean the single-command and squashed
code paths intentionally diverge here, worth knowing if adding a future feature (e.g.
per-command metrics) that *does* need every individual command observed.

### 10. Known limitations / what's not handled here

- **`ClientCleanup`'s `Drop` impl is the only cleanup path** — there's no explicit
  `CLIENT KILL`, no idle-connection timeout, and no max-connections limit. A slow-loris
  style client just occupies a `monoio` task and a `ClientInfo` entry indefinitely.
- **The mixed-pipeline squash-defeat gap** from §4 — the highest-value fix if real-world
  traffic mixes squashable and non-squashable commands in the same pipeline.
- **`MGET`/`MSET` don't participate in bucketed fan-out at all** — see Part 4 §6; the fix
  belongs in `execute_command`'s `Mget`/`Mset` arms, following the same
  bucket-by-shard-then-await-all-in-parallel shape already proven out in
  `execute_commands_squashed`.
- **`out_buf` write batching assumes a single `write_all` per read cycle is always
  worthwhile** — for a pipeline whose responses are enormous (e.g. many large
  `HGETALL`s), the entire response set is buffered in memory (`Vec<u8>`) before the
  single write, rather than streaming — a deliberate simplicity/latency trade (one
  `io_uring` write op instead of several) that assumes response sizes stay in the
  benchmarked regime (1KB payloads), not validated against very large multi-bulk replies.

---
---

## Part 4: Router and Cross-Shard Mesh

> Covers `src/router.rs`. Learning guide to shipped code, written against the real
> implementation. See Part 2 §3.5 for the receiving end of the mesh this module sends
> into, and Part 3 for the caller that decides *when* to use the single-command API
> documented here versus the batched path.

### 1. What problem this module solves

Every shard's key-value store is thread-local and unreachable from any other thread
(Part 2 §3.3). But Redis clients don't know or care which shard owns a key — `GET
foo` has to work no matter which of the `SO_REUSEPORT`-balanced connections it arrives
on. `router.rs` is the layer that hides this: given a key, decide whether it's "mine"
(execute inline, zero cost) or "theirs" (hop across the mesh to whichever shard owns it,
await the result). Every other module — `connection.rs`'s command dispatch — talks to
storage exclusively through `Router`, never through `ShardDb` directly for keys it
doesn't already know are local.

### 2. Key routing: CRC16 slots, borrowed wholesale from Redis Cluster

```rust
pub fn key_slot(key: &[u8]) -> u16 {
    let tag = extract_hash_tag(key);
    (crc16::State::<crc16::XMODEM>::calculate(tag) % 16384) as u16
}
pub fn slot_to_shard(slot: u16, num_shards: usize) -> usize {
    if num_shards <= 1 { 0 } else { ((slot as usize) * num_shards) / 16384 }
}
```

**Why reuse Redis Cluster's exact CRC16/16384-slot scheme instead of inventing a simpler
`hash(key) % num_shards`?** Two reasons:
1. **Client compatibility for free.** Any Redis client library that already knows how to
   compute cluster slots (for `MOVED`/`ASK` redirection, or just for `CLUSTER KEYSLOT`)
   works against `rudis` without modification — `CLUSTER KEYSLOT`, `SLOTS`, `NODES`, and
   `INFO` (handled in `connection.rs`) report real, standard slot numbers.
2. **Decoupling "how many slots" from "how many shards."** 16384 is fixed forever; the
   number of shards can change between restarts (`--threads`). Routing a key is a
   two-step `key → slot → shard` instead of `key → shard` directly *specifically* so
   that a future live-resharding feature only has to change the second step
   (`slot_to_shard`'s mapping) without touching how slots are computed or exposed to
   clients — this is exactly the same reason real Redis Cluster separates the two
   concepts.

**Why `((slot as usize) * num_shards) / 16384` and not `slot % num_shards`?** Modulo would
scatter contiguous slot ranges across shards in an interleaved pattern (slot 0→shard 0,
slot 1→shard 1, slot 2→shard 0 if num_shards=2, ...). The multiply-then-divide form instead
gives each shard one **contiguous** range of slots (e.g. with 4 shards: shard 0 owns
slots 0–4095, shard 1 owns 4096–8191, ...). This matters because `CLUSTER SLOTS`/`NODES`
in `connection.rs` report exactly this contiguous range per shard — the routing function
and the cluster-topology-reporting code have to agree on the same partitioning, and a
contiguous range is both what real Cluster clients expect and trivial to describe (`start_slot`,
`end_slot`) versus an interleaved assignment.

**Hash tags** (`extract_hash_tag`, the `{...}` substring convention) exist so related keys
can be forced onto the same shard/slot — e.g. `{user:1}:profile` and `{user:1}:settings`
both hash on `user:1`, guaranteeing they're co-located. This is table stakes for any
multi-key operation (a future `MULTI`/transaction, or even just wanting `MGET` on related
keys to stay a single-shard operation) and is, again, lifted directly from Redis Cluster's
own hash-tag convention rather than invented — same rationale as the slot scheme above:
compatibility with existing client behavior and documentation.

### 3. The `Router` struct: one per shard, holding the whole mesh's address book

```rust
pub struct Router {
    pub shard_id: usize,
    pub num_shards: usize,
    pub port: u16,
    pub local_db: Rc<RefCell<ShardDb>>,
    pub senders: Vec<flume::Sender<ShardMessage>>,
}
```

One `Router` is constructed per shard in `server.rs` and wrapped in `Rc` so every
connection task on that shard can cheaply clone a handle to it. It holds `senders` for
**every** shard including itself (`senders[shard_id]` is never used — routing always
checks `target == self.shard_id` first and goes local instead), which is a few wasted
bytes traded for not needing an `Option` or off-by-one indexing scheme to skip "self."

### 4. The local/remote fork, repeated per operation

Every method follows the same shape — `get` is representative:

```rust
pub async fn get(&self, key: Bytes) -> Option<Bytes> {
    let target = target_shard(&key, self.num_shards);
    if target == self.shard_id {
        self.local_db.borrow_mut().get(&key)          // zero-cost path
    } else {
        let (tx, rx) = flume::bounded(1);               // cross-core hop
        let msg = ShardMessage::Get { key, responder: tx };
        if self.senders[target].send(msg).is_ok() {
            rx.recv_async().await.ok().flatten()
        } else {
            None
        }
    }
}
```

**Why is this duplicated ~10 times (once per `ShardMessage` variant) instead of one
generic `dispatch<T>(msg, extract_target)` helper?** Each command has a different request
payload and a different response type (`Option<Bytes>` for `Get`, `bool` for `Del`,
`Result<i64, String>` for `IncrBy`, ...). A fully generic version would need either boxed
trait objects (an allocation and a dynamic dispatch on the hot path — exactly what this
codebase avoids everywhere else) or a macro. The repetition is the price paid for keeping
every method monomorphic and inlinable; given there are only ~10 commands, that trade was
made in favor of simplicity/readability over DRY-ing it up.

**A one-shot channel per remote call, even here — is that a contradiction with `agent.md`'s
"never allocate one-shot channels on the hot path" rule?** No, but it's a real edge worth
understanding precisely, not glossing over: this `flume::bounded(1)` allocation happens
*only* on this single-command API. The actual steady-state pipelined hot path
(`connection.rs::execute_commands_squashed`) never calls `Router::get`/`set`/etc. at all —
it calls `execute_local_command` directly for local keys and builds a `ShardMessage::Batch`
using the connection's pre-allocated `ResponderChannel` pool for remote keys, bypassing
`Router`'s per-op methods entirely. `Router`'s per-op methods here are the *fallback* path:
reached for a truly unpipelined single command, or for an entire pipeline that contains
even one un-batchable command (Part 3 §4). So this file's one-shot channel allocations are
real, and do run in production, but only off the benchmarked fast path — worth flagging
precisely because it would be easy to assume (wrongly) that `agent.md`'s invariant is
enforced everywhere just because it's true for `SET`-heavy pipelines.

### 5. `execute_remote`: the single-command version of batching, still allocating

```rust
pub async fn execute_remote(&self, target: usize, cmd: Command) -> Vec<u8> {
    let (tx, rx) = flume::bounded(1);
    let msg = ShardMessage::Batch { items: vec![(0, cmd)], responder: tx };
    ...
}
```

This reuses the `ShardMessage::Batch` wire format (a `Vec` of one item) purely so the
receiving shard's `Batch` handler in `server.rs` can be the single code path for "run this
command against the remote `ShardDb` and hand back the serialized RESP bytes" — rather
than adding a dedicated `ShardMessage::Single { cmd, responder }` variant. It's used by
`connection.rs::execute_command` for hash-type commands (`HSET`/`HGET`/etc.) issued as a
**lone** command, i.e. still on the non-squashed path. It still allocates a fresh
`bounded(1)` channel per call rather than drawing from the connection's pre-allocated
pool, for the same reason as §4: this is the unpipelined fallback, not the hot path.

### 6. A real, unaddressed gap: `MGET`/`MSET` don't use the mesh's parallelism at all

Look at how `connection.rs::execute_command` handles multi-key commands:

```rust
Command::Mget(keys) => {
    for key in keys {
        match router.get(key).await { ... }   // one at a time!
    }
}
```

Each key in an `MGET` is routed and awaited **sequentially**, one full cross-shard
round-trip at a time, even though the whole point of `ShardMessage::Batch` is to let
independent remote shards work **concurrently**. A 10-key `MGET` spread across 4 remote
shards pays for 10 serialized round-trips instead of up to 4 parallel ones. This is a
real, currently-unaddressed performance gap — not a design decision with a stated
trade-off, just something `MGET`/`MSET` never got wired up to use the same
bucket-by-shard-then-fan-out pattern that `execute_commands_squashed` already implements
for pipelined single-key commands. If `MGET`/`MSET` throughput on multi-shard keysets
becomes a benchmark target, this is the first place to look — the fix would mirror
`execute_commands_squashed`'s bucketing loop, applied to a single command's key list
instead of a whole pipeline's commands.

### 7. `client_list`: fan-out to every peer shard, sequentially, on every call

```rust
for (shard_id, sender) in self.senders.iter().enumerate() {
    if shard_id != self.shard_id {
        let (tx, rx) = flume::bounded(1);
        ...
        if let Ok(peer_list) = rx.recv_async().await { out.push_str(&peer_list); }
    }
}
```

`CLIENT LIST` is defined by Redis to show *all* connected clients, but each shard only
knows about its own connections (Part 2 §3.3). So this method has to query every other
shard and concatenate the results — inherently an O(num_shards) fan-out, done
sequentially rather than in parallel. This is acceptable because `CLIENT LIST` is an
administrative/debugging command, never called in a hot loop or benchmarked — unlike
`MGET` in §6, sequential-vs-parallel here was a reasonable choice given the command's
usage pattern, not an oversight.

---
---

## Part 5: RESP Parsing Engine

> Covers `src/resp.rs`. Learning guide to shipped code. Feeds `commands: Vec<Command>`
> into Part 3 §2 — read that section for what happens *after* parsing; this part is only
> about turning bytes on the wire into a `Command`.

### 1. What problem this module solves

Redis's wire protocol (RESP) is a streaming format: a client can send a command whose
bytes are split across multiple TCP reads (a large `SET` value straddling a packet
boundary), and a pipelined client sends several commands back-to-back in one `write()`,
which may or may not align with how the kernel decides to deliver them to `read()` on the
server side. `resp.rs` has to handle both directions of that mismatch: reassembling a
command that arrived in pieces, and splitting a buffer that contains more than one
complete command — while doing so **without copying the key and value bytes**, since
those bytes (potentially large `SET` payloads) get held onto by `RudisEntry` for as long
as the key lives in the store (Part 1 §5.2's `RudisEntry`).

### 2. `parse_command`: the two-frame-format dispatch, and why both exist

```rust
pub fn parse_command(buf: &mut BytesMut) -> Result<Option<Command>, String> {
    if buf.is_empty() { return Ok(None); }
    if buf[0] == b'*' { parse_resp_array(buf) } else { parse_inline_command(buf) }
}
```

Real Redis clients (and `redis-cli`, and `memtier_benchmark`) send the `*N\r\n$len\r\n...`
RESP array format. The **inline** format (`GET foo\r\n`, plain space-separated text) is a
Redis feature specifically meant for humans debugging over `nc`/`telnet` — the README
documents exactly this use case. Supporting both from one entrypoint, dispatched purely
on whether the first byte is `*`, means a single `parse_command` call site in
`connection.rs` works for either kind of client without the caller needing to know or
care which protocol mode a given connection is using — and a connection can even freely
mix the two between commands, since the check happens fresh on every call.

### 3. `parse_resp_array`: scan for completeness *before* consuming a single byte

This is the part of the file most worth understanding carefully, because it looks
inefficient (two passes over similar data) but the shape is deliberate:

```rust
// Pass 1: scan_cursor walks the whole frame WITHOUT calling buf.advance() or buf.split_to()
let mut scan_cursor = newline_pos + 2;
for _ in 0..num_args {
    ...
    if data_end + 2 > buf.len() { return Ok(None); }   // not enough bytes yet — bail, buf untouched
    ...
}
// Pass 2: only reached once pass 1 proved the whole frame is present
buf.advance(newline_pos + 2);
for _ in 0..num_args {
    ...
    let data = buf.split_to(arg_len).freeze();   // zero-copy!
    ...
}
```

**Why scan for completeness first instead of consuming bytes as they're validated, and
just... stopping (leaving `buf` partially advanced) if the frame turns out to be
incomplete?** Because `parse_command` is called in a loop from `connection.rs` that
expects an all-or-nothing contract: either a full `Command` is returned and consumed
from `buf`, or `Ok(None)` is returned and `buf` is untouched, ready to have more bytes
appended and the parse retried from the top on the next `read()`. If pass 1 didn't exist
and the code started calling `buf.advance()`/`buf.split_to()` mid-frame only to discover
arg 3 of 5 hasn't arrived yet, there would be no way to "put back" the bytes already
consumed — the caller would need a rollback mechanism (e.g. snapshotting `buf` before
every parse attempt), which is exactly the cost this two-pass structure avoids. The
tradeoff made here: pass 1 re-derives the same `arg_len`/CRLF-position information pass 2
will look up again, which is genuinely redundant work — but it's O(frame size) redundant
work done in memory, once per fully-buffered command, versus a rollback/snapshot
mechanism that would run on every partial read. Given pipelined commands are typically
small (the benchmarked workload is a 1KB `SET`), this was the right trade.

**Why is the zero-copy step (`buf.split_to(arg_len).freeze()`) only reachable in pass
2, after completeness is already proven?** `BytesMut::split_to` is not a peek — it
actually mutates `buf`, splitting off and returning ownership of a prefix. Calling it
speculatively in pass 1, only to discover the frame is incomplete a few args later, would
have the same "can't put it back cleanly" problem as above. So the zero-copy split only
ever happens once the code is certain it will consume the *entire* frame, no exceptions,
no partial commits.

### 4. `parse_inline_command`: correctness by construction, at the cost of a copy

```rust
let parts: Vec<Bytes> = line
    .split(|&b| b == b' ' || b == b'\t')
    .filter(|part| !part.is_empty())
    .map(Bytes::copy_from_slice)          // <-- a real copy, not zero-copy
    .collect();
```

Unlike `parse_resp_array`, inline parsing **copies** every argument (`Bytes::copy_from_slice`)
instead of splitting the underlying buffer. This is a deliberate scope cut, not an
oversight: the inline path exists for interactive human use (`nc`/`telnet`, per §2), never
for the benchmarked high-throughput pipelined workload, so there was no reason to spend
effort making it zero-copy — the RESP array path is the one that has to be fast, and it
is.

### 5. `build_command`: one shared command constructor for both frame formats

Both `parse_resp_array` and `parse_inline_command` end by collecting their arguments into
a flat `Vec<Bytes>` and calling the same `build_command(args)`. **Why funnel both formats
through one function instead of building the `Command` inline in each parser?** Because
"what `SET foo bar EX 10` *means*" (which argument is the key, how `EX`/`PX` options are
parsed, what error to return for wrong arity) has nothing to do with which wire format
carried those bytes — duplicating that logic in two places would mean every new command
or option has to be added twice, and would risk the two formats silently disagreeing on
edge cases (e.g. one accepting `SET k v EX abc` and the other rejecting it). This mirrors
exactly the same reasoning as `connection.rs`'s `execute_local_command` being shared
between the local and remote-batch execution paths (Part 3 §7): one implementation,
multiple callers, so there's structurally nothing to keep in sync by hand.

### 6. `Command` is a flat, closed enum — what that costs and what it buys

```rust
pub enum Command {
    Get(Bytes), Set { key, value, expire_in }, Mget(Vec<Bytes>), ...
    Unknown(String),
}
```

Every supported command is its own enum variant with typed fields (`Ttl(Bytes, bool)`
rather than a generic `Vec<Bytes>` of raw arguments that every handler re-parses). **Why
pay the cost of hand-writing a variant and a `build_command` arm for every single command
rather than keeping `Command` as `{ name: String, args: Vec<Bytes> }` and parsing
arguments lazily in the handler?** Two reasons visible directly in how `Command` gets
used downstream: `connection.rs::target_shard_of_cmd` (Part 3 §8) pattern-matches on
`Command` to decide routing *before* execution, which requires already knowing which
field is "the key" — impossible to do generically from a raw argument list without
re-parsing each command's arity rules a second time at the routing layer. And every
`execute_local_command` arm gets exhaustively checked by the compiler — adding a new
`Command` variant without handling it in `execute_local_command` or `target_shard_of_cmd`
is a compile error, not a silent runtime gap. The cost: adding a new Redis command means
touching four places (`Command` enum, `build_command`, `execute_local_command`,
`target_shard_of_cmd`), which is exactly the kind of repetition Part 4 §4 also accepts
for the same reason — monomorphic, exhaustively-checked code over a generic,
runtime-dispatched one.

**`Unknown(String)` as the catch-all** means an unrecognized command name is never a parse
error — it always successfully parses into a `Command`, deferred to execution time to
produce `-ERR unknown command`. This matters for the two-pass completeness scan in §3:
a not-yet-implemented command name shouldn't cause `parse_command` to error out and
force `should_quit` (Part 3 §2) the way a genuinely malformed frame does — an
unrecognized command name is a normal, expected, non-fatal outcome; a truncated bulk
string length or a missing CRLF is an actual protocol violation.

### 7. `find_crlf` / `find_crlf_at`: a plain byte scan, not SIMD

```rust
fn find_crlf_at(buf: &[u8], start: usize) -> Option<usize> {
    buf[start..].windows(2).position(|w| w == b"\r\n").map(|pos| start + pos)
}
```

This is a naive O(n) windowed scan, called once per bulk-string header (twice, actually,
per §3's two-pass structure) — no relation to the SIMD control-byte matching used
elsewhere in the codebase (Part 1 §5.5). **Why not vectorize this too, given how much
effort went into SIMD probing for the storage engine?** Because the strings being
scanned here are protocol *headers* (`$123\r\n`), not payload — a handful of bytes,
essentially always well under 16, so there's no group of 16+ bytes for a SIMD compare to
meaningfully amortize against. The actual bulk *data* between headers is never scanned
byte-by-byte at all — it's located by `arg_len` (an already-parsed integer) and consumed
with `split_to`, which is `O(1)` (just adjusts an offset/refcount on the underlying
buffer), not a scan. So the two hot-path costs — "find where the tiny header ends" and
"extract the (potentially large) payload" — are already handled by the right tool for
each: a trivial scan for the former, a zero-copy slice for the latter. SIMD would only
help the former, where there's nothing to vectorize.

### 8. What this parser deliberately doesn't support

- **No RESP3** (maps, sets, doubles, push messages, `HELLO`). Every reply written in
  `connection.rs` is hand-formatted RESP2 (`$len\r\n...`, `*N\r\n...`, `:N\r\n`, `+OK\r\n`,
  `-ERR ...\r\n`) — there's no version negotiation at all, so any client that requires
  RESP3 (or issues `HELLO 3`) will get `Unknown("HELLO")` today rather than a protocol
  downgrade.
- **No nested arrays.** `parse_resp_array` assumes every element of the top-level array is
  a bulk string (`$len\r\n...`) — it errors (`"Expected bulk string in command array"`) on
  anything else, including a nested `*`. This is fine for command frames (every real
  Redis command is a flat array of bulk strings) but means this parser could never be
  reused as-is for a generic RESP value parser (e.g. for a future `EVAL`-style feature
  that needs to parse an arbitrary RESP value, not just a command).
- **No maximum frame/argument size enforcement.** `arg_len` is parsed straight from the
  wire and used directly as a `split_to`/allocation size with no upper bound check — a
  client claiming a multi-gigabyte bulk string length will make the server attempt to
  buffer that much data (bounded only by however much the client actually sends before
  the read stalls, since `Ok(None)` just keeps waiting for more bytes). Real Redis
  enforces `proto-max-bulk-len`; there's no equivalent guard here.
