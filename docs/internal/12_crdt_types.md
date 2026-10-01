# Component 12: CRDT Data Types & Manual Multi-Region Sync — Implementation Reference

> **Source Files**: `src/crdt.rs` (685 lines)
> **High-Level Design Spec**: [`docs/design/12_crdt_types.md`](../design/12_crdt_types.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

This document is a line-level account of every struct, function, and wire format in
`src/crdt.rs`, plus every touchpoint in `src/shard.rs`, `src/resp.rs`, `src/connection.rs`,
and `src/aof.rs` that CRDT data flows through. Nothing here is carried forward from memory —
every claim below was re-checked against the current source in this pass.

---

## 1. Module Responsibilities

| File | Responsibility |
| :--- | :--- |
| `src/crdt.rs` | `HlcTimestamp`/`HybridLogicalClock`, the three CRDT value types (`LwwRegister`, `OrSet`, `PnCounter`), and `CrdtStore` (the per-shard container plus the manual export/merge wire format and tombstone GC). |
| `src/shard.rs` | Embeds one `CrdtStore` per shard (`ShardDb.crdt_store`, constructed as `CrdtStore::new(port)`); exposes the thin forwarding methods `crdt_set`/`crdt_get`/`crdt_del`/`crdt_incrby`/`crdt_sadd`/`crdt_smembers`/`crdt_srem`/`crdt_dump`/`crdt_merge`/`crdt_gc`; and folds the CRDT store into the per-shard RDB chunk format (`save_rdb_chunk` → `save_extended_rdb_chunk`, type byte `13`) and its restore counterpart (`restore_rdb_chunk`). |
| `src/resp.rs` | Parses `CRDT.SET\|GET\|DEL\|INCRBY\|SADD\|SMEMBERS\|SREM\|DUMP\|MERGE\|GC` (10 subcommands) into the corresponding `Command::Crdt*` variants (`resp.rs:1400-1422` for the enum arms, `resp.rs:9790-9865` for the parser). |
| `src/connection.rs` | `execute_local_command` (single-shard path, `connection.rs:17835-17916`) dispatches the seven single-key `Crdt*` variants and calls `record_change!(cmd)` on every mutation; the async router path (`connection.rs:11227-11292`) fans `CrdtDump`/`CrdtMerge`/`CrdtGc` out to every other shard via `execute_remote` and aggregates the per-shard results into one client-visible reply. |
| `src/aof.rs` | `command_to_resp` (the function that decides what gets appended to the AOF and streamed to replicas) has **no match arm for any `Command::Crdt*` variant** — see §4. `rewrite_shard_aof` (BGREWRITEAOF compaction) also never touches `db.crdt_store` — see §4. |

---

## 2. Data Structures (verbatim from `src/crdt.rs`)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HlcTimestamp {
    pub physical_ms: u64,
    pub logical: u32,
    pub node_id: u16,
}
// Ord/PartialOrd (crdt.rs:36-49): lexicographic tuple compare, in order:
// physical_ms, then logical, then node_id. physical_ms dominates; node_id is
// only a tie-breaker when both physical_ms and logical are equal.

pub struct HybridLogicalClock {
    pub node_id: u16,
    latest_physical_ms: AtomicU64,   // private
    latest_logical: AtomicU32,       // private
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LwwRegister {
    pub value: Bytes,
    pub timestamp: HlcTimestamp,
    pub tombstone: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrSet {
    pub elements: HashMap<Bytes, HashSet<HlcTimestamp>>,   // member -> set of observed "add" tags
    pub tombstones: HashSet<HlcTimestamp>,                 // tags known to have been removed
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PnCounter {
    pub p: HashMap<u16, i64>,  // per-node-id positive contributions
    pub n: HashMap<u16, i64>,  // per-node-id negative contributions
}

pub struct CrdtStore {
    pub clock: HybridLogicalClock,
    pub registers: HashMap<Bytes, LwwRegister>,
    pub sets: HashMap<Bytes, OrSet>,
    pub counters: HashMap<Bytes, PnCounter>,
}
```

Exact field counts: `HlcTimestamp` — 3 fields (`u64`+`u32`+`u16` = 14 bytes packed, though
Rust will pad the in-memory layout). `LwwRegister` — 3 fields. `OrSet` — 2 fields, both
collections unbounded (no configured max element/tombstone count anywhere in the file).
`PnCounter` — 2 fields, one `HashMap<u16, i64>` per sign, so a counter's total memory cost is
`O(distinct node_ids that have ever written it)`, and entries are **never removed** by `merge`
or anywhere else — a `PnCounter` only grows. `CrdtStore` — 4 fields (clock + 3 value-type maps).

`LwwRegister` and `OrSet` are concretely `Bytes`-keyed, not generic over a value type. `OrSet`
tracks per-element observed-add tags directly as a `HashSet<HlcTimestamp>` (no separate UUID or
dot type). `PnCounter` stores signed `i64` per-node deltas in two maps (`p`/`n`) rather than a
single unsigned counter pair; `PnCounter::inc(node_id, delta)` (`crdt.rs:284-290`) routes based
on the sign of `delta`: `if delta >= 0 { *p.entry(node_id).or_default() += delta } else {
*n.entry(node_id).or_default() += -delta }`. `dec(node_id, delta)` (`crdt.rs:292-294`) is
implemented as `self.inc(node_id, -delta)` — i.e. it is not a separate code path, just a sign
flip through `inc`. So "decrement by 3" becomes `n[node_id] += 3`, never a negative entry in `p`.

`CrdtStore` is instantiated once per shard as a field of `ShardDb` (`src/shard.rs:617`,
constructed at `src/shard.rs:637` inside `ShardDb::new(port)` as `CrdtStore::new(port)`) — keyed
by node ID = the server's listening **port**, not a cluster-wide node identity. The real
production call site is `src/server.rs:130`:
`let local_db = Rc::new(RefCell::new(ShardDb::new(port).with_shard(shard_id)));`, inside the
per-shard-worker startup path. Every shard worker in one process is constructed with the *same*
`port` value (only `shard_id` differs, via `.with_shard`), and the listener itself is bound with
`SO_REUSEPORT` (`server.rs:105-106`, `set_reuse_port(true)`). Concretely: **every shard in one
Rudis process generates HLC timestamps under the identical `node_id`** — HLC node identity is
per-listening-port, not per-shard and not per-physical-instance in any stronger sense than "one
port = one node_id, however many shards/cores sit behind it."

---

## 3. Execution Algorithms

### 3.1 HLC generation and remote-observation: lock-free CAS retry loops

```rust
// crdt.rs:72-102
pub fn now(&self) -> HlcTimestamp {
    let phys_now = wall_clock_millis();
    loop {
        let cur_phys = self.latest_physical_ms.load(Acquire);
        let cur_log = self.latest_logical.load(Acquire);
        let (next_phys, next_log) = if phys_now > cur_phys { (phys_now, 0) } else { (cur_phys, cur_log + 1) };
        if self.latest_physical_ms.compare_exchange(cur_phys, next_phys, Release, Relaxed).is_ok() {
            self.latest_logical.store(next_log, Release);
            return HlcTimestamp::new(next_phys, next_log, self.node_id);
        }
        // else: another concurrent caller updated latest_physical_ms first — retry from the top
    }
}
```

Note the `store` after the `compare_exchange`, not part of the same atomic operation — between
the successful CAS on `latest_physical_ms` and the subsequent `latest_logical.store`, another
thread reading `(latest_physical_ms, latest_logical)` as a pair could observe a stale `logical`
value for the already-updated `physical_ms`. In practice this only matters for other calls to
`now()`/`update()` also spinning past the CAS, since every genuinely concurrent caller retries
the loop until it sees a consistent pair it can build on top of; no external reader ever reads
the two atomics directly (only `HlcTimestamp`, a fully-formed copy, escapes the clock).

```rust
// crdt.rs:105-140
pub fn update(&self, remote: &HlcTimestamp) -> HlcTimestamp {
    let phys_now = wall_clock_millis();
    loop {
        let cur_phys = self.latest_physical_ms.load(Acquire);
        let cur_log = self.latest_logical.load(Acquire);
        let max_phys = phys_now.max(cur_phys).max(remote.physical_ms);
        let next_log = if max_phys == cur_phys && max_phys == remote.physical_ms {
            cur_log.max(remote.logical) + 1
        } else if max_phys == cur_phys {
            cur_log + 1
        } else if max_phys == remote.physical_ms {
            remote.logical + 1
        } else {
            0   // local wall clock alone produced the new max — reset logical to 0
        };
        if self.latest_physical_ms.compare_exchange(cur_phys, max_phys, Release, Relaxed).is_ok() {
            self.latest_logical.store(next_log, Release);
            return HlcTimestamp::new(max_phys, next_log, self.node_id);
        }
    }
}
```

This is the textbook HLC merge rule: the physical component never moves backward (it is the max
of local wall-clock time, the clock's own last-known physical value, and the remote timestamp's
physical value), and the logical counter only increments when the winning physical value is tied
with something already seen (local state and/or the remote timestamp) — a clean jump to a higher
physical value (wall clock alone wins) resets logical to 0. **Clock-skew handling**: there is no
explicit skew bound check or rejection of "too far in the future" remote timestamps anywhere in
this function — `update` unconditionally trusts `remote.physical_ms` as a candidate for the new
clock value, so a remote node with a badly skewed wall clock can permanently push a local HLC's
physical component far ahead of local wall-clock time (the `now()` call above only ever produces
`phys_now` as a *floor*, never a ceiling, once `latest_physical_ms` has been pushed higher by
`update`). `update` is called from `merge_sync_payload` (§3.3) for every single timestamp decoded
from an incoming sync payload — registers, counter deltas carry no per-write timestamp so they
don't call `update`, but every `OrSet` add-tag and tombstone does — so the local clock is
causally advanced past everything merged, subject to the no-skew-check caveat above.

### 3.2 Per-type merge algorithms

```rust
// LwwRegister::merge, crdt.rs:169-178 — strictly later HLC timestamp wins outright,
// ties (other.timestamp == self.timestamp, impossible unless two nodes forge the same tag,
// or other.timestamp < self.timestamp) keep the existing value. Not commutative on
// exact-tie inputs in the sense of "last write" — it is deterministic (existing always wins
// a tie) but there is no total order on *concurrent* equal timestamps beyond the tuple Ord.
pub fn merge(&mut self, other: &LwwRegister) -> bool {
    if other.timestamp > self.timestamp {
        self.value = other.value.clone();
        self.timestamp = other.timestamp;
        self.tombstone = other.tombstone;
        true
    } else { false }
}

// PnCounter::merge, crdt.rs:297-306 — per-node-id component-wise maximum on both the p and
// n maps (each node's own running total only grows across merges, since counter_incr always
// adds to whatever is already stored locally for that node_id before any merge happens).
pub fn merge(&mut self, other: &PnCounter) {
    for (&node_id, &val) in &other.p { let e = self.p.entry(node_id).or_default(); *e = (*e).max(val); }
    for (&node_id, &val) in &other.n { let e = self.n.entry(node_id).or_default(); *e = (*e).max(val); }
}
pub fn value(&self) -> i64 { self.p.values().sum::<i64>() - self.n.values().sum::<i64>() }

// OrSet::merge, crdt.rs:234-253 — union both tag sets and the tombstone set, then drop any
// tag now covered by a tombstone; an element whose every tag is tombstoned is removed
// entirely from `elements` (not left behind as an empty HashSet).
pub fn merge(&mut self, other: &OrSet) {
    for ts in &other.tombstones { self.tombstones.insert(*ts); }
    for (elem, other_tags) in &other.elements {
        let my_tags = self.elements.entry(elem.clone()).or_default();
        for tag in other_tags { my_tags.insert(*tag); }
    }
    self.elements.retain(|_, tags| { tags.retain(|tag| !self.tombstones.contains(tag)); !tags.is_empty() });
}
```

`OrSet::add(element, ts)` (`crdt.rs:196-198`) inserts `ts` into `elements.entry(element)
.or_default()` — every add gets a *fresh* HLC tag from `clock.now()` (see `CrdtStore::set_add`,
§3.5), so re-adding the same member twice produces two distinct tags in its `HashSet`.
`OrSet::remove(element)` (`crdt.rs:201-211`) moves **every** add-tag currently associated with
`element` into `tombstones` and drops the element's entry from `elements` outright; it can only
tombstone tags it has locally observed — a concurrent add on another replica, carrying a tag this
node's remove call never saw, is untouched by that remove. `OrSet::read()`/`contains()`
(`crdt.rs:213-231`) treat an element as present iff at least one of its recorded add-tags is
absent from `tombstones` — this is the entire mechanism behind add-wins semantics: a concurrent
add whose tag the remove never observed survives merge because that tag was never placed in
`tombstones`, matching the unit test `test_orset_add_wins` (`crdt.rs:667-684`) verbatim.

### 3.3 Manual sync format: `export_sync_payload`/`merge_sync_payload`

**`CrdtStore::export_sync_payload(&self) -> Vec<u8>`** (`crdt.rs:387-442`) serializes the
**entire** local store — every register, counter, and set currently held, iterated in
`HashMap` order (i.e. unspecified/non-deterministic across runs) — into one flat, uncompressed,
hand-rolled buffer. Exact byte layout, all integers little-endian:

- **Register** (type byte `1`): `1u8` · `key_len:u32` · `key` · `value_len:u32` · `value` ·
  `physical_ms:u64` · `logical:u32` · `node_id:u16` · `tombstone:u8` (`0`/`1`).
- **Counter** (type byte `2`): `2u8` · `key_len:u32` · `key` · `p_len:u32` ·
  `p_len × (node_id:u16, val:i64)` · `n_len:u32` · `n_len × (node_id:u16, val:i64)`.
- **Set** (type byte `3`): `3u8` · `key_len:u32` · `key` · `elem_count:u32` · for each element:
  `elem_len:u32` · `elem` · `tag_count:u32` · `tag_count × (physical_ms:u64, logical:u32,
  node_id:u16)` — followed once, after all elements, by `tombstone_count:u32` ·
  `tombstone_count × (physical_ms:u64, logical:u32, node_id:u16)`.

Items are simply concatenated back-to-back with no outer length prefix, no version byte, and no
checksum/CRC anywhere in the payload. `CRDT.DUMP` re-serializes the full store from scratch on
every call — there is no cached/incremental payload.

**`CrdtStore::merge_sync_payload(&mut self, data: &[u8]) -> Result<usize, String>`**
(`crdt.rs:445-605`) walks that exact format byte-by-byte with manual offset arithmetic (no
`serde`, no framing library), reconstructs each `LwwRegister`/`PnCounter`/`OrSet`, calls
`self.clock.update(&ts)` (§3.1) for every HLC timestamp it decodes — registers once per item,
sets once per add-tag *and* once per tombstone tag, counters not at all (counter deltas carry no
timestamp in the wire format) — and merges the reconstructed value into the matching local map
via the real `merge()` methods from §3.2. For a register key not yet present locally, it does
`self.registers.entry(k).or_insert_with(|| remote_reg.clone()).merge(&remote_reg)` — insert the
clone, then immediately merge the same value into itself, a harmless no-op merge rather than a
bug, since `merge` against an identical timestamp/value is a false-returning no-op. Returns
`Err(format!("Unknown CRDT item type: {}", item_type))` for any tag byte other than `1`/`2`/`3`,
and on success returns the count of items merged. **There is no bounds-safety net against a
truncated or corrupted payload for most fields**: length-prefixed reads for key/value/element
bytes and fixed-width reads for the HLC/counter fields use direct slice indexing
(`data[offset..offset+n].try_into().unwrap()`) with no prior bounds check — the code checks
`offset + 4 > data.len()` only immediately before reading each item's *leading* `u32` length
field (e.g. `crdt.rs:456-458`, `496-498`, `535-537`), but **not** before any of the subsequent
fixed-width reads (the `phys`/`log`/`nid`/`tombstone` byte reads for a register, the per-p/n-entry
reads for a counter, the per-tag reads for a set) or before slicing `key`/`value`/`elem` by their
declared length. A malformed or truncated payload passed to `CRDT.MERGE` — e.g. via
`CRDT.MERGE` with attacker- or corruption-supplied bytes, not just a payload produced by
`CRDT.DUMP` on a healthy peer — can **panic the shard thread** (`slice index out of range` or
`TryFromSliceError::unwrap()`) rather than returning a clean `Err`. This is a real gap: the only
partial defense is the four early `offset + 4 > data.len()` guards noted above, which cover fewer
than half of the format's variable-length reads.

This export → external transport → merge cycle is the entirety of "multi-region sync" as
implemented: **re-verified — nothing inside Rudis schedules, transports, or discovers peers for
it.** There is no background task, no peer list, no gossip, and no periodic timer anywhere in
`crdt.rs`, `shard.rs`, or `server.rs` that calls `export_sync_payload`/`merge_sync_payload`
automatically. A caller (operator script, external sidecar, or any other process with network
access to more than one Rudis instance) is solely responsible for running `CRDT.DUMP` on a
source node, moving the resulting bytes somewhere, and calling `CRDT.MERGE` with them on a
destination node — on whatever schedule that external process chooses. `CRDT.DUMP`/`CRDT.MERGE`
also always operate on the entire store (§4 fan-out) — there is no per-key or delta sync
primitive at any layer.

### 3.4 Tombstone garbage collection: on-demand only

```rust
// crdt.rs:608-625
pub fn gc_tombstones(&mut self, ttl_ms: u64) -> (usize, usize) {
    let cutoff = now_ms.saturating_sub(ttl_ms);
    let reg_pruned = { retain registers where !tombstone || timestamp.physical_ms >= cutoff };
    let set_pruned: usize = sets.values_mut().map(|s| s.prune_tombstones(cutoff)).sum();
    (reg_pruned, set_pruned)
}
```

`OrSet::prune_tombstones(cutoff_physical_ms)` (`crdt.rs:257-262`) retains only tombstone entries
whose `physical_ms >= cutoff`, i.e. drops tombstones *older* than the cutoff, and returns the
count dropped. Exposed as `CRDT.GC [ttl_ms]` (parsed at `resp.rs:9855-9865`; default TTL noted in
the doc comment at `crdt.rs:607` as `86_400_000` ms / 24h, though `CrdtGc(Option<u64>)` means the
caller must supply the TTL for it to actually take effect — there is no server-side default timer
that fires with that 24h value on its own). `gc_tombstones` prunes tombstoned `LwwRegister`s and
`OrSet` tombstone entries strictly by age of the *tombstone's own* HLC physical component versus
a cutoff derived from the current wall clock; it never touches `PnCounter` state (PN-Counters
have no tombstones — see §2, entries only grow). **Re-verified: there is no background task
anywhere in `server.rs` that calls `gc_tombstones`/`CRDT.GC` automatically** — a long-running
instance that never issues `CRDT.GC` accumulates tombstones (and therefore `export_sync_payload`
output size, and therefore `CRDT.DUMP` payload size and `merge_sync_payload` cost on the
receiving end) without bound.

### 3.5 `CrdtStore`'s per-operation methods (`crdt.rs:317-625`)

| Method | Behavior |
| :--- | :--- |
| `set(key, val) -> HlcTimestamp` | Stamps a fresh `clock.now()` timestamp, inserts a brand-new `LwwRegister::new(val, ts)` unconditionally (no read-modify-merge against any existing register — a local `CRDT.SET` always wins locally regardless of the existing timestamp, since it isn't going through `merge()` at all). Returns the timestamp so the caller sees the exact HLC address of this write. |
| `get(key) -> Option<Bytes>` | Returns `None` if the register doesn't exist *or* if `tombstone == true` (a deleted key reads as absent, but the tombstoned `LwwRegister` row is still occupying the map — see §3.4 for when it's actually dropped). |
| `del(key) -> bool` | In-place sets `tombstone = true` and stamps a new `clock.now()` timestamp on the existing register; returns `false` (no-op) if the key doesn't exist or is already tombstoned — repeated `CRDT.DEL` on an already-deleted key is a true no-op, not a timestamp bump. |
| `counter_incr(key, delta) -> i64` | `self.clock.node_id` (the shard's own node_id, not a fresh HLC timestamp — counters carry no per-op timestamp at all) is used as the map key into `p`/`n` via `PnCounter::inc`; returns the counter's new total value. |
| `counter_get(key) -> i64` | `0` if the counter key doesn't exist (`unwrap_or(0)`), not `None` — there is no way to distinguish "never written" from "written and net-zero" via `CRDT.INCRBY`'s read path. |
| `set_add(key, member) -> bool` | Stamps `clock.now()`, calls `OrSet::add`, returns `!was_present` (computed via `contains()` *before* the add) — i.e. "was this a logically new member" rather than "did storage change" (adding an already-present member still inserts a fresh tag into its `HashSet`, growing memory, even though the return value is `false`). |
| `set_members(key) -> Vec<Bytes>` | `OrSet::read()` or empty vec if the key doesn't exist. |
| `set_rem(key, member) -> bool` | `OrSet::remove()` or `false` if the key doesn't exist. |

---

## 4. Cross-Component Interactions, Durability, and the AOF/Replication Gap

- **`src/shard.rs`**: one `CrdtStore` per `ShardDb` (§2), forwarded to via the thin wrapper
  methods listed in §1 (`shard.rs:3905-3948`).
- **`src/resp.rs`**: command parsing for the ten `CRDT.*` subcommands into `Command::Crdt*`
  variants (`resp.rs:1400-1422` enum, `resp.rs:9790-9865` parser), each with standard
  arity-checking (`"wrong number of arguments for '...' command"`).
- **`src/connection.rs`**: the seven single-key variants (`CrdtSet`/`Get`/`Del`/`Incrby`/`Sadd`/
  `Smembers`/`Srem`) are routed to the shard owning the key via the same `target_shard`
  mechanism used for every other keyed command (Component 04), so a `CrdtStore` behaves as one
  logical per-node store addressed by normal key routing rather than requiring the client to
  know which shard holds a given CRDT key. The whole-store commands (`CrdtDump`/`CrdtMerge`/
  `CrdtGc`, `connection.rs:11227-11292`) execute locally on the connection's own shard and then
  call `router.execute_remote(sid, ...)` against every other shard (`0..router.num_shards`,
  skipping `router.shard_id`), aggregating: `CrdtDump` concatenates raw dump bytes from every
  shard's reply, `CrdtMerge` sums the per-shard merged-item counts (parsed back out of each
  shard's RESP integer reply), `CrdtGc` sums pruned-register and pruned-tombstone counts (parsed
  out of each shard's 4-element RESP array reply) — so the client sees one logical whole-node
  result assembled from N per-shard RESP replies.

### 4.1 AOF and replication: re-verified — zero coverage for any `CRDT.*` mutation

`execute_local_command` (`connection.rs:17835-17916`) calls `record_change!(cmd)`
(`connection.rs:12740-12761`) on every mutating `Crdt*` arm — `CrdtSet`, `CrdtDel` (only when it
actually flips a live register to tombstoned), `CrdtIncrby`, `CrdtSadd`, `CrdtSrem` (only on
actual removal), and `CrdtMerge` (only on `Ok`). `record_change!` is the single macro every
mutating command in the file goes through; its body:

```rust
// connection.rs:12740-12761
macro_rules! record_change {
    ($cmd_expr:expr) => {
        DIRTY_CHANGES.fetch_add(1, Relaxed);
        if HAS_WATCHED_KEYS.load(Relaxed) {
            for_each_cmd_key($cmd_expr, |k| touch_watched_key(db.port, k));
        }
        let need_aof = aof.is_some();
        let need_rep = crate::replication::has_connected_replicas(db.port);
        if need_aof || need_rep {
            if let Some(bytes) = crate::aof::command_to_resp($cmd_expr) {
                if let Some(aof_w) = aof { aof_w.borrow_mut().append(&bytes); }
                if need_rep { crate::replication::propagate_shard_bytes(db.port, db.shard_id, &bytes); }
            }
        }
    };
}
```

So a CRDT write **does** increment `DIRTY_CHANGES` and **does** touch `WATCH`ed keys (both of
those run unconditionally). But AOF-append and replica-propagate are both gated behind
`crate::aof::command_to_resp($cmd_expr)` returning `Some(bytes)` — and `command_to_resp`
(`src/aof.rs:166-1938`) is a giant `match cmd { ... }` covering every other command family in the
codebase (`Set`, `Hset`, `Vadd`, `AgentCheckpointPut`, etc.) that ends, at `aof.rs:1936`, in a
bare `_ => None`. **`rg -n "Crdt" src/aof.rs` returns zero matches** — there is no arm for any
`Command::Crdt*` variant anywhere in that 1,772-line match, so every single `Crdt*` command falls
through to the default `_ => None`. Concretely, for every `CRDT.SET`/`DEL`/`INCRBY`/`SADD`/
`SREM`/`MERGE` call: `need_aof || need_rep` may well be `true`, `command_to_resp` is called, it
returns `None`, and the `if let Some(bytes) = ...` body — the AOF append and the replica
propagate call — simply never executes. Nothing is written to the AOF file and nothing is sent
over the replication stream. This is exact, not inferred: the gate is a single `if let Some(...)`
with no CRDT-specific carve-out anywhere else in the file.

**BGREWRITEAOF does not recover this gap either.** `crate::aof::rewrite_shard_aof`
(`aof.rs:1965-`, invoked via `Router::bgrewriteaof` → `ShardMessage::RewriteAof`) compacts the
AOF by iterating `db.table.entries()` and re-emitting canonical `SET`/`HSET`/`RPUSH`/etc. RESP
commands for whatever is live in `RudisTable` — it never reads `db.crdt_store` at all (nor could
it meaningfully, since CRDT values are never `RudisValue` entries in `RudisTable` in the first
place — see below). A compacted AOF is therefore just as blind to CRDT state as the incremental
log it replaces.

**What *does* capture CRDT state: point-in-time RDB snapshots, confirmed.** `ShardDb::save_rdb_chunk`
(`shard.rs:2274-2318`) ends by calling `self.save_extended_rdb_chunk(buf)`
(`shard.rs:2320-2450`), whose final section (`shard.rs:2441-2450`) is:

```rust
// 7. CRDT sync state
let crdt_payload = self.crdt_store.export_sync_payload();
if !crdt_payload.is_empty() {
    let crdt_marker = Bytes::from_static(b"__rudis_crdt_sync__");
    buf.extend_from_slice(&(crdt_marker.len() as u32).to_le_bytes());
    buf.extend_from_slice(&crdt_marker);
    buf.push(13u8);   // extended-record type byte for "CRDT sync payload"
    buf.extend_from_slice(&(crdt_payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&crdt_payload);
}
```

i.e. the *entire* `CrdtStore` is embedded, using exactly the §3.3 wire format, as one extended
RDB record keyed under the sentinel name `__rudis_crdt_sync__`, tagged with type byte `13`.
`ShardDb::restore_rdb_chunk` (`shard.rs:2659-`) decodes it symmetrically at its `type_byte == 13`
branch (`shard.rs:2950-2963`), calling `self.crdt_store.merge_sync_payload(payload)` and
discarding the `Result` (`let _ = ...` — a corrupt or partially-truncated CRDT record inside an
otherwise-valid RDB chunk is silently swallowed rather than failing the whole restore, though per
§3.3 a sufficiently malformed payload can still panic before that `Result` is ever produced).
`save_rdb_chunk`/`restore_rdb_chunk` are the same per-shard functions used by **every** RDB-shaped
path in the codebase — re-verified via `rg -n "save_rdb_chunk\(" src/*.rs`:
  - `SAVE`/`BGSAVE` to `dump.rdb` (`router.rs:3238`, the on-disk snapshot path),
  - `Router::generate_full_rdb` (`router.rs:3105`), the full RDB blob assembled for a **PSYNC full
    resync** to a new or unsynced replica,
  - the cross-shard `ShardMessage::SaveRdbChunk` responder (`server.rs:1549`), and
  - `DEBUG RELOAD`-style round trips (`connection.rs:2713`, `shard.rs:4047`).

So the precise durability picture is:

- **On-disk restart recovery**: CRDT state survives a clean `SAVE`/`BGSAVE` + restart-from-RDB,
  as of whatever the last snapshot's timestamp was. If the deployment runs with AOF enabled
  instead of (or ahead of) RDB — this codebase's own startup comment at `server.rs:133-134`
  states "if AOF is enabled, AOF is authoritative; otherwise load RDB" — CRDT state is lost on
  restart regardless of how recently `CRDT.SET` etc. were called, because AOF never captured it
  (§4.1 above) and AOF being authoritative means the RDB snapshot is not consulted.
- **Replication**: a replica that completes a **full** PSYNC resync receives a point-in-time copy
  of the primary's CRDT state as of that resync (it rides along inside `generate_full_rdb`).
  After that initial transfer, **no further CRDT write ever reaches the replica** — there is no
  partial-resync/backlog coverage for CRDT commands (impossible in principle, since the backlog
  is built from exactly the same `command_to_resp` bytes that are `None` for every `Crdt*`
  command) and no periodic re-snapshot. A promoted replica after failover is therefore frozen at
  whatever CRDT state it held at its last full resync, with any primary-side CRDT writes between
  that resync and the failover silently lost.
- **`CRDT.DUMP`/`CRDT.MERGE`** (§3.3) remain the only *operator-driven* path for propagating live
  CRDT writes between two already-running instances; it is not persistence and not automatic.
- **`src/table.rs`**: no relationship — CRDT values are not `RudisValue` variants and live
  entirely in `CrdtStore`'s own maps, never in `RudisTable`. This is also why `EXPIRE`, `TYPE`,
  `OBJECT ENCODING`, and every other key-introspection command that walks `RudisTable` cannot see
  CRDT keys at all; they occupy a fully separate namespace from ordinary Rudis keys.

---

## Contributor Gotchas & Debugging Guide

* **Gotcha 1**: `HlcTimestamp.node_id` is the shard's listening **port**
  (`CrdtStore::new(port)`, `shard.rs:637`), not a cluster-wide node identity. Every shard worker
  in one process is started with the identical `port` (`server.rs:130`, `SO_REUSEPORT` listener
  at `server.rs:105-106`), so **all shards in one Rudis process generate HLC timestamps under the
  same `node_id`** — HLC node identity does not distinguish shards/cores within a process, only
  distinct listening ports (i.e. distinct processes/instances).
* **Gotcha 2 (re-verified this pass, traced to the exact gate)**: CRDT writes update
  `DIRTY_CHANGES` and the `WATCH` machinery (unconditionally, inside `record_change!`) but are
  **silently excluded** from both AOF persistence and replica streaming, because
  `crate::aof::command_to_resp` (`src/aof.rs`) has literally zero match arms for `Command::Crdt*`
  and falls through to `_ => None` — confirmed via `rg -n "Crdt" src/aof.rs` returning no hits.
  BGREWRITEAOF compaction (`rewrite_shard_aof`) doesn't touch `crdt_store` either. Do not assume
  `CRDT.SET`/`INCRBY`/`SADD`/`SREM`/`DEL`/`MERGE` survive a restart-with-AOF-authoritative or an
  ongoing replication stream the way `SET`/`HSET`/etc. do. The *only* thing that persists CRDT
  state at all is a plain RDB snapshot (`save_rdb_chunk`, type-byte-`13` extended record) or a
  PSYNC full resync (which is itself just an RDB blob) — and even that is a point-in-time copy,
  not ongoing replication.
* **Gotcha 3**: `CRDT.GC` is purely on-demand (`Command::CrdtGc(Option<u64>)` requires an
  explicit caller-supplied TTL); nothing in `server.rs` schedules it automatically, so tombstones
  — and `CRDT.DUMP`/RDB-embedded payload size — grow without bound on an instance that never
  calls it.
* **Gotcha 4**: `export_sync_payload`/`merge_sync_payload` always operate on the *entire* local
  store — there is no per-key or delta sync primitive. `CRDT.DUMP` fully re-serializes on every
  call.
* **Gotcha 5 (new finding this pass)**: `merge_sync_payload`'s bounds checking is incomplete. It
  checks `offset + 4 > data.len()` only before each item's leading length field, not before any
  of the fixed-width HLC/counter-entry/tombstone reads that follow, which use
  `data[offset..offset+n].try_into().unwrap()`. A truncated or corrupted buffer passed to
  `CRDT.MERGE` can panic the shard thread (slice-index or `TryFromSliceError` panic) instead of
  returning a clean `Err`.
* **Gotcha 6**: `PnCounter`'s `p`/`n` maps only ever grow — no node-id entry is ever removed, by
  `merge` or by anything else in `crdt.rs`. A counter touched by many distinct `node_id`s over a
  long-running cluster's life accumulates map entries without bound (independent of, and not
  addressed by, `CRDT.GC`, which only prunes register/set tombstones).
* **Gotcha 7**: `OrSet::set_add`'s return value (`!was_present`, computed from `contains()` before
  the add) answers "is this member logically new," not "did this call change stored bytes" — a
  redundant add of an already-present member still inserts a new tag into the member's
  `HashSet<HlcTimestamp>`, growing memory, while returning `false`/`0` to the client.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib crdt -- --test-threads=1
```
