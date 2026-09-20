# Component 12: CRDT Data Types & Manual Multi-Region Sync — Implementation Reference

> **Source Files**: `src/crdt.rs`
> **High-Level Design Spec**: [`docs/design/12_crdt_types.md`](../design/12_crdt_types.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Module Responsibilities

| File | Responsibility |
| :--- | :--- |
| `src/crdt.rs` | `HlcTimestamp`/`HybridLogicalClock`, the three CRDT value types (`LwwRegister`, `OrSet`, `PnCounter`), and `CrdtStore` (the per-shard container plus manual export/merge wire format). |
| `src/shard.rs` | Embeds one `CrdtStore` per shard (`ShardDb.crdt_store`) and exposes thin forwarding methods (`crdt_set`, `crdt_get`, `crdt_del`, `crdt_incrby`, `crdt_sadd`, `crdt_smembers`, `crdt_srem`, `crdt_dump`, `crdt_merge`, `crdt_gc`). |
| `src/resp.rs` | Parses `CRDT.SET\|GET\|DEL\|INCRBY\|SADD\|SMEMBERS\|SREM\|DUMP\|MERGE\|GC` into the corresponding `Command::Crdt*` variants. |
| `src/connection.rs` | Dispatches single-key `Command::Crdt*` variants through the standard key-routing path; fans `CrdtDump`/`CrdtMerge`/`CrdtGc` out to every shard and aggregates. |

---

## 2. Data Structures (verbatim from `src/crdt.rs`)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HlcTimestamp {
    pub physical_ms: u64,
    pub logical: u32,
    pub node_id: u16,
}
// Ord/PartialOrd: lexicographic tuple compare on (physical_ms, logical, node_id).

pub struct HybridLogicalClock {
    pub node_id: u16,
    latest_physical_ms: AtomicU64,
    latest_logical: AtomicU32,
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

`LwwRegister` and `OrSet` are concretely `Bytes`-keyed, not generic over a value type. `OrSet`
tracks per-element observed-add tags directly as a `HashSet<HlcTimestamp>` (no separate UUID or
dot type). `PnCounter` stores signed `i64` per-node deltas in two maps (`p`/`n`) rather than a
single unsigned counter pair; `inc`/`dec` route a call into `p` or `n` based on the sign of the
delta (`inc(node_id, delta)`: if `delta >= 0`, add to `p[node_id]`; else add `-delta` to
`n[node_id]`), so a single logical "decrement by 3" becomes `n[node_id] += 3`, not a negative
entry in `p`.

`CrdtStore` is instantiated once per shard, as a field of `ShardDb` (`src/shard.rs`), keyed by
node ID = the server's port (`CrdtStore::new(port)`), **not** a cluster-wide node identity —
worth noting because `HlcTimestamp.node_id` is therefore per-shard-listener, not per-physical-
Rudis-instance, when multiple shards share one port via `SO_REUSEPORT` in a single-process
deployment (each shard's `Router` is constructed with the same `port`, so all shards within one
Rudis process currently generate HLC timestamps under the same `node_id`).

---

## 3. Execution Algorithms

### 3.1 HLC generation and remote-observation: lock-free CAS retry loops

```rust
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
        // else: another concurrent caller updated the clock first — retry
    }
}
```

`update(&self, remote: &HlcTimestamp)` follows the same compare-exchange shape but seeds the
next physical value from `phys_now.max(cur_phys).max(remote.physical_ms)` and derives the next
logical counter from whichever of the three (local wall clock, local state, remote timestamp)
supplied the winning physical value — the standard HLC rule: the physical component never moves
backward, and the logical counter increments only when physical time does not advance the clock
on its own. `update` is called from `merge_sync_payload` (§3.3) for every timestamp decoded from
an incoming sync payload, so the local clock is always causally advanced past anything it has
merged in.

### 3.2 Per-type merge algorithms

```rust
// LwwRegister: strictly later HLC timestamp wins outright; ties keep the existing value.
pub fn merge(&mut self, other: &LwwRegister) -> bool {
    if other.timestamp > self.timestamp {
        self.value = other.value.clone();
        self.timestamp = other.timestamp;
        self.tombstone = other.tombstone;
        true
    } else { false }
}

// PnCounter: per-node component-wise maximum (each node's own running total only grows).
pub fn merge(&mut self, other: &PnCounter) {
    for (&node_id, &val) in &other.p { let e = self.p.entry(node_id).or_default(); *e = (*e).max(val); }
    for (&node_id, &val) in &other.n { let e = self.n.entry(node_id).or_default(); *e = (*e).max(val); }
}
pub fn value(&self) -> i64 { self.p.values().sum::<i64>() - self.n.values().sum::<i64>() }

// OrSet: union both tag sets and the tombstone set, then drop any tag now covered by a tombstone.
pub fn merge(&mut self, other: &OrSet) {
    for ts in &other.tombstones { self.tombstones.insert(*ts); }
    for (elem, other_tags) in &other.elements {
        let my_tags = self.elements.entry(elem.clone()).or_default();
        for tag in other_tags { my_tags.insert(*tag); }
    }
    self.elements.retain(|_, tags| { tags.retain(|tag| !self.tombstones.contains(tag)); !tags.is_empty() });
}
```

`OrSet::remove(element)` moves every add-tag currently associated with `element` into
`tombstones` and drops the element's entry from `elements`; it does not (and cannot) tombstone
an add-tag it has not yet observed. `OrSet::read()`/`contains()` treat an element as present iff
at least one of its recorded add-tags is absent from `tombstones` — this is the mechanism behind
add-wins semantics (design doc §2.3): a concurrent add carrying a tag the remove never saw
survives merge because that tag was never placed in `tombstones`.

### 3.3 Manual sync format: `export_sync_payload`/`merge_sync_payload`

`CrdtStore::export_sync_payload(&self) -> Vec<u8>` serializes the **entire** local store —
every register, counter, and set currently held — into one flat buffer using a hand-rolled,
uncompressed binary format: a one-byte type tag (`1` = register, `2` = counter, `3` = set)
followed by little-endian length-prefixed fields for that item, repeated for every item, with no
framing beyond simple concatenation and no delta/incremental support. `CRDT.DUMP` re-serializes
the full store on every call.

`CrdtStore::merge_sync_payload(&mut self, data: &[u8]) -> Result<usize, String>` walks that same
format byte-by-byte, reconstructs each `LwwRegister`/`PnCounter`/`OrSet`, calls
`self.clock.update(&ts)` for every timestamp it decodes (§3.1), and merges the reconstructed
value into the matching local map via the real `merge()` methods from §3.2 — returning
`Err(format!("Unknown CRDT item type: {}", item_type))` for any tag other than `1`/`2`/`3`, and
the count of items successfully merged on success. This export → external transport → merge
cycle is the entirety of "multi-region sync" as implemented: nothing inside Rudis schedules,
transports, or discovers peers for it. A caller (operator script, external sidecar, or any other
process with network access to more than one Rudis instance) is responsible for running
`CRDT.DUMP` on a source node, moving the resulting bytes somewhere, and calling `CRDT.MERGE`
with them on a destination node — on whatever schedule that external process chooses.

### 3.4 Tombstone garbage collection: on-demand only

```rust
pub fn gc_tombstones(&mut self, ttl_ms: u64) -> (usize, usize) {
    let cutoff = now_ms.saturating_sub(ttl_ms);
    self.registers.retain(|_, reg| !reg.tombstone || reg.timestamp.physical_ms >= cutoff);
    // + OrSet::prune_tombstones(cutoff) per set, which retains only tombstones newer than cutoff
    (registers_pruned, set_tombstones_pruned)
}
```

Exposed as `CRDT.GC [ttl_ms]`. Prunes tombstoned `LwwRegister`s and `OrSet` tombstone entries
strictly by age (physical HLC component vs. a cutoff derived from the current wall clock); it
does not touch `PnCounter` state, which has no tombstones to prune. There is no background task
anywhere in `server.rs` that calls this automatically — a long-running instance that never issues
`CRDT.GC` accumulates tombstones (and therefore `export_sync_payload` output size) without bound.

---

## 4. Cross-Component Interactions

- **`src/shard.rs`**: one `CrdtStore` per `ShardDb`, forwarded to via the thin wrapper methods
  listed in §1.
- **`src/resp.rs`**: command parsing for the ten `CRDT.*` subcommands into `Command::Crdt*`.
- **`src/connection.rs`**: single-key variants (`CrdtSet`/`Get`/`Del`/`Incrby`/`Sadd`/
  `Smembers`/`Srem`) are routed to the shard owning the key via the same `target_shard`
  mechanism used for every other keyed command (Component 04), so a `CrdtStore` behaves as one
  logical per-node store addressed by normal key routing rather than requiring the client to
  know which shard holds a given CRDT key. The whole-store commands (`CrdtDump`/`CrdtMerge`/
  `CrdtGc`) execute locally and then call `execute_remote` against every other shard, aggregating
  the results (concatenated dump bytes, summed merged-item count, summed pruned-tombstone count
  respectively) so the client sees one logical whole-node result.
- **`src/aof.rs` / `src/replication.rs`: no propagation for single-key CRDT writes, verified.**
  Mutating single-key `Crdt*` commands do invoke the same `record_change!` macro every other
  mutating command uses, which increments the dirty-key counter and touches `WATCH`ed keys — but
  the macro's AOF-append/replication-propagate branch is gated on
  `crate::aof::command_to_resp(cmd)` returning `Some(bytes)`, and `command_to_resp`'s match
  statement (`src/aof.rs`) has **no arm for any `Command::Crdt*` variant**, falling through to
  its default `_ => None`. Concretely: CRDT writes are **not** appended to the AOF and **not**
  streamed to connected `PSYNC` replicas. A node's CRDT state today survives only in memory and
  through whatever external process runs `CRDT.DUMP`/`CRDT.MERGE`; a process restart or a
  primary/replica failover loses it.
- **`src/table.rs`**: no relationship — CRDT values are not `RudisValue` variants and live
  entirely in `CrdtStore`'s own maps, never in `RudisTable`.

---

## Contributor Gotchas & Debugging Guide

* **Gotcha 1**: `HlcTimestamp.node_id` is derived from the shard's listening port, not a
  cluster-wide node identity — multiple shards sharing one port (the normal `SO_REUSEPORT`
  deployment) generate HLC timestamps under the same `node_id`.
* **Gotcha 2**: CRDT writes update the dirty-key counter and `WATCH` machinery but are silently
  excluded from AOF persistence and replica streaming (`command_to_resp` has no `Crdt*` arm) —
  do not assume `CRDT.SET` survives a restart or a failover the way `SET` does.
* **Gotcha 3**: `CRDT.GC` is purely on-demand; nothing schedules it automatically, so tombstones
  (and `CRDT.DUMP` payload size) grow without bound on an instance that never calls it.
* **Gotcha 4**: `export_sync_payload`/`merge_sync_payload` always operate on the *entire* local
  store — there is no per-key or delta sync primitive.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib crdt -- --test-threads=1
```
