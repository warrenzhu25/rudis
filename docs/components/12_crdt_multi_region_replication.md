# Component 12: CRDT Data Types & Manual Multi-Region Sync (`src/crdt.rs`)

## 1. Architectural Purpose & Scope

`src/crdt.rs` (645 lines) implements a small, self-contained library of three
**Conflict-Free Replicated Data Types (CRDTs)** — a Last-Write-Wins Register, an
Observed-Remove Set, and a Positive-Negative Counter — each ordered by a **Hybrid Logical
Clock (HLC)**, plus a binary export/import format for merging one instance's CRDT state
into another's.

**What this is not**: there is no automatic cross-region network replication. There is no
peer/region configuration anywhere in `main.rs`, no background sync task, and no wiring
into `src/replication.rs` (which handles the unrelated primary/replica `PSYNC` stream).
`CRDT.MERGE` takes its payload as a plain command argument (`Command::CrdtMerge(Bytes)`),
which means getting CRDT state from one Rudis instance to another is entirely
operator/client-driven: read it out with `CRDT.DUMP`, transport those bytes yourself
(script, sidecar, whatever), and feed them into the target instance with `CRDT.MERGE
<payload>`. The "multi-region" framing in this file's doc comments describes the
data types' *convergence properties*, not a built network protocol.

**Update — the routing gap below is now fixed.** An earlier version of this document found
that every `Command::Crdt*` handler called `router.local_db.borrow_mut().crdt_*(...)`
directly, bypassing normal key-based routing entirely, so the same key name could hold
completely independent state on different shards. As of the current source, the single-key
CRDT commands (`CrdtSet`/`CrdtGet`/`CrdtDel`/`CrdtIncrby`/`CrdtSadd`/`CrdtSmembers`/
`CrdtSrem`) are now included in both `cmd_primary_key` and `target_shard_of_cmd`
(`connection.rs`) and dispatch through the same local-vs-`execute_remote` fork every other
keyed command uses — a `CRDT.SET foo bar` now always lands on the one shard `foo` actually
hashes to, regardless of which shard's connection issued it. Separately, `CRDT.DUMP`,
`CRDT.MERGE`, and `CRDT.GC` — which operate on an entire store, not one key — now
explicitly fan out to *every* shard (`for sid in 0..router.num_shards { ... }`, via
`router.execute_remote`) and aggregate the results: `CrdtDump` concatenates every shard's
exported payload into one response, `CrdtMerge` sums the per-shard merged-item counts, and
`CrdtGc` sums the per-shard tombstones-pruned counts. In effect, `CrdtStore` is still a
genuinely separate `CrdtStore` instance per shard (the underlying data structure hasn't
changed — see §3), but the command layer now presents it as one logical whole-node store:
single-key operations are correctly routed to the one shard that owns the key, and
whole-store operations correctly touch every shard rather than just the connection's local
one.

---

## 2. Key Invariants & Concurrency Constraints

1. **Deterministic Convergence (real, and tested)**: `LwwRegister::merge`, `OrSet::merge`,
   and `PnCounter::merge` are each commutative/idempotent by construction (see §4) — the
   file's own `#[cfg(test)]` module (`test_lww_register_convergence`,
   `test_pn_counter_convergence`, `test_orset_add_wins`) exercises exactly this.
2. **HLC via lock-free CAS, not a mutex**: `HybridLogicalClock` stores
   `latest_physical_ms: AtomicU64` / `latest_logical: AtomicU32` and advances them with a
   compare-exchange retry loop (§4.1) — real lock-free code, not a fabrication.
3. **Add-Wins semantics for `OrSet`**: a concurrent add and remove of the same element
   resolve in favor of the add, because `remove` only tombstones the specific add-tags
   (`HlcTimestamp`s) it has *observed so far* — a later add carries a fresh tag the remove
   never saw, so it survives merge. Verified by `test_orset_add_wins`.
4. **No consensus, because there's no network layer to reach consensus over**: with sync
   entirely manual (§1), there's no Paxos/Raft and also no automatic conflict detection —
   whoever runs `CRDT.MERGE` decides when and with what payload merging happens.

---

## 3. Component Architecture & Data Structures

```
   Client, any shard's connection             Client, any shard's connection
        CRDT.SET foo bar                            CRDT.SET foo baz
                  │                                            │
        target_shard_of_cmd("foo")                  target_shard_of_cmd("foo")
                  │                                            │
                  └──────────── both resolve to the SAME shard ────────────┘
                                (foo's owning shard, via CRC16 —
                                 local execute or router.execute_remote,
                                 exactly like any other keyed command)

   CRDT.DUMP / CRDT.MERGE <payload> / CRDT.GC  (whole-store commands)
                  │
                  ▼
   local shard's CrdtStore  +  execute_remote(sid, same cmd) for every OTHER shard
                  │
                  ▼
   aggregated result (concatenated dump / summed merge count / summed GC count)
```

### Real data structures (verbatim from `src/crdt.rs`)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HlcTimestamp {
    pub physical_ms: u64,
    pub logical: u32,
    pub node_id: u16,
}
// Ord: physical_ms, then logical, then node_id (lexicographic tuple compare)

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
    pub elements: HashMap<Bytes, HashSet<HlcTimestamp>>,
    pub tombstones: HashSet<HlcTimestamp>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PnCounter {
    pub p: HashMap<u16, i64>,  // per-node positive contributions
    pub n: HashMap<u16, i64>,  // per-node negative contributions
}

pub struct CrdtStore {
    pub clock: HybridLogicalClock,
    pub registers: HashMap<Bytes, LwwRegister>,
    pub sets: HashMap<Bytes, OrSet>,
    pub counters: HashMap<Bytes, PnCounter>,
}
```

Note the real shapes differ from what an earlier, unverified draft of this document
claimed: `LwwRegister`/`OrSet` are concretely `Bytes`-keyed (not generic `<T>`), `OrSet`
tracks per-element `HashSet<HlcTimestamp>` tags directly (no separate UUID type), and
`PnCounter`'s fields are named `p`/`n` (not `increments`/`decrements`) and store signed
`i64` per-node deltas rather than only-positive `u64` add/remove counts.

---

## 4. Execution Algorithms & Code Logic

### 4.1 HLC generation and remote-update (real CAS loops)

```rust
pub fn now(&self) -> HlcTimestamp {
    let phys_now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
    loop {
        let cur_phys = self.latest_physical_ms.load(AtomicOrdering::Acquire);
        let cur_log = self.latest_logical.load(AtomicOrdering::Acquire);
        let (next_phys, next_log) = if phys_now > cur_phys { (phys_now, 0) } else { (cur_phys, cur_log + 1) };
        if self.latest_physical_ms.compare_exchange(cur_phys, next_phys, AtomicOrdering::Release, AtomicOrdering::Relaxed).is_ok() {
            self.latest_logical.store(next_log, AtomicOrdering::Release);
            return HlcTimestamp::new(next_phys, next_log, self.node_id);
        }
    }
}
```

`update(&self, remote: &HlcTimestamp)` is the same CAS-retry shape, but seeds `max_phys`
from `phys_now.max(cur_phys).max(remote.physical_ms)` — this is the standard HLC rule
(physical clock never goes backward; logical counter only increments when physical time
doesn't advance) and is called from `merge_sync_payload` (§4.3) every time a remote
timestamp is observed, so the local clock is always causally ahead of anything it has
merged in.

### 4.2 Merge logic for each type (all real, all as originally documented)

```rust
// LwwRegister: later HLC timestamp wins outright
pub fn merge(&mut self, other: &LwwRegister) -> bool {
    if other.timestamp > self.timestamp {
        self.value = other.value.clone();
        self.timestamp = other.timestamp;
        self.tombstone = other.tombstone;
        true
    } else { false }
}

// PnCounter: per-node component-wise max (each node's own counter only grows)
pub fn merge(&mut self, other: &PnCounter) {
    for (&node_id, &val) in &other.p { let e = self.p.entry(node_id).or_default(); *e = (*e).max(val); }
    for (&node_id, &val) in &other.n { let e = self.n.entry(node_id).or_default(); *e = (*e).max(val); }
}
pub fn value(&self) -> i64 { self.p.values().sum::<i64>() - self.n.values().sum::<i64>() }

// OrSet: union tags, union tombstones, then drop any tag that's now tombstoned
pub fn merge(&mut self, other: &OrSet) {
    for ts in &other.tombstones { self.tombstones.insert(*ts); }
    for (elem, other_tags) in &other.elements {
        let my_tags = self.elements.entry(elem.clone()).or_default();
        for tag in other_tags { my_tags.insert(*tag); }
    }
    self.elements.retain(|_, tags| { tags.retain(|tag| !self.tombstones.contains(tag)); !tags.is_empty() });
}
```

### 4.3 The manual sync format: `export_sync_payload` / `merge_sync_payload`

`CrdtStore::export_sync_payload` serializes the *entire* local store into one flat
`Vec<u8>` using a hand-rolled binary format (one byte tag per item — `1`=register,
`2`=counter, `3`=set — followed by little-endian length-prefixed fields, no compression,
no framing beyond simple concatenation):

```rust
pub fn export_sync_payload(&self) -> Vec<u8> {
    let mut buf = Vec::new();
    for (k, r) in &self.registers {
        buf.push(1u8);
        buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
        buf.extend_from_slice(k);
        buf.extend_from_slice(&(r.value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&r.value);
        buf.extend_from_slice(&r.timestamp.physical_ms.to_le_bytes());
        buf.extend_from_slice(&r.timestamp.logical.to_le_bytes());
        buf.extend_from_slice(&r.timestamp.node_id.to_le_bytes());
        buf.push(if r.tombstone { 1 } else { 0 });
    }
    // ...counters (type 2), then sets (type 3), same length-prefixed shape
    buf
}
```

`merge_sync_payload(&mut self, data: &[u8])` walks that same format byte-by-byte,
reconstructs each `LwwRegister`/`PnCounter`/`OrSet`, calls `self.clock.update(&ts)` for
every timestamp it decodes (so the local clock catches up to whatever it just merged),
and merges each reconstructed value into the matching local map via the real `merge()`
methods from §4.2 — falling through to `Err(format!("Unknown CRDT item type: {}",
item_type))` for anything but `1`/`2`/`3`. This whole export→transport→merge cycle is
what a caller (script, sidecar, whatever "region sync" job exists outside this repo)
would run periodically; nothing inside Rudis itself schedules or triggers it.

### 4.4 Tombstone GC

```rust
pub fn gc_tombstones(&mut self, ttl_ms: u64) -> (usize, usize) {
    let cutoff = now_ms.saturating_sub(ttl_ms);
    self.registers.retain(|_, reg| !reg.tombstone || reg.timestamp.physical_ms >= cutoff);
    // + OrSet::prune_tombstones(cutoff) per set
}
```
Exposed as `CRDT.GC [ttl_ms]` (default handled at the command layer, not shown here).
Only prunes registers/set-tombstones by age; there's no automatic scheduled GC task
anywhere in `server.rs` — it's an on-demand command.

---

## 5. Cross-Component Interactions

- **`src/shard.rs`**: `ShardDb.crdt_store: CrdtStore` plus thin `#[inline]` wrappers
  (`crdt_set`, `crdt_get`, `crdt_del`, `crdt_incrby`, `crdt_sadd`, `crdt_smembers`,
  `crdt_srem`, `crdt_dump`, `crdt_merge`, `crdt_gc`) that just forward to the store.
- **`src/resp.rs`**: parses `CRDT.SET|GET|DEL|INCRBY|SADD|SMEMBERS|SREM|DUMP|MERGE|GC`
  into the matching `Command::Crdt*` variants (`CrdtMerge(Bytes)` carries the raw sync
  payload as a normal bulk-string argument).
- **`src/connection.rs`**: single-key `Command::Crdt*` arms now route via
  `target_shard_of_cmd`/`execute_remote` like any other keyed command (see §1's update);
  the whole-store commands (`CrdtDump`/`CrdtMerge`/`CrdtGc`) fan out to every shard and
  aggregate. `CrdtSet`/`CrdtDel`/`CrdtIncrby`/`CrdtSadd`/`CrdtSrem` also call
  `notify_key_invalidation` (RESP3 client-side-caching) the same way ordinary mutating
  commands do.
- **`src/replication.rs`**: no interaction. Primary/replica `PSYNC` streaming is a
  separate mechanism and does not carry CRDT state.
- **`src/table.rs`**: no interaction. CRDT values are **not** stored as `RudisValue`
  variants — they live entirely in `CrdtStore`'s own maps, a parallel store next to
  `RudisTable`, not inside it.

---

## 6. Performance Characteristics

- **Lock-free clock advancement**: `HybridLogicalClock::now`/`update` use CAS retry loops,
  not a mutex — cheap even under contention from multiple connections on the same shard.
- **Export is O(total CRDT state size) and single-threaded**: `export_sync_payload` builds
  one `Vec<u8>` for the *entire* store in one call; there's no incremental/delta export —
  every `CRDT.DUMP` re-serializes everything currently held.
- **No network cost inside Rudis**: since sync is manual (§1), there's no WAN traffic,
  retry logic, or delta-batching to account for here at all — that cost (if any) lives
  entirely in whatever external process actually transports the dump/merge payloads.

---

## 7. Future Improvements

- **RESOLVED — route CRDT commands through the normal key-slot mechanism (§1's update).** Fixed: single-key `Command::Crdt*` variants now go through `target_shard_of_cmd`/`execute_remote`, and `CrdtDump`/`CrdtMerge`/`CrdtGc` now fan out to every shard and aggregate, so `CrdtStore` is presented as one logical whole-node store instead of silently-independent per-shard state.
- **High — build real automatic multi-region *network* sync.** Still open: "multi-region CRDT" is now a correct whole-node toolkit (per the fix above), but sync between separate Rudis *instances* is still entirely manual (`CRDT.DUMP` → transport the bytes yourself → `CRDT.MERGE`). The data types genuinely support real automatic sync (their merge functions are commutative/idempotent, exactly what's needed); a minimal real version would be a background task that periodically pushes each node's `export_sync_payload()`-equivalent (now whole-node, thanks to the fan-out fix) to a configured list of peer *nodes'* addresses and merges whatever it receives back — turning this from a manual toolkit into an actual active-active feature matching its name.
- **Medium — schedule `CRDT.GC` automatically (§4.4)** rather than leaving tombstone cleanup entirely on-demand — a long-running instance with many deletes/removes will accumulate tombstones indefinitely otherwise, growing `export_sync_payload`'s output and memory footprint for no ongoing benefit once tombstones are older than any plausible in-flight merge.
- **Low — add incremental/delta export** so `CRDT.DUMP` doesn't have to re-serialize the entire store on every call (§6) — matters once the store holds enough registers/sets/counters that a full dump becomes a non-trivial cost per sync cycle.
