# Component 12: CRDT Data Types & Manual Multi-Region Sync (Design)

## Component 12: CRDT Data Types & Manual Multi-Region Sync

> **Source Files**: ``src/crdt.rs``


---

### 1. Architectural Purpose & Scope

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

---

### 2. Key Invariants & Concurrency Constraints

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

---

### 6. Performance Characteristics

- **Lock-free clock advancement**: `HybridLogicalClock::now`/`update` use CAS retry loops,
  not a mutex — cheap even under contention from multiple connections on the same shard.
- **Export is O(total CRDT state size) and single-threaded**: `export_sync_payload` builds
  one `Vec<u8>` for the *entire* store in one call; there's no incremental/delta export —
  every `CRDT.DUMP` re-serializes everything currently held.
- **No network cost inside Rudis**: since sync is manual (§1), there's no WAN traffic,
  retry logic, or delta-batching to account for here at all — that cost (if any) lives
  entirely in whatever external process actually transports the dump/merge payloads.

---
