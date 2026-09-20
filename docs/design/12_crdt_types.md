# Component 12: CRDT Data Types & Manual Multi-Region Sync — Design & Architecture

> **Subsystem Scope**: `src/crdt.rs`
> **Implementation Reference**: [`docs/internal/12_crdt_types.md`](../internal/12_crdt_types.md)
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Purpose

`src/crdt.rs` provides a small set of Conflict-Free Replicated Data Types (CRDTs) — a
Last-Write-Wins register, an Observed-Remove Set, and a Positive-Negative Counter — plus a
Hybrid Logical Clock for causal timestamping, exposed through `CRDT.*` commands
(`CRDT.SET`/`GET`/`DEL`, `CRDT.SADD`/`SMEMBERS`/`SREM`, `CRDT.INCRBY`, and the whole-store
`CRDT.DUMP`/`CRDT.MERGE`/`CRDT.GC`). These types are designed to converge to the same value on
every replica regardless of the order in which concurrent updates are applied, without
requiring the replicas to coordinate through consensus at write time.

**This is manual, not automatic, multi-region replication.** Rudis does not run a background
task that discovers peer regions and streams CRDT updates to them. A caller must explicitly
export a node's CRDT state (`CRDT.DUMP`), transport the resulting bytes to another Rudis
instance by some external mechanism, and apply it there (`CRDT.MERGE`). The data types'
merge functions are what make this safe to do at any time, in any order, from any number of
sources — but the transport and scheduling of that exchange is entirely outside Rudis today.

## 2. Design Rationale ("Why")

### 2.1 Why not just use primary/replica replication for multi-region?

Rudis already has classical primary/replica replication (`PSYNC`, Component 14) for
single-region high availability: a replica applies a strictly ordered stream of writes from one
primary, so there is never a conflict to resolve — the primary's order is authoritative by
construction. This model does not extend to **active-active multi-region** deployments, where
clients in more than one region need to write to their local region for latency reasons and
have those writes eventually reflected everywhere else:

- A single global primary means every write from a non-primary region pays a full cross-region
  round trip before it is acknowledged — often hundreds of milliseconds, unacceptable for
  latency-sensitive workloads.
- Electing a new primary per region and forwarding cross-region writes reintroduces a
  coordination protocol (who is authoritative for a given key, what happens during a network
  partition between regions) that primary/replica replication was never designed to solve.
- If two regions are instead allowed to accept writes to the *same* key independently — the
  actual goal of active-active — a plain last-writer-arbitrary-order merge can silently lose an
  update, and a naive automatic merge strategy (e.g., "the union of two hash tables") does not
  generalize correctly to sets or counters, where a naive union can resurrect a deleted element
  or double-count a decrement.

### 2.2 Why CRDTs specifically

CRDTs solve exactly this problem: a value type equipped with a merge function that is
commutative, associative, and idempotent will converge to the same result on every replica no
matter what order updates and merges arrive in, and no matter how many times a given update is
merged in. This lets each region accept writes locally, commit them immediately without waiting
on any other region, and defer reconciliation to whenever a sync happens to run — with a
mathematical guarantee (not an operational convention) that reconciliation produces the same
answer everywhere. The cost of this guarantee is weaker consistency: a CRDT key does not have a
single global "latest value" until all regions have merged all outstanding updates from each
other, and different CRDT types make different trade-offs about what "converges correctly"
means for their shape of data (§2.3).

### 2.3 Conflict resolution semantics, per type

- **`LwwRegister` (backs `CRDT.SET`/`GET`/`DEL`): last-write-wins by Hybrid Logical Clock.** Of
  two concurrent writes to the same key, the one with the later HLC timestamp wins outright and
  the other is discarded — simple and cheap, but it means a genuinely concurrent write can be
  silently lost from the perspective of whichever region did not "win." This is an accepted
  trade-off for simple key/value state where losing a concurrent write to a deterministic,
  causally-consistent winner is preferable to no convergence at all.
- **`OrSet` (backs `CRDT.SADD`/`SMEMBERS`/`SREM`): add-wins set semantics.** A concurrent add and
  remove of the same element resolve in favor of the add. This is deliberate: `remove` can only
  tombstone the specific timestamped "add" instances it has observed at the time it runs, so an
  add that a remove operation never saw survives the merge — matching the standard
  Observed-Remove Set design, and avoiding the more surprising "remove-wins" outcome where a
  concurrent re-add could be silently dropped.
- **`PnCounter` (backs `CRDT.INCRBY`): per-node monotonic accumulation, merged by component-wise
  maximum.** Each node tracks its own cumulative positive and negative contributions separately;
  merging two counter states takes the maximum of each node's contribution (which can only grow
  monotonically at its origin node), so merging never loses or double-counts an
  increment/decrement regardless of how many times or in what order two states are merged.

### 2.4 Why a Hybrid Logical Clock, not wall-clock timestamps or pure Lamport clocks

Pure wall-clock timestamps are not safe for deciding "last write wins" across independent nodes,
because clock skew between regions can make an objectively earlier write appear to have a later
timestamp. Pure Lamport logical clocks solve causal ordering but discard any relationship to
real time, making timestamps unhelpful for humans and for tombstone garbage collection ("prune
anything older than 24 hours" has no meaning without some notion of physical time). A Hybrid
Logical Clock combines both: it advances with the local wall clock under normal conditions, but
falls back to a logical counter to preserve causal ordering when timestamps would otherwise
collide or move backwards relative to a just-observed remote timestamp — giving `LwwRegister`
merges both throughput-friendly local timestamps and a correctness guarantee that a remote
update's causal history is always accounted for once observed.

## 3. Architecture Overview

```
Region A: CRDT.SET foo v1 (HLC_1)        Region B: CRDT.SET foo v2 (HLC_2), independently
                    │                                          │
                    │            CRDT.DUMP  ──►  (external transport)  ──►  CRDT.MERGE
                    └──────────────────────────┬───────────────────────────┘
                                                ▼
                               Deterministic LWW merge: HLC_2 > HLC_1 wins
                               (same outcome regardless of which side merges first)
```

Within a single Rudis node, `CrdtStore` is a store parallel to the main keyspace (`RudisTable`,
Component 05) — CRDT values are not `RudisValue` variants and do not share the main keyspace's
expiration or eviction machinery. Single-key `CRDT.*` commands are routed to the shard owning
the key through the same CRC16/key-routing mechanism as any other command (Component 04);
whole-store commands (`CRDT.DUMP`/`MERGE`/`GC`) fan out to every shard and aggregate the result,
so a `CrdtStore` reads as one logical per-node store rather than silently independent per-shard
state.

## 4. Key Invariants

1. **Every provided `merge` function is commutative, associative, and idempotent.** This is the
   mathematical property that makes "merge whenever, from whoever, as many times as needed"
   safe; it is exercised directly by the module's own unit tests
   (`test_lww_register_convergence`, `test_pn_counter_convergence`, `test_orset_add_wins`).
2. **The HLC only moves forward.** Both generating a new local timestamp and observing a remote
   one during a merge only ever advance the local clock's physical/logical state, never move it
   backward — the standard HLC correctness property.
3. **No consensus, because there is no automatic network layer to reach consensus over.** With
   sync entirely manual (§1), there is no leader election, no quorum, and no automatic conflict
   detection: a merge happens exactly when and with exactly the payload an operator or external
   process supplies via `CRDT.MERGE`.
4. **Tombstones are not free.** `OrSet` removals and `LwwRegister` deletions are represented as
   tombstones (retained state marking "this was removed," not simple deletion), because a CRDT
   must be able to tell a late-arriving stale add apart from a fresh one. Tombstones are
   reclaimed only on an explicit `CRDT.GC` call (§ internal doc); an instance that never runs it
   accumulates tombstones indefinitely.

## 5. Implementation Reference

For concrete struct/enum definitions, the exact merge algorithms, the manual export/merge wire
format, and cross-component wiring, see
[`docs/internal/12_crdt_types.md`](../internal/12_crdt_types.md).
