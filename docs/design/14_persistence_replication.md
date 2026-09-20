# Component 14: Persistence & Replication Engines (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/replication.rs`, `src/aof.rs`
> **Implementation Reference**: [`docs/internal/14_persistence_replication.md`](../internal/14_persistence_replication.md)
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem

A datastore needs two largely independent guarantees: durability across a process restart (persistence) and availability of a second, warm copy of the data on another node (replication). Naive designs entangle the two — for example, driving replication purely off periodic full snapshots — which makes replication slow to catch up and persistence unable to protect against anything shorter than the snapshot interval.

### 1.2 The Rudis Solution

Rudis keeps the two mechanisms decoupled but built on the same underlying primitive: every mutating command is serialized to canonical RESP once (`crate::aof::command_to_resp`) and that single byte buffer is handed, independently, to the local Append-Only File writer (if AOF is enabled) and to the replication fan-out path (if replicas are connected or a backlog is being retained). A node can run with AOF on and no replicas, with replication and AOF both off, or with both on, without either mechanism depending on the other's state. Full-dataset transfer (for both AOF-less crash recovery via RDB, and initial replica sync) is served without `fork()` — the process is single-binary Rust with no child-process snapshotting mechanism at all — by streaming each shard's serialized key space to the destination one shard at a time (see `docs/rdbsave.md` for the RDB mechanism in detail).

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model

`record_mutation` is the single call site, invoked after a write command has already been applied to the local `ShardDb`, that fans a command out to whichever of "append to this shard's AOF" and "propagate to this port's connected replicas" are currently active. Replication itself supports two wire-compatible entry points on the master side: the standard Redis-family `PSYNC` handshake (full resync via an RDB-shaped blob, or partial resync via a retained replication backlog), and a Dragonfly-compatible `DFLY FLOW` per-shard streaming handshake. Rudis's own replica implementation (`REPLICAOF`/`SLAVEOF`, `run_replica_worker`) only ever speaks the `PSYNC` half of that pair; `DFLY FLOW` is a master-side capability reachable by any client that performs the Dragonfly handshake (`REPLCONF capa dragonfly`) itself — a real Dragonfly replica, a compatible test client, or a future rudis-to-rudis mode that does not exist yet.

### 2.2 Design Rationale (The "Why")

Classic Redis's `fork()`-based `BGSAVE`/full-resync path buys a consistent point-in-time view "for free" via the kernel's copy-on-write page tables, at the cost of memory duplication under write pressure and a fork-time pause proportional to process size. Rudis has no forking mechanism to lean on (nor, by virtue of being thread-per-core with independent shards rather than one large heap, the same CoW-doubling failure mode a single-process fork would have) and instead achieves bounded memory use during a full snapshot or resync by processing one shard's chunk at a time: only one shard's serialized bytes are ever held in memory during a save or a full-resync transfer, not the whole dataset. Redis-family single-socket replication funnels every core's mutations through one TCP connection, which becomes a serialization point on a multi-core master; `DFLY FLOW` avoids that by giving each shard its own dedicated stream to a matching shard on the replica, at the cost of only working with a client that speaks that protocol.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)

1. **AOF compaction via `BGREWRITEAOF` is real and synchronous-per-shard.** `rewrite_shard_aof` walks the shard's live keyspace and JSON store and re-emits each key as one or more canonical RESP write commands (`SET`/`HSET`/`RPUSH`/`SADD`/`ZADD`/`XADD`/`JSON.SET`/...) into a temporary file via a 64KB `std::io::BufWriter`, `fsync`s it, and atomically `rename`s it over the live AOF file, then also `fsync`s the containing directory (`sync_parent_dir`) so the rename itself survives a crash. `AofWriter::reopen_after_rewrite` then re-opens the new file and resumes appending at its new length, without dropping any write that arrived during the rewrite.
2. **Partial resync is real on both the master and the replica side.** The master (`run_master_replica_stream`, `src/connection.rs`) inspects the `PSYNC` request's replid/offset via `ReplicationHub::try_partial_resync` and replies `+CONTINUE <replid>\r\n<backlog-diff-bytes>` when the offset is still inside the retained backlog, falling back to `+FULLRESYNC <replid> <offset>\r\n$<len>\r\n<rdb-bytes>` otherwise. `run_replica_worker` caches the last known `master_replid`/`master_repl_offset` across reconnects and sends `PSYNC <cached_replid> <cached_offset>` (not an unconditional `PSYNC ? -1`) whenever it has previously completed a sync, so a rudis-to-rudis reconnect after a brief network blip can genuinely take the cheap `+CONTINUE` path instead of re-transferring the whole dataset.
3. **The replication backlog is a genuine fixed-capacity ring buffer**, not a `Vec` that grows and periodically drains. `ReplicationBacklog` preallocates `max_size` bytes once and tracks a write cursor plus a logical length, wrapping writes and reads with modular arithmetic (§4.3). It is retained independently of whether any replica is currently connected (`backlog_active`, distinct from `has_replicas`) precisely so that a replica which fully disconnects and reconnects later still has history to diff against instead of being forced into a full resync every time.
4. **Replication uses shared, cross-thread state (`Arc`/`RwLock`/atomics), unlike the request path.** `ReplicationHub` is looked up via `get_replication_hub(port)` from a process-wide map; every shard on a given port shares the same hub instance. This is a deliberate, narrow exception to the thread-per-core "zero locks" architecture (see Component 01 §2.3.4), required because a write executed on any one shard must become visible to every connected replica, not just the shard that executed it.
5. **AOF writing and replication propagation are decoupled but driven from one call site.** `record_mutation` both appends to a shard's local `AofWriter` (if present) and calls `propagate_bytes`/`propagate_shard_bytes` — but neither requires the other; a node can run with AOF on and no replicas, or replication with AOF disabled, independently. Only commands that have an explicit encoding in `command_to_resp` are persisted or replicated at all (§4.1 in the internal doc) — anything without an arm there is silently invisible to both mechanisms.
6. **`DFLY FLOW` and `PSYNC` are two independent master-side code paths sharing the same `ReplicationHub`.** A `DFLY FLOW` session registers a `ShardReplicaFlow` (per shard, per client) instead of a `ConnectedReplica`, gets its own RDB chunk for just that shard, and receives live mutations tagged with a per-flow, per-shard LSN. `PSYNC` sessions register a `ConnectedReplica` and receive the whole dataset (all shards' chunks concatenated) plus the full, un-partitioned mutation stream. `record_mutation`'s generic `propagate` call reaches both kinds of session; shard-scoped emission additionally has a `propagate_shard` path for code that already knows which shard produced a mutation.

---

## 3. High-Level Architecture & Workflow Diagram

```
       PSYNC replica (rudis, or any Redis/Valkey-protocol client)
                              │  single TCP connection
                              ▼
       Master: run_master_replica_stream ── ReplicationHub (per port)
                              │
              ┌───────────────┴────────────────┐
     +FULLRESYNC + full RDB              +CONTINUE + backlog diff
     (generate_full_rdb: every           (ReplicationBacklog::get_diff,
      shard's chunk, sequentially)        no RDB re-transfer)
                              │
                    live RESP mutation stream (ReplicationHub::propagate)


       DFLY FLOW client (Dragonfly-protocol-compatible; NOT rudis's own replica)
                              │  one TCP connection PER SHARD
                              ▼
       Master: run_shard_replication_flow (one instance per flow)
                              │
              this shard's RDB chunk, then this shard's live mutation
              stream (ReplicationHub::propagate / propagate_shard, LSN-tagged)
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **AOF write cost is O(1) amortized per command**, bounded by the 5ms flush / ~1s fsync cadence (Component 01 §3.1) — but AOF file size and restart replay time both grow with total lifetime write volume until a `BGREWRITEAOF` compacts the file; nothing compacts it automatically.
- **`BGREWRITEAOF` holds at most one shard's rewritten output in memory at a time** on the coordinating shard (a 64KB buffered writer streaming straight to a temp file), and rewrites remote shards sequentially, one response at a time, rather than fanning out and collecting all shards' output concurrently — bounding peak memory during compaction, at the cost of compaction wall-clock time scaling with shard count.
- **Full resync (`generate_full_rdb`) re-transfers the entire dataset**, one shard's chunk at a time so only one chunk is ever held in memory — but it is no longer the only path: a client presenting a still-in-backlog replid+offset gets `+CONTINUE` and just the missing bytes. The one caveat: rudis's own replica now genuinely uses this path on reconnect (§2.3.2), so this is real, exercised behavior for rudis-to-rudis pairs, not only a capability that requires an external client.
- **Replication fan-out is O(num_replicas) per write**, a loop of non-blocking `flume` sends independent of dataset size; the same is true per-shard for `DFLY FLOW` sessions.
- **The 1MB replication backlog and the AOF flush/fsync cadence are compile-time constants, not runtime-configurable.** A workload with a write rate high enough to exceed 1MB of RESP-encoded mutations within a typical reconnect window will exhaust the backlog and fall back to a full resync regardless of how briefly a replica was disconnected.

No throughput/latency numbers are asserted here; none have been independently measured for this document.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/14_persistence_replication.md`**](../internal/14_persistence_replication.md): Low-level implementation and code reference.
* [**`docs/replication.md`**](../replication.md): Operator-facing explanation of the replication protocols and their observable behavior.
* [**`docs/rdbsave.md`**](../rdbsave.md): Operator-facing explanation of the RDB snapshot mechanism.
* **Source Files**: `src/replication.rs`, `src/aof.rs`
