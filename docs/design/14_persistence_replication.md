# Component 14: Persistence & Replication Engines (Design)

> **Source Files**: `src/replication.rs`, `src/aof.rs`


---

### 1. Architectural Purpose & Scope

This subsystem covers two related but independent durability mechanisms:

1. **Append-Only File (AOF) Engine (`src/aof.rs`)**: converts mutating `Command`s back into
   RESP bytes (`command_to_resp`) and appends them to a per-shard file via a buffered,
   periodically-flushed `AofWriter`. On restart, `replay_aof` re-parses the file and replays
   every command through the normal command-execution path.
2. **Replication Hub (`src/replication.rs`)**: a per-port, process-wide `ReplicationHub`
   (master or slave role) that fans out every mutating command to connected replicas over
   plain `flume` channels. The master side now supports **real partial resync** (`+CONTINUE`
   from the backlog) when a reconnecting client presents a valid replid+offset, falling back
   to a full RDB snapshot otherwise — see §4.3 for the update to this (this doc previously,
   correctly, documented this as entirely unimplemented; it has since been built).

**Update**: AOF rewrite/compaction via `BGREWRITEAOF` is fully implemented across shards
(§4.1), snapshotting non-expired table entries, JSON documents, sets, lists, hashes, streams,
and preserving TTLs, with atomic temp-file rename and live reopen on active writers. Partial
resynchronization is real on both the **master** and **replica** sides
(§4.3) — `run_replica_worker` tracks its `master_replid` and `master_repl_offset`,
reconnects automatically with `PSYNC <replid> <offset>`, and applies `+CONTINUE` diffs
without full RDB snapshots.

---

### 2. Key Invariants & Concurrency Constraints

1. **AOF compaction via `BGREWRITEAOF` is supported.** `AofWriter::append` grows `self.buffer`
   (later flushed to disk via `write_all_at` at the current end-of-file `offset`). Periodic
   or explicit `BGREWRITEAOF` snapshots memory state to a temporary file, syncs it, atomically
   renames it to replace the AOF file, and reopens `AofWriter` on the new file at the new offset.
2. **Partial resync is supported on both master and replica sides.**
   `run_master_replica_stream` (`src/connection.rs`) inspects the `PSYNC` command's
   replid/offset arguments via `ReplicationHub::try_partial_resync`, and replies `+CONTINUE <replid>\r\n<backlog-diff-bytes>`
   when the requested offset falls inside the retained backlog window for a matching
   replid (or `replid2`), falling back to `+FULLRESYNC <replid> <offset>\r\n$<len>\r\n<rdb-bytes>` otherwise
   (§4.3). `run_replica_worker` tracks its `master_repl_offset` and `master_replid`, sending
   `PSYNC <cached_replid> <cached_offset>` on reconnects, receiving `+CONTINUE`, and executing
   the backlog diff commands without requesting a full RDB snapshot.
3. **The replication backlog is maintained, and now genuinely read back — by the master
   serving a partial resync.** `ReplicationHub::propagate` still appends every propagated
   command to `self.backlog` (a `ReplicationBacklog`), and that data is no longer write-only:
   `try_partial_resync`/`ReplicationBacklog::get_diff` (§4.3) slice it to answer a
   `+CONTINUE` request. It's also still consulted for `INFO replication`'s `repl_backlog_*`
   fields, as before.
4. **Replication uses shared, cross-thread state (`Arc`/`RwLock`), unlike the request path.**
   `ReplicationHub` is looked up via `get_replication_hub(port)` from a process-wide
   `LazyLock<RwLock<HashMap<u16, Arc<ReplicationHub>>>>` — every shard on a given port shares
   the *same* hub instance, guarded by `RwLock`s and atomics throughout. This is a second,
   independent exception to the "zero locks" architecture (the first being the blocking-op
   `BlockHub` documented in Component 06), needed because replication fan-out is inherently
   cross-shard: any shard's write must reach every connected replica, not just the shard that
   handled it.
5. **AOF writing and replication propagation are decoupled but both driven from the same
   call site.** `record_mutation` (in `replication.rs`) is the single function that both
   appends to a shard's local `AofWriter` (if present) and calls `propagate_bytes` — but nothing
   requires both to be enabled together; a node can run with AOF on and no replicas, or
   replication with AOF disabled, independently.

---

### 3. Performance Characteristics

- **AOF write cost is O(1) amortized per command**, bounded by the 50ms/~1s flush-fsync
  cadence — but **AOF file size and restart replay time are both unbounded** relative to
  total lifetime write volume, since nothing ever compacts the file (§4.1). A long-running,
  write-heavy node with AOF enabled will have an ever-growing file and an ever-growing
  startup replay cost.
- **Full resync still re-transfers the entire dataset** via `generate_full_rdb` (fans out to
  every shard, waits for all chunks) — but this is no longer the *only* path (§4.3): a
  client presenting a still-in-backlog offset gets a `+CONTINUE` and just the missing bytes
  instead. The catch, per §4.3, is that this codebase's own replica never actually asks for
  the cheap path — a rudis-to-rudis pair still pays full-dataset-transfer cost on every
  reconnect today, even though the master is capable of doing better.
- **Replication fan-out itself is cheap per write**: `propagate` is an `O(num_replicas)`
  loop of non-blocking `flume` sends per mutating command, independent of dataset size.

---
