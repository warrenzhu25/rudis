# Component 14: Persistence & Replication Engines (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/replication.rs, src/aof.rs`  
> **Implementation Reference**: [`docs/internal/14_persistence_replication.md`](../internal/14_persistence_replication.md)  
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
Traditional in-memory datastores encounter severe scalability barriers on modern multi-core, high-throughput cloud hardware. Single-threaded architectures (such as Redis) saturate a single CPU core while leaving the remaining 95%+ of server cores idle. Multi-threaded mutex architectures (such as Memcached) suffer from heavy spinlock contention, CPU cache line bouncing, and global memory allocator lock bottlenecks.

### 1.2 The Rudis Solution
Rudis implements the **Thread-Per-Core (Shared-Nothing)** architectural paradigm natively on Linux `io_uring` via Monoio. Each physical CPU core owns its own isolated event loop, its own thread-local memory database, and its own kernel `SO_REUSEPORT` listener. Operations on local keys execute in nanoseconds with zero locks, zero atomic operations, and zero cross-core cache invalidations.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model
Fork-less streaming snapshots and parallel multi-flow TCP replication. Replicas open N parallel TCP connections (DFLY FLOW), streaming mutations directly from worker cores with zero locks.

### 2.2 Design Rationale (The "Why")
Redis fork() triggers catastrophic copy-on-write memory doubling and main thread stalls. Funneling replication through a single TCP socket bottlenecks multi-core servers. Rudis streams snapshots sequentially and replicates in parallel.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
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

## 3. High-Level Architecture & Workflow Diagram

```
Master Worker Core 0 ──(DFLY FLOW 0)──► Replica Worker Core 0
       Master Worker Core 1 ──(DFLY FLOW 1)──► Replica Worker Core 1
       (Parallel zero-lock streaming direct from worker cores)
```

---

## 4. Performance Guarantees & Theoretical Complexity

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

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/14_persistence_replication.md`**](../internal/14_persistence_replication.md): Low-level implementation and code reference.
* **Source Files**: `src/replication.rs, src/aof.rs`
