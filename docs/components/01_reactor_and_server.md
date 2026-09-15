# Component 01: Reactor Runtime & Server Lifecycle (`src/main.rs`, `src/server.rs`)

## 1. Architectural Purpose & Scope

The **Reactor Runtime & Server Lifecycle** subsystem is responsible for bootstrapping the Rudis server process, pinning worker threads to physical CPU cores, setting up Linux `io_uring` instances via the `monoio` asynchronous runtime, and running each shard's event loop for its entire lifetime.

Unlike Redis (single-threaded event loop) or lock-based multi-threaded servers, Rudis uses a **Shared-Nothing Multi-Reactor** pattern: every worker core runs its own independent `monoio` runtime driving an isolated Linux `io_uring` ring, its own `SO_REUSEPORT` listener, and its own thread-local database. `src/main.rs` parses CLI arguments and spawns one OS thread per shard; `src/server.rs::run_shard_worker` is the entire body of that thread — it never returns.

---

## 2. Key Invariants & Concurrency Constraints

1. **Thread-per-Core Pinning**: Every worker thread is pinned to an exclusive CPU core using `core_affinity::set_for_current`, unless `--no-pin` is passed. No worker thread is migrated by the OS scheduler once pinned.
2. **`SO_REUSEPORT` Ingress Balancing**: Every worker thread opens its own listening socket bound to the same port (`socket.set_reuse_port(true)`). The kernel distributes new incoming connections across all bound sockets by a 4-tuple hash, with zero userspace dispatch.
3. **Thread-local database, no locks in the data path**: `ShardDb` lives in a plain `Rc<RefCell<ShardDb>>` — not `Arc<Mutex<_>>`. Because `Rc`/`RefCell` aren't `Send`, the compiler itself refuses to let a `ShardDb` handle cross a thread boundary.
4. **One real exception to "zero locks": the blocking-command hub.** `crate::block::get_block_hub_for_port(port)` returns an `Arc<Mutex<BlockHub>>` from a process-wide `static` map keyed by port (`PORT_BLOCK_HUBS`), so it *is* shared and mutex-guarded across every shard thread serving that port. This is a deliberate, narrow exception: blocking commands (`BLPOP`, `BZPOPMIN`, ...) need cross-shard wakeups, which the shared-nothing model can't give them for free, so a small locked structure was introduced specifically for that coordination rather than for the data path itself.
5. **No graceful shutdown.** There is no signal handler anywhere in `src/server.rs` or `src/main.rs` — no `SIGINT`/`SIGTERM` trap, no drain, no flush-on-exit logic. The process only stops if every thread's infinite loop is killed externally (the accept loop and the cross-shard receiver loop both run forever).

---

## 3. Component Architecture & Data Structures

```
                             Process Startup (main.rs)
                                        │
                          Args::parse() (clap) + tiering config
                                        │
                     Detect Available Hardware Cores (N)
                                        │
             Build N flume::unbounded channels (cross-shard mesh)
                                        │
             ┌──────────────────────────┴──────────────────────────┐
             ▼                                                     ▼
    thread::Builder "rudis-shard-0"                    thread::Builder "rudis-shard-i"
             │                                                     │
    run_shard_worker(0, N, port, senders,               run_shard_worker(i, N, port, senders,
        rx_0, core_id_0, aof_config)                        rx_i, core_id_i, aof_config)
```

### What `main.rs` actually builds before spawning threads

```rust
struct Args {
    port: u16,
    threads: Option<usize>,
    aof: bool,
    aof_dir: std::path::PathBuf,
    maxmemory: Option<String>,
    tiered_offload_threshold: u64,
    tiered_upload_threshold: u64,
    no_pin: bool,
}
```

`num_shards` defaults to `num_cores.min(8)` (capped, not "one shard per core", so a large
shared host doesn't silently spawn one `io_uring` ring and listener per core with no flags
given). `main.rs` also applies the tiering config globally via `rudis::tiering::set_max_memory`
/ `set_offload_threshold_pct` / `set_upload_threshold_pct` (keyed by port) before any thread
starts, and builds one `AofConfig { enabled, dir, fsync_every_sec: true }` shared by every
shard.

### `run_shard_worker`'s signature (what each thread actually receives)

```rust
pub fn run_shard_worker(
    shard_id: usize,
    num_shards: usize,
    port: u16,
    senders: Vec<flume::Sender<ShardMessage>>,
    rx: flume::Receiver<ShardMessage>,
    core_id: Option<core_affinity::CoreId>,
    aof_config: crate::aof::AofConfig,
)
```

Every shard gets a clone of the full `senders` vector (so it can reach any other shard) but
only its own `rx`.

---

## 4. Execution Algorithms & Code Logic

### 4.1 Startup sequence inside `run_shard_worker`

In order, each shard thread:

1. Pins to its core (if assigned), then builds a `monoio::RuntimeBuilder<IoUringDriver>` with `enable_timer()`.
2. Inside `rt.block_on(async move { ... })`, creates its `SO_REUSEPORT`/`SO_REUSEADDR` socket, sets 512KB send/recv buffers, binds, and listens (backlog 4096).
3. Creates `let local_db = Rc::new(RefCell::new(ShardDb::new(port)))`.
4. **RDB restore**: if AOF is *not* enabled and `<aof_dir>/dump.rdb` exists, calls `crate::table::load_rdb(&rdb_path, &mut local_db.borrow_mut(), shard_id, num_shards)`.
5. **AOF replay + writer**: if AOF is enabled, replays `<aof_dir>/appendonly-<shard_id>.aof` via `crate::aof::replay_aof`, then opens an `AofWriter`. If opened, spawns a dedicated `monoio::spawn` task that wakes every 50ms, flushes any pending write chunk (`take_flush_chunk`), and `fsync`s roughly every 20 ticks (~1s) when `fsync_every_sec` is set.
6. **Cluster bus**: only `shard_id == 0` calls `crate::cluster::start_cluster_bus(port)` — the gossip/cluster-bus listener is a single, once-per-process background task, not one per shard.
7. **Tiered storage**: opens a `crate::tiering::ShardTierManager` for this shard/port and, if successful, attaches it as `local_db.borrow_mut().tier_manager`.
8. Builds `client_registry` (`Rc<RefCell<HashMap<u64, ClientInfo>>>`), a `pubsub` hub (`Rc<RefCell<PubSubHub>>`), and the shard's `Router` (which itself owns `slot_states`, `slot_owners`, the AOF writer handle, the pubsub handle, and a small transaction lock/waiter queue — see Component 04).
9. Spawns three independent periodic `monoio::spawn` tasks sharing the same `Rc<RefCell<ShardDb>>`/`Router` with no synchronization:
   - every 100ms: `active_db.borrow_mut().active_expire_cycle()`
   - every 20ms: `offload_router.check_auto_tier().await` (auto-tiering pressure check)
   - every 2s: `gc_router.gc_local()` (tiered-storage GC / hole punching)
10. Spawns the **cross-shard receiver task** (§4.2) — the server side of the `Router`/`ShardMessage` mesh.
11. Prints the shard's startup banner, then enters the **accept loop** (§4.3) forever.

### 4.2 The cross-shard receiver loop: one `match` arm per `ShardMessage` variant

```rust
monoio::spawn(async move {
    while let Ok(msg) = rx.recv_async().await {
        match msg {
            ShardMessage::Get { key, responder } => { /* ... */ }
            ShardMessage::Set { key, value, expire_in, responder } => { /* ... */ }
            ShardMessage::Batch { items, responder, is_resp3 } => { /* ... */ }
            // ~35 variants total: Del, Exists, IncrBy, Expire, Persist, Ttl,
            // CountKeysInSlot, GetKeysInSlot, ClientList, SetSlotState, SetSlotOwner,
            // DumpKey, SyncAof, SaveRdbChunk, Publish, PubsubChannels, PubsubNumsub,
            // PubsubNumpat, Keys, Scan, RandomKey, ExpireTime, AcquireTxLock,
            // ReleaseTxLock, RestoreRdbChunk, ExecuteReplicaCmd, NotifyList,
            // TierSpill, TierLoad, TierSpillAll, TierCool, TierDecommit,
            // GetUsedMemory, StreamColdRead, TierGc, TierSnapshot, FlushSlots,
            // Stick, Unstick, IsSticky, Delex
        }
    }
});
```

This has grown a great deal since the original squashing design: it now covers not just plain
key ops (`Get`/`Set`/`Del`/...) but slot ownership/migration (`SetSlotState`, `SetSlotOwner`,
`FlushSlots`), RDB/AOF durability (`SaveRdbChunk`, `SyncAof`, `RestoreRdbChunk`), pub/sub
fan-out (`Publish`, `PubsubChannels`, `PubsubNumsub`, `PubsubNumpat`), a distributed
transaction lock (`AcquireTxLock`/`ReleaseTxLock`, backed by `Router::tx_lock` +
`tx_waiters: VecDeque`), replication apply (`ExecuteReplicaCmd`), blocking-op wakeups
(`NotifyList`, which locks the port's shared `BlockHub` — see §2.4), and the NVMe tiering
control plane (`TierSpill`/`TierLoad`/`TierSpillAll`/`TierCool`/`TierDecommit`/`TierGc`/
`TierSnapshot`/`StreamColdRead`/`GetUsedMemory`).

`ShardMessage::Batch` is still the pipeline-squashing primitive (Component 02/`connection.rs`
sends one `Batch` per remote shard per pipeline flush). It has grown a tiering-aware fast/slow
split: if any `Get` in the batch misses locally *and* the key is tiered
(`db.table.is_tiered(key)`), the whole batch is handled inside a separate `monoio::spawn`ed
task that can `.await` a cold read (`stream_cold_read_local`) per item; otherwise it's handled
synchronously in the receiver task itself with `execute_local_command`, avoiding a task-spawn
per batch in the common (fully in-RAM) case. Either way, writes in the batch trigger
`router.check_auto_tier().await` afterward.

### 4.3 The accept loop

```rust
let mut next_client_id: u64 = ((shard_id as u64) << 48) + 1;
loop {
    match listener.accept().await {
        Ok((stream, client_addr)) => {
            let _ = stream.set_nodelay(true);
            let client_id = next_client_id;
            next_client_id += 1;
            monoio::spawn(async move {
                handle_connection(stream, client_addr, client_id, reg, r).await;
            });
        }
        Err(e) => eprintln!("[Shard {}] Accept error: {}", shard_id, e),
    }
}
```

Each accepted connection becomes its own `monoio::spawn`ed task. Client IDs encode the owning
shard in the top 16 bits (`(shard_id << 48) + counter`), so IDs never collide across shards
without any cross-shard coordination. An `accept()` error is logged and the loop just retries —
a transient error never brings down the shard's listener.

---

## 5. Cross-Component Interactions

- **`src/connection.rs`**: every accepted socket is handed off as `handle_connection(...)`, spawned as a task on this shard's runtime. See Component 02.
- **`src/router.rs`**: `Router` is constructed here once per shard and shared (`Rc`) with every connection task; it owns the `senders` mesh, slot ownership state, and the tiering/AOF/pubsub handles used throughout this file. See Component 04.
- **`src/aof.rs`**: AOF replay on startup, the periodic flush/fsync task, and per-command `command_to_resp` appends inside several `ShardMessage` handlers.
- **`src/tiering.rs`**: `ShardTierManager::open`, the 20ms auto-tier check, the 2s GC task, and the `Tier*`/`StreamColdRead` message variants.
- **`src/cluster.rs`**: `start_cluster_bus(port)`, started once from `shard_id == 0` only.
- **`src/block.rs`**: `get_block_hub_for_port(port)` is consulted both for `CLIENT LIST`'s blocked-flag and for `ShardMessage::NotifyList` wakeups — the one place this subsystem reaches for a real, shared mutex instead of thread-local state.
- **`src/pubsub.rs`**: `PubSubHub` is created per-shard but `Publish`/`PubsubChannels`/`PubsubNumsub`/`PubsubNumpat` are also reachable as `ShardMessage` variants so a publish on one shard can fan out to subscribers connected via other shards.

---

## 6. Performance Characteristics

- **Zero-syscall-per-connection ingress**: `SO_REUSEPORT` means the kernel — not userspace — decides which shard's listener gets each new connection.
- **No cross-core cache traffic in the common case**: local key access never leaves the owning thread; only the `ShardMessage` mesh and the shared `BlockHub` mutex cross cores, and both are only exercised on non-local or blocking operations.
- **Bounded periodic work, not full scans**: the 100ms expiration cycle, 20ms auto-tier check, and 2s GC task are all designed to do fixed, small amounts of work per tick rather than scanning the whole shard, so they never show up as a latency spike on the shared single-threaded runtime.
