# Component 01: Reactor Runtime & Server Lifecycle (Implementation)

## Component 01: Reactor Runtime & Server Lifecycle — Code Reference & Implementation

> **Source Files**: ``src/main.rs`, `src/server.rs``


---

### 3. Component Architecture & Data Structures

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

#### What `main.rs` actually builds before spawning threads

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
    tls_port: Option<u16>,
    tls_cert_file: Option<std::path::PathBuf>,
    tls_key_file: Option<std::path::PathBuf>,
}
```

`num_shards` defaults to `num_cores.min(8)` (capped, not "one shard per core", so a large
shared host doesn't silently spawn one `io_uring` ring and listener per core with no flags
given). `main.rs` also applies the tiering config globally via `rudis::tiering::set_max_memory`
/ `set_offload_threshold_pct` / `set_upload_threshold_pct` (keyed by port) before any thread
starts, and builds one `AofConfig { enabled, dir, fsync_every_sec: true }` shared by every
shard.

**TLS is now real and wired up here** (previously dead code per Component 15 — that finding is
now stale). If `--tls-port` is passed, `main.rs` builds one `rustls::ServerConfig` up front
(before any shard thread starts) — from `--tls-cert-file`/`--tls-key-file` if both are given, via
`crate::tls::load_certs_and_key_from_files`, otherwise a fresh in-memory self-signed cert for
`localhost`/`127.0.0.1` via `crate::tls::generate_self_signed_cert` — and wraps it in a
`Clone`-able `TlsWorkerConfig { tls_port, server_config }` that's cloned once per shard, exactly
like `AofConfig`.

#### `run_shard_worker`'s signature (what each thread actually receives)

```rust
pub fn run_shard_worker(
    shard_id: usize,
    num_shards: usize,
    port: u16,
    senders: Vec<flume::Sender<ShardMessage>>,
    rx: flume::Receiver<ShardMessage>,
    core_id: Option<core_affinity::CoreId>,
    aof_config: crate::aof::AofConfig,
    tls_config: Option<crate::tls::TlsWorkerConfig>,
)
```

Every shard gets a clone of the full `senders` vector (so it can reach any other shard) but
only its own `rx`, plus its own clone of the optional `tls_config`.

---

---

### 4. Execution Algorithms & Code Logic

#### 4.1 Startup sequence inside `run_shard_worker`

In order, each shard thread:

1. Pins to its core (if assigned), then builds a `monoio::RuntimeBuilder<IoUringDriver>` with `enable_timer()`.
2. Inside `rt.block_on(async move { ... })`, creates its `SO_REUSEPORT`/`SO_REUSEADDR` socket, sets 512KB send/recv buffers, binds, and listens (backlog 4096). **If `tls_config` is `Some`, immediately builds a second, independent `SO_REUSEPORT` socket bound to `tls_cfg.tls_port`** — the setup code is a near-verbatim copy of the plain-port socket setup (same buffer sizes, same reuse flags), just a different port and address variable, kept as a separate `Option<TcpListener>` (`tls_listener`) alongside the plain `listener`.
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

#### 4.2 The cross-shard receiver loop: one `match` arm per `ShardMessage` variant, now with burst draining

```rust
monoio::spawn(async move {
    while let Ok(mut msg) = rx.recv_async().await {
        let mut burst = 0;
        loop {
            match msg {
                ShardMessage::Get { key, responder } => { /* ... */ }
                ShardMessage::Set { key, value, expire_in, responder } => { /* ... */ }
                ShardMessage::Batch { items, responder, is_resp3 } => { /* ... */ }
                ShardMessage::Mget { keys, responder } => { /* ... */ }
                ShardMessage::Mset { pairs, responder } => { /* ... */ }
                // ~37 variants total: Del, Exists, IncrBy, Expire, Persist, Ttl,
                // CountKeysInSlot, GetKeysInSlot, ClientList, SetSlotState, SetSlotOwner,
                // DumpKey, SyncAof, SaveRdbChunk, Publish, PubsubChannels, PubsubNumsub,
                // PubsubNumpat, Keys, Scan, RandomKey, ExpireTime, AcquireTxLock,
                // ReleaseTxLock, RestoreRdbChunk, ExecuteReplicaCmd, NotifyList,
                // TierSpill, TierLoad, TierSpillAll, TierCool, TierDecommit,
                // GetUsedMemory, StreamColdRead, TierGc, TierSnapshot, FlushSlots,
                // Stick, Unstick, IsSticky, Delex
            }
            burst += 1;
            if burst >= 64 { break; }
            match rx.try_recv() {
                Ok(next) => msg = next,
                Err(_) => break,
            }
        }
    }
});
```

**New since the last verified version of this doc: the receiver loop drains up to 64 messages
per wakeup instead of one.** The outer `while let Ok(mut msg) = rx.recv_async().await` still
suspends the task when the mailbox is empty, but once woken it now loops on a cheap, non-async
`rx.try_recv()` to keep processing already-queued messages inline — up to a hardcoded cap of
64 — before yielding back to `recv_async().await`. Under sustained cross-shard load (many
remote `Batch`/`Mget`/`Mset` messages arriving back-to-back) this amortizes the async
wakeup/poll overhead across up to 64 messages instead of paying it once per message.

Beyond the plain key ops (`Get`/`Set`/`Del`/...), this match also covers slot ownership/
migration (`SetSlotState`, `SetSlotOwner`, `FlushSlots`), RDB/AOF durability (`SaveRdbChunk`,
`SyncAof`, `RestoreRdbChunk`), pub/sub fan-out (`Publish`, `PubsubChannels`, `PubsubNumsub`,
`PubsubNumpat`), a distributed transaction lock (`AcquireTxLock`/`ReleaseTxLock`, backed by
`Router::tx_lock` + `tx_waiters: VecDeque`), replication apply (`ExecuteReplicaCmd`), blocking-op
wakeups (`NotifyList`, which locks the port's shared `BlockHub` — see §2.4), and the NVMe
tiering control plane (`TierSpill`/`TierLoad`/`TierSpillAll`/`TierCool`/`TierDecommit`/`TierGc`/
`TierSnapshot`/`StreamColdRead`/`GetUsedMemory`).

`ShardMessage::Batch` is still the pipeline-squashing primitive (Component 02/`connection.rs`
sends one `Batch` per remote shard per pipeline flush), and it now checks `has_tier_manager =
db.tier_manager.is_some()` **before** scanning the batch for tiered misses — a shard with
tiering disabled entirely skips the per-item `is_tiered` check rather than paying it on every
`Get` in every batch. When tiering is enabled and at least one `Get` in the batch misses
locally *and* is tiered, the whole batch is handled inside a separate `monoio::spawn`ed task
that can `.await` a cold read (`stream_cold_read_local`) per item; otherwise it's handled
synchronously in the receiver task itself with `execute_local_command`. Writes in the batch now
call the synchronous `router.check_auto_tier_after_write()` directly (not the old async
`check_auto_tier().await` spawned as a separate fire-and-forget task) — this is a real change
from the previously-verified version of this doc, worth noting if you've read an earlier copy.

**`ShardMessage::Mget`/`Mset` are new** — the server-side half of the parallel cross-shard
`MGET`/`MSET` fan-out implemented in Component 04/02 (previously a documented gap: each key was
routed and awaited one at a time). `Mget`'s handler mirrors `Get`'s tiering-aware fast/slow
split, but batched: it fast-paths every key with `db.tier_manager.is_none()` (a plain
per-key `db.get` loop, no async at all), and when tiering is enabled, walks the key list
synchronously until it hits the first tiered miss, then hands the *remaining* keys off to a
spawned task that resolves each one exactly the way a single tiered `Get` would (checking
memory pressure, then either a streaming cold read or a full `ensure_loaded`). `Mset`'s
handler is simpler: apply every pair to the local `ShardDb` inline, append one AOF record for
the whole batch (`Command::Mset(pairs)`, not one record per pair), then call
`check_auto_tier_after_write()` once.

#### 4.3 The accept loop(s): plain TCP always, plus an optional TLS accept loop

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

**If `tls_listener` is `Some`, a second accept loop is spawned first** (before the plain accept
loop above, both coexisting as separate `monoio::spawn`ed tasks on the same shard runtime).
Its client IDs are deliberately carved from a disjoint bit range of the same shard: `((shard_id
as u64) << 48) | 0x8000_0000_0000` plus an incrementing counter — i.e. the *upper* half of the
shard's 48-bit per-shard ID space, while the plain loop's `+ 1, 2, 3, ...` counter occupies the
*lower* half. This guarantees a TLS client and a plain-TCP client on the same shard can never
collide on client ID without any coordination between the two loops. Each accepted TLS
connection is upgraded via `crate::tls::TlsSession::new` + `session.handshake_monoio(&mut
stream)` (a real `rustls` handshake driven over the `monoio` stream) *before* being handed to
`crate::connection::handle_tls_connection` — a distinct entry point from the plain loop's
`handle_connection`, not the same function with a wrapped stream type.

---

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: every accepted socket is handed off as `handle_connection(...)`, spawned as a task on this shard's runtime. See Component 02.
- **`src/router.rs`**: `Router` is constructed here once per shard and shared (`Rc`) with every connection task; it owns the `senders` mesh, slot ownership state, and the tiering/AOF/pubsub handles used throughout this file. See Component 04.
- **`src/aof.rs`**: AOF replay on startup, the periodic flush/fsync task, and per-command `command_to_resp` appends inside several `ShardMessage` handlers.
- **`src/tiering.rs`**: `ShardTierManager::open`, the 20ms auto-tier check, the 2s GC task, and the `Tier*`/`StreamColdRead` message variants.
- **`src/cluster.rs`**: `start_cluster_bus(port)`, started once from `shard_id == 0` only.
- **`src/block.rs`**: `get_block_hub_for_port(port)` is consulted both for `CLIENT LIST`'s blocked-flag and for `ShardMessage::NotifyList` wakeups — the one place this subsystem reaches for a real, shared mutex instead of thread-local state.
- **`src/pubsub.rs`**: `PubSubHub` is created per-shard but `Publish`/`PubsubChannels`/`PubsubNumsub`/`PubsubNumpat` are also reachable as `ShardMessage` variants so a publish on one shard can fan out to subscribers connected via other shards.
- **`src/tls.rs`** (Component 15 — no longer dead code): `TlsWorkerConfig`, `TlsSession::new`/`handshake_monoio`, and the cert-loading/self-signed-generation functions are now genuinely called from here when `--tls-port` is set; `crate::connection::handle_tls_connection` is the TLS-specific counterpart to the plain loop's `handle_connection`.

---

---

### 7. Future Improvements

- **High — add a graceful shutdown path.** Still open — there is no `SIGINT`/`SIGTERM` handler anywhere (§2.5), so a restart or deploy today is a hard kill: any buffered AOF chunk not yet flushed, any RDB save in progress, and any in-flight client request are simply dropped. Now that a shard can own up to *two* listeners (plain + optional TLS, §4.3), a clean shutdown needs to stop accepting on both. A minimal fix: catch the signal on the main thread, broadcast a shutdown `ShardMessage` (or a shared `AtomicBool` each accept loop polls) to every shard, have each shard flush its `AofWriter` and stop accepting new connections on both listeners, then join all threads before exiting.
- **Medium — reconcile this file's slot/shard model with live cluster migration.** Still open — `run_shard_worker` assigns shards statically at startup and never changes `num_shards`; meanwhile Component 04/11 already have real (if still partially disconnected — see their own Future Improvements) live-migration machinery.
- **Medium — give the 100ms/20ms/2s periodic tasks jitter or adaptive backoff.** Still open — all three are fixed-interval `monoio::time::sleep` loops; on a host running many shards, every shard's tasks tick in near-lockstep, a minor but avoidable source of correlated CPU bursts.
- **Low — de-duplicate the plain and TLS socket setup code (new, §4.1).** The TLS listener's `Socket::new`/`set_reuse_port`/`set_reuse_address`/`set_nonblocking`/buffer-size/bind/listen sequence is a near-verbatim copy of the plain listener's, differing only in which port variable is used. A small `fn bind_reuseport_listener(port: u16) -> io::Result<TcpListener>` helper, called once for the plain port and once (conditionally) for the TLS port, would remove the duplication risk (a future socket-tuning change applied to one copy and forgotten in the other).
- **Low — reduce `SO_REUSEPORT` imbalance sensitivity.** Still open, and now applies independently to *two* listeners per shard when TLS is enabled — the kernel's 4-tuple hash balancing is not perfectly even for a small number of long-lived connections (§5's "no connection-level backpressure" limitation).
- **Low — document/guard the `BlockHub` exception's failure mode.** Still open — if `get_block_hub_for_port`'s `Mutex` were ever poisoned by a panic while held, every future blocking operation on that port would panic on `.lock().unwrap()`.

---
---
