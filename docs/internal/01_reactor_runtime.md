# Component 01: Reactor Runtime & Server Lifecycle (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/main.rs`, `src/server.rs`
> **High-Level Design Spec**: [`docs/design/01_reactor_runtime.md`](../design/01_reactor_runtime.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/main.rs` | Process entry point: OS-level tuning, config loading, thread spawning | `Args` (clap), `main` |
| `src/server.rs` | Per-shard worker: socket setup, restore, periodic tasks, accept loops, shutdown | `run_shard_worker`, `CatchUnwind`/`catch_unwind_async` |

---

## 2. Process Startup (`main.rs`)

```
Process Startup (main.rs)
        │
   #[cfg(target_os = "linux")] libc::prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0)
        │
   rudis::telemetry::init_telemetry()          (tracing_subscriber, EnvFilter "info,rudis=info")
   rudis::shutdown::install_signal_handlers()   (SIGINT/SIGTERM → shutdown flag; SIGPIPE ignored)
        │
   Args::parse() (clap)
        │
   RudisConfig::load_file(--config) or RudisConfig::default(), then merge_cli(...)
        │
   tiering::set_max_memory / set_offload_threshold_pct / set_upload_threshold_pct (keyed by port)
   connection::set_max_clients / set_max_memory_policy
        │
   core_affinity::get_core_ids() → num_cores
   num_shards = config.threads.unwrap_or(num_cores.min(8))
        │
   if cluster_enabled: cluster::get_cluster_hub(port) primed with num_shards
        │
   build AofConfig { enabled, dir, fsync_every_sec: true }
   build Option<TlsWorkerConfig> (file-based cert/key, or in-memory self-signed)
        │
   print startup banner + syscheck::run_system_sanity_checks()
        │
   mailbox::create_shard_mesh(num_shards) → (senders_mesh, receivers)
        │
   for each shard: thread::Builder::new().name("rudis-shard-{i}").spawn(run_shard_worker(...))
        │
   for each handle: handle.join()
        │
   println!("rudis server gracefully stopped. Goodbye!")
```

### 2.1 What `Args` actually looks like today

```rust
#[derive(Parser, Debug)]
#[command(name = "rudis", version = "0.1.0", about = "...")]
struct Args {
    config: Option<PathBuf>,               // -c/--config
    port: Option<u16>,                     // -p/--port
    threads: Option<usize>,                // -t/--threads
    aof: Option<bool>,                     // --aof
    aof_dir: Option<PathBuf>,              // --aof-dir
    maxmemory: Option<String>,             // --maxmemory
    tiered_offload_threshold: Option<u64>, // --tiered-offload-threshold
    tiered_upload_threshold: Option<u64>,  // --tiered-upload-threshold
    no_pin: bool,                          // --no-pin (default false)
    tls_port: Option<u16>,                 // --tls-port
    tls_cert_file: Option<PathBuf>,        // --tls-cert-file
    tls_key_file: Option<PathBuf>,         // --tls-key-file
    cluster_enabled: Option<String>,       // --cluster-enabled yes|true|1
}
```

Every CLI value is `Option`, and is merged on top of a `RudisConfig` that was itself either loaded from a `redis.conf`-style file (`-c/--config`) or defaulted (`RudisConfig::default()`, `src/config.rs`). This is a real change from an earlier revision of this document, which described `Args` as the sole source of configuration with no file-based config layer; `RudisConfig` (bind, port, threads, maxclients, maxmemory/maxmemory_bytes/maxmemory_policy, appendonly, dir, requirepass, tls_port/tls_cert_file/tls_key_file, cluster_enabled, tiered_offload_threshold, tiered_upload_threshold, extra_directives) is now the actual source of truth, with CLI flags applied as overrides via `merge_cli`. There is no `appendfsync`-style directive in `RudisConfig` — AOF fsync cadence is not currently configurable (see Component 14).

`num_shards` is `config.threads.unwrap_or_else(|| num_cores.min(8))` — capped at 8 by default so a large shared host does not silently spawn one `io_uring`/epoll ring and listener per core with no flags given; pass `--threads`/`threads` explicitly to use more.

**TLS** is real, not dead code: if a TLS port is configured, `main.rs` builds one `rustls::ServerConfig` up front (before any shard thread starts) — from `--tls-cert-file`/`--tls-key-file` via `crate::tls::load_certs_and_key_from_files` if both are given, otherwise a fresh in-memory self-signed certificate for `localhost`/`127.0.0.1` via `crate::tls::generate_self_signed_cert` — and wraps it in a `Clone`-able `TlsWorkerConfig { tls_port, server_config }` cloned once per shard, exactly like `AofConfig`.

**Cluster mode**: if `cluster_enabled` is set, `main.rs` primes the process-wide cluster hub (`rudis::cluster::get_cluster_hub(port)`) with `cluster_enabled = true` and `num_shards` before any shard starts, and each shard is later given `cluster_enabled: bool` as an explicit argument to `run_shard_worker` so it knows to bind `base_port + shard_id` instead of sharing `base_port` via `SO_REUSEPORT` (§3.1).

**Linux-only THP disablement**: `libc::prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0)` is called once, at the very top of `main`, behind `#[cfg(target_os = "linux")]`. This opts the process and all of its threads out of Transparent Huge Page promotion before any memory of consequence is allocated.

### 2.2 `run_shard_worker`'s current signature

```rust
pub fn run_shard_worker(
    shard_id: usize,
    num_shards: usize,
    port: u16,
    senders: Vec<crate::mailbox::ShardSender>,
    rx: crate::mailbox::ShardReceiver,
    core_id: Option<core_affinity::CoreId>,
    aof_config: crate::aof::AofConfig,
    tls_config: Option<crate::tls::TlsWorkerConfig>,
    cluster_enabled: bool,
)
```

Every shard gets a clone of the full sender vector (so it can reach any other shard) but only its own receiver, plus its own clone of the optional `tls_config`. The channel pair type (`ShardSender`/`ShardReceiver`) is built by `crate::mailbox::create_shard_mesh`, not constructed ad hoc per shard in `main.rs`.

---

## 3. Execution Algorithms & Code Logic (`run_shard_worker`, `src/server.rs`)

### 3.1 Startup sequence inside `run_shard_worker`

In order, each shard thread:

1. Pins to its core (if assigned) via `core_affinity::set_for_current`.
2. Seeds a thread-local `ADOPTED_CLIENT_SEQ` counter (used only for connections handed off from another shard, §3.5) with `(shard_id << 48) | (1 << 47) | 1` — bit 47 set, so ids minted here can never collide with either this shard's own accept-loop ids or its TLS accept-loop ids.
3. Builds `monoio::RuntimeBuilder::<monoio::FusionDriver>::new().enable_timer()`. **`FusionDriver`, not a hardcoded `IoUringDriver`** — this is a change from an earlier revision of this document. `FusionDriver` selects an `io_uring`-backed driver where available and falls back to a legacy poll-based driver otherwise, rather than requiring `io_uring` unconditionally.
4. Inside `rt.block_on(async move { ... })`:
   - Computes `shard_port`: `base_port + shard_id` if `cluster_enabled`, else `base_port` (every shard shares the same port via `SO_REUSEPORT` in the non-cluster case).
   - Builds the plain TCP listener: `Socket::new(..STREAM..)`, `set_reuse_port(true)`, `set_reuse_address(true)`, `set_nonblocking(true)`, 512KB send/recv buffers, `bind`, `listen(4096)`, then converts to a `monoio::net::TcpListener`.
   - **If `tls_config` is `Some`**, repeats the same socket setup for `tls_cfg.tls_port` into a second, independent `Option<TcpListener>` (`tls_listener`).
   - Creates `let local_db = Rc::new(RefCell::new(ShardDb::new(port).with_shard(shard_id)))`.
   - **RDB restore** (only if AOF is *not* enabled): if `<aof_dir>/dump.rdb` exists, calls `crate::table::load_rdb(&rdb_path, &mut local_db.borrow_mut(), shard_id, num_shards)`. Every shard parses the *entire* RDB file independently and keeps only the keys that hash to itself (`crate::router::target_shard(key, num_shards) == shard_id`) — see Component 05 / `docs/rdbsave.md` for the on-disk format.
   - **AOF replay + writer** (only if AOF is enabled): replays `<aof_dir>/appendonly-<shard_id>.aof` via `crate::aof::replay_aof`, then opens an `AofWriter`. If opened, spawns a dedicated `monoio::spawn` task that wakes **every 5ms**, flushes any pending write chunk (`take_flush_chunk` → `write_all_at`), recycles the drained buffer (`recycle_chunk`, capped at 4MB to avoid holding an oversized buffer forever), and calls `sync_data()` (fsync) every 200th tick — i.e. roughly once per second — when `fsync_every_sec` is set. This is a real change from an earlier revision of this document, which described a 50ms flush / ~20-tick fsync cadence; the current cadence is 5ms flush / 200-tick (~1s) fsync, the same effective ~1s fsync interval reached differently.
   - **Cluster bus**: only `shard_id == 0` calls `crate::cluster::start_cluster_bus(port)` — a single, once-per-process background listener, not one per shard.
   - **Tiered storage**: opens a `crate::tiering::ShardTierManager` for this shard/port (directory from `RUDIS_TIER_DIR` env var, or a per-port temp directory) and, on success, attaches it to `local_db.borrow_mut().tier_manager`.
   - Builds `client_registry` (`Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>`), a `pubsub` hub (`Rc<RefCell<PubSubHub>>`), and the shard's `Router` (`Router::new(...)`, then `r.base_port = base_port; r.cluster_enabled = cluster_enabled;` set explicitly after construction).
   - Spawns three independent periodic `monoio::spawn` tasks, all sharing the same `Rc<RefCell<ShardDb>>`/`Router` with no synchronization:
     - every 100ms: `active_db.borrow_mut().active_expire_cycle()`
     - every 20ms: `offload_router.check_auto_tier().await`
     - every 2s: `gc_router.gc_local()`
   - Spawns an **AF_XDP kernel-bypass ingress loop** (`crate::xdp`, Component 10's domain): polls a zero-copy Rx ring for packet descriptors via `xsk_socket.rx_burst`, and for `Pass`/`Redirect` actions parses and either executes a command locally or forwards it to the owning shard via `execute_remote`, sleeping 5ms when nothing is pending. This loop did not exist in earlier revisions of this document; it runs unconditionally per shard (the underlying XDP engine no-ops if no AF_XDP socket is actually attached to the interface).
   - Spawns the **cross-shard receiver task** (§3.2).
   - Prints the shard's startup banner.
   - Spawns the **TLS accept loop**, if configured, then enters the **plain accept loop** (§3.3), both forever (until shutdown, §3.4).
5. After the accept loop returns (shutdown), flushes and `fsync`s the AOF writer if one is open, then the `async move` block — and `block_on` — return, ending the thread's work; `main.rs`'s `handle.join()` then unblocks for this shard.

### 3.2 The cross-shard receiver loop: burst-draining `ShardMessage` dispatch

```rust
monoio::spawn(async move {
    while let Ok(mut msg) = rx.recv_async().await {
        let mut burst = 0;
        loop {
            match msg {
                ShardMessage::AdoptConnection { fd, peer } => { /* see §3.5 */ }
                ShardMessage::Get { key, responder } => { /* ... */ }
                ShardMessage::Set { key, value, expire_in, responder } => { /* ... */ }
                ShardMessage::Batch { items, results, responder, is_resp3 } => { /* ... */ }
                ShardMessage::Mget { keys, responder } => { /* ... */ }
                ShardMessage::Mset { pairs, responder } => { /* ... */ }
                ShardMessage::ScatterMget { .. } | ShardMessage::ScatterMset { .. } => { /* ... */ }
                ShardMessage::FastGet { .. } | ShardMessage::FastSet { .. } => { /* ... */ }
                // ... 60+ variants in total as of this writing (src/shard.rs), spanning:
                //   key/value ops (Get, Set, Del, DelKeys, Exists, IncrBy, Expire,
                //     Persist, Ttl, ActiveDefrag),
                //   batched cross-shard fan-out (Batch, Mget, Mset, ScatterMget,
                //     ScatterMset, FastGet, FastSet, JsonMget),
                //   cluster/slot control (CountKeysInSlot, GetKeysInSlot,
                //     SetSlotState, SetSlotOwner, FlushSlots, Stick, Unstick, IsSticky),
                //   persistence (SaveRdbChunk, RestoreRdbChunk, SyncAof, RewriteAof,
                //     DumpKey),
                //   replication apply (ExecuteReplicaCmd),
                //   pub/sub fan-out (Publish, PubsubChannels, PubsubNumsub, PubsubNumpat),
                //   blocking-op wakeups (NotifyList),
                //   distributed transaction locking (AcquireTxLock, ReleaseTxLock),
                //   NVMe tiering control (TierSpill, TierLoad, TierSpillAll, TierCool,
                //     TierDecommit, TierGc, TierSnapshot, StreamColdRead, GetUsedMemory),
                //   admin/stats (ClientList, FlushCommandStats, ResetCommandStats), and
                //   connection handoff (AdoptConnection, §3.5).
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

The outer `while let Ok(mut msg) = rx.recv_async().await` suspends the task when the mailbox is empty; once woken, it loops on the cheap, non-async `rx.try_recv()` to drain up to 64 already-queued messages inline before yielding back to `recv_async().await`. This amortizes async wakeup/poll overhead under sustained cross-shard load (fan-out `MGET`/`MSET`, pipeline squashing from many peers hitting one shard at once).

`ShardMessage::Batch` is the pipeline-squashing primitive (`connection.rs` sends one `Batch` per remote shard per pipeline flush): it checks whether the shard has a tier manager at all before scanning for tiered misses, so a shard with tiering disabled skips the per-item check entirely; when tiering is enabled and a batched `Get` misses locally and is tiered, the whole batch is handled in a separate spawned task that can `.await` a cold read, otherwise it runs synchronously via `execute_local_command`.

### 3.3 The accept loop(s): plain TCP, optional TLS, and connection rebalancing

```rust
let mut next_client_id: u64 = ((shard_id as u64) << 48) + 1;
loop {
    if crate::shutdown::is_shutting_down() { break; }
    let accept_res = match monoio::time::timeout(
        Duration::from_millis(200), listener.accept()
    ).await {
        Ok(res) => res,
        Err(_) => continue, // 200ms timeout elapsed with no connection; re-check the shutdown flag
    };
    match accept_res {
        Ok((stream, client_addr)) => {
            let _ = stream.set_nodelay(true);
            // also sets TCP_NODELAY and TCP_QUICKACK directly via setsockopt
            let client_id = next_client_id; next_client_id += 1;
            let owner = if cluster_enabled {
                crate::conn_balance::register_conn(shard_id);
                shard_id
            } else {
                crate::conn_balance::claim_owner(shard_id, num_shards)
            };
            if owner != shard_id {
                let fd = std::os::unix::io::IntoRawFd::into_raw_fd(stream);
                let handed = router.senders[owner]
                    .send(ShardMessage::AdoptConnection { fd, peer: client_addr })
                    .is_ok();
                if !handed {
                    crate::conn_balance::unregister_conn(owner);
                    unsafe { libc::close(fd) };
                }
            } else {
                monoio::spawn(async move {
                    let res = catch_unwind_async(async move {
                        handle_connection(stream, client_addr, client_id, reg, r).await;
                    }).await;
                    crate::conn_balance::unregister_conn(shard_id);
                    if let Err(e) = res {
                        crate::connection::inc_isolated_panics();
                        tracing::error!(...);
                    }
                });
            }
        }
        Err(e) => eprintln!("[Shard {}] Accept error: {}", shard_id, e),
    }
}
```

Client IDs encode the *owning* shard in the top 16 bits (`(shard_id << 48) + counter`), so IDs never collide across shards without cross-shard coordination. `accept()` errors are logged and the loop retries.

**Connection rebalancing** (non-cluster mode only): `SO_REUSEPORT` assigns a new connection to a shard by hashing its 4-tuple, which — per `src/conn_balance.rs`'s own documentation — is badly uneven for a small number of long-lived connections. After accepting, the shard calls `crate::conn_balance::claim_owner(shard_id, num_shards)`, which atomically reserves a slot on whichever shard currently holds the fewest connections (ties keep the connection local). If that is a different shard, the raw fd is released via `into_raw_fd` (no `Drop`, so the fd stays open) and handed to the target shard as a `ShardMessage::AdoptConnection { fd, peer }` — valid without `SCM_RIGHTS` because all shards are threads in one process sharing a single fd table. The receiving shard's cross-shard receiver loop (§3.2) reconstructs a `monoio::net::TcpStream` from the raw fd via `from_raw_fd`/`from_std` and spawns `handle_connection` on it, deliberately skipping `register_conn` on arrival (the sending shard already reserved the slot). Cluster mode is exempt: each shard binds its own port there, so distribution is client-chosen and rebalancing would break `MOVED` semantics for no benefit.

**TLS accept loop**: if `tls_listener` is `Some`, a second loop is spawned before the plain one, structurally identical (same 200ms shutdown-poll timeout) but with client IDs carved from the *upper* half of the shard's 48-bit ID space (`(shard_id << 48) | 0x8000_0000_0000` plus an incrementing counter), so a TLS client and a plain client on the same shard can never collide on ID. Each accepted TLS connection is upgraded via `crate::tls::TlsSession::new` + `session.handshake_monoio` (a real `rustls` handshake driven over the `monoio` stream) before being handed to `crate::connection::handle_tls_connection` — a distinct entry point from the plain loop's `handle_connection`. Both the accept and the post-handshake work are wrapped in `catch_unwind_async` exactly like the plain path.

### 3.4 Shutdown

Both accept loops poll `crate::shutdown::is_shutting_down()` once per iteration and break out of their `loop` when it is `true`, discovering the flag within ~200ms of a `SIGINT`/`SIGTERM` (the polling interval is enforced by wrapping `listener.accept()` in `monoio::time::timeout(Duration::from_millis(200), ...)`). After the plain accept loop returns, if this shard opened an `AofWriter`, its pending write buffer is flushed (`write_all_at`) and `sync_data()`'d (fsync) before the shard's `async move` block completes. The shard's `monoio::RuntimeBuilder::block_on` then returns, the OS thread's closure ends, and `main.rs`'s `handle.join()` for that shard unblocks. Once every shard thread has joined, `main` prints `"rudis server gracefully stopped. Goodbye!"`.

This is real but partial: the cross-shard receiver task, the three periodic maintenance tasks, and the XDP ingress loop are not explicitly cancelled — they stop only because dropping the runtime drops every task still scheduled on it. Already-accepted client connections are not drained or given a chance to finish an in-flight request; they end the same way. No `SAVE`/`BGSAVE` is triggered automatically on shutdown.

### 3.5 `catch_unwind_async` / `CatchUnwind` (panic isolation)

```rust
pub struct CatchUnwind<F> { inner: F }

pub fn catch_unwind_async<F: Future>(f: F) -> CatchUnwind<F> {
    CatchUnwind { inner: f }
}

impl<F: Future> Future for CatchUnwind<F> {
    type Output = Result<F::Output, Box<dyn std::any::Any + Send + 'static>>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = unsafe { Pin::new_unchecked(&mut self.get_unchecked_mut().inner) };
        match std::panic::catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Ready(val)) => Poll::Ready(Ok(val)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}
```

A hand-rolled `Future` combinator: every `poll` call is wrapped in `std::panic::catch_unwind`, so a panic inside the wrapped future's execution (e.g. an indexing bug while servicing one client's command) becomes an `Err` result instead of an unwind that would otherwise propagate up through `monoio::spawn` and, because the runtime is single-threaded per shard, potentially take down every other task — and connection — sharing that shard's runtime. Every accept path (plain, TLS, and cross-shard-adopted) wraps its call to `handle_connection`/`handle_tls_connection` in this combinator and, on `Err`, calls `crate::connection::inc_isolated_panics()` and logs via `tracing::error!` instead of propagating.

---

## 4. Cross-Component Interactions

- **`src/connection.rs`**: every accepted socket becomes `handle_connection(...)` or `handle_tls_connection(...)`, spawned as a task on this shard's runtime, panic-isolated per §3.5. See Component 02.
- **`src/router.rs`**: `Router` is constructed once per shard and shared (`Rc`) with every connection task; it owns the `senders` mesh, slot ownership state, the AOF writer handle, and `db_dir`, `base_port`, `cluster_enabled`. See Component 04.
- **`src/aof.rs`**: AOF replay on startup, the 5ms flush / ~1s fsync task, and the final flush+fsync on shutdown, all driven from here.
- **`src/tiering.rs`**: `ShardTierManager::open`, the 20ms auto-tier check, the 2s GC task, and the `Tier*`/`StreamColdRead` message variants.
- **`src/cluster.rs`**: `start_cluster_bus(port)` (shard 0 only); `cluster_enabled` changes the socket-binding strategy in §3.1 and connection rebalancing exemption in §3.3.
- **`src/block.rs`**: `get_block_hub_for_port(port)` — the one place this subsystem reaches for a real, shared mutex instead of thread-local state.
- **`src/pubsub.rs`**: per-shard `PubSubHub`, reachable cross-shard via `ShardMessage::Publish`/`PubsubChannels`/`PubsubNumsub`/`PubsubNumpat`.
- **`src/tls.rs`**: `TlsWorkerConfig`, `TlsSession::new`/`handshake_monoio`, and cert loading/self-signed generation, wired up from here when a TLS port is configured.
- **`src/conn_balance.rs`**: the connection-count rebalancing logic behind §3.3's `AdoptConnection` handoff.
- **`src/xdp.rs`**: the per-shard AF_XDP ingress loop described in §3.1 (see Component 10 for the packet-processing detail).
- **`src/shutdown.rs`**: the `AtomicBool` flag and signal handlers behind §3.4.
- **`src/config.rs`**: `RudisConfig` — the file/CLI-merged configuration consumed by `main.rs` (§2.1).
- **`src/syscheck.rs`** / **`src/telemetry.rs`**: startup sanity-check reporting and `tracing`-based structured logging initialization, both invoked from `main.rs` before any shard thread is spawned.

---

## 5. Future Improvements

- **Medium — the graceful-shutdown path does not drain in-flight connections or background tasks.** §3.4 stops new accepts and durably flushes AOF, but an in-flight client request, an in-progress `BGSAVE`/`BGREWRITEAOF`, and the cross-shard/periodic/XDP tasks are all simply dropped when the runtime is torn down rather than being told to finish or cancel cleanly.
- **Medium — reconcile the shard/slot model with live cluster migration.** `run_shard_worker` assigns shards statically at startup and never changes `num_shards`; Component 04/11 carry the live-migration machinery, which is only partially wired to this static model.
- **Low — the 100ms/20ms/2s/5ms periodic tasks have no jitter or adaptive backoff.** On a host running many shards, their fixed intervals tend to tick in near-lockstep, a minor but avoidable source of correlated CPU bursts.
- **Low — de-duplicate the plain and TLS socket setup code.** The TLS listener's socket configuration sequence in §3.1 is a near-verbatim copy of the plain listener's, differing only in the port variable used.
- **Low — `SO_REUSEPORT` imbalance is mitigated, not eliminated.** `conn_balance` (§3.3) corrects for it after the fact per-connection; it does not change the kernel's initial hash-based assignment, and now applies independently to two listeners per shard when TLS is enabled.

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Never introduce `Arc<Mutex<_>>` or any cross-thread handle to `ShardDb`. `ShardDb` is strictly `!Send` by construction (`Rc<RefCell<_>>`), and that is intentional, not an oversight to work around.
* **Gotcha 2**: Any new listening socket must set both `SO_REUSEPORT` and `SO_REUSEADDR`, matching the pattern in §3.1 for both the plain and TLS listeners.
* **Gotcha 3**: Transparent Huge Pages are disabled once, in `main.rs`, before any shard thread starts — do not rely on per-shard THP handling.
* **Gotcha 4**: The cross-shard receiver loop drains up to 64 messages per wakeup via `rx.try_recv()`; a handler that blocks the async task for a long time (e.g. a synchronous, non-yielding loop) delays every other message already queued behind it in that burst.
* **Gotcha 5**: `is_shutting_down()` is polled inside the accept loops via a 200ms `monoio::time::timeout` around `accept()`, not via a dedicated cancellation future — a new long-lived per-shard loop added elsewhere will not automatically observe shutdown unless it adopts the same polling pattern (or an equivalent).
* **Gotcha 6**: Any new per-connection entry point should be wrapped in `catch_unwind_async` (§3.5) the same way `handle_connection`/`handle_tls_connection` are, or a panic in that path will take down the whole shard.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
