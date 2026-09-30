# Component 01: Reactor Runtime & Server Lifecycle (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/main.rs` (278 lines), `src/server.rs` (2,079 lines)
> **High-Level Design Spec**: [`docs/design/01_reactor_runtime.md`](../design/01_reactor_runtime.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)
>
> This document is self-contained: every claim below was re-verified directly against the current
> source (not carried forward from a previous pass) as of commit history through `13dc14d`. Where
> this revision corrects or adds to a previous version of this document, that is called out
> explicitly.

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Structs / Functions |
| :--- | :--- | :--- |
| `src/main.rs` | Process entry point: THP disablement, telemetry/signal init, CLI+config merge, ACL priming, core-affinity discovery, mesh creation, thread spawn/join | `Args` (clap), `get_process_affinity_cores`, `main` |
| `src/server.rs` | Per-shard worker: socket setup, RDB/AOF restore, periodic tasks, cross-shard receiver, accept loops (plain + TLS), shutdown, panic isolation | `run_shard_worker`, `CatchUnwind`/`catch_unwind_async` |
| `src/config.rs` | `RudisConfig` — file/CLI-merged configuration consumed by `main.rs` | `RudisConfig`, `load_file`, `merge_cli` |
| `src/mailbox.rs` | Lock-free SPSC cross-shard mesh construction, consumed once by `main.rs` | `create_shard_mesh`, `ShardSender`, `ShardReceiver` |
| `src/conn_balance.rs` | Per-connection load rebalancing census consulted by the accept loop | `claim_owner`, `plan_handoff`, `register_conn`/`unregister_conn` |
| `src/syscheck.rs` | Startup OS/kernel sanity checks, printed once before shards spawn | `run_system_sanity_checks`, `SystemSanityReport` |
| `src/shutdown.rs` | Process-wide shutdown flag + signal handlers | `is_shutting_down`, `install_signal_handlers` |
| `src/agent.rs` / `src/mcp.rs` | Agent memory / MCP server — **not wired into startup at all**; see §5 | (state lives on `ShardDb`, see §5) |

---

## 2. Process Startup (`main.rs`)

```
Process Startup (main.rs)
        │
   #[cfg(target_os = "linux")] libc::prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0)     (line 90-95)
        │
   rudis::telemetry::init_telemetry()                                         (line 97)
   rudis::shutdown::install_signal_handlers()                                 (line 98)
        │
   Args::parse()                                                              (line 99)
        │
   RudisConfig::load_file(--config) or RudisConfig::default()                 (line 102-107)
        │
   cluster_opt = args.cluster_enabled.map(|s| "yes"|"true"|"1")               (line 110-113)
   server_config.merge_cli(port, threads, aof, aof_dir, maxmemory,
       tiered_offload_threshold, tiered_upload_threshold,
       tls_port, tls_cert_file, tls_key_file, cluster_opt)                    (line 114-126)
        │
   tiering::set_max_memory / set_offload_threshold_pct / set_upload_threshold_pct (keyed by port)
   connection::set_max_clients / set_max_memory_policy                        (line 128-135)
        │
   requirepass → prime default ACL user's password + hash (line 137-153)      (NEW, see §2.3)
        │
   allowed_cores = get_process_affinity_cores()  (cgroup/taskset-aware)       (line 155-162, see §2.2)
   num_shards = server_config.threads.unwrap_or_else(|| num_cores.min(8))     (line 164)
        │
   if cluster_enabled: cluster::get_cluster_hub(port) primed with num_shards  (line 166-174)
        │
   build AofConfig { enabled, dir, fsync_every_sec: true }                    (line 176-180)
   build Option<TlsWorkerConfig> (file-based cert/key, or in-memory self-signed) (line 182-204)
        │
   print startup banner + syscheck::run_system_sanity_checks()                (line 206-235)
        │
   mailbox::create_shard_mesh(num_shards) → (senders_mesh, receivers)         (line 238)
        │
   for each shard: thread::Builder::new().name("rudis-shard-{i}").spawn(run_shard_worker(...)) (line 242-272)
        │
   for each handle: handle.join()                                             (line 274-276)
        │
   println!("rudis server gracefully stopped. Goodbye!")                      (line 277)
```

### 2.1 `Args` — every CLI flag, verified against current source

```rust
#[derive(Parser, Debug)]
#[command(name = "rudis", version = "0.1.0", about = "Multi-threaded Shared-Nothing Redis in Rust based on io_uring")]
struct Args {
    config: Option<PathBuf>,               // -c/--config
    port: Option<u16>,                     // -p/--port
    threads: Option<usize>,                // -t/--threads
    aof: Option<bool>,                     // --aof
    aof_dir: Option<PathBuf>,              // --aof-dir
    maxmemory: Option<String>,             // --maxmemory (e.g. "512mb", "1gb")
    tiered_offload_threshold: Option<u64>, // --tiered-offload-threshold (default: 60)
    tiered_upload_threshold: Option<u64>,  // --tiered-upload-threshold (default: 80)
    no_pin: bool,                          // --no-pin (default_value_t = false)
    tls_port: Option<u16>,                 // --tls-port
    tls_cert_file: Option<PathBuf>,        // --tls-cert-file
    tls_key_file: Option<PathBuf>,         // --tls-key-file
    cluster_enabled: Option<String>,       // --cluster-enabled yes|true|1
}
```

This is the exact same flag set as the previous revision of this document described — **no new CLI
flags were added** despite `main.rs` growing from 235 to 278 lines. The growth is entirely new
*logic* inside `main`, not new surface area: the requirepass/ACL block (§2.3) and the
`get_process_affinity_cores` helper (§2.2) account for it.

Every CLI value is `Option` and is merged on top of a `RudisConfig` (`src/config.rs`) that was
itself either loaded from a `redis.conf`-style file (`-c/--config`) or defaulted
(`RudisConfig::default()`). `RudisConfig`'s fields, verified against `src/config.rs`:

```rust
pub struct RudisConfig {
    pub bind: String,                          // default "127.0.0.1"
    pub port: u16,                              // default 6379
    pub threads: Option<usize>,
    pub maxclients: usize,                      // default 10000
    pub maxmemory: Option<String>,
    pub maxmemory_bytes: Option<u64>,
    pub maxmemory_policy: String,                // default "noeviction"
    pub appendonly: bool,
    pub dir: PathBuf,                            // default "."
    pub requirepass: Option<String>,
    pub tls_port: Option<u16>,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    pub cluster_enabled: bool,
    pub tiered_offload_threshold: u64,           // default 60
    pub tiered_upload_threshold: u64,             // default 80
    pub extra_directives: HashMap<String, String>,
}
```

`RudisConfig::merge_cli` takes 11 positional `Option<T>` arguments (port, threads, aof, aof_dir,
maxmemory, tiered_offload_threshold, tiered_upload_threshold, tls_port, tls_cert_file,
tls_key_file, cluster_enabled) and overwrites the corresponding field only when `Some`. There is
still no `appendfsync`-style directive — AOF fsync cadence remains hardcoded (see §3.1 and
Component 14).

### 2.2 Core-affinity discovery: `get_process_affinity_cores` (main.rs:66-87)

**This is a real correction to a previous revision of this document**, which stated `num_shards`
derives from `core_affinity::get_core_ids()`. The current code instead calls a dedicated helper
that is cgroup/taskset-aware:

```rust
fn get_process_affinity_cores() -> Vec<usize> {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) == 0 {
            let cores: Vec<usize> = (0..libc::CPU_SETSIZE as usize)
                .filter(|&i| libc::CPU_ISSET(i, &set))
                .collect();
            if !cores.is_empty() { return cores; }
        }
    }
    core_affinity::get_core_ids().unwrap_or_default().into_iter().map(|c| c.id).collect()
}
```

On Linux, it calls `sched_getaffinity(0, ...)` directly and returns the exact set of CPU indices
the *process* is currently allowed to run on — this correctly narrows to whatever a container
cgroup, `taskset`, or `numactl` restriction has imposed, which `core_affinity::get_core_ids()`
alone does not reliably do (that crate enumerates all online CPUs, not the process's actual
affinity mask). Only if the syscall fails or returns an empty set does it fall back to
`core_affinity::get_core_ids()`.

`num_cores` (main.rs:156-162) is then `allowed_cores.len()` if non-empty, else
`std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)` — a second fallback layer
beyond `get_process_affinity_cores`'s own internal one.

`num_shards = server_config.threads.unwrap_or_else(|| num_cores.min(8))` — **unchanged**: still
capped at 8 by default absent an explicit `--threads`/`threads` override, so a large shared host
does not silently spawn one `io_uring` ring and listener per core with no flags given.

**Per-shard pinning** (main.rs:246-252) uses `allowed_cores[shard_id]` directly (not a second call
to `core_affinity::get_core_ids()`), and only when `!args.no_pin && shard_id < allowed_cores.len()`;
otherwise `core_id` is `None` and that shard's thread is left unpinned.

### 2.3 requirepass → default-user ACL priming (main.rs:137-153, new since the prior doc revision)

```rust
if let Some(ref pass) = server_config.requirepass {
    let acl = rudis::acl::get_acl_for_port(port);
    let mut acl_guard = acl.write().unwrap();
    if let Some(user) = acl_guard.get_user_mut("default") {
        user.passwords.clear();
        user.password_hashes.clear();
        if !pass.is_empty() {
            user.passwords.push(pass.clone());
            let h = rudis::acl::hash_password_sha256(pass);
            user.password_hashes.push(h);
            user.nopass = false;
            rudis::acl::HAS_CUSTOM_ACL.store(true, Ordering::Release);
        } else {
            user.nopass = true;
        }
    }
}
```

If `requirepass` is set (from config file or, indirectly, CLI), `main.rs` reaches into the
process-wide, port-keyed ACL registry (`crate::acl::get_acl_for_port`, an `RwLock`) *before any
shard thread starts* and mutates the `default` user directly: clears any existing
plaintext/hashed passwords, pushes the new plaintext password and its salted SHA-256 hash
(`crate::acl::hash_password_sha256`), clears `nopass`, and flips the global
`HAS_CUSTOM_ACL` atomic so the rest of the server knows ACL enforcement is now non-trivial. An
empty `requirepass` string instead sets `nopass = true` (password requirement lifted). This is the
*only* startup-time ACL mutation `main.rs` performs; everything else about ACL enforcement is
Component 15's domain.

### 2.4 `run_shard_worker`'s signature (unchanged)

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

Every shard gets a clone of the full sender vector (so it can reach any other shard) but only its
own receiver, plus its own clone of the optional `tls_config` and `aof_config`. The
`(ShardSender, ShardReceiver)` mesh is built once by `mailbox::create_shard_mesh(num_shards)`
(§2.5), not per-shard.

### 2.5 The cross-shard mesh: `mailbox::create_shard_mesh` (src/mailbox.rs:627+)

Concrete shape, verified against `src/mailbox.rs`:

- Allocates an **N×N matrix of `SpscQueue<ShardMessage>` rings** (`rings[i][j]` = the
  single-producer/single-consumer ring carrying messages from shard `i` to shard `j`), each ring
  constructed with capacity **256** (`SpscQueue::new(256)`).
- Allocates one `flume::bounded::<()>(1)` "doorbell" channel per shard and one
  `Arc<CachePadded<AtomicBool>>` "sleeping" flag per shard.
- `ShardSender { target_shard, ring: Arc<SpscQueue<ShardMessage>>, target_notify: flume::Sender<()>, target_sleeping: Arc<CachePadded<AtomicBool>> }`:
  `send()` pushes onto the ring unconditionally, then only `try_send(())` on the doorbell if the
  target's `sleeping` flag is currently `true` — a classic low-wakeup-overhead pattern: the common
  case (target already awake and polling) costs one lock-free push and one atomic load, no channel
  send at all.
- `ShardReceiver { shard_id, incoming_rings: Vec<Arc<SpscQueue<ShardMessage>>>, notify_rx, sleeping }`:
  `try_recv()` linearly scans every incoming ring (one per peer shard) and pops the first
  non-empty one — an O(num_shards) scan per drain attempt, not O(1). `recv_async()` (used by
  `run_shard_worker`, §3.2) is a double-checked sleep loop: try once, set `sleeping = true`, try
  again (closing the race where a sender checked `sleeping` as `false` just before this), then
  `.await` the doorbell, drain any queued doorbell pings, and try once more.

This construction happens **once**, entirely inside `main.rs`'s call to `create_shard_mesh`, before
any shard thread is spawned; `run_shard_worker` itself never constructs channels.

---

## 3. Execution Algorithms (`run_shard_worker`, `src/server.rs`)

### 3.1 Startup sequence inside `run_shard_worker`

In order (line numbers refer to `src/server.rs`):

1. **(58-60)** Pins to its core via `core_affinity::set_for_current`, if `core_id` is `Some`.
2. **(62)** Seeds thread-local `ADOPTED_CLIENT_SEQ` (used only for connections handed off from
   another shard, §3.4) to `(shard_id << 48) | (1 << 47) | 1` — bit 47 set, so ids minted here can
   never collide with this shard's own plain-accept or TLS-accept id ranges.
3. **(64-67)** Builds `monoio::RuntimeBuilder::<monoio::FusionDriver>::new().enable_timer()`.
   `FusionDriver`, not a hardcoded `IoUringDriver` — it selects an `io_uring`-backed driver where
   available and falls back to a legacy poll-based driver otherwise.
4. **(69-2047)** Inside `rt.block_on(async move { ... })`:
   - **(70-75)** Computes `shard_port`: `base_port + shard_id` if `cluster_enabled`, else
     `base_port` (every shard shares the same port via `SO_REUSEPORT` otherwise).
   - **(77-99)** Plain TCP listener: `Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))`,
     `set_reuse_port(true)`, `set_reuse_address(true)`, `set_nonblocking(true)`,
     `set_recv_buffer_size(512*1024)` / `set_send_buffer_size(512*1024)`, `bind`, `listen(4096)`,
     converted into a `monoio::net::TcpListener`.
   - **(101-127)** **If `tls_config` is `Some`**, an independent, byte-for-byte identical socket
     setup sequence (only the bound port differs) produces a second `Option<TcpListener>`
     (`tls_listener`).
   - **(130)** `let local_db = Rc::new(RefCell::new(ShardDb::new(port).with_shard(shard_id)))`.
   - **(132-148) RDB restore**, only if AOF is *not* enabled: if `<aof_dir>/dump.rdb` exists,
     `crate::table::load_rdb(&rdb_path, &mut local_db.borrow_mut(), shard_id, num_shards)` — every
     shard parses the **entire** RDB file independently and keeps only the keys that hash to itself
     (see Component 05).
   - **(150-203) AOF replay + writer**, only if AOF is enabled: `crate::aof::replay_aof` against
     `<aof_dir>/appendonly-<shard_id>.aof`, then `AofWriter::open`. If opened, spawns a dedicated
     `monoio::spawn` task that wakes **every 5ms** (`monoio::time::sleep(Duration::from_millis(5))`),
     flushes any pending write chunk (`take_flush_chunk` → `write_all_at`), recycles the drained
     buffer (`recycle_chunk`), and — when `fsync_every_sec` is set (always `true` today, see §2) —
     `sync_data()`s (fsync) every **200th** tick, i.e. roughly once per second.
   - **(205-208) Cluster bus**: only `shard_id == 0` calls `crate::cluster::start_cluster_bus(port)`
     — a single, once-per-process background listener.
   - **(210-221) Tiered storage**: opens a `crate::tiering::ShardTierManager` for this shard/port
     (directory from `RUDIS_TIER_DIR` env var, else a per-port temp directory under
     `std::env::temp_dir()`) and, on success, attaches it to `local_db.borrow_mut().tier_manager`.
   - **(223-241)** Builds `client_registry` (`Rc<RefCell<HashMap<u64, ClientInfo>>>`), `pubsub`
     (`Rc<RefCell<PubSubHub>>`), and the shard's `Router` via `Router::new(shard_id, num_shards,
     shard_port, local_db.clone(), senders, aof_writer.clone(), pubsub.clone(), aof_config.dir.clone())`,
     then sets `r.base_port = base_port; r.cluster_enabled = cluster_enabled;` explicitly after
     construction. **Line 241, new since the previous revision of this document**:
     `crate::connection::set_current_router(router.clone())` — populates a thread-local
     (`CURRENT_ROUTER: RefCell<Option<Rc<Router>>>` in `src/connection.rs`) so that code deep in the
     call stack (notably `notify_keyspace_event`, used for keyspace-notification pub/sub) can reach
     the shard's `Router` without it being threaded explicitly through every function signature.
     This is set once per shard, at startup, and never changed again.
   - **(243-268)** Spawns three independent periodic `monoio::spawn` tasks, all sharing the same
     `Rc<RefCell<ShardDb>>`/`Router` with no synchronization:
     - every **100ms**: `active_db.borrow_mut().active_expire_cycle()`
     - every **20ms**: `offload_router.check_auto_tier().await`
     - every **2s**: `gc_router.gc_local()`
   - **(270-314)** Spawns an **AF_XDP kernel-bypass ingress loop** (`crate::xdp`, Component 10's
     domain): polls a zero-copy Rx ring via `xsk_socket.rx_burst(&mut frames, 32)`; for
     `Pass`/`Redirect` actions, parses the extracted command and either executes it locally
     (`execute_local_command`) or forwards it to the owning shard (`execute_remote`), sleeping
     **5ms** when nothing is pending. Runs unconditionally per shard; the underlying engine no-ops
     if no AF_XDP socket is actually attached to the interface.
   - **(316-1853)** Spawns the **cross-shard receiver task** (§3.2).
   - **(1856-1859)** Prints the shard's startup banner.
   - **(1861-1926)** Spawns the **TLS accept loop**, if configured, then **(1929-2027)** enters the
     **plain accept loop** — both run until shutdown (§3.3, §3.4).
5. **(2029-2046)** After the plain accept loop returns (shutdown), flushes and `fsync`s the AOF
   writer if one is open, then `block_on` returns and the thread's closure ends;
   `main.rs`'s `handle.join()` for this shard then unblocks.

### 3.2 The cross-shard receiver loop: burst-draining `ShardMessage` dispatch

```rust
monoio::spawn(async move {
    while let Ok(mut msg) = rx.recv_async().await {
        let mut burst = 0;
        loop {
            match msg { /* ~66 variants */ }
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

The outer `recv_async().await` suspends the task when the mailbox is empty (per the
double-checked-sleep protocol in §2.5); once woken, it loops on the cheap, non-async `try_recv()`
to drain up to **64** already-queued messages inline before yielding back. This amortizes async
wakeup/poll overhead under sustained cross-shard load (fan-out `MGET`/`MSET`, pipeline squashing
from many peers hitting one shard at once).

**`ShardMessage` now has 66 variants** (verified by direct enumeration of `src/shard.rs`'s
`pub enum ShardMessage`, up from "60+" in the previous revision of this document), grouped by
purpose:

| Category | Variants |
| :--- | :--- |
| Key/value ops | `Get`, `Set`, `Del`, `Delex`, `DelKeys`, `Exists`, `ExpireTime`, `IncrBy`, `Expire`, `Persist`, `Ttl`, `Keys`, `Scan`, `RandomKey`, `ActiveDefrag` |
| Batched/scatter-gather fan-out | `Batch`, `Mget`, `Mset`, `ScatterMget`, `ScatterMset`, `FastGet`, `FastSet`, `JsonMget` |
| Cluster/slot control | `CountKeysInSlot`, `GetKeysInSlot`, `SetSlotState`, `SetSlotOwner`, `FlushSlots`, `Stick`, `Unstick`, `IsSticky` |
| Persistence | `SaveRdbChunk`, `RestoreRdbChunk`, `SyncAof`, `RewriteAof`, `DumpKey` |
| Replication apply | `ExecuteReplicaCmd` |
| Pub/Sub (global + sharded, Redis 7) | `Publish`, `PubsubChannels`, `PubsubNumsub`, `PubsubNumpat`, `Spublish`, `Ssubscribe`, `Sunsubscribe`, `PubsubShardchannels`, `PubsubShardnumsub`, `RemoveClientPubSub` |
| Blocking-op wakeups | `NotifyList` |
| Distributed transaction locking | `AcquireTxLock`, `ReleaseTxLock` |
| NVMe tiering control | `TierSpill`, `TierLoad`, `TierSpillAll`, `TierCool`, `TierDecommit`, `TierGc`, `TierSnapshot`, `StreamColdRead`, `GetUsedMemory` |
| Full-text search | `InitSearchIndex`, `DropSearchIndex`, `SearchQuery` |
| Admin/stats | `ClientList`, `FlushCommandStats`, `ResetCommandStats` |
| Connection handoff | `AdoptConnection` (§3.4) |

`RemoveClientPubSub`, `Spublish`/`Ssubscribe`/`Sunsubscribe`/`PubsubShardchannels`/`PubsubShardnumsub`
(sharded pub/sub), `Scan`/`Keys`/`RandomKey`/`ExpireTime`/`Delex`, and
`InitSearchIndex`/`DropSearchIndex`/`SearchQuery` did not exist in the previous revision of this
document's enumeration.

#### `ShardMessage::Batch` — precise mechanics, including a dead-code finding

`ShardMessage::Batch { items, responder, is_resp3 }` is the pipeline-squashing primitive
(`connection.rs` sends one `Batch` per remote shard per pipeline flush). Its handler
(`src/server.rs:619-1325`) reads:

```rust
let has_tier_manager = cross_shard_db.borrow().tier_manager.is_some();
let needs_async = false;                       // <-- hardcoded, never reassigned
if needs_async {
    // ~340 lines (627-966): a duplicate fast-path dispatcher (Get/Set/IncrBy/
    // Exists/Del/Hget/Hset/Sismember/Sadd/Lpush/Lpop/Rpop/Lrange/Zrange) that
    // spawns an async task and can .await a cold-tier read per GET.
    // UNREACHABLE — `needs_async` is a `let`-bound constant `false` with no
    // assignment anywhere else in the file. This entire branch is dead code.
} else {
    // The branch that always runs (967-1325):
    let mut cold_gets: SmallVec<[(usize, Bytes); 8]> = SmallVec::new();
    for (idx, key_hash, cmd) in items.drain(..) {
        // same command set as the dead branch (Get/Set/IncrBy/Exists/Del/Hget/
        // Hset/Sismember/Sadd/Lpush/Lpop/Rpop/Lrange/Zrange/Zadd), handled
        // synchronously via *_with_hash fast paths, with `execute_local_command`
        // as the generic fallback for anything not special-cased;
        // a tiered GET miss is pushed onto `cold_gets` instead of resolved inline.
    }
    if cold_gets.is_empty() {
        responder.finish(items);               // fully synchronous path
    } else {
        monoio::spawn(async move {
            // resolves only the deferred cold GETs via stream_cold_read_local,
            // then responder.finish(items)
        });
    }
}
```

So the accurate behavior is: **every** `Batch` message runs its fast-path command dispatch
synchronously, item by item, in the calling task; only keys that miss the in-memory table *and*
are confirmed tiered are deferred, and even then only those specific slots are resolved by a single
follow-up `monoio::spawn` task after the synchronous loop finishes — not "the whole batch" moving
to an async task. See §6 for why the dead `if needs_async { ... }` branch matters.

### 3.3 The accept loop(s): plain TCP, optional TLS, and connection rebalancing

```rust
let mut next_client_id: u64 = ((shard_id as u64) << 48) + 1;
loop {
    if crate::shutdown::is_shutting_down() { break; }
    let accept_res = match monoio::time::timeout(Duration::from_millis(200), listener.accept()).await {
        Ok(res) => res,
        Err(_) => continue, // 200ms timeout elapsed with no connection; re-check the shutdown flag
    };
    match accept_res {
        Ok((stream, client_addr)) => {
            let _ = stream.set_nodelay(true);
            // also sets TCP_NODELAY and TCP_QUICKACK directly via setsockopt (server.rs:1946-1963)
            let client_id = next_client_id; next_client_id += 1;
            let owner = if cluster_enabled {
                crate::conn_balance::register_conn(shard_id);
                shard_id
            } else {
                crate::conn_balance::claim_owner(shard_id, num_shards)
            };
            if owner != shard_id {
                let fd = std::os::unix::io::IntoRawFd::into_raw_fd(stream);
                let handed = router.senders[owner].send(ShardMessage::AdoptConnection { fd, peer: client_addr }).is_ok();
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
                    if let Err(e) = res { crate::connection::inc_isolated_panics(); tracing::error!(...); }
                });
            }
        }
        Err(e) => eprintln!("[Shard {}] Accept error: {}", shard_id, e),
    }
}
```

Client IDs encode the *owning* shard in the top 16 bits (`(shard_id << 48) + counter`), so IDs
never collide across shards.

**Connection rebalancing algorithm** (`src/conn_balance.rs`, verified line-for-line against source):
`SO_REUSEPORT` assigns a new connection to a shard by hashing its 4-tuple; for loopback clients
only the ephemeral source port varies, so this is effectively a random ball-into-bins draw,
re-rolled per reconnect. The module's own doc comment gives **measured** numbers for this: *2.74M
ops/s at 64 connections versus 5.44M ops/s at 256 connections* on an otherwise identical
configuration — 64 connections over 16 shards puts ~8 on the busiest shard and 1-2 on others, and
since throughput is gated by the most-loaded shard the server runs at ~51.9% of ideal capacity
(matching a Monte-Carlo prediction). Deferring `accept()` on an overloaded shard does **not** help:
`SO_REUSEPORT` commits a connection to a specific listener's accept queue at SYN time.

The fix is explicit, not kernel-level:
- `CONN_COUNTS: [AtomicUsize; 256]` (`MAX_TRACKED_SHARDS = 256`) is a process-global per-shard
  connection census, touched exactly twice per connection (accept, close) — never on the
  per-command hot path.
- `claim_owner(accepting_shard, num_shards)` scans for the least-loaded shard
  (`least_loaded`, an O(num_shards) linear scan starting from the accepting shard's own count) and,
  if it differs from the accepting shard, **reserves the slot atomically** via a single
  `compare_exchange(best_count, best_count + 1, Relaxed, Relaxed)` against the count observed
  during the scan — so two shards racing for the same target cannot both win; the loser retries
  the scan. It retries up to **`MAX_CLAIM_RETRIES = 4`** times before giving up and keeping the
  connection locally (a bounded retry loop specifically to avoid livelocking the accept path under
  a hot census). The reservation happens *before* the fd is handed off, specifically so a burst of
  concurrent accepts across shards cannot all independently pick the same "idle-looking" target
  (a bug the module's own test suite — `test_claim_owner_reservation_is_visible_immediately` —
  exists explicitly to pin down).
- If `owner != shard_id`, the raw fd is released via `into_raw_fd` (no `Drop`, so the fd stays
  open) and handed to the target shard as `ShardMessage::AdoptConnection { fd, peer }` — valid
  without `SCM_RIGHTS` because all shards are threads in one process sharing a single fd table. The
  receiving shard's cross-shard receiver loop reconstructs a `monoio::net::TcpStream`
  (`from_raw_fd`/`from_std`) and spawns `handle_connection` on it, **deliberately skipping**
  `register_conn` on arrival — the sending shard already reserved the census slot, so double-
  registering would make the target look busier than reality for the whole in-flight window.
- **Cluster mode is exempt**: each shard binds its own port there (`register_conn` is still called,
  but `claim_owner`/handoff never runs), so distribution is client-chosen and rebalancing would
  break `MOVED` semantics for no benefit.

**TLS accept loop** (`src/server.rs:1861-1926`): structurally near-identical to the plain loop
(same 200ms shutdown-poll timeout via `monoio::time::timeout`), with client IDs carved from the
*upper* half of the shard's 48-bit ID space (`(shard_id << 48) | 0x8000_0000_0000` plus an
incrementing counter). Each accepted connection is upgraded via `crate::tls::TlsSession::new` +
`session.handshake_monoio` (a real `rustls` handshake driven over the `monoio` stream) before being
handed to `crate::connection::handle_tls_connection` — a distinct entry point from the plain loop's
`handle_connection`. Both accept and post-handshake work are wrapped in `catch_unwind_async`.

**Verified gap: TLS connections never touch `conn_balance` at all.** Reading `src/server.rs:1861-1926`
line-by-line shows no call to `conn_balance::register_conn`, `claim_owner`, or `unregister_conn`
anywhere in the TLS accept path — unlike the plain-TCP path's explicit calls at lines 1981-1991 and
2015. This means: (1) TLS connections are never rebalanced off an overloaded shard, even though
they suffer from exactly the same `SO_REUSEPORT` hashing skew the plain path works around; and
(2) because TLS connections never increment `CONN_COUNTS`, the plain-TCP accept loop's
least-loaded scan is *blind to TLS load* on every shard — a shard holding hundreds of TLS
connections and zero plain ones looks perfectly idle to `claim_owner` and will keep absorbing
plain-TCP handoffs. See §6.

### 3.4 Shutdown

Both accept loops poll `crate::shutdown::is_shutting_down()` once per iteration and break when
`true`, discovering the flag within ~200ms of `SIGINT`/`SIGTERM` (enforced by wrapping
`listener.accept()`/`tls_listener.accept()` in `monoio::time::timeout(Duration::from_millis(200), ...)`).
After the **plain** accept loop returns, if this shard opened an `AofWriter`, its pending write
buffer is flushed (`write_all_at`) and `fsync`'d (`sync_data()`) before the shard's `async move`
block completes (`src/server.rs:2029-2046`). The shard's `block_on` then returns, the thread's
closure ends, and `main.rs`'s `handle.join()` for that shard unblocks. Once every shard thread has
joined, `main` prints `"rudis server gracefully stopped. Goodbye!"`.

**This is real but partial — re-verified, still true**: the cross-shard receiver task, the three
periodic maintenance tasks, the AF_XDP ingress loop, and the TLS accept loop are not explicitly
cancelled; they stop only because dropping the runtime drops every task still scheduled on it.
Already-accepted client connections are **not** drained or given a chance to finish an in-flight
request — they end the same abrupt way. **No `SAVE`/`BGSAVE` is triggered automatically on
shutdown.** Only the AOF writer (if any) gets a final flush+fsync; an RDB-only deployment
(`appendonly no`) loses any writes since the last manual/scheduled `SAVE`/`BGSAVE` on a clean
`SIGTERM`.

`shutdown.rs` itself (76 lines, unchanged) is a single `static SHUTDOWN_REQUESTED: AtomicBool`
behind `is_shutting_down`/`request_shutdown`/`reset_shutdown` (test-only), plus
`install_signal_handlers()` which does exactly three things: `libc::signal(SIGPIPE, SIG_IGN)`, and
installs `handle_signal` (which just calls `request_shutdown()`) for both `SIGINT` and `SIGTERM`.
There is no signal-safety concern beyond the bare `AtomicBool` store — no allocation, no logging,
no async machinery runs inside the signal handler itself.

### 3.5 `catch_unwind_async` / `CatchUnwind` (panic isolation)

```rust
pub struct CatchUnwind<F> { inner: F }
pub fn catch_unwind_async<F: Future>(f: F) -> CatchUnwind<F> { CatchUnwind { inner: f } }
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

A hand-rolled `Future` combinator (unchanged, `src/server.rs:12-31`): every `poll` call is wrapped
in `std::panic::catch_unwind`, so a panic inside the wrapped future becomes an `Err` instead of an
unwind that would otherwise propagate through `monoio::spawn` and — because the runtime is
single-threaded per shard — potentially take down every other task and connection sharing that
shard's runtime. Every accept path (plain, TLS, and cross-shard-adopted) wraps its call to
`handle_connection`/`handle_tls_connection` in this combinator and, on `Err`, calls
`crate::connection::inc_isolated_panics()` and logs via `tracing::error!` instead of propagating.

---

## 4. Router construction and its object pools (context for §3.1)

`Router::new` (`src/router.rs:186+`) takes 8 arguments (`shard_id, num_shards, port, local_db,
senders, aof, pubsub, db_dir`) — unchanged in shape from the previous revision — but the `Router`
struct itself has grown substantially and now carries several `Rc<RefCell<Vec<...>>>` **object
pools** used to avoid per-request heap allocation on the cross-shard scatter-gather path:
`notify_channel_pool`, `remote_responder_pool`, `mget_batch_pool`, `mset_batch_pool`,
`mget_desc_pool`, `mset_desc_pool`, `pubsub_responder_pool`. It also carries
`presence_table: Arc<ShardedPresenceTable>` and `tier_stats: Arc<TieringStats>` — genuinely shared
`Arc<Atomic...>`-backed state (not `Arc<Mutex<...>>`), consistent with Gotcha 1 below: these are
cross-shard *counters/flags*, not a shared mutable `ShardDb`. Full field-by-field treatment of
`Router` and these pools belongs to Component 04; this document only needs to establish that
`run_shard_worker` constructs exactly one `Router` per shard and everything downstream (accept
loops, cross-shard receiver, periodic tasks, XDP loop) shares that single `Rc<Router>`.

---

## 5. `agent.rs` / `mcp.rs`: verified to have **no** special startup wiring

The task of checking whether the new `src/agent.rs` (802 lines) and `src/mcp.rs` (748 lines)
modules need wiring into server startup was investigated directly, by grepping every
`agent::`/`mcp::` reference across the codebase. **Answer: no wiring exists, and none is needed.**

- `src/lib.rs` declares `pub mod agent;` and `pub mod mcp;` — that is the entire extent of their
  presence in the module tree from a startup perspective. Neither is referenced anywhere in
  `src/main.rs` or `src/server.rs`.
- **Agent memory state lives directly on `ShardDb`** (`src/shard.rs:606-620`):
  ```rust
  pub struct ShardDb {
      // ...
      pub agent_memories: hashbrown::HashMap<Bytes, crate::agent::AgentMemorySession>,
      pub llm_quotas: hashbrown::HashMap<Bytes, crate::agent::LlmQuotaBucket>,
      pub agent_checkpoints: hashbrown::HashMap<Bytes, crate::agent::AgentCheckpointThread>,
      pub agent_tools: hashbrown::HashMap<Bytes, crate::agent::AgentToolRegistry>,
      // ...
  }
  ```
  These four maps are ordinary, empty-initialized `HashMap` fields, populated lazily the same way
  `ShardDb::new(port)` (called once per shard at `server.rs:130`, §3.1 step 2) initializes every
  other per-shard collection. There is no separate constructor call, no separate spawn task, and
  no config knob for agent memory — it rides along for free inside the existing per-shard restore
  path, and (like everything else on `ShardDb`) is per-shard, in-memory, and **not** restored from
  RDB/AOF on its own (no `agent_memories`/`agent_checkpoints` handling was found in the RDB/AOF
  read paths reached from `server.rs`; persistence for this state, if any, is Component 20's
  concern, not something `run_shard_worker` sets up).
- **Commands are routed exactly like every other Redis command** — through `src/resp.rs`'s parser
  and the normal per-connection `execute_command` dispatch in `src/connection.rs`, with no separate
  listener, port, or protocol:
  - `AGENT.MEM.ADD`, `AGENT.MEM.CONTEXT`, `AGENT.MEM.COMPACT`, `AGENT.MEM.INFO`, `AGENT.MEM.CLEAR`,
    `AGENT.CHECKPOINT.PUT`, `AGENT.CHECKPOINT.GET`, `AGENT.CHECKPOINT.HISTORY`,
    `AGENT.TOOL.CLAIM`, `AGENT.TOOL.COMPLETE` are parsed as ordinary RESP command names in
    `src/resp.rs` (~line 12442+).
  - `MCP.TOOLS`, `MCP.CALL`, `MCP.RPC` are likewise parsed as RESP commands (`src/resp.rs:12949-12962`).
    Their handlers (`src/connection.rs:5743-5880`) are pure in-process glue:
    `Command::McpCall` resolves a tool name + JSON args to a *regular* `Command` via
    `crate::mcp::plan_tool_command`, then recursively calls `execute_command(planned, router, ...)`
    — the exact same dispatch function every other command goes through — and reformats the
    sub-result as MCP JSON. `Command::McpRpc` implements JSON-RPC 2.0 `initialize`/`ping`/
    `tools/list`/`tools/call` by the same mechanism. There is no standalone MCP server process,
    port, or task anywhere in `main.rs`/`server.rs` — "the MCP server" is three RESP commands
    served by the same per-connection task as `GET`/`SET`.

Net effect: a reader auditing `main.rs`/`server.rs` for AI-native runtime wiring will correctly
find nothing, because there is nothing there by design — the integration point is the command
dispatcher, not the reactor.

---

## 6. Known Bugs & Limitations (verified by direct reading, this pass)

1. **New — `ShardMessage::Batch`'s async fast-path branch is unreachable dead code.**
   `src/server.rs:625`: `let needs_async = false;` is never reassigned, so the
   `if needs_async { ... }` block spanning roughly `src/server.rs:627-966` (~340 lines, a full
   duplicate of the fast-path command dispatcher, including a second, subtly different cold-tier
   `.await` strategy) can never execute. It is safe to delete, and anyone extending "the async
   Batch path" should be aware they are editing dead code unless they also flip the condition —
   search for `needs_async` before assuming either branch is "the" implementation. See §3.2 for
   the precise mechanics of the branch that actually runs.
2. **New — TLS connections are invisible to `conn_balance`.** The TLS accept loop
   (`src/server.rs:1861-1926`) never calls `register_conn`/`claim_owner`/`unregister_conn`. TLS
   connections are therefore never rebalanced off an overloaded shard, and — because they don't
   increment `CONN_COUNTS` — their presence silently skews the plain-TCP accept loop's
   least-loaded-shard calculation, making a TLS-heavy shard look artificially idle. See §3.3.
3. **Re-verified, still true — graceful shutdown does not drain in-flight work.** §3.4: accept
   loops stop and AOF is flushed/fsync'd, but in-flight client requests, an in-progress
   `BGSAVE`/`BGREWRITEAOF`, and the cross-shard/periodic/XDP/TLS-accept tasks are all simply
   dropped with the runtime rather than told to finish or cancel cleanly. No automatic
   `SAVE`/`BGSAVE` runs on shutdown.
4. **Re-verified, still true — static shard/slot model.** `run_shard_worker` assigns shards
   statically at startup and never changes `num_shards`; Components 04/11 carry live-migration
   machinery that is only partially wired to this static model.
5. **Re-verified, still true — no jitter on periodic tasks.** The 100ms/20ms/2s/5ms periodic tasks
   (§3.1) have no jitter or adaptive backoff; on a host running many shards they tend to tick in
   near-lockstep, a minor but avoidable source of correlated CPU bursts.
6. **Re-verified, still true — duplicated plain/TLS socket setup.** `src/server.rs:101-127`'s TLS
   listener setup is a near-verbatim copy of the plain listener's at lines 77-99, differing only in
   the bound port.
7. **Re-verified, still true — `SO_REUSEPORT` imbalance is mitigated, not eliminated.**
   `conn_balance` corrects for it after the fact, per-connection (§3.3); it does not change the
   kernel's initial hash-based assignment, is bounded to 4 retries before giving up
   (`MAX_CLAIM_RETRIES`), tracks at most 256 shards (`MAX_TRACKED_SHARDS`), and — per finding 2
   above — now has a blind spot for TLS load specifically.

---

## 7. Cross-Component Interactions

- **`src/connection.rs`**: every accepted socket becomes `handle_connection(...)` or
  `handle_tls_connection(...)`, spawned as a task on this shard's runtime, panic-isolated per §3.5.
  `CURRENT_ROUTER`/`set_current_router` (§3.1) also lives here. See Component 02.
- **`src/router.rs`**: `Router` is constructed once per shard (§4) and shared (`Rc`) with every
  connection task; owns the `senders` mesh, slot ownership state, the AOF writer handle, `db_dir`,
  `base_port`, `cluster_enabled`, and the object pools described in §4. See Component 04.
- **`src/mailbox.rs`**: `create_shard_mesh` (§2.5) builds the N×N SPSC ring matrix consumed by
  `main.rs`; `ShardSender`/`ShardReceiver` are the types `run_shard_worker` is handed. See
  Component 04.
- **`src/conn_balance.rs`**: the connection-count rebalancing logic behind §3.3's `AdoptConnection`
  handoff — a pure-atomics module with no async/thread-per-core concerns of its own.
- **`src/aof.rs`**: AOF replay on startup, the 5ms flush / ~1s fsync task, and the final
  flush+fsync on shutdown, all driven from here.
- **`src/tiering.rs`**: `ShardTierManager::open`, the 20ms auto-tier check, the 2s GC task, and the
  `Tier*`/`StreamColdRead` message variants.
- **`src/cluster.rs`**: `start_cluster_bus(port)` (shard 0 only); `cluster_enabled` changes the
  socket-binding strategy (§3.1) and the connection-rebalancing exemption (§3.3).
- **`src/block.rs`**: `get_block_hub_for_port(port)` — the one place this subsystem reaches for a
  real, shared mutex instead of thread-local state.
- **`src/pubsub.rs`**: per-shard `PubSubHub`, reachable cross-shard via `ShardMessage::Publish`/
  `PubsubChannels`/`PubsubNumsub`/`PubsubNumpat`/`Spublish`/`Ssubscribe`/`Sunsubscribe`/
  `PubsubShardchannels`/`PubsubShardnumsub`.
- **`src/tls.rs`**: `TlsWorkerConfig`, `TlsSession::new`/`handshake_monoio`, and cert
  loading/self-signed generation, wired up from `main.rs` when a TLS port is configured.
- **`src/xdp.rs`**: the per-shard AF_XDP ingress loop described in §3.1 (Component 10 owns the
  packet-processing detail).
- **`src/shutdown.rs`**: the `AtomicBool` flag and signal handlers behind §3.4.
- **`src/config.rs`**: `RudisConfig` — the file/CLI-merged configuration consumed by `main.rs`
  (§2.1).
- **`src/acl.rs`**: `get_acl_for_port`, `hash_password_sha256`, `HAS_CUSTOM_ACL` — the requirepass
  priming main.rs performs before any shard starts (§2.3). See Component 15.
- **`src/syscheck.rs`** / **`src/telemetry.rs`**: startup sanity-check reporting and
  `tracing`-based structured logging initialization, both invoked from `main.rs` before any shard
  thread is spawned.
- **`src/agent.rs`** / **`src/mcp.rs`**: deliberately **not** referenced from `main.rs`/`server.rs`
  — see §5.

---

## 8. Startup Sanity Checks (`src/syscheck.rs`, 209 lines, in full)

`main.rs` calls `rudis::syscheck::run_system_sanity_checks()` (line 233) after building the banner
strings but before printing the closing `====` line, then `print_sanity_warnings(&report)` prints
any accumulated warnings. `run_system_sanity_checks()` is a thin wrapper around
`run_sanity_checks_with_paths` with the real `/proc`/`/sys` paths hardcoded (the path-parameterized
version exists purely so tests can point it at temp files). It runs exactly four checks, each
independently optional (a missing file yields `None`, not a warning):

| Check | Source path | Threshold | Warning text (abridged) |
| :--- | :--- | :--- | :--- |
| `overcommit_memory` | `/proc/sys/vm/overcommit_memory` | must equal `1` | "background saves may fail under low memory conditions" + `sysctl vm.overcommit_memory=1` |
| `somaxconn` | `/proc/sys/net/core/somaxconn` | must be `>= 512` | "High connection rate may be throttled" + `sysctl -w net.core.somaxconn=4096` |
| max open files | `getrlimit(RLIMIT_NOFILE)` | must be `>= 10000` | "Connection limit may be restricted" + `ulimit -n 65536` |
| Transparent Huge Pages | `/sys/kernel/mm/transparent_hugepage/enabled` | must **not** contain the literal substring `"[always]"` | "will cause high latency spikes and memory bloat" + `echo never > .../enabled` |

`SystemSanityReport::is_optimal()` is simply `self.warnings.is_empty()`. Note this check is purely
advisory/observational — it runs *after* `main.rs` has already unconditionally called
`libc::prctl(PR_SET_THP_DISABLE, ...)` at process start (§2), so the THP warning here is about the
*system-wide* kernel default, not this process's own (already-disabled) THP behavior; a warning can
legitimately fire even though this specific `rudis` process is unaffected.

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Never introduce `Arc<Mutex<_>>` or any cross-thread handle to `ShardDb`. `ShardDb`
  is strictly `!Send` by construction (`Rc<RefCell<_>>`), and that is intentional. `Router`'s own
  `Arc<Atomic...>` fields (`is_saving`, `last_save_time`, `presence_table`, `tier_stats`, §4) are a
  different, acceptable pattern — shared atomics/counters, never a shared mutable `ShardDb`.
* **Gotcha 2**: Any new listening socket must set both `SO_REUSEPORT` and `SO_REUSEADDR`, matching
  the pattern in §3.1 for both the plain and TLS listeners — and if it should participate in load
  balancing, it must also call into `conn_balance` explicitly (the TLS listener currently does
  not — see §6 finding 2 — do not copy that omission into new code without a reason).
* **Gotcha 3**: Transparent Huge Pages are disabled once, in `main.rs`, before any shard thread
  starts (`#[cfg(target_os = "linux")] libc::prctl(...)`) — do not rely on per-shard THP handling.
* **Gotcha 4**: The cross-shard receiver loop drains up to 64 messages per wakeup via
  `rx.try_recv()`; a handler that blocks the async task for a long time (a synchronous,
  non-yielding loop) delays every other message already queued behind it in that burst.
* **Gotcha 5**: `is_shutting_down()` is polled inside the accept loops via a 200ms
  `monoio::time::timeout` around `accept()`, not via a dedicated cancellation future — a new
  long-lived per-shard loop added elsewhere will not automatically observe shutdown unless it
  adopts the same polling pattern (or an equivalent).
* **Gotcha 6**: Any new per-connection entry point should be wrapped in `catch_unwind_async`
  (§3.5) the same way `handle_connection`/`handle_tls_connection` are, or a panic in that path will
  take down the whole shard.
* **Gotcha 7 (new)**: Before assuming "the" implementation of a `ShardMessage::Batch`-style
  branching construct, check whether the condition that selects it is actually reachable —
  `needs_async` in `src/server.rs` is a compile-time-dead `false` (§6 finding 1). `cargo clippy`
  does not currently flag this (the branch is syntactically live code, just never selected by a
  `const`-foldable-but-not-`const` local), so it will not be caught automatically.
* **Gotcha 8 (new)**: `crate::connection::set_current_router` (called once per shard at startup,
  §3.1) populates a thread-local `CURRENT_ROUTER`. Code that calls `notify_keyspace_event` (or
  anything else that reads `CURRENT_ROUTER`) before this call has run on a given shard thread will
  silently no-op rather than panic — the read is `if let Some(router) = cr.borrow().as_ref()`, not
  an `unwrap()`. Keep this call early in `run_shard_worker` if you reorder the startup sequence.
* **Gotcha 9 (new)**: `agent.rs`/`mcp.rs` state and commands need **no** startup wiring (§5) — if
  you add a new `AGENT.*`/`MCP.*` command, add it to `src/resp.rs`'s parser and
  `src/connection.rs`'s dispatch exactly like any other Redis command; there is no separate
  initialization path in `main.rs`/`server.rs` to update.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
