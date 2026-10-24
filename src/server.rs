use smallvec::{SmallVec, smallvec};
use std::cell::RefCell;
use std::future::Future;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

pub struct CatchUnwind<F> {
    // Boxed so the wrapper is `Unpin` and polling needs no unsafe pin
    // projection; one allocation per connection or worker task.
    inner: Pin<Box<F>>,
}

pub fn catch_unwind_async<F: Future>(f: F) -> CatchUnwind<F> {
    CatchUnwind { inner: Box::pin(f) }
}

impl<F: Future> Future for CatchUnwind<F> {
    type Output = Result<F::Output, Box<dyn std::any::Any + Send + 'static>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.inner.as_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| inner.poll(cx))) {
            Ok(Poll::Ready(val)) => Poll::Ready(Ok(val)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

use crate::connection::{execute_local_command, handle_connection};
use crate::resp::Command;
use crate::router::Router;
use crate::shard::{ShardDb, ShardMessage};

thread_local! {
    /// Client id sequence for connections adopted from another shard.
    ///
    /// Seeded in `run_shard_worker` with bit 47 set so adopted ids can never
    /// collide with the `(shard_id << 48) + n` ids minted by this shard's own
    /// accept loop.
    static ADOPTED_CLIENT_SEQ: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Stack size for shard worker threads.
///
/// Shard threads run deeply nested async state machines plus the embedded Lua
/// VM; unoptimised (debug/coverage) builds overflow the 2 MiB std default.
/// Every spawner of `run_shard_worker` must use this so correctness does not
/// depend on the caller exporting `RUST_MIN_STACK`.
pub const SHARD_THREAD_STACK_SIZE: usize = 32 * 1024 * 1024;

/// Abort the whole process on an unrecoverable startup error (exit code 1),
/// like Redis does when it cannot load its data files. Serving an empty
/// dataset instead would let the next save overwrite the original files.
fn fatal_startup_error(shard_id: usize, msg: &str) -> ! {
    // One line, so it is the last one logged whichever shard fails last;
    // "Fatal error loading" is Redis's wording, which tooling looks for.
    crate::log_fatal!(
        "[Shard {}] FATAL: Fatal error loading the DB: {}. Refusing to start so existing data files are not overwritten. Exiting.",
        shard_id,
        msg
    );
    std::process::exit(1);
}

/// Startup loading of one server, summed over its shards so the summary
/// (Redis's "DB loaded from disk") is logged once, by the last shard.
struct StartupLoad {
    started: std::time::Instant,
    shards_done: usize,
    loaded: u64,
    /// Keys the RDB load dropped as already expired.
    expired: Vec<Vec<u8>>,
}

/// `rdb_last_load_keys_expired` of INFO persistence, by base port.
static LAST_LOAD_KEYS_EXPIRED: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<u16, u64>>,
> = std::sync::LazyLock::new(Default::default);

/// Keys dropped as expired by the last RDB load of the server on `port`.
pub fn rdb_last_load_keys_expired(port: u16) -> u64 {
    LAST_LOAD_KEYS_EXPIRED
        .lock()
        .get(&port)
        .copied()
        .unwrap_or(0)
}

static STARTUP_LOADS: std::sync::LazyLock<
    parking_lot::Mutex<std::collections::HashMap<(u16, bool), StartupLoad>>,
> = std::sync::LazyLock::new(Default::default);

/// Keys the last startup RDB load restored (INFO rdb_last_load_keys_loaded).
pub static LAST_LOAD_KEYS_LOADED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Records that a shard of the server on `base_port` finished loading
/// `loaded` keys (RDB) or commands (AOF), having started at `started`.
fn note_startup_load(
    base_port: u16,
    num_shards: usize,
    aof: bool,
    started: std::time::Instant,
    loaded: u64,
    expired: Vec<Vec<u8>>,
) {
    let mut loads = STARTUP_LOADS.lock();
    let entry = loads.entry((base_port, aof)).or_insert(StartupLoad {
        started,
        shards_done: 0,
        loaded: 0,
        expired: Vec::new(),
    });
    entry.started = entry.started.min(started);
    entry.shards_done += 1;
    entry.loaded += loaded;
    entry.expired.extend(expired);
    if entry.shards_done < num_shards {
        return;
    }
    let StartupLoad {
        started,
        loaded,
        expired,
        ..
    } = loads.remove(&(base_port, aof)).expect("entry just updated");
    drop(loads);
    let secs = started.elapsed().as_secs_f64();
    if aof {
        if loaded > 0 {
            crate::log_notice!("Done loading AOF, commands replayed: {}", loaded);
            crate::log_notice!("DB loaded from append only file: {:.3} seconds", secs);
        }
    } else {
        LAST_LOAD_KEYS_LOADED.store(loaded, std::sync::atomic::Ordering::Relaxed);
        crate::log_notice!(
            "Done loading RDB, keys loaded: {}, keys expired: {}.",
            loaded,
            expired.len()
        );
        crate::log_notice!("DB loaded from disk: {:.3} seconds", secs);
        LAST_LOAD_KEYS_EXPIRED
            .lock()
            .insert(base_port, expired.len() as u64);
        // Like Redis, a master tells its replicas (through the backlog they
        // continue from) about the keys it dropped as expired while loading.
        crate::replication::propagate_expired_on_load(base_port, &expired);
    }
}

/// io_uring settings for shard runtimes. With `COOP_TASKRUN` the kernel
/// runs a ring's completions when its thread next enters the kernel instead
/// of interrupting it, and `TASKRUN_FLAG` shows that some are pending, which
/// the runtime's idle poll (`idle-poll-us`) watches. Both need Linux 5.19;
/// older kernels get the default ring.
fn shard_uring_builder() -> io_uring::Builder {
    static SUPPORTED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let flags = |b: &mut io_uring::Builder| {
        b.setup_coop_taskrun().setup_taskrun_flag();
    };
    let supported = *SUPPORTED.get_or_init(|| {
        let mut b = io_uring::IoUring::builder();
        flags(&mut b);
        b.build(8).is_ok()
    });
    let mut b = io_uring::IoUring::builder();
    if supported {
        flags(&mut b);
    }
    b
}

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
) {
    if let Some(core) = core_id {
        core_affinity::set_for_current(core);
    }

    ADOPTED_CLIENT_SEQ.with(|s| s.set(((shard_id as u64) << 48) | (1u64 << 47) | 1));

    let mut rt = match monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_timer()
        .uring_builder(shard_uring_builder())
        .build()
    {
        Ok(rt) => rt,
        // io_uring is supported (monoio probed it) but this ring couldn't be
        // set up, typically ENOMEM from RLIMIT_MEMLOCK. Serving on epoll is
        // slower but beats a shard thread that never starts.
        Err(e) => {
            crate::log_warning!(
                "WARNING: shard {} could not set up io_uring ({}); falling back to the epoll driver, \
                 which is slower. Raise the locked-memory limit (ulimit -l / systemd \
                 LimitMEMLOCK=infinity / docker --ulimit memlock=-1) to use io_uring.",
                shard_id,
                e
            );
            monoio::RuntimeBuilder::<monoio::LegacyDriver>::new()
                .enable_timer()
                .build()
                .expect("Failed to initialize the Monoio epoll runtime")
                .into()
        }
    };

    rt.block_on(async move {
        let base_port = port;
        if shard_id == 0 {
            // Startup value of `appendfsync`; CONFIG SET may change it later.
            crate::aof::set_fsync_every_sec(base_port, aof_config.fsync_every_sec);
        }
        let shard_port = if cluster_enabled {
            base_port + shard_id as u16
        } else {
            base_port
        };
        // Users/requirepass are server-wide: every shard port uses one ACL table.
        crate::acl::share_acl_with_port(shard_port, base_port);

        // 1. One SO_REUSEPORT listener per configured `bind` address.
        let bind_addrs = crate::netsec::bind_addrs(base_port);
        let bound = crate::netsec::bind_all(&bind_addrs, shard_port, 4096)
            .unwrap_or_else(|e| panic!("[Shard {}] {}", shard_id, e));
        let listen_addrs: Vec<SocketAddr> = bound.iter().map(|(a, _)| *a).collect();
        let listeners: Vec<monoio::net::TcpListener> = bound
            .into_iter()
            .map(|(_, l)| {
                monoio::net::TcpListener::from_std(l)
                    .expect("Failed to convert socket into Monoio TcpListener")
            })
            .collect();

        let tls_listeners: Vec<monoio::net::TcpListener> = if let Some(ref tls_cfg) = tls_config {
            crate::netsec::bind_all(&bind_addrs, tls_cfg.tls_port, 4096)
                .unwrap_or_else(|e| panic!("[Shard {}] TLS: {}", shard_id, e))
                .into_iter()
                .map(|(_, l)| {
                    monoio::net::TcpListener::from_std(l)
                        .expect("Failed to convert TLS socket into Monoio TcpListener")
                })
                .collect()
        } else {
            Vec::new()
        };

        // 2. Pure thread-local Shard DB (no Mutex, no Arc)
        let local_db = Rc::new(RefCell::new(ShardDb::new(port).with_shard(shard_id)));

        // 2.5 RDB Snapshot Restore on startup
        // Follow Redis specification: if AOF is enabled, AOF is authoritative; otherwise load RDB.
        if !aof_config.enabled {
            let rdb_path = aof_config.dir.join(crate::config::dbfilename(base_port));
            if rdb_path.exists() {
                let started = std::time::Instant::now();
                let _ = crate::redis_rdb::take_loaded_repl_info();
                crate::redis_rdb::record_expired_on_load(true);
                let loaded = crate::table::load_rdb(&rdb_path, &mut local_db.borrow_mut(), shard_id, num_shards);
                let expired = crate::redis_rdb::take_expired_on_load();
                match loaded {
                    Ok(n) => {
                        // Like Redis, continue the replication history
                        // saved with the data (PSYNC after a restart).
                        if shard_id == 0
                            && let Some((replid, offset)) = crate::redis_rdb::take_loaded_repl_info()
                        {
                            crate::replication::get_replication_hub(shard_port).restore_saved_history(
                                &replid,
                                offset,
                                crate::redis_rdb::last_rdb_repl_stream_db(),
                                crate::replication::has_startup_replicaof(base_port),
                            );
                        }
                        if n > 0 {
                            crate::log_verbose!("[Shard {}/{}] Restored {} keys from {:?}", shard_id, num_shards, n, rdb_path);
                        }
                        note_startup_load(base_port, num_shards, false, started, n as u64, expired);
                    }
                    Err(e) => fatal_startup_error(
                        shard_id,
                        &format!("failed to load RDB {:?}: {}", rdb_path, e),
                    ),
                }
            }
        }

        // 3. AOF Replay on startup & Open AofWriter
        let aof_path = aof_config.dir.join(format!("appendonly-{}.aof", shard_id));
        if aof_config.enabled {
            let started = std::time::Instant::now();
            match crate::aof::replay_aof(&aof_path, &mut local_db.borrow_mut()) {
                Ok(n) => {
                    if n > 0 {
                        crate::log_verbose!(
                            "[Shard {}] Replayed {} commands from {:?}",
                            shard_id, n, aof_path
                        );
                    }
                    note_startup_load(base_port, num_shards, true, started, n as u64, Vec::new());
                }
                Err(e) => fatal_startup_error(
                    shard_id,
                    &format!("failed to replay AOF {:?}: {}", aof_path, e),
                ),
            }
        }

        let aof_writer = if aof_config.enabled {
            match crate::aof::AofWriter::open(aof_path).await {
                Ok(mut w) => {
                    w.set_progress(crate::aof::aof_progress(base_port, shard_id));
                    let writer = Rc::new(RefCell::new(w));
                    let flush_writer = writer.clone();
                    let fsync_every_sec = crate::aof::fsync_every_sec_flag(base_port);
                    monoio::spawn(async move {
                        let mut ticker = 0u64;
                        loop {
                            monoio::time::sleep(std::time::Duration::from_millis(5)).await;
                            // Not under appendfsync always (whose fsync path
                            // writes chunks asynchronously): a plain write,
                            // so replies' write-through never waits on an
                            // in-flight chunk.
                            if !crate::aof::appendfsync_is_always(base_port)
                                && flush_writer
                                    .borrow_mut()
                                    .write_pending_sync()
                                    .unwrap_or(false)
                            {
                                ticker += 1;
                                if ticker.is_multiple_of(200)
                                    && fsync_every_sec.load(std::sync::atomic::Ordering::Relaxed)
                                {
                                    let file = flush_writer.borrow().get_file();
                                    if let Some(file) = file {
                                        let _ = file.sync_data().await;
                                    }
                                }
                                continue;
                            }
                            let flush_chunk = flush_writer.borrow_mut().take_flush_chunk();
                            if let Some((file, chunk, offset)) = flush_chunk {
                                let (res, returned_chunk) = file.write_all_at(chunk, offset).await;
                                flush_writer
                                    .borrow_mut()
                                    .chunk_written(&res, returned_chunk, offset);
                            }
                            ticker += 1;
                            if ticker.is_multiple_of(200)
                                && fsync_every_sec.load(std::sync::atomic::Ordering::Relaxed)
                            {
                                let file = flush_writer.borrow().get_file();
                                if let Some(file) = file {
                                    let _ = file.sync_data().await;
                                }
                            }
                        }
                    });
                    Some(writer)
                }
                Err(e) => fatal_startup_error(
                    shard_id,
                    &format!("failed to open AOF for appending: {}", e),
                ),
            }
        } else {
            None
        };
        // Expired keys are logged as DELs from wherever they expire.
        crate::replication::set_shard_log_context(base_port, shard_id, aof_writer.clone());

        // 4. Background Cluster Bus & Gossip Engine (on Shard 0), cluster mode only.
        if shard_id == 0 && cluster_enabled {
            crate::cluster::start_cluster_bus(port);
        }

        // Initialize NVMe Tiered Storage Manager (io_uring)
        // Under `dir` by default, like the RDB and AOF: the system temp dir
        // is often tmpfs (RAM), which defeats tiering.
        let tier_dir = std::env::var("RUDIS_TIER_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| aof_config.dir.join(format!("rudis_tier_{}", port)));
        match crate::tiering::ShardTierManager::open(shard_id, port, &tier_dir).await {
            Ok(tm) => {
                local_db.borrow_mut().tier_manager = Some(Rc::new(tm));
            }
            Err(e) => {
                crate::log_warning!("[Shard {}] Failed to open Tier manager: {}", shard_id, e);
            }
        }

        let client_registry = Rc::new(RefCell::new(hashbrown::HashMap::<
            u64,
            crate::connection::ClientInfo,
        >::new()));
        let pubsub = Rc::new(RefCell::new(crate::pubsub::PubSubHub::new()));
        let mut r = Router::new(
            shard_id,
            num_shards,
            shard_port,
            local_db.clone(),
            senders,
            aof_writer.clone(),
            pubsub.clone(),
            aof_config.dir.clone(),
        );
        r.set_base_port(base_port);
        r.cluster_enabled = cluster_enabled;
        // Judge maxmemory by allocator counts (see TieringStats::shard_real).
        r.tier_stats
            .real_accounting
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let router = Rc::new(r);
        crate::connection::set_current_router(router.clone());

        if shard_id == 0 {
            crate::tiering::register_server();
        }
        router.publish_memory_state();

        // Active expiration cycle: run every 100ms
        let active_db = local_db.clone();
        let mem_router = router.clone();
        monoio::spawn(async move {
            loop {
                monoio::time::sleep(std::time::Duration::from_millis(100)).await;
                // Like Redis, a replica does not expire keys itself: it waits
                // for its master's DEL (its own expiry could race a PERSIST
                // or EXPIRE from the master and lose the key).
                let is_replica = crate::replication::get_replication_hub(base_port).is_slave();
                crate::replication::set_readonly_replica(
                    is_replica
                        && crate::connection::REPLICA_READ_ONLY
                            .load(std::sync::atomic::Ordering::Relaxed),
                );
                if !is_replica {
                    active_db.borrow_mut().active_expire_cycle();
                }
                // Give back table segments emptied by deletes and expiry.
                // Not while a snapshot walks the segments: moving entries
                // would make it restart as one blocking pass.
                if !mem_router.is_saving.load(std::sync::atomic::Ordering::Relaxed) {
                    let mut db = active_db.borrow_mut();
                    db.table.shrink_step(32);
                    // Compact arenas left part empty by deletes/eviction.
                    db.table.trim_arena_step(32);
                }
                // Recount part of the dataset estimate (values grown or shrunk
                // in place); read-only, so safe during a snapshot.
                active_db.borrow_mut().refresh_data_step(50_000);
                // Deletes, flushes and CONFIG SET maxmemory don't go through
                // the post-write check; publish this shard's usage here.
                mem_router.publish_memory_state();
                if shard_id == 0 {
                    mem_router.calibrate_real_memory();
                }
                // Rates for INFO instantaneous_*; extra calls are no-ops.
                crate::server_stats::sample();
            }
        });

        if shard_id == 0
            && let Some(l) = crate::telemetry::take_metrics_listener(base_port)
        {
            let listener = monoio::net::TcpListener::from_std(l)
                .expect("Failed to convert metrics socket into Monoio TcpListener");
            monoio::spawn(crate::telemetry::serve_metrics(router.clone(), listener));
        }

        // Background Tiered Storage Offload cycle: run every 20ms
        let offload_router = router.clone();
        monoio::spawn(async move {
            loop {
                monoio::time::sleep(std::time::Duration::from_millis(20)).await;
                offload_router.check_auto_tier_local().await;
            }
        });

        // Background Tiered Storage GC & Hole Punching: run every 2s
        let gc_router = router.clone();
        monoio::spawn(async move {
            loop {
                monoio::time::sleep(std::time::Duration::from_secs(2)).await;
                gc_router.gc_local();
            }
        });

        // 5. AF_XDP Kernel Bypass Ingress Loop: poll zero-copy Rx ring for packet descriptors
        let xdp_engine = crate::xdp::get_xdp_engine();
        let xdp_db = local_db.clone();
        let xdp_aof = aof_writer.clone();
        let xdp_router = router.clone();
        monoio::spawn(async move {
            let mut frames = Vec::new();
            loop {
                let xsk_opt = xdp_engine.get_socket(shard_port, shard_id as u32);
                let count = xsk_opt
                    .as_ref()
                    .map_or(0, |s| s.rx_burst(&mut frames, 32));
                if count > 0 {
                    for frame in frames.drain(..) {
                        let action = xdp_engine.process_packet(&frame);
                        if (action == crate::xdp::XdpAction::Pass
                            || action == crate::xdp::XdpAction::Redirect)
                            && let Some(cmd_payload) = crate::xdp::extract_transport_payload(&frame)
                        {
                            let mut b_mut = bytes::BytesMut::from(&cmd_payload[..]);
                            if let Ok(Some(cmd)) = crate::resp::parse_command(&mut b_mut) {
                                let target_sid = crate::connection::target_shard_of_cmd(&cmd, xdp_router.num_shards)
                                    .unwrap_or(xdp_router.shard_id);
                                let mut out = Vec::new();
                                if target_sid == xdp_router.shard_id {
                                    let mut db = xdp_db.borrow_mut();
                                    let _ = crate::connection::execute_local_command(
                                        &cmd,
                                        &mut db,
                                        &mut out,
                                        xdp_aof.as_ref().map(|a| a.as_ref()),
                                    );
                                } else {
                                    let resp = xdp_router.execute_remote(target_sid, cmd).await;
                                    out.extend_from_slice(&resp);
                                }
                                if !out.is_empty()
                                    && let Some(ref xsk_socket) = xsk_opt
                                {
                                    xsk_socket.tx_burst(&[&out]);
                                }
                            }
                        }
                    }
                } else {
                    // Packets only arrive through the experimental XDP.*
                    // commands; poll rarely while those are disabled.
                    let idle_ms = if crate::resp::experimental_commands_enabled() {
                        5
                    } else {
                        200
                    };
                    monoio::time::sleep(std::time::Duration::from_millis(idle_ms)).await;
                }
            }
        });

        // 4. Spawn background worker to handle incoming cross-shard messages from peer cores
        let cross_shard_db = local_db.clone();
        let cross_shard_router = router.clone();
        let cross_shard_clients = client_registry.clone();
        let cross_shard_slot_states = router.slot_states.clone();
        let cross_shard_slot_owners = router.slot_owners.clone();
        let cross_shard_aof = aof_writer.clone();
        let cross_shard_pubsub = pubsub.clone();
        let cross_shard_tx_lock = router.tx_lock.clone();
        let cross_shard_tx_waiters = router.tx_waiters.clone();
        monoio::spawn(async move {
            while let Ok(mut msg) = rx.recv_async().await {
                let mut burst = 0;
                loop {
                    match msg {
                    ShardMessage::AdoptConnection { fd, peer } => {
                        // Take ownership of a connection handed over by an
                        // overloaded shard. Safe: the sending shard released the
                        // fd via into_raw_fd without closing it, and we are in
                        // the same process, so the fd is valid in our table.
                        // SAFETY: The sending shard gave up `fd` with into_raw_fd and never uses it
                        // again, and each AdoptConnection is received once, so we become its sole
                        // owner.
                        let std_stream = unsafe {
                            <std::net::TcpStream as std::os::unix::io::FromRawFd>::from_raw_fd(fd)
                        };
                        match monoio::net::TcpStream::from_std(std_stream) {
                            Ok(stream) => {
                                // Deliberately no register_conn here: the
                                // handing-off shard already reserved our census
                                // slot before sending. Registering on arrival
                                // would leave us looking idle for the whole
                                // in-flight window, and every other shard
                                // accepting concurrently would pile onto us.
                                let r = cross_shard_router.clone();
                                let reg = cross_shard_clients.clone();
                                let client_id = ADOPTED_CLIENT_SEQ.with(|s| {
                                    let id = s.get();
                                    s.set(id + 1);
                                    id
                                });
                                monoio::spawn(async move {
                                    let res = catch_unwind_async(async move {
                                        handle_connection(stream, peer, client_id, reg, r).await;
                                    })
                                    .await;
                                    crate::conn_balance::unregister_conn(shard_id);
                                    if let Err(e) = res {
                                        crate::connection::inc_isolated_panics();
                                        crate::log_warning!(
                                            "Panic isolated in adopted connection id={}: {}",
                                            client_id,
                                            crate::log::panic_message(&*e)
                                        );
                                    }
                                });
                            }
                            Err(e) => {
                                // The connection never starts, so release the
                                // slot the sender reserved for it.
                                crate::conn_balance::unregister_conn(shard_id);
                                crate::log_warning!(
                                    "[Shard {}] Failed to adopt handed-off connection: {}",
                                    shard_id,
                                    e
                                );
                            }
                        }
                    }
                    ShardMessage::FastGet { descriptor } => {
                        crate::connection::CURRENT_DB.set(descriptor.db);
                        cross_shard_db.borrow_mut().select_db(descriptor.db);
                        let prev_no_touch = crate::connection::CLIENT_NO_TOUCH.replace(descriptor.no_touch);
                        let val = cross_shard_db.borrow_mut().get_checked(&descriptor.key);
                        crate::connection::CLIENT_NO_TOUCH.set(prev_no_touch);
                        if let Ok(Some(v)) = val {
                            descriptor.finish(Some(v));
                        } else if val.is_err() {
                            descriptor.finish_wrong_type();
                        } else if cross_shard_db.borrow_mut().table.is_tiered(&descriptor.key).is_some() {
                            let r = cross_shard_router.clone();
                            monoio::spawn(async move {
                                let val = r.read_cold_key_local(&descriptor.key).await;
                                descriptor.finish(val);
                            });
                        } else {
                            descriptor.finish(None);
                        }
                    }
                    ShardMessage::FastSet { descriptor } => {
                        crate::connection::CURRENT_DB.set(descriptor.db);
                        cross_shard_db.borrow_mut().select_db(descriptor.db);
                        crate::replication::log_shard_mutation(port, shard_id, cross_shard_aof.as_deref(), || crate::resp::Command::Set {
                                    key: descriptor.key.clone(),
                                    value: descriptor.value.clone(),
                                    expire_in: descriptor.expire_in,
                                    condition: crate::resp::SetCondition::None,
                                    get: false,
                                    keepttl: false,
                                    past_expired: false,
                                });
                        cross_shard_db
                            .borrow_mut()
                            .set(descriptor.key.clone(), descriptor.value.clone(), descriptor.expire_in);
                        crate::connection::notify_keyspace_event_sync(&cross_shard_router, crate::connection::NOTIFY_STRING, "set", &descriptor.key);
                        crate::connection::notify_stream_or_defer(&mut cross_shard_db.borrow_mut(), &descriptor.key);
                        if descriptor.expire_in.is_some() {
                            crate::connection::notify_keyspace_event_sync(&cross_shard_router, crate::connection::NOTIFY_GENERIC, "expire", &descriptor.key);
                        }
                        cross_shard_router.check_auto_tier_after_write();
                        descriptor.finish();
                    }
                    ShardMessage::Del { db, key, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let deleted = sdb.del(&key);
                        if deleted {
                            sdb.delete_document_local(&String::from_utf8_lossy(&key));
                            crate::connection::notify_keyspace_event_sync(&cross_shard_router, crate::connection::NOTIFY_GENERIC, "del", &key);
                            crate::connection::notify_stream_or_defer(&mut sdb, &key);
                            crate::replication::log_shard_mutation(port, shard_id, cross_shard_aof.as_deref(), || crate::resp::Command::Del(smallvec![
                                        key,
                                    ]));
                        }
                        let _ = responder.send(deleted);
                    }
                    ShardMessage::DelIfUnchanged { db, key, payload, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        cross_shard_db.borrow_mut().select_db(db);
                        let _ = responder.send(cross_shard_router.del_if_unchanged_local(key, &payload));
                    }
                    ShardMessage::DelKeys { db, keys, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut count = 0usize;
                        let mut deleted_keys = Vec::with_capacity(keys.len());
                        {
                            let mut sdb = cross_shard_db.borrow_mut();
                            sdb.select_db(db);
                            for k in keys {
                                if sdb.del(&k) {
                                    count += 1;
                                    sdb.delete_document_local(&String::from_utf8_lossy(&k));
                                    crate::connection::notify_keyspace_event_sync(&cross_shard_router, crate::connection::NOTIFY_GENERIC, "del", &k);
                                    deleted_keys.push(k);
                                }
                            }
                        }
                        if count > 0 {
crate::replication::log_shard_mutation(port, shard_id, cross_shard_aof.as_deref(), || crate::resp::Command::Del(SmallVec::from_vec(deleted_keys)));
}
                        let _ = responder.send(count);
                    }
                    ShardMessage::ActiveDefrag { responder } => {
                        let freed = cross_shard_db.borrow_mut().active_defrag();
                        let _ = responder.send(freed);
                    }
                    ShardMessage::Exists { db, key, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let exists = sdb.exists(&key);
                        let _ = responder.send(exists);
                    }
                    ShardMessage::IncrBy {
                        db,
                        key,
                        delta,
                        name,
                        responder,
                    } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        crate::shard::before_write(&sdb, &key);
                        let res = sdb.incr_by(key.clone(), delta);
                        if res.is_ok() {
                            crate::replication::log_shard_mutation(
                                port,
                                shard_id,
                                cross_shard_aof.as_deref(),
                                || crate::resp::Command::IncrBy(key, delta, name),
                            );
                        }
                        let _ = responder.send(res);
                    }
                    ShardMessage::Expire {
                        db,
                        key,
                        duration,
                        opts,
                        responder,
                    } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let res = sdb.expire(&key, duration, opts);
                        drop(sdb);
                        if res {
                            if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                            }
                            if crate::connection::HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                crate::connection::notify_key_invalidation(cross_shard_router.port, key.as_ref(), 0);
                            }
                            crate::replication::log_shard_mutation(port, shard_id, cross_shard_aof.as_deref(), || crate::resp::Command::Expire {
                                        key,
                                        duration,
                                        opts,
                                    });
                        }
                        let _ = responder.send(res);
                    }
                    ShardMessage::Persist { db, key, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let res = sdb.persist(&key);
                        drop(sdb);
                        if res {
                            if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                            }
                            if crate::connection::HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                crate::connection::notify_key_invalidation(cross_shard_router.port, key.as_ref(), 0);
                            }
                            crate::replication::log_shard_mutation(port, shard_id, cross_shard_aof.as_deref(), || crate::resp::Command::Persist(key));
                        }
                        let _ = responder.send(res);
                    }
                    ShardMessage::Ttl {
                        db,
                        key,
                        in_millis,
                        responder,
                    } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let res = sdb.ttl(&key, in_millis);
                        let _ = responder.send(res);
                    }
                    ShardMessage::CountKeysInSlot { slot, responder } => {
                        let count = cross_shard_db.borrow_mut().count_keys_in_slot(slot);
                        let _ = responder.send(count);
                    }
                    ShardMessage::GetKeysInSlot {
                        slot,
                        count,
                        responder,
                    } => {
                        let keys = cross_shard_db.borrow_mut().get_keys_in_slot(slot, count);
                        let _ = responder.send(keys);
                    }
                    ShardMessage::ClientList { filter_ids, responder } => {
                        let mut out = String::new();
                        let reg = cross_shard_clients.borrow();
                        let now = std::time::Instant::now();
                        for client in reg.values() {
                            if client.stats.killed.load(std::sync::atomic::Ordering::Relaxed) {
                                continue;
                            }
                            if !filter_ids.is_empty() && !filter_ids.contains(&client.id) {
                                continue;
                            }
                            let age = now.duration_since(client.connected_at).as_secs();
                            let idle = now.duration_since(client.last_active).as_secs();
                            let is_blocked = crate::block::get_block_hub_for_port(port).lock().is_blocked(client.id);
                            let flags = if client.is_monitor {
                                "O"
                            } else if client.stats.is_replica.load(std::sync::atomic::Ordering::Relaxed) {
                                "S"
                            } else if is_blocked {
                                "b"
                            } else {
                                "N"
                            };
                            let (qbuf, qbuf_free) = client.effective_qbuf(idle);
                            let tot_net_in = client.stats.tot_net_in.load(std::sync::atomic::Ordering::Relaxed);
                            let tot_net_out = client.stats.tot_net_out.load(std::sync::atomic::Ordering::Relaxed);
                            let tot_cmds = client.stats.tot_cmds.load(std::sync::atomic::Ordering::Relaxed);
                            let read_events = client.stats.read_events.load(std::sync::atomic::Ordering::Relaxed);
                            let pipe_sum = client.stats.pipeline_len_sum.load(std::sync::atomic::Ordering::Relaxed);
                            let pipe_cnt = client.stats.pipeline_len_cnt.load(std::sync::atomic::Ordering::Relaxed);
                            out.push_str(&format!(
                                "id={} addr={} laddr=127.0.0.1:{} fd=8 name={} age={} idle={} flags={} db={} sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf={} qbuf-free={} argv-mem=10 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem={} omem-shared=0 omem-unshared=0 tot-mem=22306 events=r cmd={} user=default redir=-1 resp=2 lib-name={} lib-ver={} io-thread=0 tot-net-in={} tot-net-out={} tot-cmds={} read-events={} avg-pipeline-len-sum={} avg-pipeline-len-cnt={}\n",
                                client.id,
                                client.addr,
                                port,
                                client.name.as_deref().unwrap_or(""),
                                age,
                                idle,
                                flags,
                                client.db,
                                qbuf,
                                qbuf_free,
                                client.omem,
                                client.last_cmd.to_lowercase(),
                                client.lib_name.as_deref().unwrap_or(""),
                                client.lib_ver.as_deref().unwrap_or(""),
                                tot_net_in,
                                tot_net_out,
                                tot_cmds,
                                read_events,
                                pipe_sum,
                                pipe_cnt,
                            ));
                        }
                        let _ = responder.send(out);
                    }
                    ShardMessage::Batch {
                        db: batch_db,
                        items,
                        responder,
                        is_resp3,
                        client_id,
                        no_touch,
                        defer_bcast,
                    } => {
                        let has_tier_manager = cross_shard_db.borrow().tier_manager.is_some();
                        {
                            crate::connection::CURRENT_CLIENT_RESP3.set(is_resp3);
                            crate::connection::CURRENT_DB.set(batch_db);
                            let prev_no_touch = crate::connection::CLIENT_NO_TOUCH.replace(no_touch);
                            let prev_defer = crate::connection::DEFER_BCAST_FLUSH.replace(defer_bcast);
                            let has_tracking = crate::connection::HAS_TRACKING_CLIENTS
                                .load(std::sync::atomic::Ordering::Relaxed);
                            // Run as the requesting client (tracked reads, NOLOOP,
                            // script stats).
                            let prev_cid = crate::connection::CURRENT_CLIENT_ID.replace(client_id);
                            let mut db = cross_shard_db.borrow_mut();
                            db.select_db(batch_db);
                            let aof_ref = cross_shard_aof.as_deref();
                            let mut temp_buf = Vec::new();
                            let mut has_writes = false;
                            let mut cold_gets: smallvec::SmallVec<[(usize, bytes::Bytes); 8]> =
                                smallvec::SmallVec::new();
                            let slow_threshold = crate::connection::batch_slow_threshold();
                            for item in items.iter() {
                                // Iterated in place: the owner drops the commands when it takes the
                                // batch back, so their keys' refcounts (shared with its read buffer)
                                // never bounce to this core.
                                let (idx, key_hash, cmd) = (item.0, item.1, &item.2);
                                let slow_t0 = slow_threshold.map(|_| crate::cycleclock::now());
                                'item: {
                                    temp_buf.clear();
                                    db.hydrate_cmd_keys(cmd);
                                    if !has_tracking && let Command::Get(ref key) = *cmd {
                                        match db.table.get_compact_with_hash(key.as_ref(), key_hash) {
                                            Ok(Some(resp)) => {
                                                responder.write_slot(idx, resp);
                                                break 'item;
                                            }
                                            Ok(None) => {
                                                if has_tier_manager
                                                    && db.table.is_tiered(key).is_some()
                                                {
                                                    cold_gets.push((idx, key.clone()));
                                                } else {
                                                    responder.write_slot(
                                                        idx,
                                                        crate::shard::CompactResp::null(is_resp3),
                                                    );
                                                }
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && let Command::Set {
                                            ref key,
                                            ref value,
                                            expire_in,
                                            condition: crate::resp::SetCondition::None,
                                            get: false,
                                            keepttl: false,
                                            past_expired: false,
                                        } = *cmd
                                    {
                                        has_writes = true;
                                        if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                            crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                        }
                                        // Borrowed, not moved: the command stays whole for
                                        // the slowlog check after the item.
                                        db.table.set_with_hash(key.clone(), key_hash, value.clone(), expire_in);
                                        responder.write_slot(idx, crate::shard::CompactResp::OK);
                                        break 'item;
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && let Command::IncrBy(ref key, delta, _) = *cmd
                                    {
                                        has_writes = true;
                                        match db.table.incr_by_slice_with_hash(key, key_hash, delta) {
                                            Ok(val) => {
                                                if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                    crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                }
                                                if val == 1 {
                                                    responder.write_slot(idx, crate::shard::CompactResp::INT_1);
                                                } else if val == 0 {
                                                    responder.write_slot(idx, crate::shard::CompactResp::INT_0);
                                                } else {
                                                    responder.write_slot(idx, crate::shard::CompactResp::from_integer(val));
                                                }
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if !has_tracking
                                        && let Command::Exists(ref keys) = *cmd
                                        && keys.len() == 1
                                    {
                                        let exists = db.table.exists_with_hash(keys[0].as_ref(), key_hash);
                                        crate::server_stats::note_key_lookup(exists);
                                        responder.write_slot(idx, if exists { crate::shard::CompactResp::INT_1 } else { crate::shard::CompactResp::INT_0 });
                                        break 'item;
                                    } else if !has_tracking
                                        && let Command::Touch(ref keys) = *cmd
                                        && keys.len() == 1
                                    {
                                        let touched = db.table.touch_with_hash(keys[0].as_ref(), key_hash);
                                        responder.write_slot(idx, if touched { crate::shard::CompactResp::INT_1 } else { crate::shard::CompactResp::INT_0 });
                                        break 'item;
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && !crate::search::has_active_search_indices()
                                        && let Command::Del(ref keys) | Command::Unlink(ref keys) = *cmd
                                        && keys.len() == 1
                                    {
                                        let is_unlink = matches!(cmd, Command::Unlink(_));
                                        let lazy = is_unlink && db.table.is_lazyfree_worthy(keys[0].as_ref());
                                        let deleted = db.del_with_hash(keys[0].as_ref(), key_hash);
                                        if deleted {
                                            if lazy {
                                                crate::table::add_lazyfreed_objects(1);
                                            }
                                            has_writes = true;
                                            if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                crate::connection::touch_watched_key(cross_shard_router.port, keys[0].as_ref());
                                            }
                                            responder.write_slot(idx, crate::shard::CompactResp::INT_1);
                                        } else {
                                            responder.write_slot(idx, crate::shard::CompactResp::INT_0);
                                        }
                                        break 'item;
                                    } else if !has_tracking
                                        && let Command::Hget { ref key, ref field } = *cmd
                                    {
                                        match db.table.hget_compact_with_hash(key.as_ref(), key_hash, field.as_ref()) {
                                            Ok(resp) => {
                                                responder.write_slot(idx, resp);
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if !has_tracking
                                        && let Command::Hmget { ref key, ref fields } = *cmd
                                    {
                                        match db.table.hmget_compact_with_hash(key.as_ref(), key_hash, fields, &mut temp_buf) {
                                            Ok(resp) => {
                                                responder.write_slot(idx, resp);
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && !crate::search::has_active_search_indices()
                                        && let Command::Hset { ref key, ref fields } = *cmd
                                    {
                                        has_writes = true;
                                        let res = if fields.len() == 1 {
                                            let (ref f, ref v) = fields[0];
                                            db.table.hset_single_field_with_hash(key, key_hash, f, v)
                                        } else {
                                            db.table.hset_slice_with_hash(key, key_hash, fields)
                                        };
                                        match res {
                                            Ok(count) => {
                                                if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                    crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                }
                                                if count == 1 {
                                                    responder.write_slot(idx, crate::shard::CompactResp::INT_1);
                                                } else if count == 0 {
                                                    responder.write_slot(idx, crate::shard::CompactResp::INT_0);
                                                } else {
                                                    responder.write_slot(idx, crate::shard::CompactResp::from_integer(count as i64));
                                                }
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if !has_tracking
                                        && let Command::Sismember { ref key, ref member } = *cmd
                                    {
                                        match db.table.sismember_compact_with_hash(key.as_ref(), key_hash, member.as_ref()) {
                                            Ok(resp) => {
                                                responder.write_slot(idx, resp);
                                                break 'item;
                                            }
                                            Err(err) => crate::connection::write_resp_err(&mut temp_buf, err),
                                        }
                                    } else if !has_tracking
                                        && let Command::Smismember { ref key, ref members } = *cmd
                                    {
                                        match db.table.smismember_compact_with_hash(key.as_ref(), key_hash, members, &mut temp_buf) {
                                            Ok(resp) => {
                                                responder.write_slot(idx, resp);
                                                break 'item;
                                            }
                                            Err(err) => crate::connection::write_resp_err(&mut temp_buf, err),
                                        }
                                    } else if !has_tracking
                                        && let Command::Zmscore { ref key, ref members } = *cmd
                                    {
                                        match db.table.zmscore_compact_with_hash(key.as_ref(), key_hash, members, &mut temp_buf) {
                                            Ok(resp) => {
                                                responder.write_slot(idx, resp);
                                                break 'item;
                                            }
                                            Err(err) => crate::connection::write_resp_err(&mut temp_buf, err),
                                        }
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && let Command::Sadd { ref key, ref members } = *cmd
                                    {
                                        has_writes = true;
                                        let res = if members.len() == 1 {
                                            db.table.sadd_single_member_with_hash(key, key_hash, &members[0])
                                        } else {
                                            db.table.sadd_slice_with_hash(key, key_hash, members)
                                        };
                                        match res {
                                            Ok(count) => {
                                                if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                    crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                }
                                                if count == 1 {
                                                    responder.write_slot(idx, crate::shard::CompactResp::INT_1);
                                                } else if count == 0 {
                                                    responder.write_slot(idx, crate::shard::CompactResp::INT_0);
                                                } else {
                                                    responder.write_slot(idx, crate::shard::CompactResp::from_integer(count as i64));
                                                }
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && !crate::block::has_blocked_waiters(cross_shard_router.port)
                                        && let Command::Lpush { ref key, ref values } = *cmd
                                    {
                                        has_writes = true;
                                        match db.table.lpush_slice_with_hash(key, key_hash, values) {
                                            Ok(len) => {
                                                if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                    crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                }
                                                if len == 1 {
                                                    responder.write_slot(idx, crate::shard::CompactResp::INT_1);
                                                } else if len == 0 {
                                                    responder.write_slot(idx, crate::shard::CompactResp::INT_0);
                                                } else {
                                                    responder.write_slot(idx, crate::shard::CompactResp::from_integer(len as i64));
                                                }
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && let Some((key, items, kind)) = match cmd {
                                            Command::Hdel { key, fields } if !crate::search::has_active_search_indices() => {
                                                Some((key, fields, crate::connection::RemKind::Hash))
                                            }
                                            Command::Srem { key, members } => Some((key, members, crate::connection::RemKind::Set)),
                                            Command::Zrem { key, members } => Some((key, members, crate::connection::RemKind::ZSet)),
                                            _ => None,
                                        }
                                    {
                                        let res = match kind {
                                            crate::connection::RemKind::Hash => db.table.hdel_with_hash(key, key_hash, items),
                                            crate::connection::RemKind::Set => db.table.srem_with_hash(key, key_hash, items),
                                            crate::connection::RemKind::ZSet => db.table.zrem_with_hash(key, key_hash, items),
                                        };
                                        match res {
                                            Ok(count) => {
                                                if count > 0 {
                                                    has_writes = true;
                                                    if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                        crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                    }
                                                }
                                                responder.write_slot(idx, crate::shard::CompactResp::from_integer(count as i64));
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && !crate::block::has_blocked_waiters(cross_shard_router.port)
                                        && let Command::Rpush { ref key, ref values } = *cmd
                                    {
                                        has_writes = true;
                                        match db.table.rpush_slice_with_hash(key, key_hash, values) {
                                            Ok(len) => {
                                                if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                    crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                }
                                                responder.write_slot(idx, crate::shard::CompactResp::from_integer(len as i64));
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && let Command::Spop { ref key, count: None } = *cmd
                                    {
                                        match db.table.spop_one_with_hash(key.as_ref(), key_hash) {
                                            Ok(Some(v)) => {
                                                has_writes = true;
                                                if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                    crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                }
                                                responder.write_slot(idx, crate::shard::CompactResp::from_owned_bulk(v));
                                                break 'item;
                                            }
                                            Ok(None) => {
                                                responder.write_slot(idx, crate::shard::CompactResp::NULL);
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && let Command::Zpopmin { ref key, count: None } = *cmd
                                    {
                                        match db.table.zpopmin_with_hash(key.as_ref(), key_hash, 1) {
                                            Ok(popped) => {
                                                if let Some((m, s)) = popped.into_iter().next() {
                                                    has_writes = true;
                                                    if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                        crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                    }
                                                    temp_buf.extend_from_slice(b"*2\r\n");
                                                    crate::connection::write_resp_bulk(&mut temp_buf, &m);
                                                    crate::connection::write_resp_score(&mut temp_buf, s);
                                                    responder.write_slot(idx, crate::shard::CompactResp::from_slice(&temp_buf));
                                                } else {
                                                    responder.write_slot(idx, crate::shard::CompactResp::EMPTY_ARRAY);
                                                }
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && let Command::Lpop { ref key, count } = *cmd
                                    {
                                        if count.is_none() {
                                            match db.table.lpop_one_with_hash(key.as_ref(), key_hash) {
                                                Ok(Some(v)) => {
                                                    has_writes = true;
                                                    if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                        crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                    }
                                                    responder.write_slot(idx, crate::shard::CompactResp::from_owned_bulk(v));
                                                    break 'item;
                                                }
                                                Ok(None) => {
                                                    responder.write_slot(idx, crate::shard::CompactResp::null(is_resp3));
                                                    break 'item;
                                                }
                                                Err(err) => {
                                                    crate::connection::write_resp_err(&mut temp_buf, err);
                                                }
                                            }
                                        } else {
                                            match db.table.write_lpop_resp_with_hash(key.as_ref(), key_hash, count, &mut temp_buf) {
                                                Ok(has_pop) => {
                                                    if has_pop {
                                                        has_writes = true;
                                                        if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                            crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                        }
                                                    }
                                                }
                                                Err(err) => {
                                                    crate::connection::write_resp_err(&mut temp_buf, err);
                                                }
                                            }
                                        }
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && let Command::Rpop { ref key, count } = *cmd
                                    {
                                        if count.is_none() {
                                            match db.table.rpop_one_with_hash(key.as_ref(), key_hash) {
                                                Ok(Some(v)) => {
                                                    has_writes = true;
                                                    if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                        crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                    }
                                                    responder.write_slot(idx, crate::shard::CompactResp::from_owned_bulk(v));
                                                    break 'item;
                                                }
                                                Ok(None) => {
                                                    responder.write_slot(idx, crate::shard::CompactResp::null(is_resp3));
                                                    break 'item;
                                                }
                                                Err(err) => {
                                                    crate::connection::write_resp_err(&mut temp_buf, err);
                                                }
                                            }
                                        } else {
                                            match db.table.write_rpop_resp_with_hash(key.as_ref(), key_hash, count, &mut temp_buf) {
                                                Ok(has_pop) => {
                                                    if has_pop {
                                                        has_writes = true;
                                                        if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                            crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                        }
                                                    }
                                                }
                                                Err(err) => {
                                                    crate::connection::write_resp_err(&mut temp_buf, err);
                                                }
                                            }
                                        }
                                    } else if !has_tracking
                                        && let Command::Lrange { ref key, start, stop } = *cmd
                                    {
                                        match db.table.lrange_compact_with_hash(key.as_ref(), key_hash, start, stop, &mut temp_buf) {
                                            Ok(resp) => {
                                                responder.write_slot(idx, resp);
                                                break 'item;
                                            }
                                            Err(err) => crate::connection::write_resp_err(&mut temp_buf, err),
                                        }
                                    } else if !has_tracking
                                        && let Command::Zrange { ref key, ref opts } = *cmd
                                    {
                                        match db.table.zrange_compact_with_hash(key.as_ref(), key_hash, opts, is_resp3, &mut temp_buf) {
                                            Ok(resp) => {
                                                responder.write_slot(idx, resp);
                                                break 'item;
                                            }
                                            Err(err) => crate::connection::write_resp_err(&mut temp_buf, err),
                                        }
                                    } else if !has_tracking
                                        && aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                        && crate::connection::NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0
                                        && !crate::block::has_blocked_waiters(cross_shard_router.port)
                                        && let Command::Zadd { ref key, ref elements, flags } = *cmd
                                    {
                                        has_writes = true;
                                        match db.table.zadd_slice_with_hash(key, key_hash, elements, flags) {
                                            Ok((count, incr_score)) => {
                                                if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                    crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                                }
                                                if flags.incr {
                                                    temp_buf.clear();
                                                    if let Some(score) = incr_score {
                                                        crate::connection::write_resp_score(&mut temp_buf, score);
                                                    } else {
                                                        crate::connection::write_resp_null(&mut temp_buf);
                                                    }
                                                    responder.write_slot(idx, crate::shard::CompactResp::from_slice(&temp_buf));
                                                } else if count == 1 {
                                                    responder.write_slot(idx, crate::shard::CompactResp::INT_1);
                                                } else if count == 0 {
                                                    responder.write_slot(idx, crate::shard::CompactResp::INT_0);
                                                } else {
                                                    responder.write_slot(idx, crate::shard::CompactResp::from_integer(count as i64));
                                                }
                                                break 'item;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if let Command::Eval { script, keys, args, read_only, auth_user } = cmd {
                                        crate::connection::CURRENT_AUTH_USER.with(|u| *u.borrow_mut() = auth_user.clone());
                                        let saved_db = crate::connection::CURRENT_DB.get();
                                        drop(db);
                                        let script_str = String::from_utf8_lossy(script);
                                        crate::scripting::load_script(script);
                                        match crate::scripting::eval_script(
                                            &script_str,
                                            keys,
                                            args,
                                            &cross_shard_db,
                                            cross_shard_aof.as_deref(),
                                            *read_only,
                                        ) {
                                            Ok(resp) => temp_buf.extend_from_slice(&resp),
                                            Err(e) => {
                                                crate::connection::write_resp_err(&mut temp_buf, e);
                                            }
                                        }
                                        crate::connection::CURRENT_DB.set(saved_db);
                                        db = cross_shard_db.borrow_mut();
                                        db.select_db(saved_db);
                                    } else if let Command::Evalsha { sha, keys, args, read_only, auth_user } = cmd {
                                        crate::connection::CURRENT_AUTH_USER.with(|u| *u.borrow_mut() = auth_user.clone());
                                        let saved_db = crate::connection::CURRENT_DB.get();
                                        drop(db);
                                        let sha_str = String::from_utf8_lossy(sha);
                                        if let Some(script) = crate::scripting::get_script(&sha_str) {
                                            match crate::scripting::eval_script(
                                                &script,
                                                keys,
                                                args,
                                                &cross_shard_db,
                                                cross_shard_aof.as_deref(),
                                                *read_only,
                                            ) {
                                                Ok(resp) => temp_buf.extend_from_slice(&resp),
                                                Err(e) => {
                                                    crate::connection::write_resp_err(&mut temp_buf, e);
                                                }
                                            }
                                        } else {
                                            temp_buf.extend_from_slice(b"-NOSCRIPT No matching script. Please use EVAL.\r\n");
                                        }
                                        crate::connection::CURRENT_DB.set(saved_db);
                                        db = cross_shard_db.borrow_mut();
                                        db.select_db(saved_db);
                                    } else if let Command::Fcall { function, keys, args, read_only, auth_user } = cmd {
                                        crate::connection::CURRENT_AUTH_USER.with(|u| *u.borrow_mut() = auth_user.clone());
                                        let saved_db = crate::connection::CURRENT_DB.get();
                                        drop(db);
                                        match crate::scripting::call_function(
                                            function,
                                            keys,
                                            args,
                                            &cross_shard_db,
                                            cross_shard_aof.as_deref(),
                                            *read_only,
                                        ) {
                                            Ok(res) => temp_buf.extend_from_slice(&res),
                                            Err(err) => crate::connection::write_resp_err(&mut temp_buf, err),
                                        }
                                        crate::connection::CURRENT_DB.set(saved_db);
                                        db = cross_shard_db.borrow_mut();
                                        db.select_db(saved_db);
                                    } else {
                                        if cmd.is_write_command() {
                                            has_writes = true;
                                        }
                                        let _ = execute_local_command(cmd, &mut db, &mut temp_buf, aof_ref);
                                    }
                                    responder.write_slot(idx, crate::shard::CompactResp::from_slice(&temp_buf));
                                }
                                // Timed like the owner's local commands; it logs the report with
                                // its client's address and name.
                                if let (Some(t0), Some(thr)) = (slow_t0, slow_threshold) {
                                    let us = crate::cycleclock::micros_since(t0);
                                    if us >= thr {
                                        responder.report_slow(idx, us, cmd.clone());
                                    }
                                }
                            }
                            db.drain_dropped_tier();
                            drop(db);
                            crate::connection::CURRENT_CLIENT_ID.set(prev_cid);
                            crate::connection::CLIENT_NO_TOUCH.set(prev_no_touch);
                            crate::connection::DEFER_BCAST_FLUSH.set(prev_defer);
                            if has_writes {
                                cross_shard_router.check_auto_tier_after_write();
                            }
                            if cold_gets.is_empty() {
                                responder.finish(items);
                            } else {
                                let r = cross_shard_router.clone();
                                monoio::spawn(async move {
                                    for (idx, key) in cold_gets {
                                        if let Some(v) = r.stream_cold_read_local(&key).await {
                                            responder.write_slot(
                                                idx,
                                                crate::shard::CompactResp::Bulk(v),
                                            );
                                        } else {
                                            responder
                                                .write_slot(idx, crate::shard::CompactResp::null(is_resp3));
                                        }
                                    }
                                    responder.finish(items);
                                });
                            }
                        }
                    }
                    ShardMessage::Mget { db: mget_db, mut keys, responder } => {
                        crate::connection::CURRENT_DB.set(mget_db);
                        let mut db = cross_shard_db.borrow_mut();
                        db.select_db(mget_db);
                        let mut cold_idx = None;
                        for (i, item) in keys.iter_mut().enumerate() {
                            let key = item.1.as_ref().unwrap();
                            let val = db.get(key);
                            if val.is_none() && db.table.is_tiered(key).is_some() {
                                cold_idx = Some(i);
                                break;
                            }
                            item.1 = val;
                        }

                        if let Some(start_idx) = cold_idx {
                            let mut results = Vec::with_capacity(keys.len());
                            for (idx, val) in keys.drain(..start_idx) {
                                results.push((idx, val));
                            }
                            drop(db);
                            let r = cross_shard_router.clone();
                            monoio::spawn(async move {
                                for (idx, key_opt) in keys.into_iter() {
                                    let key = key_opt.unwrap();
                                    let val = r.local_db.borrow_mut().get(&key);
                                    if let Some(v) = val {
                                        results.push((idx, Some(v)));
                                    } else if r.local_db.borrow_mut().table.is_tiered(&key).is_some() {
                                        let val = r.read_cold_key_local(&key).await;
                                        results.push((idx, val));
                                    } else {
                                        results.push((idx, None));
                                    }
                                }
                                let _ = responder.send(results);
                            });
                        } else {
                            let _ = responder.send(keys);
                        }
                    }
                    ShardMessage::Mset { db: mset_db, mut pairs, responder } => {
                        crate::connection::CURRENT_DB.set(mset_db);
                        {
                            let mut db = cross_shard_db.borrow_mut();
                            db.select_db(mset_db);
                            if cross_shard_aof.is_some() {
                                for (k, v) in &pairs {
                                    db.set(k.clone(), v.clone(), None);
                                }
                            } else {
                                for (k, v) in pairs.drain(..) {
                                    db.set(k, v, None);
                                }
                            }
                        }
                        crate::replication::log_shard_mutation(port, shard_id, cross_shard_aof.as_deref(), || crate::resp::Command::Mset(pairs.clone()));
                        cross_shard_router.check_auto_tier_after_write();
                        pairs.clear();
                        let _ = responder.send(pairs);
                    }
                    ShardMessage::ScatterMget {
                        shard_id,
                        mut keys,
                        descriptor,
                        no_touch,
                    } => {
                        let req_db = descriptor.db.load(std::sync::atomic::Ordering::Relaxed);
                        crate::connection::CURRENT_DB.set(req_db);
                        let prev_no_touch = crate::connection::CLIENT_NO_TOUCH.replace(no_touch);
                        let mut db = cross_shard_db.borrow_mut();
                        db.select_db(req_db);
                        let has_tiering = db.tier_manager.is_some();
                        let mut cold_keys = None;

                        if keys.len() == 1 {
                            let (idx, ref key) = keys[0];
                            let val = db.get(key);
                            if val.is_none() && has_tiering && db.table.is_tiered(key).is_some() {
                                cold_keys = Some(vec![(idx, key.clone())]);
                            } else {
                                descriptor.write_result(idx, val);
                            }
                        } else {
                            for (idx, key) in &keys {
                                let val = db.get(key);
                                if val.is_none() && has_tiering && db.table.is_tiered(key).is_some() {
                                    cold_keys.get_or_insert_with(Vec::new).push((*idx, key.clone()));
                                } else {
                                    descriptor.write_result(*idx, val);
                                }
                            }
                        }
                        crate::connection::CLIENT_NO_TOUCH.set(prev_no_touch);

                        if let Some(cold_keys) = cold_keys {
                            drop(db);
                            let r = cross_shard_router.clone();
                            monoio::spawn(async move {
                                if r.is_memory_constrained() {
                                    for (idx, key) in cold_keys {
                                        let val = r.stream_cold_read_local(&key).await;
                                        if val.is_some() {
                                            r.tier_stats.streaming_reads.fetch_add(1, Ordering::Relaxed);
                                            r.tier_stats.ram_misses.fetch_add(1, Ordering::Relaxed);
                                        }
                                        descriptor.write_result(idx, val);
                                    }
                                } else {
                                    for (idx, key) in cold_keys {
                                        r.load_local(&key).await;
                                        let val = r.local_db.borrow_mut().get(&key);
                                        descriptor.write_result(idx, val);
                                    }
                                }
                                keys.clear();
                                descriptor.recycle_keys(shard_id, keys);
                                descriptor.finish_shard();
                                drop(descriptor);
                            });
                        } else {
                            keys.clear();
                            descriptor.recycle_keys(shard_id, keys);
                            descriptor.finish_shard();
                            drop(descriptor);
                        }
                    }
                    ShardMessage::ScatterMset {
                        shard_id,
                        mut pairs,
                        descriptor,
                    } => {
                        let req_db = descriptor.db.load(std::sync::atomic::Ordering::Relaxed);
                        crate::connection::CURRENT_DB.set(req_db);
                        cross_shard_db.borrow_mut().select_db(req_db);
                        crate::replication::log_shard_mutation(port, shard_id, cross_shard_aof.as_deref(), || crate::resp::Command::Mset(pairs.clone()));
                        {
                            let mut db = cross_shard_db.borrow_mut();
                            for (k, v) in pairs.drain(..) {
                                db.set(k, v, None);
                            }
                        }
                        cross_shard_router.check_auto_tier_after_write();
                        descriptor.recycle_pairs(shard_id, pairs);
                        descriptor.finish_shard();
                        drop(descriptor);
                    }
                    ShardMessage::JsonMget { keys, path, responder } => {
                        let db = cross_shard_db.borrow();
                        let path_ref = path.as_str();
                        let mut results = Vec::with_capacity(keys.len());
                        for (idx, key) in keys {
                            let val = db.json_store.json_get(&key, &[path_ref]);
                            results.push((idx, val));
                        }
                        let _ = responder.send(results);
                    }
                    ShardMessage::RewriteAof {
                        dir,
                        shard_id,
                        responder,
                    } => {
                        // Built between this shard's other work (see
                        // `aof::rewrite_shard_aof_live`).
                        let db = cross_shard_db.clone();
                        let aof = cross_shard_aof.clone();
                        monoio::spawn(async move {
                            let res = crate::aof::rewrite_shard_aof_live(
                                &db,
                                &dir,
                                shard_id,
                                aof.as_ref(),
                            )
                            .await
                            .map_err(|e| e.to_string());
                            let _ = responder.send(res);
                        });
                    }
                    ShardMessage::SetSlotState { slot, state } => {
                        if state == crate::shard::SlotState::Stable {
                            cross_shard_slot_states.borrow_mut().remove(&slot);
                        } else {
                            cross_shard_slot_states.borrow_mut().insert(slot, state);
                        }
                    }
                    ShardMessage::SetSlotOwner { slot, owner } => {
                        cross_shard_slot_states.borrow_mut().remove(&slot);
                        let mut owners = cross_shard_slot_owners.borrow_mut();
                        if owners.is_empty() {
                            owners.reserve_exact(16384);
                            for s in 0..16384 {
                                owners.push(crate::router::slot_to_shard(s as u16, num_shards));
                            }
                        }
                        owners[slot as usize] = owner;
                    }
                    ShardMessage::SlotBarrier { responder } => {
                        let _ = responder.send(());
                    }
                    ShardMessage::WriteAof { responder } => {
                        // Like SyncAof, in a task so the write does not hold
                        // up this shard's mailbox.
                        let aof = cross_shard_aof.clone();
                        monoio::spawn(async move {
                            if let Some(aof) = aof {
                                let _ = crate::aof::AofWriter::write_through(&aof).await;
                            }
                            let _ = responder.send(());
                        });
                    }
                    ShardMessage::SyncAof { responder } => {
                        // In a task: the fsync must not hold up this shard's
                        // mailbox (appendfsync always sends one per batch).
                        let aof = cross_shard_aof.clone();
                        monoio::spawn(async move {
                            if let Some(aof) = aof {
                                let _ = crate::aof::AofWriter::sync_durable(&aof).await;
                            }
                            let _ = responder.send(());
                        });
                    }
                    ShardMessage::SaveRdbChunk { responder, arm_replica } => {
                        // Full syncs and shard flows arm their stream right
                        // around this, so the shard must not change between.
                        let mut buf = Vec::new();
                        let res = cross_shard_db.borrow_mut().save_rdb_chunk(&mut buf);
                        if let Some(id) = arm_replica
                            && res.is_ok()
                        {
                            crate::replication::get_replication_hub(port).arm_full_sync(id, shard_id);
                        }
                        let _ = responder.send(res.map(|()| buf));
                    }
                    ShardMessage::StartSyncImage { replica, responder } => {
                        let res = crate::shard::start_sync_image(&cross_shard_db.borrow(), shard_id, replica);
                        let _ = responder.send(res);
                    }
                    ShardMessage::SyncImageStep {
                        replica,
                        recycle,
                        responder,
                    } => {
                        let res = crate::shard::sync_image_step(&cross_shard_db.borrow(), replica, recycle);
                        let _ = responder.send(res);
                    }
                    ShardMessage::DropSyncImage { replica } => {
                        crate::shard::take_image(crate::shard::ImageOwner::FullSync(replica));
                    }
                    ShardMessage::StreamRdbChunk { sink, done } => {
                        let db = cross_shard_db.clone();
                        monoio::spawn(async move {
                            let res = crate::shard::save_rdb_chunk_yielding(&db, &sink).await;
                            let _ = done.send(res);
                        });
                    }
                    ShardMessage::Publish {
                        channel,
                        message,
                        responder,
                    } => {
                        let count = cross_shard_pubsub.borrow().publish(&channel, &message);
                        let _ = responder.send(count);
                    }
                    ShardMessage::Spublish {
                        channel,
                        message,
                        responder,
                    } => {
                        let count = cross_shard_pubsub.borrow().spublish(&channel, &message);
                        let _ = responder.send(count);
                    }
                    ShardMessage::Ssubscribe {
                        client_id,
                        channel,
                        sender,
                        is_resp3,
                        responder,
                    } => {
                        cross_shard_pubsub
                            .borrow_mut()
                            .ssubscribe(client_id, channel, sender, is_resp3);
                        let _ = responder.send(());
                    }
                    ShardMessage::Sunsubscribe {
                        client_id,
                        channel,
                        responder,
                    } => {
                        cross_shard_pubsub
                            .borrow_mut()
                            .sunsubscribe(client_id, &channel);
                        let _ = responder.send(());
                    }
                    ShardMessage::PubsubChannels { pattern, responder } => {
                        let channels = cross_shard_pubsub.borrow().channels(pattern.as_deref());
                        let _ = responder.send(channels);
                    }
                    ShardMessage::PubsubShardchannels { pattern, responder } => {
                        let channels =
                            cross_shard_pubsub.borrow().shard_channels(pattern.as_deref());
                        let _ = responder.send(channels);
                    }
                    ShardMessage::PubsubNumsub {
                        channels,
                        responder,
                    } => {
                        let hub = cross_shard_pubsub.borrow();
                        let counts = channels
                            .into_iter()
                            .map(|ch| {
                                let cnt = hub.numsub(&ch);
                                (ch, cnt)
                            })
                            .collect();
                        let _ = responder.send(counts);
                    }
                    ShardMessage::PubsubShardnumsub {
                        channels,
                        responder,
                    } => {
                        let hub = cross_shard_pubsub.borrow();
                        let counts = channels
                            .into_iter()
                            .map(|ch| {
                                let cnt = hub.shard_numsub(&ch);
                                (ch, cnt)
                            })
                            .collect();
                        let _ = responder.send(counts);
                    }
                    ShardMessage::PubsubNumpat { responder } => {
                        let pats = cross_shard_pubsub.borrow().patterns.keys().cloned().collect();
                        let _ = responder.send(pats);
                    }
                    ShardMessage::RemoveClientPubSub { client_id } => {
                        cross_shard_pubsub
                            .borrow_mut()
                            .remove_client_with_presence(client_id, None);
                    }
                    ShardMessage::InitSearchIndex {
                        schema,
                        aof_record,
                        responder,
                    } => {
                        cross_shard_db.borrow_mut().init_search_index(*schema);
                        if let (Some(aof), Some(rec)) = (cross_shard_aof.as_deref(), aof_record) {
                            aof.borrow_mut().append(&rec);
                        }
                        let _ = responder.send(());
                    }
                    ShardMessage::DropSearchIndex {
                        name,
                        aof_record,
                        responder,
                    } => {
                        let dropped = cross_shard_db.borrow_mut().drop_search_index(&name);
                        if dropped
                            && let (Some(aof), Some(rec)) = (cross_shard_aof.as_deref(), aof_record)
                        {
                            aof.borrow_mut().append(&rec);
                        }
                        let _ = responder.send(dropped);
                    }
                    ShardMessage::SearchQuery {
                        index,
                        ast,
                        options,
                        responder,
                    } => {
                        let db = cross_shard_db.borrow();
                        let res = if let Some(idx) = db.search_indices.get(&index) {
                            crate::search::execute_search(idx, &ast, &options)
                        } else {
                            (0, Vec::new())
                        };
                        let _ = responder.send(res);
                    }
                    ShardMessage::FlushDb { db, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut db_ref = cross_shard_db.borrow_mut();
                        db_ref.select_db(db);
                        db_ref.flushdb();
                        crate::block::get_block_hub_for_port(db_ref.port)
                            .lock()
                            .notify_all_streams(&mut db_ref);
                        drop(db_ref);
                        if let Some(aof) = cross_shard_aof.as_deref()
                            && let Some(bytes) = crate::aof::command_to_resp(&crate::resp::Command::Flushdb)
                        {
                            aof.borrow_mut().append(&bytes);
                        }
                        cross_shard_router.publish_memory_state();
                        let _ = responder.send(());
                    }
                    ShardMessage::FlushAll { responder } => {
                        let mut db_ref = cross_shard_db.borrow_mut();
                        db_ref.flushall();
                        crate::block::get_block_hub_for_port(db_ref.port)
                            .lock()
                            .notify_all_streams(&mut db_ref);
                        drop(db_ref);
                        if let Some(aof) = cross_shard_aof.as_deref()
                            && let Some(bytes) = crate::aof::command_to_resp(&crate::resp::Command::Flushall)
                        {
                            aof.borrow_mut().append(&bytes);
                        }
                        cross_shard_router.publish_memory_state();
                        let _ = responder.send(());
                    }
                    ShardMessage::SwapDb { db1, db2, responder } => {
                        cross_shard_db.borrow_mut().swap_dbs(db1, db2);
                        if let Some(aof) = cross_shard_aof.as_deref()
                            && let Some(bytes) = crate::aof::command_to_resp(&crate::resp::Command::Swapdb {
                                db1: db1 as i64,
                                db2: db2 as i64,
                            })
                        {
                            aof.borrow_mut().append(&bytes);
                        }
                        let _ = responder.send(());
                    }
                    ShardMessage::KeyspaceStats { responder } => {
                        let stats = cross_shard_db.borrow().keyspace_stats_by_db();
                        let _ = responder.send(stats);
                    }
                    ShardMessage::Keys { db, pattern, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let keys = sdb.keys(&pattern);
                        let _ = responder.send(keys);
                    }
                    ShardMessage::Scan { db, params, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let res = sdb.scan(
                            params.slot,
                            params.pattern.as_deref(),
                            params.count,
                            params.key_type.as_deref(),
                        );
                        let _ = responder.send(res);
                    }
                    ShardMessage::RandomKey { db, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let key = sdb.random_key();
                        let _ = responder.send(key);
                    }
                    ShardMessage::ExpireTime {
                        db,
                        key,
                        in_millis,
                        responder,
                    } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let res = sdb.expiretime(&key, in_millis);
                        let _ = responder.send(res);
                    }
                    ShardMessage::AcquireTxLock { tx_id, responder } => {
                        let mut lock = cross_shard_tx_lock.borrow_mut();
                        if lock.is_none() {
                            *lock = Some(tx_id);
                            let _ = responder.send(());
                        } else {
                            cross_shard_tx_waiters.borrow_mut().push_back((tx_id, responder));
                        }
                    }
                    ShardMessage::ReleaseTxLock { tx_id } => {
                        let mut lock = cross_shard_tx_lock.borrow_mut();
                        if *lock == Some(tx_id) {
                            *lock = crate::router::grant_next_tx_lock(
                                &mut cross_shard_tx_waiters.borrow_mut(),
                            );
                        }
                    }
                    ShardMessage::RestoreRdbChunk {
                        data,
                        aof_dir,
                        responder,
                    } => {
                        let mut db = cross_shard_db.borrow_mut();
                        let aof = aof_dir.as_deref().zip(cross_shard_aof.as_ref());
                        let _ = responder.send(db.load_full_sync_rdb(&data, shard_id, num_shards, aof));
                    }
                    ShardMessage::ExecuteReplicaCmd { db, cmd, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let mut dummy_out = Vec::new();
                        let aof_ref = cross_shard_aof.as_deref();
                        crate::connection::apply_replicated_command(&cmd, &mut sdb, &mut dummy_out, aof_ref);
                        let _ = responder.send(());
                    }
                    ShardMessage::ExecuteReplicaCmds { db, cmds, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let mut dummy_out = Vec::new();
                        let aof_ref = cross_shard_aof.as_deref();
                        for cmd in &cmds {
                            crate::connection::apply_replicated_command(
                                cmd,
                                &mut sdb,
                                &mut dummy_out,
                                aof_ref,
                            );
                        }
                        let _ = responder.send(());
                    }
                    ShardMessage::FlushCommandStats { responder } => {
                        crate::connection::flush_local_cmd_stats();
                        let _ = responder.send(());
                    }
                    ShardMessage::ResetCommandStats { responder } => {
                        crate::connection::reset_local_cmd_stats();
                        let _ = responder.send(());
                    }
                    ShardMessage::NotifyList { db: target_db, keys } => {
                        crate::connection::CURRENT_DB.set(target_db);
                        let mut db = cross_shard_db.borrow_mut();
                        db.select_db(target_db);
                        let hub_arc = crate::block::get_block_hub_for_port(db.port);
                        let mut hub = hub_arc.lock();
                        for k in keys {
                            hub.notify_stream(&mut db, &k);
                            hub.notify_list(&mut db.table, &k);
                            hub.notify_zset(&mut db.table, &k);
                        }
                    }
                    ShardMessage::TierSpill { key, responder } => {
                        let r = cross_shard_router.clone();
                        monoio::spawn(async move {
                            let ok = r.spill_local(&key).await;
                            let _ = responder.send(ok);
                        });
                    }
                    ShardMessage::TierLoad { key, responder } => {
                        let r = cross_shard_router.clone();
                        monoio::spawn(async move {
                            let ok = r.load_local(&key).await;
                            let _ = responder.send(ok);
                        });
                    }
                    ShardMessage::TierSpillAll { responder } => {
                        let r = cross_shard_router.clone();
                        monoio::spawn(async move {
                            let count = r.spill_all().await;
                            let _ = responder.send(count);
                        });
                    }
                    ShardMessage::TierCool { key, responder } => {
                        let r = cross_shard_router.clone();
                        monoio::spawn(async move {
                            let ok = r.cool_local(&key).await;
                            let _ = responder.send(ok);
                        });
                    }
                    ShardMessage::TierDecommit { key, responder } => {
                        let count = cross_shard_router.decommit_local(key.as_deref());
                        let _ = responder.send(count);
                    }
                    ShardMessage::TierAutoSpill { responder } => {
                        let r = cross_shard_router.clone();
                        monoio::spawn(async move {
                            r.check_auto_tier_local().await;
                            let used = r.local_db.borrow().total_used_memory();
                            let _ = responder.send(used);
                        });
                    }
                    ShardMessage::MemoryBreakdown { responder } => {
                        let _ = responder.send(cross_shard_db.borrow().memory_breakdown());
                    }
                    ShardMessage::GetUsedMemory { responder } => {
                        // Also republishes this shard's allocator count.
                        cross_shard_router.publish_memory_state();
                        let used = crate::router::shard_size_for_eviction(
                            &cross_shard_router.tier_stats,
                            &cross_shard_db.borrow(),
                        );
                        let _ = responder.send(used);
                    }
                    ShardMessage::EvictUntilUnder {
                        policy,
                        floor,
                        client_id,
                        responder,
                    } => {
                        // Invalidations owed to the requesting client ride
                        // back in the reply, ahead of its command response.
                        let ((), track) = crate::connection::with_evict_capture(client_id, || {
                            cross_shard_router.evict_local_until_under(&policy, floor);
                        });
                        let used = crate::router::shard_size_for_eviction(
                            &cross_shard_router.tier_stats,
                            &cross_shard_db.borrow(),
                        );
                        let _ = responder.send((used, track));
                    }
                    ShardMessage::TryEvictOneKey { policy, responder } => {
                        let freed = cross_shard_db
                            .borrow_mut()
                            .try_evict_one_key(&policy, cross_shard_router.aof.as_deref());
                        let _ = responder.send(freed);
                    }
                    ShardMessage::StreamColdRead { key, responder } => {
                        let r = cross_shard_router.clone();
                        monoio::spawn(async move {
                            let val = r.stream_cold_read_local(&key).await;
                            let _ = responder.send(val);
                        });
                    }
                    ShardMessage::TierGc { responder } => {
                        let reclaimed = cross_shard_router.gc_local();
                        let _ = responder.send(reclaimed);
                    }
                    ShardMessage::TierSnapshot { backup_dir, responder } => {
                        let r = cross_shard_router.clone();
                        monoio::spawn(async move {
                            let res = r.snapshot_local(&backup_dir).await;
                            let _ = responder.send(res);
                        });
                    }
                    ShardMessage::FlushSlots { ranges, responder } => {
                        let count = cross_shard_db.borrow_mut().flush_slots(&ranges);
                        let _ = responder.send(count);
                    }
                    ShardMessage::Stick { keys, responder } => {
                        let mut db = cross_shard_db.borrow_mut();
                        let mut count = 0;
                        for k in keys {
                            if db.stick(k) {
                                count += 1;
                            }
                        }
                        let _ = responder.send(count);
                    }
                    ShardMessage::Unstick { keys, responder } => {
                        let mut db = cross_shard_db.borrow_mut();
                        let mut count = 0;
                        for k in &keys {
                            if db.unstick(k) {
                                count += 1;
                            }
                        }
                        let _ = responder.send(count);
                    }
                    ShardMessage::IsSticky { key, responder } => {
                        let sticky = cross_shard_db.borrow().is_sticky(&key);
                        let _ = responder.send(sticky);
                    }
                    ShardMessage::Delex { db, key, condition, responder } => {
                        crate::connection::CURRENT_DB.set(db);
                        let mut sdb = cross_shard_db.borrow_mut();
                        sdb.select_db(db);
                        let should_del = match condition {
                            None => true,
                            Some(cond) => {
                                let (op, expected) = &*cond;
                                if let Some(val) = sdb.get(&key) {
                                    match op.to_uppercase().as_str() {
                                        "IFEQ" => val == expected,
                                        "IFNE" => val != expected,
                                        "IFGT" => val > expected,
                                        "IFLT" => val < expected,
                                        _ => false,
                                    }
                                } else {
                                    false
                                }
                            }
                        };
                        if should_del {
                            let deleted = sdb.del(&key);
                            let _ = responder.send(deleted);
                        } else {
                            let _ = responder.send(false);
                        }
                    }
                }
                burst += 1;
                if burst >= 64 {
                    // recv_async is Ready while messages are queued, so
                    // without a yield a steady stream from other shards
                    // would keep this task running and starve the local
                    // connections on this thread.
                    aof_write_after_mailbox(&cross_shard_aof, base_port);
                    crate::mailbox::yield_now().await;
                    break;
                }
                match rx.try_recv() {
                    Ok(next) => msg = next,
                    Err(_) => {
                        aof_write_after_mailbox(&cross_shard_aof, base_port);
                        break;
                    }
                }
            }
        }
    });

        crate::log_verbose!(
            "[Shard {}/{}] Worker started and listening on {} via {}",
            shard_id,
            num_shards,
            listen_addrs
                .iter()
                .map(|a| a.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            crate::connection::io_driver_name()
        );
        // Redis's readiness line (log scrapers, supervisors and the Redis
        // test harness wait for it), once every shard of this server is up.
        {
            static READY: std::sync::LazyLock<parking_lot::Mutex<std::collections::HashMap<u16, usize>>> =
                std::sync::LazyLock::new(Default::default);
            let mut ready = READY.lock();
            let n = ready.entry(base_port).or_insert(0);
            *n += 1;
            if *n == num_shards {
                router.calibrate_real_memory();
                crate::log_notice!("Ready to accept connections tcp");
                let _ = crate::shutdown::sd_notify("READY=1\n");
            }
        }

        // 4.9 Spawn TLS Accept loop if enabled
        if !tls_listeners.is_empty()
            && let Some(tls_cfg) = tls_config
        {
            // Bit 46 keeps TLS ids apart from both the plain accept loop's
            // `(shard_id << 48) + n` and the adopted-connection range (bit 47).
            let next_tls_client_id = Rc::new(std::cell::Cell::new(
                ((shard_id as u64) << 48) | (1u64 << 46) | 1,
            ));
            for tls_listener in tls_listeners {
            let r = router.clone();
            let reg = client_registry.clone();
            let tls_cfg = tls_cfg.clone();
            let next_tls_client_id = next_tls_client_id.clone();
            monoio::spawn(async move {
                loop {
                    let Some(accept_res) = accept_until_shutdown(&tls_listener).await else {
                        break;
                    };
                    match accept_res {
                        Ok((mut stream, client_addr)) => {
                            let _ = stream.set_nodelay(true);
                            crate::connection::apply_tcp_keepalive(
                                std::os::unix::io::AsRawFd::as_raw_fd(&stream),
                            );
                            let client_id = next_tls_client_id.get();
                            next_tls_client_id.set(client_id + 1);
                            let router_clone = r.clone();
                            let reg_clone = reg.clone();
                            let s_cfg =
                                crate::tls::active_tls_server_config(router_clone.base_port)
                                    .unwrap_or_else(|| tls_cfg.server_config.clone());
                            monoio::spawn(async move {
                                let res = catch_unwind_async(async move {
                                    let mut session = match crate::tls::TlsSession::new(s_cfg) {
                                        Ok(s) => s,
                                        Err(e) => {
                                            crate::log_warning!("[Shard {}] Failed to create TlsSession: {}", shard_id, e);
                                            return;
                                        }
                                    };
                                    match monoio::time::timeout(
                                        TLS_HANDSHAKE_TIMEOUT,
                                        session.handshake_monoio(&mut stream),
                                    )
                                    .await
                                    {
                                        Ok(Ok(())) => {}
                                        Ok(Err(e)) => {
                                            // Like Redis, failed handshakes are verbose: any
                                            // port scanner would flood the log otherwise.
                                            crate::log_verbose!("[Shard {}] TLS handshake error: {}", shard_id, e);
                                            return;
                                        }
                                        Err(_) => {
                                            crate::log_verbose!("[Shard {}] TLS handshake timed out", shard_id);
                                            return;
                                        }
                                    }
                                    let mut transport =
                                        crate::tls::TlsTransport::new(stream, session);
                                    if crate::netsec::protected_mode_denies(
                                        router_clone.base_port,
                                        client_addr.ip(),
                                    ) {
                                        let _ = crate::transport::ClientTransport::write_all(
                                            &mut transport,
                                            crate::netsec::PROTECTED_MODE_DENIED.to_vec(),
                                        )
                                        .await;
                                        return;
                                    }
                                    crate::connection::handle_client(
                                        transport,
                                        client_addr,
                                        client_id,
                                        reg_clone,
                                        router_clone,
                                    )
                                    .await;
                                })
                                .await;
                                if let Err(e) = res {
                                    crate::connection::inc_isolated_panics();
                                    crate::log_warning!(
                                        "Panic isolated in TLS client connection id={}: {}",
                                        client_id,
                                        crate::log::panic_message(&*e)
                                    );
                                }
                            });
                        }
                        Err(e) => {
                            crate::log_warning!("[Shard {}] TLS accept error: {}", shard_id, e);
                        }
                    }
                }
            });
            }
        }

        // 4.75 `replicaof` from the config file: start syncing once the
        // shards are up, as if REPLICAOF had been sent.
        if shard_id == 0
            && let Some((host, mport)) = crate::replication::take_startup_replicaof(base_port)
        {
            crate::replication::start_replica_sync(router.base_port, host, mport, (*router).clone());
        }

        // 4.8 `save <secs> <changes>` schedule (Redis serverCron).
        if shard_id == 0 {
            let r = router.clone();
            // Counts start now: data loaded at startup is not "changes".
            crate::snapshot::state(r.base_port);
            // The AOF as loaded is the base automatic rewrites grow from.
            let aof_on = r.aof.is_some();
            let rewrite_state = crate::snapshot::aof_rewrite_state(r.base_port);
            if aof_on {
                rewrite_state.set_base_size(crate::aof::aof_files_size(&r.db_dir, r.num_shards));
            }
            monoio::spawn(async move {
                let mut tick: u64 = 0;
                while !crate::shutdown::is_shutting_down() {
                    monoio::time::sleep(std::time::Duration::from_millis(100)).await;
                    tick += 1;
                    let snap = crate::snapshot::state(r.base_port);
                    // `in_progress` too: a save releases `is_saving` just
                    // before it records what it covered.
                    if r.is_saving.load(std::sync::atomic::Ordering::SeqCst)
                        || snap.in_progress()
                        || rewrite_state.in_progress()
                    {
                        continue;
                    }
                    let now = crate::snapshot::unix_now();
                    let points = crate::config::save_points(r.base_port);
                    if !points.is_empty() && snap.save_due(&points, now) {
                        crate::log_notice!(
                            "{} changes in {} seconds. Saving...",
                            snap.changes_since_last_save(),
                            now.saturating_sub(snap.last_save_unix())
                        );
                        let _ = r.bgsave().await;
                        continue;
                    }
                    // `auto-aof-rewrite-*`, checked once a second (it stats
                    // every shard's file).
                    if aof_on && tick.is_multiple_of(10) && !rewrite_state.auto_rewrite_limited(now) {
                        let cfg = crate::aof::auto_rewrite_config(r.base_port);
                        if let Some(growth) = crate::aof::auto_rewrite_due(
                            cfg.percentage.load(std::sync::atomic::Ordering::Relaxed),
                            cfg.min_size.load(std::sync::atomic::Ordering::Relaxed),
                            crate::aof::aof_files_size(&r.db_dir, r.num_shards),
                            rewrite_state.base_size(),
                        ) {
                            crate::log_notice!(
                                "Starting automatic rewriting of AOF on {}% growth",
                                growth
                            );
                            let _ = r.bgrewriteaof().await;
                        }
                    }
                }
            });
        }

        // 4.9 SIGTERM/SIGINT: save like a plain SHUTDOWN, and stay up if that
        // fails (Redis logs "can't exit" and keeps serving).
        if shard_id == 0 {
            let r = router.clone();
            monoio::spawn(async move {
                while !crate::shutdown::is_shutting_down() {
                    monoio::time::sleep(std::time::Duration::from_millis(50)).await;
                    if !crate::shutdown::take_shutdown_signal() {
                        continue;
                    }
                    crate::log_warning!(
                        "Received {} scheduling shutdown...",
                        crate::shutdown::last_shutdown_signal_name()
                    );
                    match r.save_before_shutdown(None, 0).await {
                        Ok(()) => crate::shutdown::request_shutdown(),
                        Err(e) => crate::log_warning!(
                            "Error trying to save the DB, can't exit: {}. Fix the problem and send the signal again, or use SHUTDOWN NOSAVE.",
                            e
                        ),
                    }
                }
            });
        }

        // 5. Accept loops: one per bound address, sharing the client id sequence.
        let next_client_id = Rc::new(std::cell::Cell::new(((shard_id as u64) << 48) + 1));
        let mut listeners = listeners.into_iter();
        let primary = listeners.next().expect("bind_all returns at least one listener");
        for extra in listeners {
            monoio::spawn(accept_loop(
                extra,
                next_client_id.clone(),
                router.clone(),
                client_registry.clone(),
                shard_id,
                num_shards,
                cluster_enabled,
            ));
        }
        accept_loop(
            primary,
            next_client_id,
            router.clone(),
            client_registry.clone(),
            shard_id,
            num_shards,
            cluster_enabled,
        )
        .await;

        // 6. Drain, then sync the AOF for the last time.
        drain_clients(&client_registry).await;
        crate::shutdown::mark_shard_drained();
        let deadline = std::time::Instant::now() + crate::shutdown::SHARD_BARRIER_TIMEOUT;
        while !crate::shutdown::all_shards_drained(num_shards)
            && std::time::Instant::now() < deadline
        {
            monoio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        if let Some(aof) = aof_writer {
            let _ = crate::aof::AofWriter::sync_durable(&aof).await;
        }
    });
}

/// A TLS client must finish its handshake within this time, so idle or
/// stalled sockets cannot pile up before they are ever counted as clients.
const TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Stops reading from this shard's clients and waits for them to finish.
///
/// `shutdown(SHUT_RD)` makes an idle client's pending read return EOF right
/// away, while a client in the middle of a command still runs it to the end
/// and writes the reply before its next read sees EOF. Repeated each round so
/// connections adopted from another shard meanwhile are drained too. Clients
/// that never get back to reading (blocked commands) are dropped at the
/// timeout.
async fn drain_clients(
    client_registry: &Rc<RefCell<hashbrown::HashMap<u64, crate::connection::ClientInfo>>>,
) {
    let deadline = std::time::Instant::now() + crate::shutdown::CLIENT_DRAIN_TIMEOUT;
    loop {
        let fds: Vec<_> = client_registry
            .borrow()
            .values()
            .map(|c| c.raw_fd)
            .collect();
        if fds.is_empty() || std::time::Instant::now() >= deadline {
            return;
        }
        for fd in fds {
            // SAFETY: shutdown(2) takes no pointers. The fds were collected from this
            // shard's registry with no await since, and the connection removes its registry entry (ClientCleanup / PubsubCleanup) before its socket is closed, so the fd is not a reused number.
            unsafe { libc::shutdown(fd, libc::SHUT_RD) };
        }
        monoio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

/// How often a listener waiting for a client re-checks for shutdown.
const ACCEPT_SHUTDOWN_POLL: std::time::Duration = std::time::Duration::from_millis(200);

type Accepted = std::io::Result<(monoio::net::TcpStream, std::net::SocketAddr)>;

/// Waits for the next client on `listener`, or `None` once shutdown starts.
async fn accept_until_shutdown(listener: &monoio::net::TcpListener) -> Option<Accepted> {
    accept_polling(
        listener,
        ACCEPT_SHUTDOWN_POLL,
        crate::shutdown::is_shutting_down,
    )
    .await
}

/// Waits for the next client on `listener`, checking `stop` every `poll`.
///
/// The accept op is kept alive across those checks rather than wrapped in a
/// fresh `timeout(.., listener.accept())` each time: dropping an in-flight
/// io_uring accept only requests cancellation, and if the kernel has already
/// accepted a connection by then, monoio discards the completion. The
/// connection stays established, with its fd leaked and its requests unread,
/// and the client hangs on its first command.
async fn accept_polling(
    listener: &monoio::net::TcpListener,
    poll: std::time::Duration,
    stop: fn() -> bool,
) -> Option<Accepted> {
    let mut accept = std::pin::pin!(listener.accept());
    loop {
        if stop() {
            return None;
        }
        if let Ok(res) = monoio::time::timeout(poll, accept.as_mut()).await {
            return Some(res);
        }
    }
}

/// Accepts plain-TCP clients on one listener until shutdown.
async fn accept_loop(
    listener: monoio::net::TcpListener,
    next_client_id: Rc<std::cell::Cell<u64>>,
    router: Rc<Router>,
    client_registry: Rc<RefCell<hashbrown::HashMap<u64, crate::connection::ClientInfo>>>,
    shard_id: usize,
    num_shards: usize,
    cluster_enabled: bool,
) {
    loop {
        let Some(accept_res) = accept_until_shutdown(&listener).await else {
            break;
        };
        match accept_res {
            Ok((stream, client_addr)) => {
                let raw_fd = std::os::unix::io::AsRawFd::as_raw_fd(&stream);
                if crate::netsec::protected_mode_denies(router.base_port, client_addr.ip()) {
                    let msg = crate::netsec::PROTECTED_MODE_DENIED;
                    // SAFETY: `msg` is a 'static byte slice and the length matches; `raw_fd` is
                    // `stream`'s socket, which is still open (dropped below).
                    unsafe {
                        libc::send(
                            raw_fd,
                            msg.as_ptr() as *const libc::c_void,
                            msg.len(),
                            libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
                        );
                    }
                    drop(stream);
                    continue;
                }
                let _ = stream.set_nodelay(true);
                crate::connection::apply_tcp_keepalive(raw_fd);
                // SAFETY: `yes` is a live c_int and optlen is its size; `raw_fd` belongs to
                // `stream`, which is alive here.
                unsafe {
                    let yes: libc::c_int = 1;
                    libc::setsockopt(
                        raw_fd,
                        libc::IPPROTO_TCP,
                        libc::TCP_NODELAY,
                        &yes as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&yes) as libc::socklen_t,
                    );
                    libc::setsockopt(
                        raw_fd,
                        libc::IPPROTO_TCP,
                        libc::TCP_QUICKACK,
                        &yes as *const _ as *const libc::c_void,
                        std::mem::size_of_val(&yes) as libc::socklen_t,
                    );
                }
                let r = router.clone();
                let client_id = next_client_id.get();
                next_client_id.set(client_id + 1);
                let reg = client_registry.clone();

                // SO_REUSEPORT picked this shard by hashing the 4-tuple,
                // which is badly uneven (see crate::conn_balance). If we
                // already hold more connections than the least loaded
                // shard, hand this one over instead of compounding the
                // imbalance.
                //
                // Cluster mode is exempt: there each shard binds its own
                // port (base_port + shard_id) and the client picks the
                // shard on purpose to reach the slots it owns. Moving the
                // connection elsewhere would answer from the wrong shard
                // and break MOVED redirection. It also has nothing to fix,
                // since the distribution is client-chosen, not hashed.
                let owner = if cluster_enabled {
                    crate::conn_balance::register_conn(shard_id);
                    shard_id
                } else {
                    crate::conn_balance::claim_owner(shard_id, num_shards)
                };
                if owner != shard_id {
                    // claim_owner already reserved the target's slot, so
                    // the target must not register again on arrival; it
                    // only unregisters when the connection ends.
                    //
                    // Release the fd without closing it; the target shard
                    // adopts it. into_raw_fd consumes the stream so no Drop
                    // runs and the fd stays valid.
                    let fd = std::os::unix::io::IntoRawFd::into_raw_fd(stream);
                    let handed = router.senders[owner]
                        .send(ShardMessage::AdoptConnection {
                            fd,
                            peer: client_addr,
                        })
                        .is_ok();
                    if !handed {
                        // Target mailbox is gone; we still own the fd, so
                        // close it rather than leak it, and give back the
                        // slot that was reserved for it.
                        crate::conn_balance::unregister_conn(owner);
                        // SAFETY: The message (and with it the fd) was not handed over, so we still
                        // own the fd from into_raw_fd and close it exactly once.
                        unsafe { libc::close(fd) };
                    }
                } else {
                    monoio::spawn(async move {
                        let res = catch_unwind_async(async move {
                            handle_connection(stream, client_addr, client_id, reg, r).await;
                        })
                        .await;
                        crate::conn_balance::unregister_conn(shard_id);
                        if let Err(e) = res {
                            crate::connection::inc_isolated_panics();
                            crate::log_warning!(
                                "Panic isolated in client connection id={}: {}",
                                client_id,
                                crate::log::panic_message(&*e)
                            );
                        }
                    });
                }
            }
            Err(e) => {
                crate::log_warning!("[Shard {}] Accept error: {}", shard_id, e);
            }
        }
    }
}

/// After a run of mailbox messages: writes the AOF bytes they appended to
/// the kernel (not under `appendfsync always`, which syncs its own way), so
/// connections waiting to reply to those writes (`Router::write_aof_through`)
/// see them written without sending this shard a message.
#[inline]
fn aof_write_after_mailbox(
    aof: &Option<std::rc::Rc<std::cell::RefCell<crate::aof::AofWriter>>>,
    base_port: u16,
) {
    if let Some(aof) = aof
        && aof.borrow().has_pending()
        && !crate::aof::appendfsync_is_always(base_port)
    {
        let _ = aof.borrow_mut().write_pending_sync();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_thread_stack_size_supports_deep_frames() {
        const { assert!(SHARD_THREAD_STACK_SIZE >= 16 * 1024 * 1024) };
        // A frame far larger than the 2 MiB std default must fit.
        let h = std::thread::Builder::new()
            .stack_size(SHARD_THREAD_STACK_SIZE)
            .spawn(|| {
                let buf = [7u8; 8 * 1024 * 1024];
                std::hint::black_box(&buf)[buf.len() - 1]
            })
            .expect("spawn");
        assert_eq!(h.join().unwrap(), 7);
    }

    #[monoio::test]
    async fn test_catch_unwind_async_catches_panic() {
        let fut = catch_unwind_async(async {
            panic!("deliberate panic for test");
        });
        let res = fut.await;
        assert!(res.is_err());
    }

    #[monoio::test]
    async fn test_catch_unwind_async_success() {
        let fut = catch_unwind_async(async { 42 });
        let res = fut.await;
        assert_eq!(res.unwrap(), 42);
    }

    /// With a poll interval far shorter than a connect, nearly every accept
    /// races its timeout. Dropping the accept on each tick lost a connection
    /// within the first ~100 here; every client must get its byte.
    #[test]
    fn test_accept_polling_never_loses_a_connection() {
        const CONNS: usize = 2000;
        let std_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        std_listener.set_nonblocking(true).unwrap();
        let addr = std_listener.local_addr().unwrap();
        let client = std::thread::spawn(move || {
            use std::io::Read;
            for i in 0..CONNS {
                let mut c = std::net::TcpStream::connect(addr).unwrap();
                c.set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut b = [0u8; 1];
                if let Err(e) = c.read_exact(&mut b) {
                    panic!("connection {i} was accepted but never served: {e}");
                }
            }
        });
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_timer()
            .build()
            .unwrap();
        rt.block_on(async move {
            use monoio::io::AsyncWriteRentExt;
            let listener = monoio::net::TcpListener::from_std(std_listener).unwrap();
            // Bounded, so a lost connection fails via the client's panic
            // instead of leaving this loop waiting for it forever.
            let _ = monoio::time::timeout(std::time::Duration::from_secs(10), async {
                for _ in 0..CONNS {
                    let (mut stream, _) =
                        accept_polling(&listener, std::time::Duration::from_micros(10), || false)
                            .await
                            .unwrap()
                            .unwrap();
                    stream.write_all(b"y").await.0.unwrap();
                }
            })
            .await;
        });
        client.join().unwrap();
    }

    #[test]
    fn test_sadd_compact_resp_encoding() {
        assert_eq!(crate::shard::CompactResp::INT_1.as_slice(), b":1\r\n");
        assert_eq!(crate::shard::CompactResp::INT_0.as_slice(), b":0\r\n");
        assert_eq!(
            crate::shard::CompactResp::from_integer(42).as_slice(),
            b":42\r\n"
        );
    }
}
