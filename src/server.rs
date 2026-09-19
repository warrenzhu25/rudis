use socket2::{Domain, Protocol, Socket, Type};
use std::cell::RefCell;
use std::future::Future;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

pub struct CatchUnwind<F> {
    inner: F,
}

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

use crate::connection::{execute_local_command, handle_connection};
use crate::resp::Command;
use crate::router::Router;
use crate::shard::{ShardDb, ShardMessage};

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

    let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
        .enable_timer()
        .build()
        .expect("Failed to initialize Monoio io_uring runtime");

    rt.block_on(async move {
        let base_port = port;
        let shard_port = if cluster_enabled {
            base_port + shard_id as u16
        } else {
            base_port
        };

        // 1. Configure socket with SO_REUSEPORT and SO_REUSEADDR
        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))
            .expect("Failed to create socket");
        socket
            .set_reuse_port(true)
            .expect("Failed to set SO_REUSEPORT");
        socket
            .set_reuse_address(true)
            .expect("Failed to set SO_REUSEADDR");
        socket
            .set_nonblocking(true)
            .expect("Failed to set non-blocking");
        let _ = socket.set_recv_buffer_size(512 * 1024);
        let _ = socket.set_send_buffer_size(512 * 1024);

        let addr: SocketAddr = format!("0.0.0.0:{}", shard_port)
            .parse()
            .expect("Invalid address");
        socket.bind(&addr.into()).expect("Failed to bind socket");
        socket.listen(4096).expect("Failed to listen on socket");

        let listener = monoio::net::TcpListener::from_std(socket.into())
            .expect("Failed to convert socket into Monoio TcpListener");

        let tls_listener = if let Some(ref tls_cfg) = tls_config {
            let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))
                .expect("Failed to create TLS socket");
            socket
                .set_reuse_port(true)
                .expect("Failed to set SO_REUSEPORT on TLS socket");
            socket
                .set_reuse_address(true)
                .expect("Failed to set SO_REUSEADDR on TLS socket");
            socket
                .set_nonblocking(true)
                .expect("Failed to set non-blocking on TLS socket");
            let _ = socket.set_recv_buffer_size(512 * 1024);
            let _ = socket.set_send_buffer_size(512 * 1024);

            let addr: SocketAddr = format!("0.0.0.0:{}", tls_cfg.tls_port)
                .parse()
                .expect("Invalid TLS address");
            socket.bind(&addr.into()).expect("Failed to bind TLS socket");
            socket.listen(4096).expect("Failed to listen on TLS socket");

            let listener = monoio::net::TcpListener::from_std(socket.into())
                .expect("Failed to convert TLS socket into Monoio TcpListener");
            Some(listener)
        } else {
            None
        };

        // 2. Pure thread-local Shard DB (no Mutex, no Arc)
        let local_db = Rc::new(RefCell::new(ShardDb::new(port)));

        // 2.5 RDB Snapshot Restore on startup
        // Follow Redis specification: if AOF is enabled, AOF is authoritative; otherwise load RDB.
        if !aof_config.enabled {
            let rdb_path = aof_config.dir.join("dump.rdb");
            if rdb_path.exists() {
                match crate::table::load_rdb(&rdb_path, &mut local_db.borrow_mut(), shard_id, num_shards) {
                    Ok(n) => {
                        if n > 0 {
                            println!("[Shard {}/{}] Restored {} keys from {:?}", shard_id, num_shards, n, rdb_path);
                        }
                    }
                    Err(e) => {
                        eprintln!("[Shard {}/{}] Failed to restore from RDB: {}", shard_id, num_shards, e);
                    }
                }
            }
        }

        // 3. AOF Replay on startup & Open AofWriter
        let aof_path = aof_config.dir.join(format!("appendonly-{}.aof", shard_id));
        if aof_config.enabled {
            match crate::aof::replay_aof(&aof_path, &mut local_db.borrow_mut()) {
                Ok(n) => {
                    if n > 0 {
                        println!(
                            "[Shard {}] Replayed {} commands from {:?}",
                            shard_id, n, aof_path
                        );
                    }
                }
                Err(e) => {
                    eprintln!("[Shard {}] Failed to replay AOF: {}", shard_id, e);
                }
            }
        }

        let aof_writer = if aof_config.enabled {
            match crate::aof::AofWriter::open(aof_path).await {
                Ok(w) => {
                    let writer = Rc::new(RefCell::new(w));
                    let flush_writer = writer.clone();
                    let fsync_every_sec = aof_config.fsync_every_sec;
                    monoio::spawn(async move {
                        let mut ticker = 0u64;
                        loop {
                            monoio::time::sleep(std::time::Duration::from_millis(50)).await;
                            let flush_chunk = flush_writer.borrow_mut().take_flush_chunk();
                            if let Some((file, chunk, offset)) = flush_chunk {
                                let _ = file.write_all_at(chunk, offset).await;
                            }
                            ticker += 1;
                            if fsync_every_sec && ticker.is_multiple_of(20) {
                                let file = flush_writer.borrow().get_file();
                                if let Some(file) = file {
                                    let _ = file.sync_data().await;
                                }
                            }
                        }
                    });
                    Some(writer)
                }
                Err(e) => {
                    eprintln!("[Shard {}] Failed to open AOF writer: {}", shard_id, e);
                    None
                }
            }
        } else {
            None
        };

        // 4. Background Cluster Bus & Gossip Engine (on Shard 0)
        if shard_id == 0 {
            crate::cluster::start_cluster_bus(port);
        }

        // Initialize NVMe Tiered Storage Manager (io_uring)
        let tier_dir = std::env::var("RUDIS_TIER_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir().join(format!("rudis_tier_{}", port)));
        match crate::tiering::ShardTierManager::open(shard_id, port, &tier_dir).await {
            Ok(tm) => {
                local_db.borrow_mut().tier_manager = Some(Rc::new(tm));
            }
            Err(e) => {
                eprintln!("[Shard {}] Failed to open Tier manager: {}", shard_id, e);
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
        r.base_port = base_port;
        r.cluster_enabled = cluster_enabled;
        let router = Rc::new(r);

        // Active expiration cycle: run every 100ms
        let active_db = local_db.clone();
        monoio::spawn(async move {
            loop {
                monoio::time::sleep(std::time::Duration::from_millis(100)).await;
                active_db.borrow_mut().active_expire_cycle();
            }
        });

        // Background Tiered Storage Offload cycle: run every 20ms
        let offload_router = router.clone();
        monoio::spawn(async move {
            loop {
                monoio::time::sleep(std::time::Duration::from_millis(20)).await;
                offload_router.check_auto_tier().await;
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
        let xsk_socket = xdp_engine.get_or_create_socket(shard_port, shard_id as u32);
        let xdp_db = local_db.clone();
        let xdp_aof = aof_writer.clone();
        let xdp_router = router.clone();
        monoio::spawn(async move {
            let mut frames = Vec::with_capacity(32);
            loop {
                let count = xsk_socket.rx_burst(&mut frames, 32);
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
                                if !out.is_empty() {
                                    xsk_socket.tx_burst(&[&out]);
                                }
                            }
                        }
                    }
                } else {
                    monoio::time::sleep(std::time::Duration::from_millis(5)).await;
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
                    ShardMessage::Get { key, responder } => {
                        let val = cross_shard_db.borrow_mut().get(&key);
                        if let Some(v) = val {
                            let _ = responder.send(Some(v));
                        } else if cross_shard_db.borrow_mut().table.is_tiered(&key).is_some() {
                            let r = cross_shard_router.clone();
                            monoio::spawn(async move {
                                let val = r.read_cold_key_local(&key).await;
                                let _ = responder.send(val);
                            });
                        } else {
                            let _ = responder.send(None);
                        }
                    }
                    ShardMessage::Set {
                        key,
                        value,
                        expire_in,
                        responder,
                    } => {
                        if let Some(aof) = &cross_shard_aof
                            && let Some(bytes) =
                                crate::aof::command_to_resp(&crate::resp::Command::Set {
                                    key: key.clone(),
                                    value: value.clone(),
                                    expire_in,
                                    condition: crate::resp::SetCondition::None,
                                    get: false,
                                    keepttl: false,
                                    past_expired: false,
                                })
                        {
                            aof.borrow_mut().append(&bytes);
                        }
                        cross_shard_db
                            .borrow_mut()
                            .set(key, value, expire_in);
                        cross_shard_router.check_auto_tier_after_write();
                        let _ = responder.send(());
                    }
                    ShardMessage::FastGet { descriptor } => {
                        let val = cross_shard_db.borrow_mut().get(&descriptor.key);
                        if let Some(v) = val {
                            descriptor.finish(Some(v));
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
                        if let Some(aof) = &cross_shard_aof
                            && let Some(bytes) =
                                crate::aof::command_to_resp(&crate::resp::Command::Set {
                                    key: descriptor.key.clone(),
                                    value: descriptor.value.clone(),
                                    expire_in: descriptor.expire_in,
                                    condition: crate::resp::SetCondition::None,
                                    get: false,
                                    keepttl: false,
                                    past_expired: false,
                                })
                        {
                            aof.borrow_mut().append(&bytes);
                        }
                        cross_shard_db
                            .borrow_mut()
                            .set(descriptor.key.clone(), descriptor.value.clone(), descriptor.expire_in);
                        cross_shard_router.check_auto_tier_after_write();
                        descriptor.finish();
                    }
                    ShardMessage::Del { key, responder } => {
                        let deleted = cross_shard_db.borrow_mut().del(&key);
                        if deleted
                            && let Some(aof) = &cross_shard_aof
                                && let Some(bytes) =
                                    crate::aof::command_to_resp(&crate::resp::Command::Del(vec![
                                        key,
                                    ]))
                                {
                                    aof.borrow_mut().append(&bytes);
                                }
                        let _ = responder.send(deleted);
                    }
                    ShardMessage::DelKeys { keys, responder } => {
                        let mut count = 0usize;
                        let mut deleted_keys = Vec::with_capacity(keys.len());
                        {
                            let mut db = cross_shard_db.borrow_mut();
                            for k in keys {
                                if db.del(&k) {
                                    count += 1;
                                    deleted_keys.push(k);
                                }
                            }
                        }
                        if count > 0
                            && let Some(aof) = &cross_shard_aof
                            && let Some(bytes) =
                                crate::aof::command_to_resp(&crate::resp::Command::Del(deleted_keys))
                        {
                            aof.borrow_mut().append(&bytes);
                        }
                        let _ = responder.send(count);
                    }
                    ShardMessage::ActiveDefrag { responder } => {
                        let freed = cross_shard_db.borrow_mut().active_defrag();
                        let _ = responder.send(freed);
                    }
                    ShardMessage::Exists { key, responder } => {
                        let exists = cross_shard_db.borrow_mut().exists(&key);
                        let _ = responder.send(exists);
                    }
                    ShardMessage::IncrBy {
                        key,
                        delta,
                        responder,
                    } => {
                        let res = cross_shard_db.borrow_mut().incr_by(key.clone(), delta);
                        if res.is_ok()
                            && let Some(aof) = &cross_shard_aof
                                && let Some(bytes) = crate::aof::command_to_resp(
                                    &crate::resp::Command::IncrBy(key, delta),
                                ) {
                                    aof.borrow_mut().append(&bytes);
                                }
                        let _ = responder.send(res);
                    }
                    ShardMessage::Expire {
                        key,
                        duration,
                        responder,
                    } => {
                        let res = cross_shard_db.borrow_mut().expire(&key, duration);
                        if res
                            && let Some(aof) = &cross_shard_aof
                                && let Some(bytes) = crate::aof::command_to_resp(
                                    &crate::resp::Command::Expire(key, duration),
                                ) {
                                    aof.borrow_mut().append(&bytes);
                                }
                        let _ = responder.send(res);
                    }
                    ShardMessage::Persist { key, responder } => {
                        let res = cross_shard_db.borrow_mut().persist(&key);
                        if res
                            && let Some(aof) = &cross_shard_aof
                                && let Some(bytes) =
                                    crate::aof::command_to_resp(&crate::resp::Command::Persist(key))
                                {
                                    aof.borrow_mut().append(&bytes);
                                }
                        let _ = responder.send(res);
                    }
                    ShardMessage::Ttl {
                        key,
                        in_millis,
                        responder,
                    } => {
                        let res = cross_shard_db.borrow_mut().ttl(&key, in_millis);
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
                            if !filter_ids.is_empty() && !filter_ids.contains(&client.id) {
                                continue;
                            }
                            let age = now.duration_since(client.connected_at).as_secs();
                            let idle = now.duration_since(client.last_active).as_secs();
                            let is_blocked = crate::block::get_block_hub_for_port(port).lock().unwrap().is_blocked(client.id);
                            let flags = if is_blocked { "b" } else { "N" };
                            out.push_str(&format!(
                                "id={} addr={} laddr=127.0.0.1:{} fd=8 name={} age={} idle={} flags={} db=0 sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=20448 argv-mem=10 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 omem-shared=0 omem-unshared=0 tot-mem=22306 events=r cmd={} user=default redir=-1 resp=2 lib-name= lib-ver= io-thread=0 tot-net-in=0 tot-net-out=0 tot-cmds=0 read-events=0 avg-pipeline-len-sum=0 avg-pipeline-len-cnt=0\n",
                                client.id,
                                client.addr,
                                port,
                                client.name.as_deref().unwrap_or(""),
                                age,
                                idle,
                                flags,
                                client.last_cmd.to_lowercase()
                            ));
                        }
                        let _ = responder.send(out);
                    }
                    ShardMessage::Batch {
                        mut items,
                        mut results,
                        responder,
                        is_resp3,
                    } => {
                        let r = cross_shard_router.clone();
                        let aof_ref = cross_shard_aof.clone();
                        let has_tier_manager = cross_shard_db.borrow().tier_manager.is_some();
                        let needs_async = has_tier_manager && items.iter().any(|(_, cmd)| {
                            if let Command::Get(key) = cmd {
                                r.local_db.borrow_mut().get(key).is_none()
                                    && r.local_db.borrow_mut().table.is_tiered(key).is_some()
                            } else {
                                false
                            }
                        });

                        if needs_async {
                            monoio::spawn(async move {
                                crate::connection::CURRENT_CLIENT_RESP3.set(is_resp3);
                                results.clear();
                                let mut temp_buf = Vec::with_capacity(128);
                                let mut has_writes = false;
                                for (idx, cmd) in items.drain(..) {
                                    temp_buf.clear();
                                    if let Command::Get(ref key) = cmd {
                                        let val = r.local_db.borrow_mut().get(key);
                                        if let Some(v) = val {
                                            crate::connection::write_resp_bulk(&mut temp_buf, &v);
                                        } else if r.local_db.borrow_mut().table.is_tiered(key).is_some() {
                                            if let Some(v) = r.stream_cold_read_local(key).await {
                                                crate::connection::write_resp_bulk(&mut temp_buf, &v);
                                            } else {
                                                crate::connection::write_resp_null(&mut temp_buf);
                                            }
                                        } else {
                                            crate::connection::write_resp_null(&mut temp_buf);
                                        }
                                    } else if aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(r.port)
                                        && let Command::Set {
                                            key,
                                            value,
                                            expire_in,
                                            condition: crate::resp::SetCondition::None,
                                            get: false,
                                            keepttl: false,
                                            past_expired: false,
                                        } = cmd
                                    {
                                        has_writes = true;
                                        r.local_db.borrow_mut().table.set(key, value, expire_in);
                                        results.push((idx, crate::shard::CompactResp::OK));
                                        continue;
                                    } else if aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(r.port)
                                        && let Command::IncrBy(ref key, delta) = cmd
                                    {
                                        has_writes = true;
                                        match r.local_db.borrow_mut().table.incr_by_slice_fast(key, delta) {
                                            Ok(val) => {
                                                if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                    crate::connection::touch_watched_key(r.port, key.as_ref());
                                                }
                                                if val == 1 {
                                                    results.push((idx, crate::shard::CompactResp::INT_1));
                                                } else if val == 0 {
                                                    results.push((idx, crate::shard::CompactResp::INT_0));
                                                } else {
                                                    results.push((idx, crate::shard::CompactResp::from_integer(val)));
                                                }
                                                continue;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if let Command::Exists(ref keys) = cmd && keys.len() == 1 {
                                        let exists = r.local_db.borrow_mut().exists(keys[0].as_ref());
                                        results.push((idx, if exists { crate::shard::CompactResp::INT_1 } else { crate::shard::CompactResp::INT_0 }));
                                        continue;
                                    } else if aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(r.port)
                                        && let Command::Del(ref keys) = cmd && keys.len() == 1
                                    {
                                        let deleted = r.local_db.borrow_mut().del(keys[0].as_ref());
                                        if deleted {
                                            has_writes = true;
                                            if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                crate::connection::touch_watched_key(r.port, keys[0].as_ref());
                                            }
                                            results.push((idx, crate::shard::CompactResp::INT_1));
                                        } else {
                                            results.push((idx, crate::shard::CompactResp::INT_0));
                                        }
                                        continue;
                                    } else if let Command::Hget { ref key, ref field } = cmd {
                                        match r.local_db.borrow_mut().hget(key.as_ref(), field.as_ref()) {
                                            Ok(Some(v)) => crate::connection::write_resp_bulk(&mut temp_buf, &v),
                                            Ok(None) => crate::connection::write_resp_null(&mut temp_buf),
                                            Err(err) => crate::connection::write_resp_err(&mut temp_buf, err),
                                        }
                                    } else if aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(r.port)
                                        && let Command::Hset { ref key, ref fields } = cmd
                                    {
                                        has_writes = true;
                                        match r.local_db.borrow_mut().table.hset_slice_fast(key, fields) {
                                            Ok(count) => {
                                                if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                    crate::connection::touch_watched_key(r.port, key.as_ref());
                                                }
                                                crate::connection::write_resp_integer(&mut temp_buf, count as i64);
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if let Command::Sismember { ref key, ref member } = cmd {
                                        match r.local_db.borrow_mut().sismember_compact(key.as_ref(), member.as_ref()) {
                                            Ok(resp) => {
                                                results.push((idx, resp));
                                                continue;
                                            }
                                            Err(err) => crate::connection::write_resp_err(&mut temp_buf, err),
                                        }
                                    } else if aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(r.port)
                                        && let Command::Sadd { ref key, ref members } = cmd
                                    {
                                        has_writes = true;
                                        match r.local_db.borrow_mut().table.sadd_slice_fast(key, members) {
                                            Ok(count) => {
                                                if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                    crate::connection::touch_watched_key(r.port, key.as_ref());
                                                }
                                                if count == 1 {
                                                    results.push((idx, crate::shard::CompactResp::INT_1));
                                                } else if count == 0 {
                                                    results.push((idx, crate::shard::CompactResp::INT_0));
                                                } else {
                                                    results.push((idx, crate::shard::CompactResp::from_integer(count as i64)));
                                                }
                                                continue;
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(r.port)
                                        && !crate::block::has_blocked_waiters(r.port)
                                        && let Command::Lpush { ref key, ref values } = cmd
                                    {
                                        has_writes = true;
                                        match r.local_db.borrow_mut().table.lpush_slice_fast(key, values) {
                                            Ok(len) => {
                                                if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                    crate::connection::touch_watched_key(r.port, key.as_ref());
                                                }
                                                crate::connection::write_resp_integer(&mut temp_buf, len as i64);
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if aof_ref.is_none()
                                        && !crate::replication::has_connected_replicas(r.port)
                                        && let Command::Lpop { ref key, count } = cmd
                                    {
                                        match r.local_db.borrow_mut().table.lpop(key.as_ref(), count.unwrap_or(1)) {
                                            Ok(vals) => {
                                                if !vals.is_empty() {
                                                    has_writes = true;
                                                    if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                        crate::connection::touch_watched_key(r.port, key.as_ref());
                                                    }
                                                }
                                                if count.is_some() {
                                                    crate::connection::write_resp_array_header(&mut temp_buf, vals.len());
                                                    for v in &vals {
                                                        crate::connection::write_resp_bulk(&mut temp_buf, v);
                                                    }
                                                } else if let Some(v) = vals.first() {
                                                    crate::connection::write_resp_bulk(&mut temp_buf, v);
                                                } else {
                                                    crate::connection::write_resp_null(&mut temp_buf);
                                                }
                                            }
                                            Err(err) => {
                                                crate::connection::write_resp_err(&mut temp_buf, err);
                                            }
                                        }
                                    } else if let Command::Lrange { ref key, start, stop } = cmd {
                                        if let Err(err) = r.local_db.borrow_mut().write_lrange_resp(key.as_ref(), start, stop, &mut temp_buf) {
                                            crate::connection::write_resp_err(&mut temp_buf, err);
                                        }
                                    } else if let Command::Zrange { ref key, ref opts } = cmd {
                                        if let Err(err) = r.local_db.borrow_mut().write_zrange_resp(key.as_ref(), opts, is_resp3, &mut temp_buf) {
                                            crate::connection::write_resp_err(&mut temp_buf, err);
                                        }
                                    } else {
                                        if matches!(cmd, Command::Set { .. } | Command::Del(_) | Command::IncrBy { .. }) {
                                            has_writes = true;
                                        }
                                        let mut db = r.local_db.borrow_mut();
                                        let _ = execute_local_command(&cmd, &mut db, &mut temp_buf, aof_ref.as_deref());
                                    }
                                    results.push((idx, crate::shard::CompactResp::from_slice(&temp_buf)));
                                }
                                if has_writes {
                                    r.check_auto_tier_after_write();
                                }
                                responder.finish(items, results);
                            });
                        } else {
                            crate::connection::CURRENT_CLIENT_RESP3.set(is_resp3);
                            let mut db = cross_shard_db.borrow_mut();
                            results.clear();
                            let aof_ref = cross_shard_aof.as_deref();
                            let mut temp_buf = Vec::with_capacity(128);
                            let mut has_writes = false;
                            for (idx, cmd) in items.drain(..) {
                                temp_buf.clear();
                                if let Command::Get(ref key) = cmd {
                                    match db.get_compact(key.as_ref()) {
                                        Ok(Some(resp)) => {
                                            results.push((idx, resp));
                                            continue;
                                        }
                                        Ok(None) => {
                                            results.push((idx, crate::shard::CompactResp::NULL));
                                            continue;
                                        }
                                        Err(err) => {
                                            crate::connection::write_resp_err(&mut temp_buf, err);
                                        }
                                    }
                                } else if aof_ref.is_none()
                                    && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                    && let Command::Set {
                                        key,
                                        value,
                                        expire_in,
                                        condition: crate::resp::SetCondition::None,
                                        get: false,
                                        keepttl: false,
                                        past_expired: false,
                                    } = cmd
                                {
                                    has_writes = true;
                                    db.table.set(key, value, expire_in);
                                    results.push((idx, crate::shard::CompactResp::OK));
                                    continue;
                                } else if aof_ref.is_none()
                                    && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                    && let Command::IncrBy(ref key, delta) = cmd
                                {
                                    has_writes = true;
                                    match db.table.incr_by_slice_fast(key, delta) {
                                        Ok(val) => {
                                            if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                            }
                                            if val == 1 {
                                                results.push((idx, crate::shard::CompactResp::INT_1));
                                            } else if val == 0 {
                                                results.push((idx, crate::shard::CompactResp::INT_0));
                                            } else {
                                                results.push((idx, crate::shard::CompactResp::from_integer(val)));
                                            }
                                            continue;
                                        }
                                        Err(err) => {
                                            crate::connection::write_resp_err(&mut temp_buf, err);
                                        }
                                    }
                                } else if let Command::Exists(ref keys) = cmd && keys.len() == 1 {
                                    let exists = db.exists(keys[0].as_ref());
                                    results.push((idx, if exists { crate::shard::CompactResp::INT_1 } else { crate::shard::CompactResp::INT_0 }));
                                    continue;
                                } else if aof_ref.is_none()
                                    && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                    && let Command::Del(ref keys) = cmd && keys.len() == 1
                                {
                                    let deleted = db.del(keys[0].as_ref());
                                    if deleted {
                                        has_writes = true;
                                        if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                            crate::connection::touch_watched_key(cross_shard_router.port, keys[0].as_ref());
                                        }
                                        results.push((idx, crate::shard::CompactResp::INT_1));
                                    } else {
                                        results.push((idx, crate::shard::CompactResp::INT_0));
                                    }
                                    continue;
                                } else if let Command::Hget { ref key, ref field } = cmd {
                                    match db.hget_compact(key.as_ref(), field.as_ref()) {
                                        Ok(resp) => {
                                            results.push((idx, resp));
                                            continue;
                                        }
                                        Err(err) => {
                                            crate::connection::write_resp_err(&mut temp_buf, err);
                                        }
                                    }
                                } else if aof_ref.is_none()
                                    && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                    && let Command::Hset { ref key, ref fields } = cmd
                                {
                                    has_writes = true;
                                    match db.table.hset_slice_fast(key, fields) {
                                        Ok(count) => {
                                            if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                            }
                                            if count == 1 {
                                                results.push((idx, crate::shard::CompactResp::INT_1));
                                            } else if count == 0 {
                                                results.push((idx, crate::shard::CompactResp::INT_0));
                                            } else {
                                                results.push((idx, crate::shard::CompactResp::from_integer(count as i64)));
                                            }
                                            continue;
                                        }
                                        Err(err) => {
                                            crate::connection::write_resp_err(&mut temp_buf, err);
                                        }
                                    }
                                } else if let Command::Sismember { ref key, ref member } = cmd {
                                    match db.sismember_compact(key.as_ref(), member.as_ref()) {
                                        Ok(resp) => {
                                            results.push((idx, resp));
                                            continue;
                                        }
                                        Err(err) => crate::connection::write_resp_err(&mut temp_buf, err),
                                    }
                                } else if aof_ref.is_none()
                                    && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                    && let Command::Sadd { ref key, ref members } = cmd
                                {
                                    has_writes = true;
                                    match db.table.sadd_slice_fast(key, members) {
                                        Ok(count) => {
                                            if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                            }
                                            if count == 1 {
                                                results.push((idx, crate::shard::CompactResp::INT_1));
                                            } else if count == 0 {
                                                results.push((idx, crate::shard::CompactResp::INT_0));
                                            } else {
                                                results.push((idx, crate::shard::CompactResp::from_integer(count as i64)));
                                            }
                                            continue;
                                        }
                                        Err(err) => {
                                            crate::connection::write_resp_err(&mut temp_buf, err);
                                        }
                                    }
                                } else if aof_ref.is_none()
                                    && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                    && !crate::block::has_blocked_waiters(cross_shard_router.port)
                                    && let Command::Lpush { ref key, ref values } = cmd
                                {
                                    has_writes = true;
                                    match db.table.lpush_slice_fast(key, values) {
                                        Ok(len) => {
                                            if crate::connection::HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                                crate::connection::touch_watched_key(cross_shard_router.port, key.as_ref());
                                            }
                                            if len == 1 {
                                                results.push((idx, crate::shard::CompactResp::INT_1));
                                            } else if len == 0 {
                                                results.push((idx, crate::shard::CompactResp::INT_0));
                                            } else {
                                                results.push((idx, crate::shard::CompactResp::from_integer(len as i64)));
                                            }
                                            continue;
                                        }
                                        Err(err) => {
                                            crate::connection::write_resp_err(&mut temp_buf, err);
                                        }
                                    }
                                } else if aof_ref.is_none()
                                    && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                    && let Command::Lpop { ref key, count } = cmd
                                {
                                    match db.write_lpop_resp(key.as_ref(), count, &mut temp_buf) {
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
                                } else if let Command::Lrange { ref key, start, stop } = cmd {
                                    if let Err(err) = db.write_lrange_resp(key.as_ref(), start, stop, &mut temp_buf) {
                                        crate::connection::write_resp_err(&mut temp_buf, err);
                                    }
                                } else if let Command::Zrange { ref key, ref opts } = cmd {
                                    if let Err(err) = db.write_zrange_resp(key.as_ref(), opts, is_resp3, &mut temp_buf) {
                                        crate::connection::write_resp_err(&mut temp_buf, err);
                                    }
                                } else if aof_ref.is_none()
                                    && !crate::replication::has_connected_replicas(cross_shard_router.port)
                                    && let Command::Zadd { ref key, ref elements, flags } = cmd
                                {
                                    has_writes = true;
                                    match db.zadd_slice_fast(key, elements, flags) {
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
                                                results.push((idx, crate::shard::CompactResp::from_slice(&temp_buf)));
                                            } else if count == 1 {
                                                results.push((idx, crate::shard::CompactResp::INT_1));
                                            } else if count == 0 {
                                                results.push((idx, crate::shard::CompactResp::INT_0));
                                            } else {
                                                results.push((idx, crate::shard::CompactResp::from_integer(count as i64)));
                                            }
                                            continue;
                                        }
                                        Err(err) => {
                                            crate::connection::write_resp_err(&mut temp_buf, err);
                                        }
                                    }
                                } else {
                                    if matches!(cmd, Command::Set { .. } | Command::Del(_) | Command::IncrBy { .. }) {
                                        has_writes = true;
                                    }
                                    let _ = execute_local_command(&cmd, &mut db, &mut temp_buf, aof_ref);
                                }
                                results.push((idx, crate::shard::CompactResp::from_slice(&temp_buf)));
                            }
                            drop(db);
                            if has_writes {
                                cross_shard_router.check_auto_tier_after_write();
                            }
                            responder.finish(items, results);
                        }
                    }
                    ShardMessage::Mget { mut keys, responder } => {
                        let mut db = cross_shard_db.borrow_mut();
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
                    ShardMessage::Mset { mut pairs, responder } => {
                        {
                            let mut db = cross_shard_db.borrow_mut();
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
                        if let Some(aof) = &cross_shard_aof
                            && let Some(bytes) =
                                crate::aof::command_to_resp(&crate::resp::Command::Mset(pairs.clone()))
                            {
                                aof.borrow_mut().append(&bytes);
                            }
                        cross_shard_router.check_auto_tier_after_write();
                        pairs.clear();
                        let _ = responder.send(pairs);
                    }
                    ShardMessage::ScatterMget {
                        shard_id,
                        mut keys,
                        descriptor,
                    } => {
                        let mut db = cross_shard_db.borrow_mut();
                        let mut cold_keys = Vec::new();

                        let has_tiering = db.tier_manager.is_some();
                        for (idx, key) in &keys {
                            let val = db.get(key);
                            if val.is_none() && has_tiering && db.table.is_tiered(key).is_some() {
                                cold_keys.push((*idx, key.clone()));
                            } else {
                                descriptor.write_result(*idx, val);
                            }
                        }

                        if !cold_keys.is_empty() {
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
                        if let Some(aof) = &cross_shard_aof
                            && let Some(bytes) =
                                crate::aof::command_to_resp(&crate::resp::Command::Mset(pairs.clone()))
                        {
                            aof.borrow_mut().append(&bytes);
                        }
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
                        let mut db = cross_shard_db.borrow_mut();
                        let res = crate::aof::rewrite_shard_aof(&mut db, &dir, shard_id)
                            .map_err(|e| e.to_string());
                        let _ = responder.send(res);
                    }
                    ShardMessage::SetSlotState { slot, state } => {
                        cross_shard_slot_states.borrow_mut()[slot as usize] = state;
                    }
                    ShardMessage::SetSlotOwner { slot, owner } => {
                        cross_shard_slot_states.borrow_mut()[slot as usize] =
                            crate::shard::SlotState::Stable;
                        cross_shard_slot_owners.borrow_mut()[slot as usize] = owner;
                    }
                    ShardMessage::DumpKey { key, responder } => {
                        let is_tiered = cross_shard_db.borrow_mut().table.is_tiered(&key).is_some();
                        if is_tiered {
                            let r = cross_shard_router.clone();
                            monoio::spawn(async move {
                                r.ensure_loaded(&key).await;
                                let entry = r.local_db.borrow_mut().get_entry(&key);
                                let _ = responder.send(entry);
                            });
                        } else {
                            let entry = cross_shard_db.borrow_mut().get_entry(&key);
                            let _ = responder.send(entry);
                        }
                    }
                    ShardMessage::SyncAof { responder } => {
                        let (file, chunk, offset) = if let Some(aof) = &cross_shard_aof {
                            let mut writer = aof.borrow_mut();
                            let file = writer.get_file();
                            if let Some((f, c, o)) = writer.take_flush_chunk() {
                                (Some(f), c, o)
                            } else {
                                (file, Vec::new(), 0)
                            }
                        } else {
                            (None, Vec::new(), 0)
                        };
                        if let Some(file) = file {
                            if !chunk.is_empty() {
                                let _ = file.write_all_at(chunk, offset).await;
                            }
                            let _ = file.sync_data().await;
                        }
                        let _ = responder.send(());
                    }
                    ShardMessage::SaveRdbChunk { responder } => {
                        let mut buf = Vec::new();
                        cross_shard_db.borrow_mut().save_rdb_chunk(&mut buf);
                        let _ = responder.send(buf);
                    }
                    ShardMessage::Publish {
                        channel,
                        message,
                        responder,
                    } => {
                        let count = cross_shard_pubsub.borrow().publish(&channel, &message);
                        let _ = responder.send(count);
                    }
                    ShardMessage::PubsubChannels { pattern, responder } => {
                        let channels = cross_shard_pubsub.borrow().channels(pattern.as_deref());
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
                    ShardMessage::PubsubNumpat { responder } => {
                        let cnt = cross_shard_pubsub.borrow().numpat();
                        let _ = responder.send(cnt);
                    }
                    ShardMessage::Keys { pattern, responder } => {
                        let keys = cross_shard_db.borrow_mut().keys(&pattern);
                        let _ = responder.send(keys);
                    }
                    ShardMessage::Scan {
                        slot,
                        pattern,
                        count,
                        responder,
                    } => {
                        let res = cross_shard_db
                            .borrow_mut()
                            .scan(slot, pattern.as_deref(), count);
                        let _ = responder.send(res);
                    }
                    ShardMessage::RandomKey { responder } => {
                        let key = cross_shard_db.borrow_mut().random_key();
                        let _ = responder.send(key);
                    }
                    ShardMessage::ExpireTime {
                        key,
                        in_millis,
                        responder,
                    } => {
                        let res = cross_shard_db.borrow_mut().expiretime(&key, in_millis);
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
                            if let Some((next_tx, next_resp)) =
                                cross_shard_tx_waiters.borrow_mut().pop_front()
                            {
                                *lock = Some(next_tx);
                                let _ = next_resp.send(());
                            } else {
                                *lock = None;
                            }
                        }
                    }
                    ShardMessage::RestoreRdbChunk { data, responder } => {
                        let mut db = cross_shard_db.borrow_mut();
                        let _ = crate::table::load_rdb_bytes(&data, &mut db, shard_id, num_shards);
                        let _ = responder.send(());
                    }
                    ShardMessage::ExecuteReplicaCmd { cmd, responder } => {
                        let mut db = cross_shard_db.borrow_mut();
                        let mut dummy_out = Vec::new();
                        let aof_ref = cross_shard_aof.as_deref();
                        execute_local_command(&cmd, &mut db, &mut dummy_out, aof_ref);
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
                    ShardMessage::NotifyList { keys } => {
                        let mut db = cross_shard_db.borrow_mut();
                        let hub_arc = crate::block::get_block_hub_for_port(db.port);
                        let mut hub = hub_arc.lock().unwrap();
                        for k in keys {
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
                    ShardMessage::GetUsedMemory { responder } => {
                        let used = cross_shard_db.borrow().table.used_memory;
                        let _ = responder.send(used);
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
                    ShardMessage::Delex { key, condition, responder } => {
                        let mut db = cross_shard_db.borrow_mut();
                        let should_del = match condition {
                            None => true,
                            Some((op, expected)) => {
                                if let Some(val) = db.get(&key) {
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
                            let deleted = db.del(&key);
                            let _ = responder.send(deleted);
                        } else {
                            let _ = responder.send(false);
                        }
                    }
                }
                burst += 1;
                if burst >= 64 {
                    break;
                }
                match rx.try_recv() {
                    Ok(next) => msg = next,
                    Err(_) => break,
                }
            }
        }
    });

        println!(
            "[Shard {}/{}] Worker started and listening on {} via io_uring",
            shard_id, num_shards, addr
        );

        // 4.9 Spawn TLS Accept loop if enabled
        if let Some(tls_listener) = tls_listener
            && let Some(tls_cfg) = tls_config
        {
            let r = router.clone();
            let reg = client_registry.clone();
            monoio::spawn(async move {
                let mut next_tls_client_id: u64 = ((shard_id as u64) << 48) | 0x8000_0000_0000;
                loop {
                    if crate::shutdown::is_shutting_down() {
                        break;
                    }
                    let accept_res = match monoio::time::timeout(
                        std::time::Duration::from_millis(200),
                        tls_listener.accept(),
                    )
                    .await
                    {
                        Ok(res) => res,
                        Err(_) => continue,
                    };
                    match accept_res {
                        Ok((mut stream, client_addr)) => {
                            let _ = stream.set_nodelay(true);
                            let client_id = next_tls_client_id;
                            next_tls_client_id += 1;
                            let router_clone = r.clone();
                            let reg_clone = reg.clone();
                            let s_cfg = tls_cfg.server_config.clone();
                            monoio::spawn(async move {
                                let res = catch_unwind_async(async move {
                                    let mut session = match crate::tls::TlsSession::new(s_cfg) {
                                        Ok(s) => s,
                                        Err(e) => {
                                            tracing::error!("[Shard {}] Failed to create TlsSession: {}", shard_id, e);
                                            return;
                                        }
                                    };
                                    if let Err(e) = session.handshake_monoio(&mut stream).await {
                                        tracing::warn!("[Shard {}] TLS handshake error: {}", shard_id, e);
                                        return;
                                    }
                                    crate::connection::handle_tls_connection(
                                        stream,
                                        session,
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
                                    tracing::error!(client_id = client_id, "Panic isolated in TLS client connection: {:?}", e);
                                }
                            });
                        }
                        Err(e) => {
                            eprintln!("[Shard {}] TLS accept error: {}", shard_id, e);
                        }
                    }
                }
            });
        }

        // 5. Accept loop
        let mut next_client_id: u64 = ((shard_id as u64) << 48) + 1;
        loop {
            if crate::shutdown::is_shutting_down() {
                break;
            }
            let accept_res = match monoio::time::timeout(
                std::time::Duration::from_millis(200),
                listener.accept(),
            )
            .await
            {
                Ok(res) => res,
                Err(_) => continue,
            };
            match accept_res {
                Ok((stream, client_addr)) => {
                    let _ = stream.set_nodelay(true);
                    let raw_fd = std::os::unix::io::AsRawFd::as_raw_fd(&stream);
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
                    let client_id = next_client_id;
                    next_client_id += 1;
                    let reg = client_registry.clone();
                    monoio::spawn(async move {
                        let res = catch_unwind_async(async move {
                            handle_connection(stream, client_addr, client_id, reg, r).await;
                        })
                        .await;
                        if let Err(e) = res {
                            crate::connection::inc_isolated_panics();
                            tracing::error!(client_id = client_id, "Panic isolated in client connection: {:?}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("[Shard {}] Accept error: {}", shard_id, e);
                }
            }
        }

        // 6. Graceful shutdown cleanup: sync AOF and notify
        if let Some(aof) = aof_writer {
            let (file, chunk, offset) = {
                let mut writer = aof.borrow_mut();
                let file = writer.get_file();
                if let Some((f, c, o)) = writer.take_flush_chunk() {
                    (Some(f), c, o)
                } else {
                    (file, Vec::new(), 0)
                }
            };
            if let Some(file) = file {
                if !chunk.is_empty() {
                    let _ = file.write_all_at(chunk, offset).await;
                }
                let _ = file.sync_data().await;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

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
