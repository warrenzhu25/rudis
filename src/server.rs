use socket2::{Domain, Protocol, Socket, Type};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::Ordering;

use crate::connection::{execute_local_command, handle_connection};
use crate::resp::Command;
use crate::router::Router;
use crate::shard::{ShardDb, ShardMessage};

pub fn run_shard_worker(
    shard_id: usize,
    num_shards: usize,
    port: u16,
    senders: Vec<flume::Sender<ShardMessage>>,
    rx: flume::Receiver<ShardMessage>,
    core_id: Option<core_affinity::CoreId>,
    aof_config: crate::aof::AofConfig,
) {
    if let Some(core) = core_id {
        core_affinity::set_for_current(core);
    }

    let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
        .enable_timer()
        .build()
        .expect("Failed to initialize Monoio io_uring runtime");

    rt.block_on(async move {
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

        let addr: SocketAddr = format!("0.0.0.0:{}", port)
            .parse()
            .expect("Invalid address");
        socket.bind(&addr.into()).expect("Failed to bind socket");
        socket.listen(4096).expect("Failed to listen on socket");

        let listener = monoio::net::TcpListener::from_std(socket.into())
            .expect("Failed to convert socket into Monoio TcpListener");

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
                            if fsync_every_sec && ticker % 20 == 0 {
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
        let router = Rc::new(Router::new(
            shard_id,
            num_shards,
            port,
            local_db.clone(),
            senders,
            aof_writer.clone(),
            pubsub.clone(),
            aof_config.dir.clone(),
        ));

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
            while let Ok(msg) = rx.recv_async().await {
                match msg {
                    ShardMessage::Get { key, responder } => {
                        let val = cross_shard_db.borrow_mut().get(&key);
                        if let Some(v) = val {
                            let _ = responder.send(Some(v));
                        } else if cross_shard_db.borrow_mut().table.is_tiered(&key).is_some() {
                            let r = cross_shard_router.clone();
                            monoio::spawn(async move {
                                let max_mem = crate::tiering::get_max_memory(r.port);
                                let offload_pct = crate::tiering::get_offload_threshold_pct(r.port);
                                let is_constrained = if max_mem > 0 {
                                    let used = r.local_db.borrow().table.used_memory;
                                    let shard_threshold = (max_mem / r.num_shards.max(1) as u64) as usize;
                                    used >= (shard_threshold * offload_pct as usize) / 100
                                } else {
                                    false
                                };

                                if is_constrained {
                                    let val = r.stream_cold_read_local(&key).await;
                                    if val.is_some() {
                                        let stats = crate::tiering::get_tier_stats(r.port);
                                        stats.streaming_reads.fetch_add(1, Ordering::Relaxed);
                                        stats.ram_misses.fetch_add(1, Ordering::Relaxed);
                                    }
                                    let _ = responder.send(val);
                                } else {
                                    r.ensure_loaded(&key).await;
                                    let val = r.local_db.borrow_mut().get(&key);
                                    let _ = responder.send(val);
                                }
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
                        cross_shard_db
                            .borrow_mut()
                            .set(key.clone(), value.clone(), expire_in);
                        if let Some(aof) = &cross_shard_aof {
                            if let Some(bytes) =
                                crate::aof::command_to_resp(&crate::resp::Command::Set {
                                    key,
                                    value,
                                    expire_in,
                                })
                            {
                                aof.borrow_mut().append(&bytes);
                            }
                        }
                        let r = cross_shard_router.clone();
                        monoio::spawn(async move {
                            r.check_auto_tier().await;
                        });
                        let _ = responder.send(());
                    }
                    ShardMessage::Del { key, responder } => {
                        let deleted = cross_shard_db.borrow_mut().del(&key);
                        if deleted {
                            if let Some(aof) = &cross_shard_aof {
                                if let Some(bytes) =
                                    crate::aof::command_to_resp(&crate::resp::Command::Del(vec![
                                        key,
                                    ]))
                                {
                                    aof.borrow_mut().append(&bytes);
                                }
                            }
                        }
                        let _ = responder.send(deleted);
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
                        if res.is_ok() {
                            if let Some(aof) = &cross_shard_aof {
                                if let Some(bytes) = crate::aof::command_to_resp(
                                    &crate::resp::Command::IncrBy(key, delta),
                                ) {
                                    aof.borrow_mut().append(&bytes);
                                }
                            }
                        }
                        let _ = responder.send(res);
                    }
                    ShardMessage::Expire {
                        key,
                        duration,
                        responder,
                    } => {
                        let res = cross_shard_db.borrow_mut().expire(&key, duration);
                        if res {
                            if let Some(aof) = &cross_shard_aof {
                                if let Some(bytes) = crate::aof::command_to_resp(
                                    &crate::resp::Command::Expire(key, duration),
                                ) {
                                    aof.borrow_mut().append(&bytes);
                                }
                            }
                        }
                        let _ = responder.send(res);
                    }
                    ShardMessage::Persist { key, responder } => {
                        let res = cross_shard_db.borrow_mut().persist(&key);
                        if res {
                            if let Some(aof) = &cross_shard_aof {
                                if let Some(bytes) =
                                    crate::aof::command_to_resp(&crate::resp::Command::Persist(key))
                                {
                                    aof.borrow_mut().append(&bytes);
                                }
                            }
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
                    ShardMessage::ClientList { responder } => {
                        let mut out = String::new();
                        let reg = cross_shard_clients.borrow();
                        let now = std::time::Instant::now();
                        for client in reg.values() {
                            let age = now.duration_since(client.connected_at).as_secs();
                            let idle = now.duration_since(client.last_active).as_secs();
                            out.push_str(&format!(
                                "id={} addr={} name={} age={} idle={} cmd={}\n",
                                client.id,
                                client.addr,
                                client.name.as_deref().unwrap_or(""),
                                age,
                                idle,
                                client.last_cmd
                            ));
                        }
                        let _ = responder.send(out);
                    }
                    ShardMessage::Batch { items, responder } => {
                        let r = cross_shard_router.clone();
                        let aof_ref = cross_shard_aof.clone();
                        let needs_async = items.iter().any(|(_, cmd)| {
                            if let Command::Get(key) = cmd {
                                r.local_db.borrow_mut().get(key).is_none()
                                    && r.local_db.borrow_mut().table.is_tiered(key).is_some()
                            } else {
                                false
                            }
                        });

                        if needs_async {
                            monoio::spawn(async move {
                                let mut results = Vec::with_capacity(items.len());
                                let mut temp_buf = Vec::with_capacity(128);
                                let mut has_writes = false;
                                for (idx, cmd) in items {
                                    temp_buf.clear();
                                    if let Command::Get(ref key) = cmd {
                                        let val = r.local_db.borrow_mut().get(key);
                                        if let Some(v) = val {
                                            crate::connection::write_resp_bulk(&mut temp_buf, &v);
                                        } else if r.local_db.borrow_mut().table.is_tiered(key).is_some() {
                                            if let Some(v) = r.stream_cold_read_local(key).await {
                                                crate::connection::write_resp_bulk(&mut temp_buf, &v);
                                            } else {
                                                temp_buf.extend_from_slice(b"$-1\r\n");
                                            }
                                        } else {
                                            temp_buf.extend_from_slice(b"$-1\r\n");
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
                                    r.check_auto_tier().await;
                                }
                                let _ = responder.send(results);
                            });
                        } else {
                            let mut db = cross_shard_db.borrow_mut();
                            let mut results = Vec::with_capacity(items.len());
                            let aof_ref = cross_shard_aof.as_deref();
                            let mut temp_buf = Vec::with_capacity(128);
                            let mut has_writes = false;
                            for (idx, cmd) in items {
                                temp_buf.clear();
                                if matches!(cmd, Command::Set { .. } | Command::Del(_) | Command::IncrBy { .. }) {
                                    has_writes = true;
                                }
                                let _ = execute_local_command(&cmd, &mut db, &mut temp_buf, aof_ref);
                                results.push((idx, crate::shard::CompactResp::from_slice(&temp_buf)));
                            }
                            if has_writes {
                                let r = cross_shard_router.clone();
                                monoio::spawn(async move {
                                    r.check_auto_tier().await;
                                });
                            }
                            let _ = responder.send(results);
                        }
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

            }
        });

        println!(
            "[Shard {}/{}] Worker started and listening on {} via io_uring",
            shard_id, num_shards, addr
        );

        // 5. Accept loop
        let mut next_client_id: u64 = ((shard_id as u64) << 48) + 1;
        loop {
            match listener.accept().await {
                Ok((stream, client_addr)) => {
                    let _ = stream.set_nodelay(true);
                    let r = router.clone();
                    let client_id = next_client_id;
                    next_client_id += 1;
                    let reg = client_registry.clone();
                    monoio::spawn(async move {
                        handle_connection(stream, client_addr, client_id, reg, r).await;
                    });
                }
                Err(e) => {
                    eprintln!("[Shard {}] Accept error: {}", shard_id, e);
                }
            }
        }
    });
}
