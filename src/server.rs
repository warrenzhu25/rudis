use socket2::{Domain, Protocol, Socket, Type};
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;

use crate::connection::{execute_local_command, handle_connection};
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
        let local_db = Rc::new(RefCell::new(ShardDb::new()));

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
                            let _ = flush_writer.borrow_mut().flush().await;
                            ticker += 1;
                            if fsync_every_sec && ticker % 20 == 0 {
                                let _ = flush_writer.borrow_mut().sync().await;
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
        ));

        // Active expiration cycle: run every 100ms
        let active_db = local_db.clone();
        monoio::spawn(async move {
            loop {
                monoio::time::sleep(std::time::Duration::from_millis(100)).await;
                active_db.borrow_mut().active_expire_cycle();
            }
        });

        // 4. Spawn background worker to handle incoming cross-shard messages from peer cores
        let cross_shard_db = local_db.clone();
        let cross_shard_clients = client_registry.clone();
        let cross_shard_slot_states = router.slot_states.clone();
        let cross_shard_slot_owners = router.slot_owners.clone();
        let cross_shard_aof = aof_writer.clone();
        let cross_shard_pubsub = pubsub.clone();
        monoio::spawn(async move {
            while let Ok(msg) = rx.recv_async().await {
                match msg {
                    ShardMessage::Get { key, responder } => {
                        let val = cross_shard_db.borrow_mut().get(&key);
                        let _ = responder.send(val);
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
                        let mut db = cross_shard_db.borrow_mut();
                        let mut results = Vec::with_capacity(items.len());
                        let aof_ref = cross_shard_aof.as_deref();
                        for (idx, cmd) in items {
                            let mut out = Vec::new();
                            let _ = execute_local_command(&cmd, &mut db, &mut out, aof_ref);
                            results.push((idx, out));
                        }
                        let _ = responder.send(results);
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
                        let entry = cross_shard_db.borrow_mut().get_entry(&key);
                        let _ = responder.send(entry);
                    }
                    ShardMessage::SyncAof { responder } => {
                        if let Some(aof) = &cross_shard_aof {
                            let _ = aof.borrow_mut().sync().await;
                        }
                        let _ = responder.send(());
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
