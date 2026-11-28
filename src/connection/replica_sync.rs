use super::*;

/// Refuses a (P)SYNC while our own master link is down, like Redis.
pub(super) async fn no_master_link(writer: &mut monoio::net::tcp::TcpOwnedWriteHalf, name: &str) {
    crate::log_notice!(
        "Replica {} asks for synchronization but my master link is down",
        name
    );
    let _ = writer
        .write_all(b"-NOMASTERLINK Can't SYNC while not connected with my master\r\n".to_vec())
        .await;
}

/// Why [`snapshot_for_replica`] produced no snapshot.
pub(super) enum ReplicaSnapshotError {
    /// We are a replica whose own master link is down.
    NoMasterLink,
    /// The snapshot could not be made (e.g. a tiered value was unreadable);
    /// the replica is unregistered and should retry.
    Failed(String),
}

/// The full-sync snapshot for replica `client_id`: the RDB, the replication
/// id and offset it is at, the stream bytes that must follow it, and the
/// registered replica. An error when we are a replica whose master link is
/// down, or when the snapshot failed. On a replica the snapshot is taken
/// holding the apply token, so it matches the announced offset of our
/// master's stream exactly.
pub(super) async fn snapshot_for_replica(
    hub: &std::sync::Arc<crate::replication::ReplicationHub>,
    router: &Rc<Router>,
    client_id: u64,
    write_tx: &flume::Sender<Vec<u8>>,
) -> Result<
    (
        Vec<u8>,
        String,
        u64,
        Vec<u8>,
        std::sync::Arc<crate::replication::ConnectedReplica>,
    ),
    ReplicaSnapshotError,
> {
    let _apply = if hub.is_slave() {
        Some(hub.lock_apply().await)
    } else {
        None
    };
    hub.reset_stream_db();
    let rep = hub
        .register_full_sync_replica(client_id, write_tx.clone(), router.num_shards)
        .ok_or(ReplicaSnapshotError::NoMasterLink)?;
    crate::log_notice!("Starting BGSAVE for SYNC with target: replicas sockets");
    crate::server_stats::add(crate::server_stats::Stat::SyncFull, 1);
    // No cross-shard write may land between two shards' chunks.
    let paused = router.pause_cross_shard_writes().await;
    let rdb = router.generate_full_rdb(Some(client_id)).await;
    drop(paused);
    let rdb = match rdb {
        Ok(rdb) => rdb,
        Err(e) => {
            // Never ship a partial dataset: drop this attempt, the replica
            // reconnects and asks again.
            hub.unregister_replica(client_id);
            crate::log_warning!("BGSAVE for replication failed: {}", e);
            return Err(ReplicaSnapshotError::Failed(e));
        }
    };
    let (replid, offset, after_snapshot) = hub.finish_full_sync(client_id);
    Ok((rdb, replid, offset, after_snapshot, rep))
}

/// The end of a streamed full sync: the saved replication history (if
/// any), `OP_EOF`, the checksum (`crc` covers everything sent before), then
/// the `$EOF:` mark, which must be the last bytes on the wire until the
/// replica acks.
pub(super) fn streamed_sync_tail(
    saved_history: Option<(String, u64, usize)>,
    crc: u64,
    mark: &str,
) -> Vec<u8> {
    let mut tail = Vec::new();
    if let Some((replid, offset, stream_db)) = saved_history {
        crate::redis_rdb::write_repl_aux(&mut tail, &replid, offset, stream_db);
    }
    tail.push(crate::redis_rdb::OP_EOF);
    let crc = crate::table::crc64_update(crc, &tail);
    tail.extend_from_slice(&crc.to_le_bytes());
    tail.extend_from_slice(mark.as_bytes());
    tail
}

/// Removes a streamed full sync's images from every shard when the sync
/// ends early (an image left installed would keep taking pre-images).
pub(super) struct SyncImagesGuard<'a> {
    router: &'a Router,
    replica: u64,
    armed: bool,
}

impl Drop for SyncImagesGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        crate::shard::take_image(crate::shard::ImageOwner::FullSync(self.replica));
        crate::shard::publish_after_image_change(self.router);
        for (sid, sender) in self.router.senders.iter().enumerate() {
            if sid != self.router.shard_id {
                let _ = sender.send(crate::shard::ShardMessage::DropSyncImage {
                    replica: self.replica,
                });
            }
        }
    }
}

/// Streamed full sync to replica `client_id`, which takes `$EOF:`
/// transfers. Every shard starts a point-in-time image at once (arming the
/// replica's stream cut), so the replication offset is known before the
/// snapshot and goes out in +FULLRESYNC first; then each shard writes its
/// part in ~1 ms steps between commands, sent as it is produced. The
/// snapshot is never held whole in memory and no shard stops serving for
/// longer than a step (the buffered path serialized each shard in one go:
/// ~110 ms per GB of data, and ~3x the data in memory).
///
/// Returns the replica and the stream bytes written during the transfer,
/// which go out only after the replica's first `REPLCONF ACK`. An error
/// with a reason comes before anything was sent (the caller reports it);
/// `Err(None)` means the transfer broke off after it began (already
/// logged).
pub(super) async fn stream_full_sync(
    hub: &std::sync::Arc<crate::replication::ReplicationHub>,
    router: &Rc<Router>,
    client_id: u64,
    write_tx: &flume::Sender<Vec<u8>>,
    writer: &mut monoio::net::tcp::TcpOwnedWriteHalf,
    name: &str,
    rdb_channel: Option<flume::Receiver<Vec<u8>>>,
) -> Result<
    (
        std::sync::Arc<crate::replication::ConnectedReplica>,
        Vec<u8>,
    ),
    Option<ReplicaSnapshotError>,
> {
    let apply = if hub.is_slave() {
        Some(hub.lock_apply().await)
    } else {
        None
    };
    hub.reset_stream_db();
    let rep = hub
        .register_full_sync_replica(client_id, write_tx.clone(), router.num_shards)
        .ok_or(Some(ReplicaSnapshotError::NoMasterLink))?;
    let _sending = crate::replication::SnapshotSending::new(hub, &rep);
    crate::log_notice!(
        "Starting BGSAVE for SYNC with target: replicas sockets{}",
        if rdb_channel.is_some() {
            " (rdb-channel)"
        } else {
            ""
        }
    );
    crate::server_stats::add(crate::server_stats::Stat::SyncFull, 1);
    let mut guard = SyncImagesGuard {
        router,
        replica: client_id,
        armed: true,
    };
    let fail = |e: String| {
        hub.unregister_replica(client_id);
        crate::log_warning!("BGSAVE for replication failed: {}", e);
        Some(ReplicaSnapshotError::Failed(e))
    };
    // 1. Start every shard's image (each arms its cut in the same step),
    // with cross-shard writes held back so none lands between two shards'
    // cuts, and after this shard streamed what it holds: held bytes
    // streamed after the cut would reach the replica on top of an image
    // already holding them (other shards do the same when StartSyncImage
    // arrives).
    let paused = router.pause_cross_shard_writes().await;
    crate::replication::flush_stream_batch();
    let mut first_err =
        crate::shard::start_sync_image(&router.local_db.borrow(), router.shard_id, client_id)
            .err()
            .map(|e| format!("shard {}: {}", router.shard_id, e));
    let mut responders = Vec::new();
    for (sid, sender) in router.senders.iter().enumerate() {
        if sid == router.shard_id {
            continue;
        }
        let (tx, rx) = flume::bounded(1);
        if sender
            .send(crate::shard::ShardMessage::StartSyncImage {
                replica: client_id,
                responder: tx,
            })
            .is_ok()
        {
            responders.push((sid, rx));
        } else {
            first_err.get_or_insert(format!("shard {sid} is not reachable"));
        }
    }
    for (sid, rx) in responders {
        match rx.recv_async().await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                first_err.get_or_insert(format!("shard {sid}: {e}"));
            }
            Err(_) => {
                first_err.get_or_insert(format!("shard {sid} did not start its image"));
            }
        }
    }
    drop(paused);
    if let Some(e) = first_err {
        return Err(fail(e));
    }
    let (replid, offset, mut after_snapshot) = hub.finish_full_sync(client_id);
    drop(apply);
    let gone =
        |rep: &crate::replication::ConnectedReplica| rep.is_overflowed() || rep.is_disconnected();
    let dual = rdb_channel.is_some();
    let broke_off = |why: String| {
        if dual {
            // The replica's main connection may be streaming already.
            hub.withdraw_rdb_claim(client_id);
            rep.disconnect();
        }
        hub.unregister_replica(client_id);
        crate::log_warning!(
            "Streaming the full sync to replica {} failed: {}",
            name,
            why
        );
        None
    };
    // 2. +FULLRESYNC, the `$EOF:` header and the RDB file header. On a
    // dual-channel sync's RDB channel, `$ENDOFF:` instead tells the
    // replica where the stream continues and which connection to ask for
    // it; the stream waits here for that (see `run_master_replica_stream`).
    let mark = crate::replication::new_replid();
    let head = match rdb_channel {
        Some(wakeups) => {
            hub.offer_rdb_claim(
                client_id,
                crate::replication::RdbChannelClaim {
                    rep: rep.clone(),
                    wakeups,
                    replid: replid.clone(),
                    pre: std::mem::take(&mut after_snapshot),
                },
            );
            format!("$ENDOFF:{offset} {replid} {client_id}\r\n")
        }
        None => format!("+FULLRESYNC {replid} {offset}\r\n"),
    };
    let mut out = format!("{head}$EOF:{mark}\r\n").into_bytes();
    let mut rdb_head = Vec::new();
    let used_mem = router.local_db.borrow().total_used_memory() as u64;
    crate::redis_rdb::write_file_header(&mut rdb_head, used_mem);
    let mut crc = crate::table::crc64_update(0, &rdb_head);
    out.extend_from_slice(&rdb_head);
    if let Err(e) = writer.write_all(out).await.0 {
        return Err(broke_off(e.to_string()));
    }
    // 3. Each shard's part, a step at a time.
    // The buffer of the last piece sent, handed back with the next step
    // (see `AofImage::give_buffer`); a shard's last one goes to the next
    // shard, so no piece is freed on this thread mid-transfer. Counted
    // apart from maxmemory while here, like the images' own buffers.
    let mut recycle: Option<Vec<u8>> = None;
    // The share of the piece last sent (its buffer is `recycle`, then back
    // in its shard's image): replaced only once the next piece's share is
    // held, so the count never drops below what is out of the images.
    let mut in_flight = crate::shard::SyncImageHeld::default();
    for sid in 0..router.num_shards {
        loop {
            // `in_flight` keeps counting the piece handed back until the
            // step returns, by when its shard counts it as its spare: zeroed
            // first, the buffer counted as data in between (~1.25 MB too
            // much, enough to evict near maxmemory).
            let step = if sid == router.shard_id {
                let step =
                    crate::shard::sync_image_step_published(router, client_id, recycle.take());
                crate::mailbox::yield_now().await;
                step
            } else {
                let (tx, rx) = flume::bounded(1);
                if router.senders[sid]
                    .send(crate::shard::ShardMessage::SyncImageStep {
                        replica: client_id,
                        recycle: recycle.take(),
                        responder: tx,
                    })
                    .is_err()
                {
                    return Err(broke_off(format!("shard {sid} is not reachable")));
                }
                rx.recv_async()
                    .await
                    .unwrap_or_else(|_| Err(format!("shard {sid} did not return its data")))
            };
            let (piece, done, held) = match step {
                Ok(step) => step,
                Err(e) => return Err(broke_off(format!("shard {sid}: {e}"))),
            };
            in_flight = held;
            if !piece.is_empty() {
                crc = crate::table::crc64_update(crc, &piece);
                let (res, sent) = writer.write_all(piece).await;
                if let Err(e) = res {
                    return Err(broke_off(e.to_string()));
                }
                recycle = Some(sent);
            }
            if gone(&rep) {
                if rep.is_overflowed() {
                    replica_output_limit_reached(client_id, name);
                }
                if dual {
                    hub.withdraw_rdb_claim(client_id);
                    rep.disconnect();
                }
                hub.unregister_replica(client_id);
                return Err(None);
            }
            // `rdb-key-save-delay`, as for SAVE/BGSAVE: lets tests act
            // (kill the link, read INFO) while a full sync runs.
            crate::aof::rewrite_key_save_delay().await;
            if done {
                break;
            }
        }
    }
    guard.armed = false;
    drop(guard);
    // The last buffer is freed here, on this thread: recalibrate, so the
    // per-thread memory counts eviction reads are right again at once.
    drop(recycle);
    in_flight.set(0);
    if crate::tiering::sole_server_in_process()
        && let Some(global) = crate::allocator::global_allocated()
    {
        router.tier_stats.calibrate_unattributed(global);
    }
    // 4. The RDB's end and the mark. The stream written meanwhile is
    // returned, not sent: it may only follow once the replica acks (see
    // the caller).
    let tail = streamed_sync_tail(hub.saved_history(), crc, &mark);
    if let Err(e) = writer.write_all(tail).await.0 {
        return Err(broke_off(e.to_string()));
    }
    Ok((rep, after_snapshot))
}

/// Ends a (P)SYNC whose snapshot [`snapshot_for_replica`] could not make.
pub(super) async fn replica_snapshot_failed(
    writer: &mut monoio::net::tcp::TcpOwnedWriteHalf,
    name: &str,
    err: ReplicaSnapshotError,
) {
    match err {
        ReplicaSnapshotError::NoMasterLink => no_master_link(writer, name).await,
        ReplicaSnapshotError::Failed(e) => {
            let msg = format!(
                "-ERR full sync failed: {}\r\n",
                e.replace(['\r', '\n'], " ")
            );
            let _ = writer.write_all(msg.into_bytes()).await;
        }
    }
}

/// The RDB channel of a dual-channel full sync (see
/// `replication::dual_channel_replication`): a streamed full sync whose
/// stream waits for the replica's main connection to take it, announced by
/// `$ENDOFF:<offset> <replid> <this client id>` ahead of the `$EOF:` RDB.
/// Once the RDB is sent this connection is done; a stream nobody took
/// within `repl-timeout` is dropped.
pub(super) async fn serve_rdb_channel(
    hub: &std::sync::Arc<crate::replication::ReplicationHub>,
    router: &Rc<Router>,
    client_id: u64,
    write_tx: &flume::Sender<Vec<u8>>,
    write_rx: flume::Receiver<Vec<u8>>,
    writer: &mut monoio::net::tcp::TcpOwnedWriteHalf,
    name: &str,
) {
    if !hub.replica_takes_eof(client_id) {
        let _ = writer
            .write_all(b"-ERR an RDB channel needs REPLCONF capa eof\r\n".to_vec())
            .await;
        return;
    }
    crate::log_notice!("Replica {} opened the RDB channel of its full sync", name);
    match stream_full_sync(
        hub,
        router,
        client_id,
        write_tx,
        writer,
        name,
        Some(write_rx),
    )
    .await
    {
        Ok((rep, _)) => {
            crate::log_notice!("Synchronization with replica {} succeeded", name);
            let deadline = std::time::Instant::now() + crate::replication::repl_timeout();
            while hub.rdb_claim_pending(client_id) && std::time::Instant::now() < deadline {
                monoio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            if hub.withdraw_rdb_claim(client_id) {
                crate::log_warning!(
                    "Replica {} never came back for the stream of its full sync",
                    name
                );
                rep.disconnect();
                hub.unregister_replica(client_id);
            }
        }
        Err(Some(e)) => replica_snapshot_failed(writer, name, e).await,
        Err(None) => {}
    }
}

pub(super) async fn run_master_replica_stream(
    stream: TcpStream,
    client_id: u64,
    _client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    router: Rc<Router>,
    psync_cmd: Command,
) {
    let hub = crate::replication::get_replication_hub(router.base_port);
    let raw_fd = std::os::unix::io::AsRawFd::as_raw_fd(&stream);
    if let Ok(peer) = stream.peer_addr() {
        hub.note_replica_ip(client_id, peer.ip());
    }
    let name = hub.replica_name(client_id);
    crate::log_notice!("Replica {} asks for synchronization", name);
    let (mut reader, mut writer) = stream.into_split();
    let _unregister = UnregisterFdOnDrop {
        port: router.port,
        client_id,
    };
    let (write_tx, write_rx) = flume::unbounded::<Vec<u8>>();
    let mut link = ReplicaLink {
        hub: hub.clone(),
        client_id,
        repl: None,
        name: name.clone(),
        active: true,
    };

    let is_sync = matches!(&psync_cmd, Command::Sync);

    // Our master's FAILOVER: it asks us to take over, then continues as
    // our replica with this same PSYNC.
    if let Command::Psync {
        replid,
        failover: true,
        ..
    } = &psync_cmd
    {
        let replid = String::from_utf8_lossy(replid).into_owned();
        crate::log_notice!("Failover request received for replid {}.", replid);
        if !hub.is_slave() {
            let _ = writer
                .write_all(b"-ERR PSYNC FAILOVER can't be sent to a master.\r\n".to_vec())
                .await;
            return;
        }
        if replid != hub.current_replid() {
            let _ = writer
                .write_all(b"-ERR PSYNC FAILOVER replid must match my replid.\r\n".to_vec())
                .await;
            return;
        }
        hub.stop_sync_and_wait().await;
        hub.make_master();
        crate::log_notice!("MASTER MODE enabled (failover request from '{}')", name);
    }
    // Don't let replicas sync with us while we fail over.
    if hub.failover_phase() != crate::replication::FailoverPhase::None {
        let _ = writer
            .write_all(b"-NOMASTERLINK Can't SYNC while failing over\r\n".to_vec())
            .await;
        return;
    }

    // Like Redis, a replica serves (P)SYNC only while its own master link
    // is up: before that it has no consistent data and history to offer.
    if !hub.can_serve_replicas() {
        no_master_link(&mut writer, &name).await;
        return;
    }
    hub.ensure_pinger();

    // Dual-channel replication (see `replication::dual_channel_replication`):
    // this connection is the replica's RDB channel: it gets the snapshot
    // only, while its main connection takes the stream after it.
    if hub.is_rdb_channel(client_id) {
        link.active = false;
        serve_rdb_channel(
            &hub,
            &router,
            client_id,
            &write_tx,
            write_rx,
            &mut writer,
            &name,
        )
        .await;
        return;
    }

    let repl;
    // After a streamed (`$EOF:`) full sync: the stream written during the
    // transfer, held back until the replica's first ack (see below).
    let mut after_ack: Option<Vec<u8>> = None;
    // The wakeups of the RDB channel's stream this connection took.
    let mut claimed_wakeups = None;
    if let Some(rdb_id) = hub.rdb_client_id_of(client_id) {
        // The replica's main connection, once its RDB channel started the
        // snapshot: it takes the stream from right after it, buffering it
        // while the RDB arrives and loads.
        let Some(claim) = hub.take_rdb_claim(rdb_id) else {
            let _ = writer
                .write_all(b"-ERR no full sync waits for this rdb-client-id\r\n".to_vec())
                .await;
            return;
        };
        crate::log_notice!(
            "Replica {} takes the stream of its RDB channel's full sync",
            name
        );
        let mut msg = format!("+CONTINUE {}\r\n", claim.replid).into_bytes();
        msg.extend_from_slice(&claim.pre);
        if writer.write_all(msg).await.0.is_err() {
            claim.rep.disconnect();
            hub.unregister_replica(claim.rep.id);
            return;
        }
        link.client_id = claim.rep.id;
        repl = claim.rep;
        claimed_wakeups = Some(claim.wakeups);
    } else if !is_sync {
        let (req_replid, req_offset) = match &psync_cmd {
            Command::Psync { replid, offset, .. } => {
                (std::str::from_utf8(replid).unwrap_or(""), *offset)
            }
            _ => ("", -1),
        };

        let partial = hub.try_partial_resync(client_id, write_tx.clone(), req_replid, req_offset);
        if let Some((replid, diff, rep)) = partial {
            repl = rep;
            crate::server_stats::add(crate::server_stats::Stat::SyncPartialOk, 1);
            crate::log_notice!(
                "Partial resynchronization request from {} accepted. Sending {} bytes of backlog starting from offset {}.",
                name,
                diff.len(),
                req_offset
            );
            let mut initial_msg = format!("+CONTINUE {}\r\n", replid).into_bytes();
            initial_msg.extend_from_slice(&diff);
            if writer.write_all(initial_msg).await.0.is_err() {
                hub.unregister_replica(client_id);
                return;
            }
        } else {
            // Like Redis, a failed partial resync only counts as an error
            // when the replica asked for a specific history.
            if req_replid != "?" {
                crate::server_stats::add(crate::server_stats::Stat::SyncPartialErr, 1);
                crate::log_notice!(
                    "Partial resynchronization not accepted: replica {} asked for '{}' offset {}, my replication ID is '{}' and the offset is not in the backlog",
                    name,
                    req_replid,
                    req_offset,
                    hub.replid()
                );
            } else {
                crate::log_notice!("Full resync requested by replica {}", name);
            }
            let delay = crate::replication::REPL_DISKLESS_SYNC_DELAY
                .load(std::sync::atomic::Ordering::Relaxed);
            if delay > 0 && !hub.replica_takes_dual_channel(client_id) {
                monoio::time::sleep(std::time::Duration::from_secs(delay)).await;
            }
            if hub.replica_takes_dual_channel(client_id) {
                // The replica opens its RDB channel, then comes back for
                // the stream (see above).
                crate::log_notice!("Replica {} gets its full sync over two connections", name);
                let _ = writer.write_all(b"+DUALCHANNELSYNC\r\n".to_vec()).await;
                return;
            } else if hub.replica_takes_eof(client_id) {
                // No eviction pause: the images' buffers don't count toward
                // maxmemory (SYNC_IMAGE_BYTES), and sent pieces go back to
                // their shard rather than being freed on this thread.
                match stream_full_sync(
                    &hub,
                    &router,
                    client_id,
                    &write_tx,
                    &mut writer,
                    &name,
                    None,
                )
                .await
                {
                    Ok((rep, after_snapshot)) => {
                        repl = rep;
                        after_ack = Some(after_snapshot);
                    }
                    Err(Some(e)) => {
                        replica_snapshot_failed(&mut writer, &name, e).await;
                        return;
                    }
                    Err(None) => return,
                }
            } else {
                // Register before any shard serializes, so each shard's changes
                // after its snapshot reach the replica (see FullSyncCut).
                // No eviction until the snapshot is built and sent (dropped at
                // the end of this block, after the buffers).
                let _pause = crate::tiering::SnapshotEvictionPause::new(router.tier_stats.clone());
                let (rdb, replid, offset, after_snapshot, rep) =
                    match snapshot_for_replica(&hub, &router, client_id, &write_tx).await {
                        Ok(snapshot) => snapshot,
                        Err(e) => {
                            replica_snapshot_failed(&mut writer, &name, e).await;
                            return;
                        }
                    };
                repl = rep;
                if repl.is_overflowed() || repl.is_disconnected() {
                    if repl.is_overflowed() {
                        replica_output_limit_reached(client_id, &name);
                    }
                    hub.unregister_replica(client_id);
                    return;
                }

                let mut initial_msg =
                    format!("+FULLRESYNC {} {}\r\n${}\r\n", replid, offset, rdb.len()).into_bytes();
                initial_msg.extend_from_slice(&rdb);
                initial_msg.extend_from_slice(&after_snapshot);
                if let Err(e) = writer.write_all(initial_msg).await.0 {
                    crate::log_warning!("Sending the full sync to replica {} failed: {}", name, e);
                    hub.unregister_replica(client_id);
                    return;
                }
            }
        }
    } else {
        crate::log_notice!("Full resync requested by replica {}", name);
        // No eviction until the snapshot is built and sent (dropped at the
        // end of this block, after the buffers).
        let _pause = crate::tiering::SnapshotEvictionPause::new(router.tier_stats.clone());
        let (rdb, _, _, after_snapshot, rep) =
            match snapshot_for_replica(&hub, &router, client_id, &write_tx).await {
                Ok(snapshot) => snapshot,
                Err(e) => {
                    replica_snapshot_failed(&mut writer, &name, e).await;
                    return;
                }
            };
        repl = rep;
        if repl.is_overflowed() || repl.is_disconnected() {
            if repl.is_overflowed() {
                replica_output_limit_reached(client_id, &name);
            }
            hub.unregister_replica(client_id);
            return;
        }
        let mut initial_msg = format!("${}\r\n", rdb.len()).into_bytes();
        initial_msg.extend_from_slice(&rdb);
        initial_msg.extend_from_slice(&after_snapshot);
        if let Err(e) = writer.write_all(initial_msg).await.0 {
            crate::log_warning!("Sending the full sync to replica {} failed: {}", name, e);
            hub.unregister_replica(client_id);
            return;
        }
    }

    if repl.full_sync.is_some() && claimed_wakeups.is_none() {
        crate::log_notice!("Synchronization with replica {} succeeded", name);
    }
    // The replica's id: this connection's, or its RDB channel's.
    let client_id = repl.id;
    let write_rx = claimed_wakeups.unwrap_or(write_rx);
    repl.attach_fd(raw_fd);
    link.repl = Some(repl.clone());
    let writer_hub = hub.clone();
    // A replica that took a `$EOF:` transfer finds its end by the mark being
    // the last bytes of a read (Redis replicas do), so nothing may follow
    // the mark until it has loaded and acked, as Redis masters do it. The
    // stream meanwhile waits in the replica's buffer. Sending it right after
    // the mark left Redis replicas waiting for the end forever.
    let (ack_tx, ack_rx) = flume::bounded::<()>(1);
    let mut ack_tx = Some(ack_tx);
    monoio::spawn(async move {
        if let Some(after) = after_ack {
            if ack_rx.recv_async().await.is_err() {
                // The link ended before the replica acked.
                repl.detach_fd();
                writer_hub.unregister_replica(client_id);
                return;
            }
            if !after.is_empty() && writer.write_all(after).await.0.is_err() {
                repl.detach_fd();
                writer_hub.unregister_replica(client_id);
                return;
            }
            // Wakeups for what was queued while waiting are still in the
            // channel: the loop below sends it.
        }
        // Each wakeup carries no data: everything queued since the last one
        // is in the replica's pending buffer and goes out in one write.
        while write_rx.recv_async().await.is_ok() {
            if repl.is_overflowed() || repl.is_disconnected() {
                break;
            }
            let data = repl.take_pending();
            if data.is_empty() {
                continue;
            }
            let failed = writer.write_all(data).await.0.is_err();
            repl.finish_write();
            if failed {
                break;
            }
        }
        if repl.is_overflowed() {
            // Like Redis, drop the link; the replica reconnects and
            // resyncs. Shutting the socket down also ends the reader.
            replica_output_limit_reached(client_id, &name);
            repl.drop_link();
        }
        repl.detach_fd();
        writer_hub.unregister_replica(client_id);
    });

    read_replica_acks(&mut reader, |off, fack| {
        if let Some(tx) = ack_tx.take() {
            let _ = tx.send(());
        }
        hub.update_replica_ack(client_id, off, fack)
    })
    .await;
    drop(link);
}

pub(super) async fn run_shard_replication_flow(
    stream: TcpStream,
    client_id: u64,
    router: Rc<Router>,
    shard_id: usize,
    _lsn: Option<u64>,
) {
    let hub = crate::replication::get_replication_hub(router.base_port);
    let raw_fd = std::os::unix::io::AsRawFd::as_raw_fd(&stream);
    let (mut reader, mut writer) = stream.into_split();
    let _unregister = UnregisterFdOnDrop {
        port: router.port,
        client_id,
    };
    if shard_id >= router.num_shards {
        let _ = writer
            .write_all(b"-ERR invalid shard id\r\n".to_vec())
            .await;
        return;
    }
    let (write_tx, write_rx) = flume::bounded::<Vec<u8>>(4096);

    // Bytes this thread holds predate the snapshot below: stream them
    // before the flow exists, as unbatched writes would have been.
    crate::replication::flush_stream_batch();
    let flow_link = ShardFlowLink {
        hub: hub.clone(),
        shard_id,
        client_id,
        flow: hub.register_shard_flow(shard_id, client_id, write_tx.clone(), Some(raw_fd)),
    };

    // Fetch this shard's RDB chunk. A shard that can't serialize all its
    // keys fails the flow rather than hand out a partial dataset.
    let chunk = if shard_id == router.shard_id {
        let mut buf = Vec::new();
        router
            .local_db
            .borrow_mut()
            .save_rdb_chunk(&mut buf)
            .map(|()| buf)
            .map_err(|e| e.to_string())
    } else {
        let (tx, rx) = flume::bounded(1);
        let msg = crate::shard::ShardMessage::SaveRdbChunk {
            responder: tx,
            arm_replica: None,
        };
        if router.senders[shard_id].send(msg).is_ok() {
            match rx.recv_async().await {
                Ok(res) => res.map_err(|e| e.to_string()),
                Err(_) => Err("shard did not return its data".to_string()),
            }
        } else {
            Err("shard is not reachable".to_string())
        }
    };
    let chunk = match chunk {
        Ok(chunk) => chunk,
        Err(e) => {
            crate::log_warning!("Shard {} replication flow failed: {}", shard_id, e);
            hub.unregister_shard_flow(shard_id, client_id);
            let msg = format!(
                "-ERR shard flow failed: {}\r\n",
                e.replace(['\r', '\n'], " ")
            );
            let _ = writer.write_all(msg.into_bytes()).await;
            return;
        }
    };

    let mut initial_msg = format!("+OK FLOW {}\r\n${}\r\n", shard_id, chunk.len()).into_bytes();
    initial_msg.extend_from_slice(&chunk);
    initial_msg.extend_from_slice(b"\r\n");
    if writer.write_all(initial_msg).await.0.is_err() {
        hub.unregister_shard_flow(shard_id, client_id);
        return;
    }

    let writer_hub = hub.clone();
    monoio::spawn(async move {
        while let Ok(data) = write_rx.recv_async().await {
            if writer.write_all(data).await.0.is_err() {
                break;
            }
        }
        writer_hub.unregister_shard_flow(shard_id, client_id);
    });

    read_replica_acks(&mut reader, |off, _| {
        hub.update_shard_flow_ack(shard_id, client_id, off)
    })
    .await;
    drop(flow_link);
}
