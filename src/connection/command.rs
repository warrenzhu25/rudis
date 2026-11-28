use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) async fn execute_command_inner(
    cmd: Command,
    raw_argv: Option<crate::resp::RawArgv>,
    stat_slot: Option<u16>,
    router: &Router,
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    out: &mut Vec<u8>,
    asking: &mut bool,
    authenticated: &mut bool,
    auth_user: &mut String,
) -> bool {
    let mut initial_reply_mode = crate::resp::ClientReplyMode::On;
    let mut client_stats = None;
    if let Some(client) = client_registry.borrow().get(&client_id) {
        CURRENT_CLIENT_RESP3.set(client.is_resp3);
        CURRENT_DB.set(client.db);
        initial_reply_mode = client.reply_mode;
        client_stats = Some(client.stats.clone());
    }
    if crate::shard::aof_image_active() {
        // The router's fast paths change this shard's keys directly.
        aof_image_before_command(&router.local_db.borrow(), &cmd, |k| {
            router.target_shard(k) == router.shard_id
        });
    }
    router.local_db.borrow_mut().select_db(CURRENT_DB.get());
    struct ExecutingClientGuard {
        prev_local: u64,
        prev_exec: u64,
        stats: Option<std::sync::Arc<ClientStats>>,
    }
    impl Drop for ExecutingClientGuard {
        fn drop(&mut self) {
            CURRENT_CLIENT_ID.set(self.prev_local);
            EXECUTING_CLIENT_ID.set(self.prev_exec);
            if let Some(ref st) = self.stats {
                st.tot_cmds
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
    let _exec_client_guard = ExecutingClientGuard {
        prev_local: CURRENT_CLIENT_ID.replace(client_id),
        prev_exec: EXECUTING_CLIENT_ID.replace(client_id),
        stats: client_stats,
    };

    let is_caching_or_trackinginfo = matches!(
        cmd,
        Command::Client(ClientSubcommand::Caching(_) | ClientSubcommand::TrackingInfo)
            | Command::Multi
    );
    struct CachingResetGuard {
        port: u16,
        client_id: u64,
        should_reset: bool,
    }
    impl Drop for CachingResetGuard {
        fn drop(&mut self) {
            if self.should_reset && HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed)
            {
                reset_client_caching(self.port, self.client_id);
            }
        }
    }
    let _caching_guard = CachingResetGuard {
        port: router.port,
        client_id,
        should_reset: !is_caching_or_trackinginfo,
    };

    if crate::asm::any_write_pause() && counts_as_running_write(&cmd) {
        crate::asm::wait_write_pause(router.base_port, &cmd).await;
    }
    let _write_running = wait_if_client_paused(router.port, client_id, &cmd).await;
    if !allowed_while_busy(&cmd)
        && let Some(busy) = crate::scripting::busy_script_reply(router.port)
    {
        out.extend_from_slice(busy);
        return false;
    }
    if has_monitor_clients() && !matches!(cmd, Command::Monitor | Command::Reset | Command::Quit) {
        let addr_s = client_registry
            .borrow()
            .get(&client_id)
            .map(|c| c.addr.to_string())
            .unwrap_or_default();
        let monitor_argv = shown_argv(raw_argv.as_ref(), &cmd)
            .unwrap_or_else(|| crate::slowlog::command_to_monitor_argv(&cmd));
        broadcast_monitor(router.port, &addr_s, &monitor_argv);
    }
    // Also feeds LATENCY HISTOGRAM, all in thread-local state.
    crate::scripting::SCRIPT_RECORDED_ERROR.set(false);
    let cmd_stat_guard = CmdStatGuard {
        name: stat_cmd_name(&cmd),
        start: (!may_block(&cmd)).then(std::time::Instant::now),
        port: router.port,
        client_id,
        rejected: std::cell::Cell::new(false),
        slot: stat_slot,
        hot: crate::hotkeys::active().then(|| hotkeys_command_info(&cmd)),
    };
    if cmd.is_write_command() && !dirty_counted_on_change(&cmd) {
        crate::snapshot::note_write();
    }

    let slow_threshold =
        crate::slowlog::SLOWLOG_LOG_SLOWER_THAN.load(std::sync::atomic::Ordering::Relaxed);
    let lat_threshold = LATENCY_MONITOR_THRESHOLD.load(std::sync::atomic::Ordering::Relaxed);
    let skip_slowlog = matches!(cmd, Command::Quit | Command::Exec | Command::Commandlog(_))
        || (slow_threshold >= 10_000 && matches!(cmd, Command::Mset(_) | Command::Mget(_)));
    let (cmd_for_slowlog, exec_start) =
        if (slow_threshold >= 0 && !skip_slowlog) || lat_threshold > 0 {
            (Some(cmd.clone()), Some(std::time::Instant::now()))
        } else {
            (None, None)
        };
    struct SlowlogTracker<'a> {
        cmd: Option<Command>,
        /// The command's arguments as sent, for its log entries.
        raw_argv: Option<crate::resp::RawArgv>,
        start: Option<std::time::Instant>,
        threshold: i64,
        skip_slowlog: bool,
        reply_bytes: usize,
        client_id: u64,
        client_registry: &'a RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    }
    impl<'a> Drop for SlowlogTracker<'a> {
        fn drop(&mut self) {
            if let (Some(cmd), Some(start)) = (self.cmd.take(), self.start.take()) {
                let elapsed = start.elapsed();
                let duration_us = elapsed.as_micros() as u64;
                let duration_ms = elapsed.as_millis() as u64;
                if duration_ms > 0 {
                    record_latency_event("command", duration_ms);
                }
                if !self.skip_slowlog {
                    let raw = shown_argv(self.raw_argv.as_ref(), &cmd);
                    let req_thresh = crate::slowlog::COMMANDLOG_REQUEST_LARGER_THAN
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let rep_thresh = crate::slowlog::COMMANDLOG_REPLY_LARGER_THAN
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let is_slow = self.threshold >= 0 && duration_us >= self.threshold as u64;
                    let is_large_rep = rep_thresh >= 0 && (self.reply_bytes as i64) >= rep_thresh;
                    let req_bytes = if req_thresh >= 0 {
                        let b = raw.as_ref().map_or_else(
                            || crate::slowlog::estimate_command_request_bytes(&cmd),
                            |r| r.iter().map(|a| a.len()).sum(),
                        );
                        ((b as i64) >= req_thresh).then_some(b)
                    } else {
                        None
                    };
                    if is_slow || is_large_rep || req_bytes.is_some() {
                        let _raw = crate::slowlog::RawArgvScope::set(raw);
                        let (addr_str, name_str) =
                            if let Some(c) = self.client_registry.borrow().get(&self.client_id) {
                                (c.addr.to_string(), c.name.clone().unwrap_or_default())
                            } else {
                                ("".to_string(), "".to_string())
                            };
                        if is_slow {
                            crate::slowlog::log_command_if_slow(
                                &cmd,
                                duration_us,
                                &addr_str,
                                &name_str,
                            );
                        }
                        if let Some(b) = req_bytes {
                            crate::slowlog::log_command_if_large_request(
                                &cmd, b, &addr_str, &name_str,
                            );
                        }
                        if is_large_rep {
                            crate::slowlog::log_command_if_large_reply(
                                &cmd,
                                self.reply_bytes,
                                &addr_str,
                                &name_str,
                            );
                        }
                    }
                }
            }
        }
    }
    let mut _slowlog_guard = SlowlogTracker {
        raw_argv: if cmd_for_slowlog.is_some() {
            raw_argv
        } else {
            None
        },
        cmd: cmd_for_slowlog,
        start: exec_start,
        threshold: slow_threshold,
        skip_slowlog,
        reply_bytes: 0,
        client_id,
        client_registry,
    };

    let error_stat_start = out.len();
    let is_client_reply_cmd = matches!(cmd, Command::Client(ClientSubcommand::Reply(_)));
    // Set once the command reaches the point where CLIENT REPLY OFF/SKIP
    // applies; its reply is then dropped after the body below.
    let mut reply_mode_start: Option<usize> = None;
    // The body runs as an async block so that its many early `return`s
    // leave only the block, and the reply post-processing below sees `out`
    // through the normal borrow (it used to be done by drop guards holding
    // a raw pointer to `out`, aliasing the live `&mut`).
    let ret: bool = async {

    CURRENT_AUTH_USER.with(|u| *u.borrow_mut() = auth_user.clone());

    // Before the auth checks, as in Redis' processCommand.
    if let Some(reply) =
        protected_command_denied(&cmd, router.base_port, client_id, client_registry)
    {
        cmd_stat_guard.rejected.set(true);
        out.extend_from_slice(reply);
        return false;
    }

    if MIN_REPLICAS_TO_WRITE.load(std::sync::atomic::Ordering::Relaxed) > 0
        && cmd.is_write_command()
        && not_enough_good_replicas(router.base_port)
    {
        cmd_stat_guard.rejected.set(true);
        out.extend_from_slice(b"-NOREPLICAS Not enough good replicas to write.\r\n");
        return false;
    }

    if !*authenticated
        && !matches!(
            cmd,
            Command::Auth { .. } | Command::Hello { .. } | Command::Quit
        )
    {
        cmd_stat_guard.rejected.set(true);
        out.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
        return false;
    }

    if *authenticated
        && let Some(err) = acl_denied(router.port, auth_user, &cmd)
    {
        cmd_stat_guard.rejected.set(true);
        out.extend_from_slice(&err);
        return false;
    }

    if let Command::Asking = cmd {
        *asking = true;
        out.extend_from_slice(b"+OK\r\n");
        return false;
    }

    let is_asking = *asking || matches!(cmd, Command::Restore { asking: true, .. });
    *asking = false;

    if router.cluster_enabled {
        let keys = cmd_keys(&cmd);
        if keys.len() > 1 {
            let first_slot = key_slot(keys[0]);
            for k in &keys[1..] {
                if key_slot(k) != first_slot {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                    return false;
                }
            }
        }
    } else if cross_shard_strict()
        && router.num_shards > 1
        && keys_on_several_shards(cmd_keys(&cmd), router.num_shards)
    {
        out.extend_from_slice(b"-CROSSSLOT Keys in request don't hash to the same slot\r\n");
        return false;
    }

    if cluster_redirects(&cmd, router, is_asking, client_id, client_registry, out).await {
        return false;
    }

    if crate::replication::is_loading(router.base_port)
        && (!allowed_while_loading(&cmd)
            || (crate::replication::is_startup_loading(router.base_port)
                && refused_during_startup_load(&cmd)))
        && !crate::replication::wait_out_short_load(router.base_port).await
    {
        out.extend_from_slice(b"-LOADING Redis is loading the dataset in memory\r\n");
        return false;
    }
    if crate::replication::is_async_loading(router.base_port) && refused_during_async_load(&cmd) {
        out.extend_from_slice(b"-LOADING Redis is loading the dataset in memory\r\n");
        return false;
    }

    if let Some(err) = persistence_misconf(router)
        && (cmd.is_write_command() || crate::aof::command_to_resp(&cmd).is_some())
    {
        out.extend_from_slice(err.as_bytes());
        return false;
    }

    if !in_tx(client_id)
        && let Some(reason) = masterdown_refuses(router, &cmd)
    {
        out.push(b'-');
        out.extend_from_slice(reason);
        return false;
    }

    // Not for a command of a running EXEC: like Redis, those passed the
    // check when queued, and run even after a REPLICAOF in the same EXEC.
    if crate::replication::HAS_SLAVE_INSTANCE.load(std::sync::atomic::Ordering::Relaxed)
        && REPLICA_READ_ONLY.load(std::sync::atomic::Ordering::Relaxed)
        && crate::replication::cached_hub(router.base_port).is_slave()
        && (cmd.is_write_command() || crate::aof::command_to_resp(&cmd).is_some())
        && !in_tx(client_id)
    {
        cmd_stat_guard.rejected.set(true);
        out.extend_from_slice(b"-READONLY You can't write against a read only replica.\r\n");
        return false;
    }

    // Enforce maxmemory: evict or return OOM if memory limit is exceeded.
    // Like Redis, a replica with `replica-ignore-maxmemory` (the default)
    // leaves that to its master, whose evictions reach it as DELs.
    let max_mem = router
        .tier_stats
        .max_memory
        .load(std::sync::atomic::Ordering::Relaxed);
    if max_mem > 0 && !maxmemory_ignored_as_replica(router.base_port) {
        let policy = get_max_memory_policy();
        if policy != "noeviction" {
            if !matches!(
                cmd,
                Command::ConfigGet(_) | Command::ConfigSet { .. } | Command::Info(_)
            ) {
                let under = router
                    .evict_until_under_maxmemory(max_mem as usize, &policy, client_id, out)
                    .await;
                if !under && cmd.is_write_command() && !cmd.allows_oom() {
                    cmd_stat_guard.rejected.set(true);
                    log_oom_rejection(max_mem, &policy);
                    out.extend_from_slice(
                        b"-OOM command not allowed when used memory > 'maxmemory'.\r\n",
                    );
                    return false;
                }
            }
        } else if cmd.is_write_command() && !cmd.allows_oom() {
            // noeviction never deletes data, but values may still be offloaded
            // to the NVMe tier; reject only while the server stays over the
            // limit (other shards' published usage, refreshed when it says
            // "over") after the tier had a chance to make room.
            let over = router.over_maxmemory_checked().await && {
                router.check_auto_tier().await;
                router.over_maxmemory()
            };
            if over {
                cmd_stat_guard.rejected.set(true);
                log_oom_rejection(max_mem, &policy);
                out.extend_from_slice(
                    b"-OOM command not allowed when used memory > 'maxmemory'.\r\n",
                );
                return false;
            }
        }
    }

    reply_mode_start = Some(out.len());

    match cmd {
        Command::Get(key) => {
            let target = target_shard(&key, router.num_shards);
            let val = if target == router.shard_id {
                router.get_local_checked(&key).await
            } else {
                router.get_checked(key.clone()).await
            };
            record_client_read(router.port, client_id, key.as_ref());
            match val {
                Ok(Some(v)) => {
                    write_resp_bulk(out, &v);
                }
                Ok(None) => {
                    write_resp_null(out);
                }
                Err(err) => write_resp_err(out, err),
            }
            false
        }
        Command::Getex {
            key,
            expire_in,
            persist,
        } => {
            record_client_read(router.port, client_id, key.as_ref());
            let val = match router.get_checked(key.clone()).await {
                Ok(val) => val,
                Err(err) => {
                    write_resp_err(out, err);
                    return false;
                }
            };
            match val {
                Some(v) => {
                    let ttl_change = if persist {
                        router
                            .persist(key.clone())
                            .await
                            .then(|| ("persist", Command::Persist(key.clone())))
                    } else if let Some(exp) = expire_in {
                        let opts = crate::resp::ExpireOptions::default();
                        router.expire(key.clone(), exp, opts).await.then(|| {
                            let cmd = Command::Expire {
                                key: key.clone(),
                                duration: exp,
                                opts,
                            };
                            ("expire", cmd)
                        })
                    } else {
                        None
                    };
                    if let Some((event, change)) = ttl_change {
                        if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                            touch_watched_key(router.port, key.as_ref());
                        }
                        if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                            notify_key_invalidation(router.port, key.as_ref(), client_id);
                        }
                        // The owner shard logged and replicated the change.
                        let _ = change;
                        notify_keyspace_event_sync(router, NOTIFY_GENERIC, event, key.as_ref());
                    }
                    write_resp_bulk(out, &v);
                }
                None => {
                    write_resp_null(out);
                }
            }
            false
        }
        Command::Set {
            key,
            value,
            expire_in,
            condition,
            get,
            keepttl,
            past_expired,
        } => {
            let target = target_shard(&key, router.num_shards);
            if target == router.shard_id {
                if condition == crate::resp::SetCondition::None
                    && !get
                    && !keepttl
                    && !past_expired
                    && router.aof().is_none()
                    && !crate::replication::has_connected_replicas(router.base_port)
                    && crate::connection::NOTIFY_KEYSPACE_FLAGS
                        .load(std::sync::atomic::Ordering::Relaxed)
                        == 0
                {
                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                    {
                        let mut db = router.local_db.borrow_mut();
                        db.set_extended(key.clone(), value, expire_in, false);
                        notify_stream_or_defer(&mut db, &key);
                    }
                    notify_keyspace_event_sync(router, NOTIFY_STRING, "set", key.as_ref());
                    if expire_in.is_some() {
                        notify_keyspace_event_sync(router, NOTIFY_GENERIC, "expire", key.as_ref());
                    }
                    router.check_auto_tier_after_write();
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                }
                let mut db = router.local_db.borrow_mut();
                // The old value only for GET and the IF* conditions (see
                // `execute_local_command`).
                let needs_current = get
                    || matches!(
                        condition,
                        crate::resp::SetCondition::Ifeq(_)
                            | crate::resp::SetCondition::Ifne(_)
                            | crate::resp::SetCondition::Ifdeq(_)
                            | crate::resp::SetCondition::Ifdne(_)
                    );
                let current_val = if needs_current {
                    match db.table.get(&key) {
                        Ok(val) => val,
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    None
                };

                let exists = db.exists(&key);
                let condition_met = match &condition {
                    crate::resp::SetCondition::None => true,
                    crate::resp::SetCondition::Nx => !exists,
                    crate::resp::SetCondition::Xx => exists,
                    crate::resp::SetCondition::Ifeq(expected) => {
                        current_val.as_ref() == Some(expected)
                    }
                    crate::resp::SetCondition::Ifne(expected) => match &current_val {
                        None => true,
                        Some(val) => val != expected,
                    },
                    crate::resp::SetCondition::Ifdeq(expected_digest) => match &current_val {
                        None => false,
                        Some(val) => {
                            if expected_digest.len() != 16
                                || !expected_digest.iter().all(|b| b.is_ascii_hexdigit())
                            {
                                write_resp_err(
                                    out,
                                    "ERR digest must be exactly 16 hexadecimal characters",
                                );
                                return false;
                            }
                            let d = crate::table::compute_digest(val);
                            d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected_digest))
                        }
                    },
                    crate::resp::SetCondition::Ifdne(expected_digest) => match &current_val {
                        None => true,
                        Some(val) => {
                            if expected_digest.len() != 16
                                || !expected_digest.iter().all(|b| b.is_ascii_hexdigit())
                            {
                                write_resp_err(
                                    out,
                                    "ERR digest must be exactly 16 hexadecimal characters",
                                );
                                return false;
                            }
                            let d = crate::table::compute_digest(val);
                            !d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected_digest))
                        }
                    },
                };

                if !condition_met {
                    if get {
                        if let Some(v) = current_val {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        } else {
                            write_resp_null(out);
                        }
                    } else {
                        write_resp_null(out);
                    }
                    return false;
                }

                if past_expired {
                    crate::table::inc_expired_keys();
                    if exists {
                        db.del(&key);
                        router.log_mutation(|| server_del_cmd(&key));
                    }
                    if get {
                        if let Some(v) = current_val {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        } else {
                            write_resp_null(out);
                        }
                    } else {
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    return false;
                }

                notify_key_invalidation(router.port, key.as_ref(), client_id);
                let prev_kind = db.type_of(key.as_ref());
                db.set_extended(key.clone(), value.clone(), expire_in, keepttl);
                notify_stream_or_defer(&mut db, &key);
                router.log_mutation(|| Command::Set {
                    key: key.clone(),
                    value: value.clone(),
                    expire_in,
                    condition,
                    get,
                    keepttl,
                    past_expired,
                });

                if get {
                    if let Some(v) = current_val {
                        write_resp_bulk(out, &v);
                    } else {
                        write_resp_null(out);
                    }
                } else {
                    out.extend_from_slice(b"+OK\r\n");
                }
                notify_set_key_events(router, prev_kind, "string", key.as_ref());
                notify_keyspace_event_sync(router, NOTIFY_STRING, "set", key.as_ref());
                if expire_in.is_some() {
                    notify_keyspace_event_sync(router, NOTIFY_GENERIC, "expire", key.as_ref());
                }
                false
            } else {
                if condition == crate::resp::SetCondition::None
                    && !get
                    && !keepttl
                    && !past_expired
                    && router.aof().is_none()
                    && !crate::replication::has_connected_replicas(router.base_port)
                    && crate::connection::NOTIFY_KEYSPACE_FLAGS
                        .load(std::sync::atomic::Ordering::Relaxed)
                        == 0
                {
                    // The fast path's shard doesn't invalidate; the full
                    // path's does (its `record_change!`), so invalidating
                    // here too sent BCAST trackers every remote SET twice.
                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                    router.set(key.clone(), value, expire_in).await;
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    let resp = router
                        .execute_remote(
                            target,
                            Command::Set {
                                key,
                                value,
                                expire_in,
                                condition,
                                get,
                                keepttl,
                                past_expired,
                            },
                        )
                        .await;
                    out.extend_from_slice(&resp);
                }
                false
            }
        }
        Command::Mget(keys) => {
            // Inside EXEC the transaction's locks already keep cross-shard
            // writers out (and taking them again would wait on itself).
            let atomic = !in_tx(client_id);
            if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                for key in &keys {
                    record_client_read(router.port, client_id, key.as_ref());
                }
            }
            if atomic {
                router.write_mget_resp_atomic(keys, out).await;
            } else {
                router.write_mget_resp(keys, out).await;
            }
            false
        }
        Command::Mset(pairs) => {
            // Each owner shard logs and replicates its part of the MSET.
            if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                let was_defer = DEFER_BCAST_FLUSH.replace(true);
                for (key, _) in &pairs {
                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                }
                DEFER_BCAST_FLUSH.set(was_defer);
                if !was_defer {
                    flush_pending_bcast(router.port);
                }
            }
            let keys: Vec<Bytes> =
                if NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) != 0 {
                    pairs.iter().map(|(k, _)| k.clone()).collect()
                } else {
                    Vec::new()
                };
            if in_tx(client_id) {
                router.mset(pairs).await;
            } else {
                router.mset_atomic(pairs).await;
            }
            for key in &keys {
                notify_keyspace_event_sync(router, NOTIFY_STRING, "set", key);
            }
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Msetex {
            pairs,
            condition,
            expiry,
        } => {
            let pass = match condition {
                MsetexCondition::None => true,
                MsetexCondition::Nx => {
                    let mut ok = true;
                    for (k, _) in &pairs {
                        if router.exists(k.clone()).await {
                            ok = false;
                            break;
                        }
                    }
                    ok
                }
                MsetexCondition::Xx => {
                    let mut ok = true;
                    for (k, _) in &pairs {
                        if !router.exists(k.clone()).await {
                            ok = false;
                            break;
                        }
                    }
                    ok
                }
            };

            if !pass {
                out.extend_from_slice(b":0\r\n");
                return false;
            }

            for (key, val) in pairs {
                notify_key_invalidation(router.port, key.as_ref(), client_id);
                let ttl = match expiry {
                    MsetexExpiry::None => None,
                    MsetexExpiry::KeepTtl => {
                        let ms = router.ttl(key.clone(), true).await;
                        if ms > 0 {
                            Some(std::time::Duration::from_millis(ms as u64))
                        } else {
                            None
                        }
                    }
                    MsetexExpiry::ExpireIn(d) => Some(d),
                };
                router.set(key, val, ttl).await;
            }
            match condition {
                MsetexCondition::None => {
                    out.extend_from_slice(b"+OK\r\n");
                }
                MsetexCondition::Nx | MsetexCondition::Xx => {
                    out.extend_from_slice(b":1\r\n");
                }
            }
            false
        }
        Command::Lcs {
            key1,
            key2,
            len_only,
            idx,
            min_match_len,
            with_match_len,
        } => {
            let mut vals = [Bytes::new(), Bytes::new()];
            for (val, key) in vals.iter_mut().zip([key1, key2]) {
                match router.get_checked(key).await {
                    Ok(v) => *val = v.unwrap_or_default(),
                    Err(err) => {
                        write_resp_err(out, err);
                        return false;
                    }
                }
            }
            let [val1, val2] = vals;
            let s1 = val1.as_ref();
            let s2 = val2.as_ref();
            let m = s1.len();
            let n = s2.len();
            if let Some(err) = lcs_table_too_big(m, n) {
                write_resp_err(out, err);
                return false;
            }

            let mut dp = vec![vec![0u32; n + 1]; m + 1];
            for i in 1..=m {
                for j in 1..=n {
                    if s1[i - 1] == s2[j - 1] {
                        dp[i][j] = dp[i - 1][j - 1] + 1;
                    } else {
                        dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
                    }
                }
            }

            let lcs_len = dp[m][n] as usize;

            if len_only {
                out.extend_from_slice(format!(":{}\r\n", lcs_len).as_bytes());
                return false;
            }

            if idx {
                let mut matches = Vec::new();
                let mut i = m;
                let mut j = n;
                while i > 0 && j > 0 {
                    if s1[i - 1] == s2[j - 1] {
                        let end1 = i - 1;
                        let end2 = j - 1;
                        while i > 0 && j > 0 && s1[i - 1] == s2[j - 1] {
                            i -= 1;
                            j -= 1;
                        }
                        let start1 = i;
                        let start2 = j;
                        let match_len = end1 - start1 + 1;
                        if match_len >= min_match_len {
                            matches.push(((start1, end1), (start2, end2), match_len));
                        }
                    } else if dp[i - 1][j] >= dp[i][j - 1] {
                        i -= 1;
                    } else {
                        j -= 1;
                    }
                }

                out.extend_from_slice(b"*4\r\n$7\r\nmatches\r\n");
                out.extend_from_slice(format!("*{}\r\n", matches.len()).as_bytes());
                for ((s1_idx, e1_idx), (s2_idx, e2_idx), match_len) in matches {
                    if with_match_len {
                        out.extend_from_slice(
                            format!(
                                "*3\r\n*2\r\n:{}\r\n:{}\r\n*2\r\n:{}\r\n:{}\r\n:{}\r\n",
                                s1_idx, e1_idx, s2_idx, e2_idx, match_len
                            )
                            .as_bytes(),
                        );
                    } else {
                        out.extend_from_slice(
                            format!(
                                "*2\r\n*2\r\n:{}\r\n:{}\r\n*2\r\n:{}\r\n:{}\r\n",
                                s1_idx, e1_idx, s2_idx, e2_idx
                            )
                            .as_bytes(),
                        );
                    }
                }
                out.extend_from_slice(format!("$3\r\nlen\r\n:{}\r\n", lcs_len).as_bytes());
                return false;
            }

            let mut lcs_bytes = Vec::with_capacity(lcs_len);
            let mut i = m;
            let mut j = n;
            while i > 0 && j > 0 {
                if s1[i - 1] == s2[j - 1] {
                    lcs_bytes.push(s1[i - 1]);
                    i -= 1;
                    j -= 1;
                } else if dp[i - 1][j] >= dp[i][j - 1] {
                    i -= 1;
                } else {
                    j -= 1;
                }
            }
            lcs_bytes.reverse();
            out.extend_from_slice(format!("${}\r\n", lcs_bytes.len()).as_bytes());
            out.extend_from_slice(&lcs_bytes);
            out.extend_from_slice(b"\r\n");
            false
        }
        Command::Del(keys) => {
            for key in &keys {
                notify_key_invalidation(router.port, key.as_ref(), client_id);
            }
            let count = if keys.len() == 1 {
                let deleted = router.del(keys[0].clone()).await;
                if deleted { 1 } else { 0 }
            } else {
                router.del_keys(keys.to_vec()).await
            };
            if count > 0 {
                for key in &keys {
                    crate::search::delete_document_hook(&String::from_utf8_lossy(key));
                }
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::Unlink(keys) => {
            for key in &keys {
                notify_key_invalidation(router.port, key.as_ref(), client_id);
            }
            let (local, remote) = router.group_keys_by_shard(&keys);
            let mut count = 0usize;
            for key in &local {
                let lazy = router.local_db.borrow().table.is_lazyfree_worthy(key);
                if router.del(key.clone()).await {
                    count += 1;
                    if lazy {
                        crate::table::add_lazyfreed_objects(1);
                    }
                }
            }
            let remote = remote
                .into_iter()
                .map(|(shard, ks)| (shard, Command::Unlink(smallvec::SmallVec::from_vec(ks))))
                .collect();
            count += sum_remote_counts(router, remote).await;
            if count > 0 {
                for key in &keys {
                    crate::search::delete_document_hook(&String::from_utf8_lossy(key));
                }
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::Exists(keys) => {
            if keys.len() == 1 {
        let (target, hash) = router.target_shard_and_hash(&keys[0]);
                let exists = if target == router.shard_id {
                    router
                        .local_db
                        .borrow_mut()
                        .exists_with_hash(keys[0].as_ref(), hash)
                } else {
                    router.exists(keys[0].clone()).await
                };
                crate::server_stats::note_key_lookup(exists);
                record_client_read(router.port, client_id, keys[0].as_ref());
                write_resp_integer(out, if exists { 1 } else { 0 });
                return false;
            }
            let (local, remote) = router.group_keys_by_shard(&keys);
            let mut count = {
                let mut db = router.local_db.borrow_mut();
                local
                    .iter()
                    .filter(|k| {
                        let found = db.exists(k.as_ref());
                        crate::server_stats::note_key_lookup(found);
                        found
                    })
                    .count()
            };
            // The remote shards run EXISTS through execute_local_command,
            // which counts their keys.
            let remote = remote
                .into_iter()
                .map(|(shard, ks)| (shard, Command::Exists(smallvec::SmallVec::from_vec(ks))))
                .collect();
            count += sum_remote_counts(router, remote).await;
            for key in &keys {
                record_client_read(router.port, client_id, key.as_ref());
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::IncrBy(key, delta, name) => {
            match router.incr_by(key.clone(), delta, name).await {
                Ok(val) => {
                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                    notify_keyspace_event_sync(router, NOTIFY_STRING, "incrby", key.as_ref());
                    write_resp_integer(out, val);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Expire {
            key,
            duration,
            opts,
        } => {
            let res = router.expire(key.clone(), duration, opts).await;
            if res {
                if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                    touch_watched_key(router.port, key.as_ref());
                }
                if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                }
                notify_keyspace_event_sync(router, NOTIFY_GENERIC, "expire", key.as_ref());
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Persist(key) => {
            let res = router.persist(key.clone()).await;
            if res {
                if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                    touch_watched_key(router.port, key.as_ref());
                }
                if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                }
                notify_keyspace_event_sync(router, NOTIFY_GENERIC, "persist", key.as_ref());
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Ttl(key, in_millis) => {
            let res = router.ttl(key, in_millis).await;
            crate::server_stats::note_key_lookup(res != -2);
            out.extend_from_slice(format!(":{}\r\n", res).as_bytes());
            false
        }
        Command::Ping(msg) => {
            match msg {
                Some(m) => {
                    out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                    out.extend_from_slice(&m);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"+PONG\r\n");
                }
            }
            false
        }
        Command::Monitor => {
            if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                c.is_monitor = true;
                register_monitor_client(router.port, client_id, c.push.clone());
            }
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::CommandDocs(ref cmds) => {
            crate::command_info::write_docs(out, cmds, CURRENT_CLIENT_RESP3.get());
            false
        }
        #[cfg(not(feature = "mcp"))]
        Command::McpTools | Command::McpCall { .. } | Command::McpRpc(_) => {
            out.extend_from_slice(b"-ERR MCP is not compiled into this build (cargo feature mcp)\r\n");
            false
        }
        #[cfg(feature = "mcp")]
        Command::McpTools => {
            let tools = crate::mcp::builtin_mcp_tools();
            write_resp_array_header(out, tools.len());
            for t in tools {
                write_resp_array_header(out, 6);
                write_resp_bulk(out, b"name");
                write_resp_bulk(out, t.name.as_bytes());
                write_resp_bulk(out, b"description");
                write_resp_bulk(out, t.description.as_bytes());
                write_resp_bulk(out, b"inputSchema");
                let schema_s = serde_json::to_string(&t.input_schema).unwrap_or_default();
                write_resp_bulk(out, schema_s.as_bytes());
            }
            false
        }
        #[cfg(feature = "mcp")]
        Command::McpCall { tool, args_json } => {
            let args: serde_json::Value = match serde_json::from_slice(&args_json) {
                Ok(v) => v,
                Err(e) => {
                    write_resp_err(out, format!("invalid JSON arguments: {}", e));
                    return false;
                }
            };
            let planned = match crate::mcp::plan_tool_command(&tool, &args) {
                Ok(cmd) => cmd,
                Err(e) => {
                    let err_obj =
                        crate::mcp::format_mcp_call_result(serde_json::Value::String(e), true);
                    let s = serde_json::to_string(&err_obj).unwrap_or_default();
                    write_resp_bulk(out, s.as_bytes());
                    return false;
                }
            };
            let mut sub_out = Vec::new();
            Box::pin(execute_command(
                planned,
                router,
                client_id,
                client_registry,
                &mut sub_out,
                asking,
                authenticated,
                auth_user,
            ))
            .await;
            let (val, is_err) = crate::mcp::resp_bytes_to_json(&sub_out);
            let call_res = crate::mcp::format_mcp_call_result(val, is_err);
            let s = serde_json::to_string(&call_res).unwrap_or_default();
            write_resp_bulk(out, s.as_bytes());
            false
        }
        #[cfg(feature = "mcp")]
        Command::McpRpc(req_bytes) => {
            let req: serde_json::Value = match serde_json::from_slice(&req_bytes) {
                Ok(v) => v,
                Err(e) => {
                    let err_rpc = serde_json::json!({
                        "jsonrpc": "2.0",
                        "id": serde_json::Value::Null,
                        "error": { "code": -32700, "message": format!("Parse error: {}", e) }
                    });
                    let s = serde_json::to_string(&err_rpc).unwrap_or_default();
                    write_resp_bulk(out, s.as_bytes());
                    return false;
                }
            };
            let id = req.get("id").cloned().unwrap_or(serde_json::Value::Null);
            let method = req.get("method").and_then(|v| v.as_str()).unwrap_or("");
            let rpc_resp = match method {
                "initialize" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": { "tools": { "listChanged": false } },
                        "serverInfo": { "name": "rudis-mcp", "version": "0.1.0" }
                    }
                }),
                "ping" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": {}
                }),
                "tools/list" => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": crate::mcp::tools_list_json()
                }),
                "tools/call" => {
                    let params = req
                        .get("params")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({}));
                    let tool_name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let tool_args = params
                        .get("arguments")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({}));
                    match crate::mcp::plan_tool_command(tool_name, &tool_args) {
                        Ok(planned) => {
                            let mut sub_out = Vec::new();
                            Box::pin(execute_command(
                                planned,
                                router,
                                client_id,
                                client_registry,
                                &mut sub_out,
                                asking,
                                authenticated,
                                auth_user,
                            ))
                            .await;
                            let (val, is_err) = crate::mcp::resp_bytes_to_json(&sub_out);
                            let call_res = crate::mcp::format_mcp_call_result(val, is_err);
                            serde_json::json!({
                                "jsonrpc": "2.0",
                                "id": id,
                                "result": call_res
                            })
                        }
                        Err(e) => serde_json::json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "result": crate::mcp::format_mcp_call_result(
                                serde_json::Value::String(e),
                                true
                            )
                        }),
                    }
                }
                _ => serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": { "code": -32601, "message": format!("Method not found: {}", method) }
                }),
            };
            let s = serde_json::to_string(&rpc_resp).unwrap_or_default();
            write_resp_bulk(out, s.as_bytes());
            false
        }
        Command::Info(section) => {
            info_reply(router, section, out).await;
            false
        }
        Command::Role => {
            let hub = crate::replication::cached_hub(router.base_port);
            out.extend_from_slice(&hub.format_role_resp());
            false
        }
        Command::Replconf(args) => {
            if args.is_empty() {
                out.extend_from_slice(b"-ERR wrong number of arguments for 'replconf' command\r\n");
            } else if args[0].eq_ignore_ascii_case(b"listening-port") {
                if args.len() >= 2
                    && let Ok(s) = std::str::from_utf8(&args[1])
                    && let Ok(rport) = s.parse::<u16>()
                {
                    let hub = crate::replication::cached_hub(router.base_port);
                    hub.set_replica_port(client_id, rport);
                }
                out.extend_from_slice(b"+OK\r\n");
            } else if args[0].eq_ignore_ascii_case(b"ip-address") {
                if args.len() >= 2
                    && let Ok(s) = std::str::from_utf8(&args[1])
                {
                    let hub = crate::replication::cached_hub(router.base_port);
                    hub.set_replica_announced_ip(client_id, s);
                }
                out.extend_from_slice(b"+OK\r\n");
            } else if args[0].eq_ignore_ascii_case(b"capa") {
                if args.iter().any(|a| a.eq_ignore_ascii_case(b"eof")) {
                    crate::replication::cached_hub(router.base_port).note_replica_capa_eof(client_id);
                }
                if args.iter().any(|a| a.eq_ignore_ascii_case(b"dual-channel")) {
                    crate::replication::cached_hub(router.base_port).note_replica_capa_dual(client_id);
                }
                if args.iter().any(|a| a.eq_ignore_ascii_case(b"dragonfly")) {
                    let hub = crate::replication::cached_hub(router.base_port);
                    let replid = &hub.replid();
                    let num_shards = router.num_shards;
                    out.extend_from_slice(
                        format!(
                            "*5\r\n${}\r\n{}\r\n$5\r\nSYNC1\r\n:{}\r\n:1\r\n:0\r\n",
                            replid.len(),
                            replid,
                            num_shards
                        )
                        .as_bytes(),
                    );
                } else {
                    out.extend_from_slice(b"+OK\r\n");
                }
            } else if args[0].eq_ignore_ascii_case(b"rdb-channel") {
                // Dual-channel replication: this connection's PSYNC asks
                // for the RDB only (see `run_master_replica_stream`).
                let hub = crate::replication::cached_hub(router.base_port);
                hub.note_rdb_channel(client_id, args.get(1).is_some_and(|a| a.as_ref() == b"1"));
                out.extend_from_slice(b"+OK\r\n");
            } else if args[0].eq_ignore_ascii_case(b"rdb-client-id") {
                // ... and this one's takes that RDB channel's stream.
                match args
                    .get(1)
                    .and_then(|a| std::str::from_utf8(a).ok())
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    Some(id) => {
                        crate::replication::cached_hub(router.base_port).note_rdb_client_id(client_id, id);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    None => out.extend_from_slice(b"-ERR invalid rdb-client-id\r\n"),
                }
            } else if args[0].eq_ignore_ascii_case(b"ack") {
                if args.len() >= 2
                    && let Some(off) = parse_ack_offset(&args[1])
                {
                    let hub = crate::replication::cached_hub(router.base_port);
                    hub.update_replica_ack(client_id, off, parse_fack(&args));
                }
            } else if args[0].eq_ignore_ascii_case(b"getack") {
                let hub = crate::replication::cached_hub(router.base_port);
                let off = hub
                    .master_repl_offset
                    .load(std::sync::atomic::Ordering::SeqCst)
                    .to_string();
                out.extend_from_slice(
                    format!(
                        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                        off.len(),
                        off
                    )
                    .as_bytes(),
                );
            } else {
                out.extend_from_slice(b"+OK\r\n");
            }
            false
        }
        Command::Failover { abort, request } => {
            let hub = crate::replication::get_replication_hub(router.base_port);
            if router.cluster_enabled {
                out.extend_from_slice(b"-ERR FAILOVER not allowed in cluster mode.\r\n");
            } else if abort {
                if hub.abort_failover("Failover manually aborted") {
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    out.extend_from_slice(b"-ERR No failover in progress.\r\n");
                }
            } else {
                match hub.start_failover(request, router.clone()) {
                    Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => out.extend_from_slice(format!("-ERR {e}\r\n").as_bytes()),
                }
            }
            false
        }
        Command::Replicaof { host, port } => {
            let hub = crate::replication::cached_hub(router.base_port);
            if host.eq_ignore_ascii_case(b"no") && port.eq_ignore_ascii_case(b"one") {
                if hub.is_slave() {
                    hub.stop_sync_and_wait().await;
                    hub.make_master();
                    crate::log_notice!(
                        "MASTER MODE enabled (user request from 'id={}')",
                        client_id
                    );
                }
                out.extend_from_slice(b"+OK\r\n");
            } else {
                let host_str = String::from_utf8_lossy(&host).to_string();
                let port_str = String::from_utf8_lossy(&port);
                if let Ok(mport) = port_str.parse::<u16>() {
                    if hub.is_replicating_from(&host_str, mport) {
                        crate::log_notice!(
                            "REPLICAOF would result into synchronization with the master we are already connected with. No operation performed."
                        );
                        out.extend_from_slice(b"+OK Already connected to specified master\r\n");
                    } else {
                        crate::log_notice!(
                            "REPLICAOF {}:{} enabled (user request from 'id={}')",
                            host_str,
                            mport,
                            client_id
                        );
                        crate::replication::start_replica_sync(
                            router.base_port,
                            host_str,
                            mport,
                            router.clone(),
                        );
                        out.extend_from_slice(b"+OK\r\n");
                    }
                } else {
                    out.extend_from_slice(b"-ERR invalid port\r\n");
                }
            }
            false
        }
        Command::Psync { .. } | Command::Sync => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Trimslots(ranges) => {
            let cluster = crate::cluster::get_cluster_hub(router.base_port);
            match ranges
                .iter()
                .flat_map(|&(s, e)| s..=e)
                .find(|&s| cluster.owns_slot(s))
            {
                // As Redis: only slots this node doesn't serve are trimmed.
                Some(slot) => out.extend_from_slice(
                    format!("-ERR the slot {slot} is served by this node\r\n").as_bytes(),
                ),
                None => {
                    crate::asm::trim_slots(router, &ranges);
                    out.extend_from_slice(b"+OK\r\n");
                }
            }
            false
        }
        Command::SyncSlots(_) => {
            out.extend_from_slice(b"-ERR CLUSTER SYNCSLOTS is used between cluster nodes only\r\n");
            false
        }
        Command::Hotkeys(args) => {
            crate::hotkeys::run(router.base_port, router.cluster_enabled, &args, out);
            false
        }
        Command::Tier(sub) => {
            match sub {
                crate::resp::TierSubcommand::Spill(key) => {
                    let ok = router.spill_key(&key).await;
                    out.extend_from_slice(if ok { b":1\r\n" } else { b":0\r\n" });
                }
                crate::resp::TierSubcommand::Cool(key) => {
                    let ok = router.cool_key(&key).await;
                    out.extend_from_slice(if ok { b":1\r\n" } else { b":0\r\n" });
                }
                crate::resp::TierSubcommand::Decommit(key) => {
                    let count = router.decommit(key.as_deref()).await;
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                crate::resp::TierSubcommand::Load(key) => {
                    let ok = router.ensure_loaded(&key).await;
                    out.extend_from_slice(if ok { b":1\r\n" } else { b":0\r\n" });
                }
                crate::resp::TierSubcommand::SpillAll => {
                    let mut total = 0;
                    for s in 0..router.num_shards {
                        if s == router.shard_id {
                            total += router.spill_all().await;
                        } else {
                            let (tx, rx) = flume::bounded(1);
                            if router.senders[s]
                                .send(crate::shard::ShardMessage::TierSpillAll { responder: tx })
                                .is_ok()
                            {
                                total += rx.recv_async().await.unwrap_or(0);
                            }
                        }
                    }
                    out.extend_from_slice(format!(":{}\r\n", total).as_bytes());
                }
                crate::resp::TierSubcommand::Gc => {
                    let reclaimed = router.gc_all().await;
                    out.extend_from_slice(format!(":{}\r\n", reclaimed).as_bytes());
                }
                crate::resp::TierSubcommand::Info => {
                    let stats = crate::tiering::get_tier_stats(router.base_port);
                    let max_mem = crate::tiering::get_max_memory(router.base_port);
                    let used_mem = router.get_total_used_memory().await;
                    let info = format!(
                        "# Tiered Storage (io_uring NVMe)\r\ntier_enabled:{}\r\ntier_shards_open:{}\r\nmaxmemory:{}\r\nmaxmemory_human:{}\r\nused_memory:{}\r\nused_memory_human:{}\r\ncooled_keys:{}\r\ntiered_keys:{}\r\ntiered_bytes:{}\r\nram_saved_bytes:{}\r\ndisk_reads:{}\r\ndisk_writes:{}\r\ndead_bytes:{}\r\ngc_reclaimed_bytes:{}\r\ngc_cycles:{}\r\ndecommit_count:{}\r\ncoalesced_reads:{}\r\nbin_pages:{}\r\ntotal_stashes:{}\r\ntotal_fetches:{}\r\ntotal_deletes:{}\r\nram_hits:{}\r\nram_misses:{}\r\nstreaming_reads:{}\r\noffload_threshold_pct:{}\r\nupload_threshold_pct:{}\r\n",
                        stats.tier_enabled(router.num_shards).0,
                        stats.tier_enabled(router.num_shards).1,
                        max_mem,
                        crate::tiering::format_bytes_human(max_mem),
                        used_mem,
                        crate::tiering::format_bytes_human(used_mem as u64),
                        stats.cooled_keys.load(std::sync::atomic::Ordering::Relaxed),
                        stats.tiered_keys.load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .tiered_bytes
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .ram_saved_bytes
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats.disk_reads.load(std::sync::atomic::Ordering::Relaxed),
                        stats.disk_writes.load(std::sync::atomic::Ordering::Relaxed),
                        stats.dead_bytes.load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .gc_reclaimed_bytes
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats.gc_cycles.load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .decommit_count
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .coalesced_reads
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats.bin_pages.load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .total_stashes
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .total_fetches
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .total_deletes
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats.ram_hits.load(std::sync::atomic::Ordering::Relaxed),
                        stats.ram_misses.load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .streaming_reads
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .offload_threshold_pct
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .upload_threshold_pct
                            .load(std::sync::atomic::Ordering::Relaxed),
                    );
                    out.extend_from_slice(format!("${}\r\n{}\r\n", info.len(), info).as_bytes());
                }
            }
            false
        }
        Command::ConfigGet(_) | Command::ConfigSet(_) => {
            match cmd {
                Command::ConfigGet(patterns) => {
                    write_config_get_reply(router, &patterns, out);
                    false
                }
                Command::ConfigSet(pairs) => {
                    if pairs.len() == 1 {
                        let p0 = String::from_utf8_lossy(&pairs[0].0).to_lowercase();
                        if p0 == "resetstat" {
                            router.reset_command_stats().await;
                            crate::slot_stats::reset(None);
                            reset_error_stats();
                            crate::slowlog::reset_slowlog_stats();
                            crate::table::reset_expired_keys();
                            crate::server_stats::reset();
                            router.tier_stats.reset_peak(router.real_used_total() as u64);
                            out.extend_from_slice(b"+OK\r\n");
                            return false;
                        } else if p0 == "rewrite" {
                            match crate::config::rewrite_config_file(router.port) {
                                Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                                Err(e) => {
                                    crate::log_warning!("CONFIG REWRITE failed: {}", e);
                                    out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes())
                                }
                            }
                            return false;
                        }
                    }

                    let appendonly = pairs
                        .iter()
                        .find(|(p, _)| p.eq_ignore_ascii_case(b"appendonly"))
                        .map(|(_, v)| v.eq_ignore_ascii_case(b"yes"));
                    let start = out.len();
                    apply_config_set_pairs(router, &pairs, out);
                    // A limit (or policy) that leaves the server over
                    // maxmemory evicts now, as in Redis, not only when the
                    // next command arrives: an idle server got no memory
                    // back, and `evicted` events came late.
                    if pairs.iter().any(|(p, _)| {
                        p.eq_ignore_ascii_case(b"maxmemory")
                            || p.eq_ignore_ascii_case(b"maxmemory-policy")
                    }) && out[start..].starts_with(b"+OK")
                    {
                        let max_mem = crate::tiering::get_max_memory(router.base_port) as usize;
                        let policy = get_max_memory_policy();
                        // A replica leaves maxmemory to its master (by
                        // default), as the per-command gate does.
                        if max_mem > 0
                            && policy != "noeviction"
                            && !maxmemory_ignored_as_replica(router.base_port)
                        {
                            let mut track = Vec::new();
                            router
                                .evict_until_under_maxmemory(max_mem, &policy, client_id, &mut track)
                                .await;
                            // Tracking invalidations go ahead of the reply.
                            out.splice(start..start, track);
                        }
                    }
                    // AOF on or off at runtime, once the rest applied.
                    if let Some(on) = appendonly
                        && out[start..].starts_with(b"+OK")
                    {
                        let res = if on {
                            router.enable_aof().await
                        } else {
                            router.disable_aof().await
                        };
                        if let Err(e) = res {
                            out.truncate(start);
                            out.extend_from_slice(
                                format!(
                                    "-ERR CONFIG SET failed (possibly related to argument 'appendonly') - {}\r\n",
                                    e.replace(['\r', '\n'], " ")
                                )
                                .as_bytes(),
                            );
                        }
                    }
                    false
                }
                _ => unreachable!(),
            }
        }
        Command::Cluster(sub) => {
            // A trim of the slot that only waited for paused clients runs
            // as soon as they resume: let it finish first.
            if let ClusterSubcommand::SetSlot(slot, _) = &sub {
                crate::asm::wait_unpaused_trim(router.base_port, *slot).await;
            }
            match sub {
                ClusterSubcommand::KeySlot(key) => {
                    let slot = key_slot(&key);
                    out.extend_from_slice(format!(":{}\r\n", slot).as_bytes());
                }
                ClusterSubcommand::SlotStats(query) => {
                    write_cluster_slot_stats(router, &query, out).await;
                }
                ClusterSubcommand::CountKeysInSlot(slot) => {
                    let count = router.count_keys_in_slot(slot).await;
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                ClusterSubcommand::GetKeysInSlot(slot, count) => {
                    let keys = router.get_keys_in_slot(slot, count).await;
                    out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
                    for k in keys {
                        out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                ClusterSubcommand::Slots => {
                    let mut slots_bytes = Vec::new();
                    router.cluster_slots(&mut slots_bytes);
                    out.extend_from_slice(&slots_bytes);
                }
                ClusterSubcommand::Shards => {
                    let mut shards_bytes = Vec::new();
                    router.cluster_shards(&mut shards_bytes);
                    out.extend_from_slice(&shards_bytes);
                }
                ClusterSubcommand::Links => {
                    let mut links_bytes = Vec::new();
                    router.cluster_links(&mut links_bytes);
                    out.extend_from_slice(&links_bytes);
                }
                ClusterSubcommand::AddSlots(slots) => match router.cluster_addslots(&slots) {
                    Ok(()) => {
                        router.sync_slot_tables().await;
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
                },
                ClusterSubcommand::DelSlots(slots) => match router.cluster_delslots(&slots) {
                    Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
                },
                ClusterSubcommand::AddSlotsRange(ranges) => {
                    match router.cluster_addslotsrange(&ranges) {
                        Ok(()) => {
                            router.sync_slot_tables().await;
                            out.extend_from_slice(b"+OK\r\n");
                        }
                        Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
                    }
                }
                ClusterSubcommand::DelSlotsRange(ranges) => {
                    match router.cluster_delslotsrange(&ranges) {
                        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
                    }
                }
                ClusterSubcommand::Nodes => {
                    let nodes = router.cluster_nodes();
                    out.extend_from_slice(format!("${}\r\n", nodes.len()).as_bytes());
                    out.extend_from_slice(nodes.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClusterSubcommand::Info => {
                    let info = router.cluster_info();
                    out.extend_from_slice(format!("${}\r\n", info.len()).as_bytes());
                    out.extend_from_slice(info.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClusterSubcommand::Replicas(id) => {
                    match crate::cluster::get_cluster_hub(router.base_port).cluster_replicas(&id) {
                        Ok(lines) => {
                            out.extend_from_slice(format!("*{}\r\n", lines.len()).as_bytes());
                            for l in lines {
                                write_resp_bulk(out, l.as_bytes());
                            }
                        }
                        Err(e) => out.extend_from_slice(format!("-ERR {e}\r\n").as_bytes()),
                    }
                }
                ClusterSubcommand::CountFailureReports(id) => {
                    match crate::cluster::get_cluster_hub(router.base_port).count_failure_reports(&id)
                    {
                        Ok(n) => out.extend_from_slice(format!(":{n}\r\n").as_bytes()),
                        Err(e) => out.extend_from_slice(format!("-ERR {e}\r\n").as_bytes()),
                    }
                }
                ClusterSubcommand::MyId => {
                    let id = router.my_id();
                    out.extend_from_slice(format!("${}\r\n", id.len()).as_bytes());
                    out.extend_from_slice(id.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClusterSubcommand::Meet { ip, port } => match router.cluster_meet(ip, port) {
                    Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => {
                        let resp = format!("-ERR {}\r\n", e);
                        out.extend_from_slice(resp.as_bytes());
                    }
                },
                ClusterSubcommand::SetSlot(slot, _)
                    if crate::asm::slot_in_active_task(router.base_port, slot) =>
                {
                    out.extend_from_slice(
                        format!("-ERR Slot {slot} is in an active atomic slot migration\r\n")
                            .as_bytes(),
                    );
                }
                ClusterSubcommand::SetSlot(slot, _)
                    if crate::asm::slot_has_trim_job(router.base_port, slot) =>
                {
                    out.extend_from_slice(
                        format!("-ERR There is a pending trim job for slot {slot}\r\n")
                            .as_bytes(),
                    );
                }
                ClusterSubcommand::SetSlot(slot, sub_cmd) => match sub_cmd {
                    SetSlotSubcommand::Migrating(node) => {
                        let target_addr = if node.contains(':') {
                            node.clone()
                        } else {
                            let hub = crate::cluster::get_cluster_hub(router.base_port);
                            let nodes = hub.nodes.read();
                            if let Some(n) = nodes.get(&node) {
                                format!("{}:{}", n.ip, n.port)
                            } else {
                                node.clone()
                            }
                        };
                        router
                            .set_slot_state(slot, crate::shard::SlotState::Migrating(target_addr));
                        let hub = crate::cluster::get_cluster_hub(router.base_port);
                        hub.slot_states
                            .write()
                            .insert(slot, ("migrating".to_string(), node));
                        // Kept across a restart, as Redis saves its config
                        // after every SETSLOT.
                        let _ = hub.save_config();
                        router.sync_slot_tables().await;
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    SetSlotSubcommand::Importing(node) => {
                        let source_addr = if node.contains(':') {
                            node.clone()
                        } else {
                            let hub = crate::cluster::get_cluster_hub(router.base_port);
                            let nodes = hub.nodes.read();
                            if let Some(n) = nodes.get(&node) {
                                format!("{}:{}", n.ip, n.port)
                            } else {
                                node.clone()
                            }
                        };
                        router
                            .set_slot_state(slot, crate::shard::SlotState::Importing(source_addr));
                        let hub = crate::cluster::get_cluster_hub(router.base_port);
                        hub.slot_states
                            .write()
                            .insert(slot, ("importing".to_string(), node));
                        // Kept across a restart, as Redis saves its config
                        // after every SETSLOT.
                        let _ = hub.save_config();
                        router.sync_slot_tables().await;
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    SetSlotSubcommand::Stable => {
                        router.set_slot_state(slot, crate::shard::SlotState::Stable);
                        let hub = crate::cluster::get_cluster_hub(router.base_port);
                        hub.slot_states.write().remove(&slot);
                        let _ = hub.save_config();
                        router.sync_slot_tables().await;
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    SetSlotSubcommand::Node(node) => {
                        let my_id = router.my_id();
                        let is_myself = node == "myself"
                            || node == my_id
                            || (0..router.num_shards).any(|s| node == format!("{:040x}", s + 1));
                        let hub = crate::cluster::get_cluster_hub(router.base_port);
                        hub.slot_states.write().remove(&slot);
                        if is_myself {
                            let named_shard =
                                (0..router.num_shards).find(|&s| node == format!("{:040x}", s + 1));
                            // A shard owner only means something where keys
                            // are routed by slot. A server that isn't in a
                            // cluster routes keys by hash: recording an owner
                            // switched every key to slot routing, so keys
                            // stored before (the slot's own, just imported)
                            // were looked up on the wrong shard.
                            let slot_routed = router.cluster_enabled
                                || crate::cluster::has_active_cluster(router.base_port);
                            if slot_routed || named_shard.is_some() {
                                let shard = named_shard.unwrap_or_else(|| {
                                    crate::router::slot_to_shard(slot, router.num_shards)
                                });
                                router.set_slot_owner(slot, shard);
                            }
                            router.set_slot_state(slot, crate::shard::SlotState::Stable);
                            // Also drops the slot from the previous owner in
                            // our view, so the cluster never shows it twice.
                            hub.take_slot(slot);
                        } else {
                            let owner_id = {
                                let nodes = hub.nodes.read();
                                if nodes.contains_key(&node) {
                                    Some(node.clone())
                                } else {
                                    nodes
                                        .values()
                                        .find(|n| format!("{}:{}", n.ip, n.port) == node)
                                        .map(|n| n.id.clone())
                                }
                            };
                            let target_addr = if node.contains(':') {
                                node
                            } else {
                                let nodes = hub.nodes.read();
                                if let Some(n) = nodes.get(&node) {
                                    format!("{}:{}", n.ip, n.port)
                                } else {
                                    node
                                }
                            };
                            // Giving a slot away does not bump our epoch
                            // (as in Redis): the new owner bumped its own,
                            // and peers take our smaller slot list at the
                            // same epoch. A bump here could take our epoch
                            // past the new owner's next one (ours rose to
                            // the cluster's highest, theirs only to the
                            // highest they had seen): our claim to the next
                            // slot of the reshard, still ours, then beat
                            // theirs when they took it, they gave it back,
                            // we dropped it on our SETSLOT NODE, and the
                            // slot was left without an owner.
                            match owner_id {
                                Some(id) => {
                                    let was_mine = hub.owns_slot(slot);
                                    hub.assign_slot(slot, &id);
                                    if was_mine {
                                        hub.follow_after_last_slot(&id);
                                    }
                                }
                                None => {
                                    crate::cluster::remove_slots(&mut hub.my_slots.write(), &[slot])
                                }
                            }
                            router
                                .set_slot_state(slot, crate::shard::SlotState::Moved(target_addr));
                        }
                        router.sync_slot_tables().await;
                        // Peers learn the new owner (and its epoch) now.
                        hub.broadcast_config();
                        out.extend_from_slice(b"+OK\r\n");
                    }
                },
                ClusterSubcommand::MigrateSlot { slot, host, port } => {
                    match migrate_slot_to(router, slot, &host, port, None, 100).await {
                        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => {
                            out.extend_from_slice(format!("-ERR migration failed: {}\r\n", e).as_bytes());
                            return false;
                        }
                    }
                }
                ClusterSubcommand::Rebalance {
                    host,
                    port,
                    slots,
                    weights,
                    simulate,
                    threshold,
                    pipeline,
                } => {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    let target_hp = match (host, port) {
                        (Some(h), Some(p)) => Some((h, p, slots)),
                        _ => None,
                    };
                    let pipeline_batch = pipeline;
                    let opts = crate::cluster::RebalanceOptions {
                        weights: weights.into_iter().collect(),
                        simulate,
                        threshold,
                        pipeline: pipeline_batch,
                        target_host_port: target_hp,
                    };

                    match hub.compute_rebalance_plan(&opts) {
                        Ok(plans) => {
                            if simulate {
                                if plans.is_empty() {
                                    out.extend_from_slice(
                                        b"*1\r\n$49\r\n*** No rebalancing needed! All nodes balanced. ***\r\n",
                                    );
                                } else {
                                    let preview_count = plans.len().min(5);
                                    out.extend_from_slice(
                                        format!("*{}\r\n", preview_count).as_bytes(),
                                    );
                                    for p in plans.iter().take(preview_count) {
                                        let desc = format!(
                                            "Moving slot {} from {} ({}) to {} ({})",
                                            p.slot,
                                            p.source_node_id,
                                            p.source_addr,
                                            p.target_node_id,
                                            p.target_addr
                                        );
                                        write_resp_bulk(out, desc.as_bytes());
                                    }
                                }
                            } else {
                                match execute_rebalance_plans(router, &hub, &plans, pipeline_batch)
                                    .await
                                {
                                    Ok(moved) => write_resp_integer(out, moved as i64),
                                    Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
                                }
                            }
                        }
                        Err(e) => {
                            out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes());
                        }
                    }
                }
                ClusterSubcommand::Check => {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    let report = hub.cluster_check();
                    let desc = report.format_report();
                    write_resp_bulk(out, desc.as_bytes());
                }
                ClusterSubcommand::Reshard {
                    target_node_id,
                    source_node_id,
                    slots,
                } => {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    match hub.compute_reshard_plan(&target_node_id, &source_node_id, slots) {
                        Ok(plans) => {
                            match execute_rebalance_plans(router, &hub, &plans, 100).await {
                                Ok(moved) => write_resp_integer(out, moved as i64),
                                Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
                            }
                        }
                        Err(e) => {
                            out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes());
                        }
                    }
                }
                ClusterSubcommand::Failover { force } => {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    let repl = crate::replication::cached_hub(router.base_port);
                    // Without FORCE, like Redis: the master pauses its
                    // writers and we take over only once we processed all it
                    // acknowledged.
                    let mut caught_up = Ok(());
                    if !force && hub.replication_master_addr().is_some() {
                        caught_up = match hub.manual_failover_start().await {
                            Ok(target) => {
                                let deadline = std::time::Instant::now()
                                    + std::time::Duration::from_millis(
                                        crate::cluster::MANUAL_FAILOVER_TIMEOUT_MS,
                                    );
                                loop {
                                    if repl.master_repl_offset.load(std::sync::atomic::Ordering::SeqCst)
                                        >= target
                                    {
                                        break Ok(());
                                    }
                                    if std::time::Instant::now() >= deadline {
                                        break Err("Manual failover timed out".to_string());
                                    }
                                    monoio::time::sleep(std::time::Duration::from_millis(5)).await;
                                }
                            }
                            Err(e) => Err(e),
                        };
                    }
                    if let Err(e) = caught_up {
                        out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes());
                        return false;
                    }
                    // End the link to the old master before taking over, so
                    // nothing it sends lands after the promotion.
                    if hub.replication_master_addr().is_some() {
                        crate::replication::cached_hub(router.base_port)
                            .stop_sync_and_wait()
                            .await;
                    }
                    match router.cluster_failover(force) {
                        Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => {
                            let resp = format!("-ERR {}\r\n", e);
                            out.extend_from_slice(resp.as_bytes());
                        }
                    }
                }
                ClusterSubcommand::Reset { hard } => match router.cluster_reset(hard) {
                    Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => {
                        let resp = format!("-ERR {}\r\n", e);
                        out.extend_from_slice(resp.as_bytes());
                    }
                },
                ClusterSubcommand::Forget(node_id) => match router.cluster_forget(&node_id) {
                    Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => {
                        let resp = format!("-ERR {}\r\n", e);
                        out.extend_from_slice(resp.as_bytes());
                    }
                },
                ClusterSubcommand::Replicate(node_id) => match router.cluster_replicate(&node_id) {
                    Ok((host, mport)) => {
                        // The cluster role alone moves no data: replicate the
                        // master like REPLICAOF does.
                        let hub = crate::replication::cached_hub(router.base_port);
                        if !hub.is_replicating_from(&host, mport) {
                            crate::log_notice!(
                                "CLUSTER REPLICATE {}: replicating {}:{}",
                                node_id,
                                host,
                                mport
                            );
                            crate::replication::start_replica_sync(
                                router.base_port,
                                host,
                                mport,
                                router.clone(),
                            );
                        }
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    Err(e) => {
                        let resp = format!("-ERR {}\r\n", e);
                        out.extend_from_slice(resp.as_bytes());
                    }
                },
                ClusterSubcommand::SaveConfig => {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    match hub.save_config() {
                        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
                    }
                }
                ClusterSubcommand::BumpEpoch => {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    let new_epoch = hub.bump_config_epoch();
                    out.extend_from_slice(format!("+BUMPED {}\r\n", new_epoch).as_bytes());
                }
                ClusterSubcommand::SetConfigEpoch(epoch) => {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    match hub.set_config_epoch(epoch) {
                        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
                    }
                }
                ClusterSubcommand::Migration(m) => {
                    crate::asm::run_migration_command(router, m, out);
                }
                ClusterSubcommand::FlushSlots => {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    hub.my_slots.write().clear();
                    hub.slot_states.write().clear();
                    let _ = hub.save_config();
                    out.extend_from_slice(b"+OK\r\n");
                }
            }
            false
        }
        Command::Client(sub) => {
            let mut close_conn = false;
            match sub {
                ClientSubcommand::List(ref filter_ids, ref ty) => {
                    let list = router.client_list(client_registry, filter_ids).await;
                    let list = filter_client_list_type(list, ty.as_deref());
                    out.extend_from_slice(format!("${}\r\n", list.len()).as_bytes());
                    out.extend_from_slice(list.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClientSubcommand::Info => {
                    let reg = client_registry.borrow();
                    if let Some(c) = reg.get(&client_id) {
                        let info = format_client_line(
                            c,
                            router.port,
                            &router.pubsub.borrow(),
                            std::time::Instant::now(),
                        );
                        out.extend_from_slice(format!("${}\r\n", info.len()).as_bytes());
                        out.extend_from_slice(info.as_bytes());
                        out.extend_from_slice(b"\r\n");
                    } else {
                        write_resp_null(out);
                    }
                }
                ClientSubcommand::SetName(name) => {
                    if name.bytes().any(|b| !(b'!'..=b'~').contains(&b)) {
                        out.extend_from_slice(
                            b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n",
                        );
                    } else {
                        if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                            c.name = if name.is_empty() { None } else { Some(name) };
                        }
                        out.extend_from_slice(b"+OK\r\n");
                    }
                }
                ClientSubcommand::SetInfo { attr, val } => {
                    if !attr.eq_ignore_ascii_case("lib-name")
                        && !attr.eq_ignore_ascii_case("lib-ver")
                    {
                        let msg =
                            format!("-ERR Unrecognized option '{}' for CLIENT SETINFO\r\n", err_arg(&attr));
                        out.extend_from_slice(msg.as_bytes());
                    } else if val.bytes().any(|b| !(b'!'..=b'~').contains(&b)) {
                        let msg = format!(
                            "-ERR {} cannot contain spaces, newlines or special characters.\r\n",
                            attr.to_lowercase()
                        );
                        out.extend_from_slice(msg.as_bytes());
                    } else {
                        if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                            if attr.eq_ignore_ascii_case("lib-name") {
                                c.lib_name = if val.is_empty() { None } else { Some(val) };
                            } else if attr.eq_ignore_ascii_case("lib-ver") {
                                c.lib_ver = if val.is_empty() { None } else { Some(val) };
                            }
                        }
                        out.extend_from_slice(b"+OK\r\n");
                    }
                }
                ClientSubcommand::GetName => {
                    let name = client_registry
                        .borrow()
                        .get(&client_id)
                        .and_then(|c| c.name.clone());
                    match name {
                        Some(n) => {
                            out.extend_from_slice(format!("${}\r\n", n.len()).as_bytes());
                            out.extend_from_slice(n.as_bytes());
                            out.extend_from_slice(b"\r\n");
                        }
                        None => {
                            write_resp_null(out);
                        }
                    }
                }
                ClientSubcommand::Id => {
                    out.extend_from_slice(format!(":{}\r\n", client_id).as_bytes());
                }
                ClientSubcommand::Tracking {
                    enabled,
                    redirect,
                    bcast,
                    prefixes,
                    optin,
                    optout,
                    noloop,
                } => {
                    if enabled {
                        if let Err(err) = register_client_tracking(
                            router.port,
                            client_id,
                            redirect,
                            bcast,
                            prefixes,
                            optin,
                            optout,
                            noloop,
                        ) {
                            write_resp_err(out, &err);
                            return false;
                        }
                    } else {
                        unregister_client_tracking(router.port, client_id);
                    }
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClientSubcommand::Caching(yes) => {
                    if let Err(err) = set_client_caching(router.port, client_id, yes) {
                        write_resp_err(out, err);
                    } else {
                        out.extend_from_slice(b"+OK\r\n");
                    }
                }
                ClientSubcommand::GetRedir => {
                    let redir = get_client_redir(router.port, client_id);
                    write_resp_integer(out, redir);
                }
                ClientSubcommand::TrackingInfo => {
                    let is_resp3 = client_registry
                        .borrow()
                        .get(&client_id)
                        .map(|c| c.is_resp3)
                        .unwrap_or(false);
                    write_client_tracking_info(router.port, client_id, is_resp3, out);
                }
                ClientSubcommand::Kill(ref args) => {
                    if args.is_empty() {
                        out.extend_from_slice(
                            b"-ERR wrong number of arguments for 'client|kill' command\r\n",
                        );
                    } else if args.len() == 1 {
                        let target_addr = String::from_utf8_lossy(&args[0]);
                        let list = router.client_list(client_registry, &[]).await;
                        let mut killed_cids = Vec::new();
                        for line in list.lines() {
                            let mut cid_opt = None;
                            let mut addr_opt = None;
                            for tok in line.split_whitespace() {
                                if let Some(v) = tok.strip_prefix("id=") {
                                    cid_opt = v.parse::<u64>().ok();
                                } else if let Some(v) = tok.strip_prefix("addr=") {
                                    addr_opt = Some(v);
                                }
                            }
                            if let (Some(cid), Some(addr)) = (cid_opt, addr_opt)
                                && addr == target_addr
                            {
                                killed_cids.push(cid);
                            }
                        }
                        if killed_cids.is_empty() {
                            // A replica's link to its master is a client in
                            // Redis, listed under the master's address.
                            let hub = crate::replication::cached_hub(router.base_port);
                            if hub.drop_master_link_to(&target_addr) {
                                out.extend_from_slice(b"+OK\r\n");
                            } else {
                                out.extend_from_slice(b"-ERR No such client\r\n");
                            }
                        } else {
                            let map = GLOBAL_CLIENTS.read();
                            for cid in killed_cids {
                                if cid == client_id {
                                    close_conn = true;
                                } else if let Some(entry) = global_client_entry(&map, router.base_port, client_port_span(router), cid) {
                                    if entry
                                        .stats
                                        .killed
                                        .compare_exchange(
                                            false,
                                            true,
                                            std::sync::atomic::Ordering::Relaxed,
                                            std::sync::atomic::Ordering::Relaxed,
                                        )
                                        .is_ok()
                                    {
                                        ACTIVE_CLIENTS
                                            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                    // SAFETY: shutdown(2) takes no pointers. The entry is read under the
                                    // GLOBAL_CLIENTS read lock and the connection removes its registry entry (ClientCleanup / PubsubCleanup) before its socket is closed, so the fd is not a reused number.
                                    unsafe {
                                        libc::shutdown(entry.raw_fd, libc::SHUT_RDWR);
                                    }
                                    crate::block::get_block_hub_for_port(router.port)
                                        .lock()
                                        .remove_waiters_for_client(cid);
                                }
                            }
                            out.extend_from_slice(b"+OK\r\n");
                        }
                    } else if args.len() % 2 != 0 {
                        out.extend_from_slice(b"-ERR syntax error\r\n");
                    } else {
                        let mut filter_ids: Vec<u64> = Vec::new();
                        let mut filter_type: Option<String> = None;
                        let mut filter_user: Option<String> = None;
                        let mut filter_addr: Option<String> = None;
                        let mut filter_laddr: Option<String> = None;
                        let mut skipme = true;
                        let mut maxage: Option<u64> = None;
                        let mut err_msg: Option<String> = None;

                        let mut i = 0;
                        while i < args.len() {
                            let k = String::from_utf8_lossy(&args[i]).to_uppercase();
                            let v = String::from_utf8_lossy(&args[i + 1]);
                            match k.as_str() {
                                "ID" => match v.parse::<i64>() {
                                    Ok(id) if id > 0 => filter_ids.push(id as u64),
                                    _ => {
                                        err_msg = Some(
                                            "-ERR client-id should be greater than 0\r\n"
                                                .to_string(),
                                        );
                                        break;
                                    }
                                },
                                "TYPE" => {
                                    let t = v.to_lowercase();
                                    if matches!(
                                        t.as_str(),
                                        "normal" | "master" | "slave" | "replica" | "pubsub"
                                    ) {
                                        filter_type = Some(t);
                                    } else {
                                        err_msg =
                                            Some(format!("-ERR Unknown client type '{}'\r\n", err_arg(&v)));
                                        break;
                                    }
                                }
                                "USER" => {
                                    let acl = crate::acl::get_acl_for_port(router.port);
                                    if acl.read().get_user(&v).is_none() {
                                        err_msg = Some(format!("-ERR No such user '{}'\r\n", err_arg(&v)));
                                        break;
                                    }
                                    filter_user = Some(v.to_string());
                                }
                                "ADDR" => {
                                    filter_addr = Some(v.to_string());
                                }
                                "LADDR" => {
                                    filter_laddr = Some(v.to_string());
                                }
                                "SKIPME" => {
                                    if v.eq_ignore_ascii_case("yes") {
                                        skipme = true;
                                    } else if v.eq_ignore_ascii_case("no") {
                                        skipme = false;
                                    } else {
                                        err_msg = Some("-ERR syntax error\r\n".to_string());
                                        break;
                                    }
                                }
                                "MAXAGE" => match v.parse::<i64>() {
                                    Ok(a) if a >= 0 => maxage = Some(a as u64),
                                    Ok(_) => {
                                        err_msg = Some(
                                            "-ERR maxage should be greater than 0\r\n".to_string(),
                                        );
                                        break;
                                    }
                                    Err(_) => {
                                        err_msg = Some(
                                            "-ERR maxage is not an integer or out of range\r\n"
                                                .to_string(),
                                        );
                                        break;
                                    }
                                },
                                _ => {
                                    err_msg = Some("-ERR syntax error\r\n".to_string());
                                    break;
                                }
                            }
                            i += 2;
                        }

                        if let Some(err) = err_msg {
                            out.extend_from_slice(err.as_bytes());
                        } else {
                            let list = router.client_list(client_registry, &[]).await;
                            let map = GLOBAL_CLIENTS.read();
                            let mut killed_cids = Vec::new();
                            for line in list.lines() {
                                let mut cid_opt = None;
                                let mut addr_opt = None;
                                let mut laddr_opt = None;
                                let mut age_opt = None;
                                for tok in line.split_whitespace() {
                                    if let Some(v) = tok.strip_prefix("id=") {
                                        cid_opt = v.parse::<u64>().ok();
                                    } else if let Some(v) = tok.strip_prefix("addr=") {
                                        addr_opt = Some(v);
                                    } else if let Some(v) = tok.strip_prefix("laddr=") {
                                        laddr_opt = Some(v);
                                    } else if let Some(v) = tok.strip_prefix("age=") {
                                        age_opt = v.parse::<u64>().ok();
                                    }
                                }
                                let Some(cid) = cid_opt else { continue };
                                if skipme && cid == client_id {
                                    continue;
                                }
                                if !filter_ids.is_empty() && !filter_ids.contains(&cid) {
                                    continue;
                                }
                                if let Some(ref f_addr) = filter_addr
                                    && addr_opt != Some(f_addr.as_str())
                                {
                                    continue;
                                }
                                if let Some(ref f_laddr) = filter_laddr
                                    && laddr_opt != Some(f_laddr.as_str())
                                {
                                    continue;
                                }
                                if let Some(ma) = maxage
                                    && age_opt.unwrap_or(0) <= ma
                                {
                                    continue;
                                }
                                let entry_opt = global_client_entry(&map, router.base_port, client_port_span(router), cid);
                                if let Some(ref f_user) = filter_user {
                                    let u = entry_opt
                                        .map(|e| e.auth_user.as_str())
                                        .unwrap_or("default");
                                    if u != f_user {
                                        continue;
                                    }
                                }
                                if let Some(ref f_type) = filter_type {
                                    let is_ps = entry_opt.map(|e| e.is_pubsub).unwrap_or(false);
                                    let is_rep = entry_opt.is_some_and(|e| {
                                        e.stats.is_replica.load(std::sync::atomic::Ordering::Relaxed)
                                    });
                                    let matches_type = match f_type.as_str() {
                                        "pubsub" => is_ps,
                                        "replica" | "slave" => is_rep,
                                        "normal" => !is_ps && !is_rep,
                                        _ => false,
                                    };
                                    if !matches_type {
                                        continue;
                                    }
                                }
                                killed_cids.push(cid);
                            }

                            let mut killed_count = killed_cids.len();
                            // A replica's master link is not in the client
                            // registry; TYPE master (alone or with ADDR)
                            // reaches it.
                            if filter_type.as_deref() == Some("master")
                                && filter_ids.is_empty()
                                && filter_user.is_none()
                                && filter_laddr.is_none()
                            {
                                let hub = crate::replication::cached_hub(router.base_port);
                                let addr = match &*hub.role.read() {
                                    crate::replication::ReplicationRole::Slave {
                                        master_host,
                                        master_port,
                                        ..
                                    } => Some(format!("{master_host}:{master_port}")),
                                    _ => None,
                                };
                                if let Some(addr) = addr
                                    && filter_addr.as_ref().is_none_or(|a| *a == addr)
                                    && hub.drop_master_link_to(&addr)
                                {
                                    killed_count += 1;
                                }
                            }
                            for cid in killed_cids {
                                if cid == client_id {
                                    close_conn = true;
                                } else if let Some(entry) = global_client_entry(&map, router.base_port, client_port_span(router), cid) {
                                    if entry
                                        .stats
                                        .killed
                                        .compare_exchange(
                                            false,
                                            true,
                                            std::sync::atomic::Ordering::Relaxed,
                                            std::sync::atomic::Ordering::Relaxed,
                                        )
                                        .is_ok()
                                    {
                                        ACTIVE_CLIENTS
                                            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                                    }
                                    // SAFETY: shutdown(2) takes no pointers. The entry is read under the
                                    // GLOBAL_CLIENTS read lock and the connection removes its registry entry (ClientCleanup / PubsubCleanup) before its socket is closed, so the fd is not a reused number.
                                    unsafe {
                                        libc::shutdown(entry.raw_fd, libc::SHUT_RDWR);
                                    }
                                    crate::block::get_block_hub_for_port(router.port)
                                        .lock()
                                        .remove_waiters_for_client(cid);
                                }
                            }
                            out.extend_from_slice(format!(":{}\r\n", killed_count).as_bytes());
                        }
                    }
                }
                ClientSubcommand::Unblock {
                    client_id: target_id,
                    unblock_type,
                } => {
                    let hub_arc = crate::block::get_block_hub_for_port(router.port);
                    let mut hub = hub_arc.lock();
                    let unblocked = hub.unblock_client(target_id, unblock_type)
                        || crate::replication::unblock_waiter(
                            target_id,
                            unblock_type != crate::block::ClientUnblockType::Timeout,
                        );
                    if unblocked {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                ClientSubcommand::Pause(timeout_ms, write_only) => {
                    set_client_pause(timeout_ms, write_only);
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClientSubcommand::Unpause => {
                    let woken = unpause_clients();
                    if woken > 0 {
                        monoio::time::sleep(std::time::Duration::from_millis(2)).await;
                    }
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClientSubcommand::NoTouch(enabled) => {
                    CLIENT_NO_TOUCH.set(enabled);
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClientSubcommand::NoEvict(on) => {
                    if let Some(c) = client_registry.borrow().get(&client_id) {
                        c.stats
                            .no_evict
                            .store(on, std::sync::atomic::Ordering::Relaxed);
                    }
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClientSubcommand::Reply(mode) => {
                    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                        c.reply_mode = mode;
                    }
                    if mode == crate::resp::ClientReplyMode::On {
                        out.extend_from_slice(b"+OK\r\n");
                    }
                }
            }
            close_conn
        }
        // Only connections' resolved HIMPORT SETs (above) reach a shard.
        Command::HimportSet { .. } => {
            out.extend_from_slice(b"-ERR HIMPORT SET must name a fieldset\r\n");
            false
        }
        Command::Himport(ref sub) => {
            let resolved = {
                let mut reg = client_registry.borrow_mut();
                match reg.get_mut(&client_id) {
                    Some(c) => himport_session(&mut c.himport_fieldsets, sub, out),
                    None => himport_session(&mut hashbrown::HashMap::new(), sub, out),
                }
            };
            if let Some(set) = resolved
                && let Some(target) = target_shard_of_cmd(&set, router.num_shards)
            {
                if target == router.shard_id {
                    execute_local_command(
                        &set,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof().as_deref(),
                    );
                } else {
                    let res = router.execute_remote(target, set).await;
                    out.extend_from_slice(&res);
                }
            }
            false
        }
        Command::Hset { .. }
        | Command::Hsetnx { .. }
        | Command::Hmset { .. }
        | Command::Hget { .. }
        | Command::Hmget { .. }
        | Command::Hdel { .. }
        | Command::Hexists { .. }
        | Command::Hlen(_)
        | Command::Hgetall(_)
        | Command::Hkeys(_)
        | Command::Hvals(_)
        | Command::Hstrlen { .. }
        | Command::Hgetdel { .. }
        | Command::Lpush { .. }
        | Command::Rpush { .. }
        | Command::Lpushx { .. }
        | Command::Rpushx { .. }
        | Command::Lpop { .. }
        | Command::Rpop { .. }
        | Command::Lrange { .. }
        | Command::Llen(_)
        | Command::Lindex { .. }
        | Command::Sadd { .. }
        | Command::Srem { .. }
        | Command::Smembers(_)
        | Command::Sismember { .. }
        | Command::Scard(_)
        | Command::Spop { .. }
        | Command::Zadd { .. }
        | Command::Zrem { .. }
        | Command::Zscore { .. }
        | Command::Zcard(_)
        | Command::Zrank { .. }
        | Command::Zrevrank { .. }
        | Command::Zcount { .. }
        | Command::Zincrby { .. }
        | Command::Zrange { .. }
        | Command::Zpopmin { .. }
        | Command::Zpopmax { .. }
        | Command::Type(_)
        | Command::Setnx { .. }
        | Command::Getset { .. }
        | Command::Getdel(_)
        | Command::Append { .. }
        | Command::Strlen(_)
        | Command::Setbit { .. }
        | Command::Getbit { .. }
        | Command::Bitcount { .. }
        | Command::Bitpos { .. }
        | Command::Bitfield { .. }
        | Command::Pfadd { .. }
        | Command::PfdebugGetreg(_)
        | Command::PfdebugEncoding(_)
        | Command::PfdebugTodense(_)
        | Command::Dump(_)
        | Command::Restore { .. }
        | Command::Xadd { .. }
        | Command::Xcfgset { .. }
        | Command::Xsetid { .. }
        | Command::Xdelex { .. }
        | Command::Xidmprecord { .. }
        | Command::Xlen(_)
        | Command::Xrange { .. }
        | Command::Xrevrange { .. }
        | Command::Xdel { .. }
        | Command::Xtrim { .. }
        | Command::XgroupCreate { .. }
        | Command::XgroupDestroy { .. }
        | Command::XgroupCreateConsumer { .. }
        | Command::XgroupDelConsumer { .. }
        | Command::XgroupSetId { .. }
        | Command::Xack { .. }
        | Command::Xackdel { .. }
        | Command::Xnack { .. }
        | Command::Xpending { .. }
        | Command::Hincrby { .. }
        | Command::Hincrbyfloat { .. }
        | Command::Hrandfield { .. }
        | Command::Hscan { .. }
        | Command::Ltrim { .. }
        | Command::Lset { .. }
        | Command::Lrem { .. }
        | Command::Lpos { .. }
        | Command::Linsert { .. }
        | Command::Smismember { .. }
        | Command::Srandmember { .. }
        | Command::Sscan { .. }
        | Command::Zmscore { .. }
        | Command::Zlexcount { .. }
        | Command::Zrandmember { .. }
        | Command::Zremrangebyrank { .. }
        | Command::Zremrangebyscore { .. }
        | Command::Zremrangebylex { .. }
        | Command::Zscan { .. }
        | Command::Incrbyfloat { .. }
        | Command::Setrange { .. }
        | Command::Getrange { .. }
        | Command::JsonSet { .. }
        | Command::JsonGet { .. }
        | Command::JsonDel { .. }
        | Command::JsonType { .. }
        | Command::JsonNumIncrBy { .. }
        | Command::JsonNumMultBy { .. }
        | Command::JsonStrAppend { .. }
        | Command::JsonStrLen { .. }
        | Command::JsonArrAppend { .. }
        | Command::JsonArrLen { .. }
        | Command::JsonArrPop { .. }
        | Command::JsonObjKeys { .. }
        | Command::JsonObjLen { .. }
        | Command::JsonToggle { .. }
        | Command::JsonClear { .. }
        | Command::Geoadd { .. }
        | Command::Geodist { .. }
        | Command::Geopos { .. }
        | Command::Geohash { .. }
        | Command::Georadius { .. }
        | Command::Georadiusbymember { .. }
        | Command::Geosearch { .. }
        | Command::Geosearchstore { .. }
        | Command::BfReserve { .. }
        | Command::BfAdd { .. }
        | Command::BfMadd { .. }
        | Command::BfExists { .. }
        | Command::BfMexists { .. }
        | Command::BfInfo(_)
        | Command::CfReserve { .. }
        | Command::CfAdd { .. }
        | Command::CfAddnx { .. }
        | Command::CfExists { .. }
        | Command::CfDel { .. }
        | Command::CfInfo(_)
        | Command::CmsInitbydim { .. }
        | Command::CmsInitbyprob { .. }
        | Command::CmsIncrby { .. }
        | Command::CmsQuery { .. }
        | Command::CmsInfo(_)
        | Command::TopkReserve { .. }
        | Command::TopkAdd { .. }
        | Command::TopkQuery { .. }
        | Command::TopkList(_)
        | Command::TopkInfo(_)
        | Command::ProbRestore { .. }
        | Command::Digest(_)
        | Command::Delex { .. }
        | Command::CrdtSet { .. }
        | Command::CrdtGet(_)
        | Command::CrdtDel(_)
        | Command::CrdtIncrby { .. }
        | Command::CrdtSadd { .. }
        | Command::CrdtSmembers(_)
        | Command::CrdtSrem { .. }
        | Command::SemanticSet { .. }
        | Command::SemanticGet { .. }
        | Command::SemanticDel { .. }
        | Command::SemanticFlush(_)
        | Command::SemanticInfo(_)
        | Command::AgentMemAdd { .. }
        | Command::AgentMemContext { .. }
        | Command::AgentMemCompact { .. }
        | Command::AgentMemInfo(_)
        | Command::AgentMemClear(_)
        | Command::LlmQuotaReserve { .. }
        | Command::LlmQuotaSettle { .. }
        | Command::LlmQuotaInfo(_)
        | Command::AgentCheckpointPut { .. }
        | Command::AgentCheckpointGet { .. }
        | Command::AgentCheckpointHistory { .. }
        | Command::AgentToolClaim { .. }
        | Command::AgentToolComplete { .. }
        | Command::Vadd { .. }
        | Command::Vquery { .. }
        | Command::Vsim { .. }
        | Command::Vdist { .. }
        | Command::Vdel { .. }
        | Command::Vinfo(_)
        | Command::Vcard(_)
        | Command::Vdim(_)
        | Command::Vemb { .. }
        | Command::Vlinks { .. }
        | Command::Vrandmember { .. }
        | Command::Vsetattr { .. }
        | Command::Vgetattr { .. }
        | Command::Vismember { .. }
        | Command::Object(crate::resp::ObjectSubcommand::Encoding(_))
        | Command::Object(crate::resp::ObjectSubcommand::Freq(_))
        | Command::Object(crate::resp::ObjectSubcommand::Idletime(_))
        | Command::Object(crate::resp::ObjectSubcommand::Refcount(_))
        | Command::Xinfo(crate::resp::XinfoSubcommand::Stream(_))
        | Command::Xinfo(crate::resp::XinfoSubcommand::StreamFull { .. })
        | Command::Xinfo(crate::resp::XinfoSubcommand::Groups(_))
        | Command::Xinfo(crate::resp::XinfoSubcommand::Consumers { .. }) => {
            let had_waiters = crate::block::has_blocked_waiters(router.port);
            if let Some(target) = target_shard_of_cmd(&cmd, router.num_shards) {
                if target == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof().as_deref(),
                    );
                } else {
                    let res = router.execute_remote(target, cmd).await;
                    out.extend_from_slice(&res);
                }
            }
            if had_waiters && !in_tx(client_id) {
                monoio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
            false
        }
        Command::Increx {
            ref key,
            increment,
            lbound,
            ubound,
            saturate,
            expire,
            enx,
        } => {
            let target = target_shard(key, router.num_shards);
            if target == router.shard_id {
                let mut db = router.local_db.borrow_mut();
                match db.increx(
                    key.clone(),
                    increment,
                    lbound,
                    ubound,
                    saturate,
                    expire,
                    enx,
                ) {
                    Ok(out_res) => {
                        let rep_cmd = out_res.rep_cmd.clone();
                        let events = out_res.events.clone();
                        let is_float = out_res.is_float;
                        let val_float = out_res.val_float;
                        let delta_float = out_res.delta_float;
                        let val_int = out_res.val_int;
                        let delta_int = out_res.delta_int;
                        drop(db);

                        if let Some(rep) = rep_cmd {
                            if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                for_each_cmd_key(&rep, |k| {
                                    touch_watched_key(router.port, k);
                                });
                            }
                            let need_aof = router.aof().is_some();
                            let need_rep = crate::replication::has_connected_replicas(router.base_port);
                            if (need_aof || need_rep)
                                && let Some(bytes) = crate::aof::command_to_resp(&rep)
                            {
                                if let Some(aof_writer) = router.aof().as_deref() {
                                    aof_writer.borrow_mut().append(&bytes);
                                }
                                if need_rep {
                                    crate::replication::propagate_shard_bytes(
                                        router.base_port,
                                        router.shard_id,
                                        &bytes,
                                    );
                                }
                            }
                        }

                        for ev in events {
                            let ev_type = match ev {
                                "incrby" | "incrbyfloat" => NOTIFY_STRING,
                                _ => NOTIFY_GENERIC,
                            };
                            notify_keyspace_event_sync(router, ev_type, ev, key);
                        }

                        out.extend_from_slice(b"*2\r\n");
                        if is_float {
                            let s_val = val_float.to_string();
                            out.extend_from_slice(
                                format!("${}\r\n{}\r\n", s_val.len(), s_val).as_bytes(),
                            );
                            let s_delta = delta_float.to_string();
                            out.extend_from_slice(
                                format!("${}\r\n{}\r\n", s_delta.len(), s_delta).as_bytes(),
                            );
                        } else {
                            let s_val = val_int.to_string();
                            out.extend_from_slice(format!(":{}\r\n", s_val).as_bytes());
                            let s_delta = delta_int.to_string();
                            out.extend_from_slice(format!(":{}\r\n", s_delta).as_bytes());
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                }
            } else {
                let res = router.execute_remote(target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::JsonMget { keys, path } => {
            let results = router.json_mget(keys, &path).await;
            out.extend_from_slice(format!("*{}\r\n", results.len()).as_bytes());
            for val in results {
                match val {
                    Some(s) => write_resp_bulk(out, s.as_bytes()),
                    None => write_resp_null(out),
                }
            }
            false
        }

        Command::Xread {
            ref keys,
            ref ids,
            block_ms,
            ..
        }
        | Command::Xreadgroup {
            ref keys,
            ref ids,
            block_ms,
            ..
        } => {
            if keys.is_empty() {
                write_resp_null(out);
                return false;
            }
            let first_target = router.target_shard(&keys[0]);
            let all_same = keys[1..]
                .iter()
                .all(|k| router.target_shard(k) == first_target);
            if all_same {
                let start_len = out.len();
                if first_target == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof().as_deref(),
                    );
                } else {
                    let res = router.execute_remote(first_target, cmd.clone()).await;
                    out.extend_from_slice(&res);
                }

                let can_block = match &cmd {
                    Command::Xread { .. } => true,
                    Command::Xreadgroup { ids, .. } => ids.iter().all(|id| id == ">"),
                    _ => false,
                };
                let produced_empty = &out[start_len..] == b"$-1\r\n"
                    || &out[start_len..] == b"_\r\n"
                    || &out[start_len..] == b"*0\r\n";
                if let Some(wait_ms) = block_ms
                    && produced_empty
                    && can_block
                    && !in_tx(client_id)
                {
                    out.truncate(start_len);
                    let _guard = BlockedClientGuard {
                        port: router.port,
                        client_id,
                    };
                    let resolved_ids = resolve_stream_last_ids(router, keys, ids).await;
                    let (tx, rx) = flume::bounded(1);

                    let mut unblocked_cmd = cmd.clone();
                    match &mut unblocked_cmd {
                        Command::Xread {
                            block_ms: b,
                            ids: cmd_ids,
                            ..
                        }
                        | Command::Xreadgroup {
                            block_ms: b,
                            ids: cmd_ids,
                            ..
                        } => {
                            *b = None;
                            *cmd_ids = resolved_ids;
                        }
                        _ => {}
                    }

                    {
                        let hub_arc = crate::block::get_block_hub_for_port(router.port);
                        let mut hub = hub_arc.lock();
                        for k in keys {
                            hub.register_stream_waiter(
                                client_id,
                                k.clone(),
                                unblocked_cmd.clone(),
                                CURRENT_CLIENT_RESP3.get(),
                                false,
                                tx.clone(),
                            );
                        }
                    }
                    recheck_blocking_keys(router, keys.iter());

                    let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
                    if let Command::Xreadgroup {
                        claim: Some(min_idle),
                        ref group,
                        ..
                    } = cmd
                    {
                        let deadline = if wait_ms > 0 {
                            Some(
                                std::time::Instant::now()
                                    + std::time::Duration::from_millis(wait_ms),
                            )
                        } else {
                            None
                        };
                        loop {
                            let remaining_ms = match deadline {
                                Some(dl) => remaining_wait_ms(dl, std::time::Instant::now()),
                                None => 0,
                            };
                            if deadline.is_some() && remaining_ms == 0 {
                                write_resp_null(out);
                                return false;
                            }
                            let mut earliest_pel_wait: Option<u64> = None;
                            if first_target == router.shard_id {
                                let mut db = router.local_db.borrow_mut();
                                for k in keys {
                                    if let Some(w) = db.earliest_claim_wait_ms(k, group, *min_idle) {
                                        earliest_pel_wait =
                                            Some(earliest_pel_wait.map_or(w, |m| m.min(w)));
                                    }
                                }
                            }
                            let target_wait = earliest_pel_wait.map_or(50, |w| w.clamp(1, 50));
                            let wait_timeout_ms = if deadline.is_some() {
                                remaining_ms.min(target_wait)
                            } else {
                                target_wait
                            };

                            let (wait_res, client_disconnected) =
                                wait_for_stream_result(&rx, wait_timeout_ms, raw_fd).await;
                            if client_disconnected {
                                return true;
                            }
                            match wait_res {
                                Some(crate::block::BlockedStreamResult::Data(data)) => {
                                    out.extend_from_slice(&data);
                                    return false;
                                }
                                Some(crate::block::BlockedStreamResult::Error(err)) => {
                                    out.extend_from_slice(&err);
                                    return false;
                                }
                                Some(crate::block::BlockedStreamResult::Unblocked(
                                    unblock_type,
                                )) => {
                                    match unblock_type {
                                        crate::block::ClientUnblockType::Timeout => {
                                            write_resp_null(out)
                                        }
                                        crate::block::ClientUnblockType::NoPerm => {
                                            out.extend_from_slice(BLOCKED_NOPERM_REPLY)
                                        }
                                        crate::block::ClientUnblockType::Error => {
                                            out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n")
                                        }
                                        crate::block::ClientUnblockType::Moved(slot) => {
                                            blocked_moved_reply(router, slot, out)
                                        }
                                        crate::block::ClientUnblockType::WrongType => {
                                            out.extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                                        }
                                    }
                                    return false;
                                }
                                _ => {
                                    let read_start = out.len();
                                    if first_target == router.shard_id {
                                        execute_local_command(
                                            &unblocked_cmd,
                                            &mut router.local_db.borrow_mut(),
                                            out,
                                            router.aof().as_deref(),
                                        );
                                    } else {
                                        let res = router
                                            .execute_remote(first_target, unblocked_cmd.clone())
                                            .await;
                                        out.extend_from_slice(&res);
                                    }
                                    let unblocked_produced_empty = &out[read_start..] == b"$-1\r\n"
                                        || &out[read_start..] == b"_\r\n"
                                        || &out[read_start..] == b"*0\r\n";
                                    if !unblocked_produced_empty {
                                        return false;
                                    }
                                    out.truncate(read_start);
                                }
                            }
                        }
                    } else {
                        let (wait_res, client_disconnected) =
                            wait_for_stream_result(&rx, wait_ms, raw_fd).await;
                        if client_disconnected {
                            return true;
                        }
                        match wait_res {
                            Some(crate::block::BlockedStreamResult::Data(data)) => {
                                out.extend_from_slice(&data);
                                return false;
                            }
                            Some(crate::block::BlockedStreamResult::Error(err)) => {
                                out.extend_from_slice(&err);
                                return false;
                            }
                            Some(crate::block::BlockedStreamResult::Unblocked(unblock_type)) => {
                                match unblock_type {
                                    crate::block::ClientUnblockType::Timeout => {
                                        write_resp_null(out)
                                    }
                                    crate::block::ClientUnblockType::NoPerm => {
                                        out.extend_from_slice(BLOCKED_NOPERM_REPLY)
                                    }
                                    crate::block::ClientUnblockType::Error => {
                                        out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n")
                                    }
                                    crate::block::ClientUnblockType::Moved(slot) => {
                                        blocked_moved_reply(router, slot, out)
                                    }
                                    crate::block::ClientUnblockType::WrongType => {
                                        out.extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                                    }
                                }
                                return false;
                            }
                            _ => {
                                write_resp_null(out);
                                return false;
                            }
                        }
                    }
                }
                return false;
            }

            if router.cluster_enabled
                && keys[1..]
                    .iter()
                    .any(|k| crate::router::key_slot(k) != crate::router::key_slot(&keys[0]))
            {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }

            // Cross-shard multi-stream execution
            match query_cross_shard_streams(router, &cmd).await {
                Ok(stream_blocks) => {
                    if !stream_blocks.is_empty() {
                        write_resp_array_header(out, stream_blocks.len());
                        for b in stream_blocks {
                            out.extend_from_slice(&b);
                        }
                    } else if let Some(wait_ms) = block_ms
                        && (match &cmd {
                            Command::Xread { .. } => true,
                            Command::Xreadgroup { ids, .. } => ids.iter().all(|id| id == ">"),
                            _ => false,
                        })
                        && !in_tx(client_id)
                    {
                        let _guard = BlockedClientGuard {
                            port: router.port,
                            client_id,
                        };
                        let resolved_ids = resolve_stream_last_ids(router, keys, ids).await;
                        let (tx, rx) = flume::bounded(1);
                        let start_wait = std::time::Instant::now();
                        let deadline = if wait_ms > 0 {
                            Some(start_wait + std::time::Duration::from_millis(wait_ms))
                        } else {
                            None
                        };

                        let mut unblocked_cmd = cmd.clone();
                        match &mut unblocked_cmd {
                            Command::Xread {
                                block_ms: b,
                                ids: cmd_ids,
                                ..
                            }
                            | Command::Xreadgroup {
                                block_ms: b,
                                ids: cmd_ids,
                                ..
                            } => {
                                *b = None;
                                *cmd_ids = resolved_ids;
                            }
                            _ => {}
                        }

                        loop {
                            {
                                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                                let mut hub = hub_arc.lock();
                                for k in keys {
                                    hub.register_stream_waiter(
                                        client_id,
                                        k.clone(),
                                        unblocked_cmd.clone(),
                                        CURRENT_CLIENT_RESP3.get(),
                                        true,
                                        tx.clone(),
                                    );
                                }
                            }
                            recheck_blocking_keys(router, keys.iter());

                            let remaining_ms = match deadline {
                                Some(dl) => remaining_wait_ms(dl, std::time::Instant::now()),
                                None => 0,
                            };

                            if deadline.is_some() && remaining_ms == 0 {
                                write_resp_null(out);
                                return false;
                            }

                            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
                            let (wait_res, client_disconnected) =
                                wait_for_stream_result(&rx, remaining_ms, raw_fd).await;
                            if client_disconnected {
                                return true;
                            }
                            match wait_res {
                                Some(crate::block::BlockedStreamResult::Data(data)) => {
                                    out.extend_from_slice(&data);
                                    return false;
                                }
                                Some(crate::block::BlockedStreamResult::Error(err)) => {
                                    out.extend_from_slice(&err);
                                    return false;
                                }
                                Some(crate::block::BlockedStreamResult::Unblocked(
                                    unblock_type,
                                )) => {
                                    match unblock_type {
                                        crate::block::ClientUnblockType::Timeout => {
                                            write_resp_null(out)
                                        }
                                        crate::block::ClientUnblockType::NoPerm => {
                                            out.extend_from_slice(BLOCKED_NOPERM_REPLY)
                                        }
                                        crate::block::ClientUnblockType::Error => {
                                            out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n")
                                        }
                                        crate::block::ClientUnblockType::Moved(slot) => {
                                            blocked_moved_reply(router, slot, out)
                                        }
                                        crate::block::ClientUnblockType::WrongType => {
                                            out.extend_from_slice(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")
                                        }
                                    }
                                    return false;
                                }
                                Some(crate::block::BlockedStreamResult::CrossShard) => {
                                    match query_cross_shard_streams(router, &unblocked_cmd).await {
                                        Ok(unblocked_blocks) if !unblocked_blocks.is_empty() => {
                                            write_resp_array_header(out, unblocked_blocks.len());
                                            for b in unblocked_blocks {
                                                out.extend_from_slice(&b);
                                            }
                                            return false;
                                        }
                                        _ => {}
                                    }
                                }
                                None => {
                                    write_resp_null(out);
                                    return false;
                                }
                            }
                        }
                    } else {
                        write_resp_null(out);
                    }
                }
                Err(err) => {
                    out.extend_from_slice(&err);
                }
            }
            false
        }
        Command::Auth { username, password } => {
            let uname = username.as_deref().unwrap_or("default");
            let pass = password.as_str();
            let acl = crate::acl::get_acl_for_port(router.port);
            // As Redis does, the one-argument form is an error while the
            // default user has no password (it used to reply +OK, hiding a
            // client configured with a password the server doesn't have).
            if username.is_none() && acl.read().users.get("default").is_some_and(|u| u.nopass) {
                out.extend_from_slice(
                    b"-ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?\r\n",
                );
                return false;
            }
            let res = acl.read().check_auth(Some(uname), pass);
            if let Ok(authed_user) = res {
                *authenticated = true;
                *auth_user = authed_user.clone();
                update_global_client_auth(router.port, client_id, &authed_user);
                out.extend_from_slice(b"+OK\r\n");
            } else {
                let ctx = if in_tx(requesting_client_id()) {
                    "multi"
                } else {
                    "toplevel"
                };
                record_acl_denial(
                    router.port,
                    crate::acl::AclDeniedReason::Auth,
                    ctx,
                    "AUTH",
                    uname,
                    "auth",
                );
                out.extend_from_slice(
                    b"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
                );
            }
            false
        }
        Command::Acl(subcmd) => {
            let acl = crate::acl::get_acl_for_port(router.port);
            let mut close_conn = false;
            match subcmd {
                crate::resp::AclSubcommand::WhoAmI => {
                    out.extend_from_slice(
                        format!("${}\r\n{}\r\n", auth_user.len(), auth_user).as_bytes(),
                    );
                }
                crate::resp::AclSubcommand::Users => {
                    let users = acl.read().users();
                    out.extend_from_slice(format!("*{}\r\n", users.len()).as_bytes());
                    for u in users {
                        out.extend_from_slice(format!("${}\r\n{}\r\n", u.len(), u).as_bytes());
                    }
                }
                crate::resp::AclSubcommand::List => {
                    let list = acl.read().list();
                    out.extend_from_slice(format!("*{}\r\n", list.len()).as_bytes());
                    for line in list {
                        out.extend_from_slice(
                            format!("${}\r\n{}\r\n", line.len(), line).as_bytes(),
                        );
                    }
                }
                crate::resp::AclSubcommand::GetUser(username) => {
                    if let Some(user) = acl.read().get_user(&username) {
                        // As Redis 7: a map (flat array in RESP2), flags as a
                        // set, without Redis 6's allkeys/allchannels/allcommands
                        // (the rules below say that), and no selectors.
                        write_resp_map_len(out, 6);
                        out.extend_from_slice(b"$5\r\nflags\r\n");
                        let flags: Vec<String> = user
                            .flags()
                            .into_iter()
                            .filter(|f| !matches!(f.as_str(), "allkeys" | "allchannels" | "allcommands"))
                            .collect();
                        write_resp_set_len(out, flags.len());
                        for f in flags {
                            out.extend_from_slice(format!("${}\r\n{}\r\n", f.len(), f).as_bytes());
                        }
                        out.extend_from_slice(b"$9\r\npasswords\r\n");
                        // Redis reports SHA-256 hex digests, never plaintext.
                        out.extend_from_slice(
                            format!("*{}\r\n", user.password_hashes.len()).as_bytes(),
                        );
                        for h in &user.password_hashes {
                            let p = h.strip_prefix('#').unwrap_or(h);
                            out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
                        }
                        out.extend_from_slice(b"$8\r\ncommands\r\n");
                        let cmd_str = user.commands_rule_string();
                        out.extend_from_slice(
                            format!("${}\r\n{}\r\n", cmd_str.len(), cmd_str).as_bytes(),
                        );
                        out.extend_from_slice(b"$4\r\nkeys\r\n");
                        let key_str = user.keys_rule_string();
                        out.extend_from_slice(
                            format!("${}\r\n{}\r\n", key_str.len(), key_str).as_bytes(),
                        );
                        out.extend_from_slice(b"$8\r\nchannels\r\n");
                        let chan_str = user.channels_rule_string();
                        out.extend_from_slice(
                            format!("${}\r\n{}\r\n", chan_str.len(), chan_str).as_bytes(),
                        );
                        out.extend_from_slice(b"$9\r\nselectors\r\n*0\r\n");
                    } else {
                        write_resp_null(out);
                    }
                }
                crate::resp::AclSubcommand::SetUser { username, rules } => {
                    flush_pending_bcast(router.port);
                    let res = acl.write().set_user(&username, &rules);
                    match res {
                        Ok(()) => {
                            out.extend_from_slice(b"+OK\r\n");
                            if router.kill_revoked_subscribers(client_id).await {
                                close_conn = true;
                            }
                        }
                        Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
                    }
                }
                crate::resp::AclSubcommand::DelUser(usernames) => {
                    if usernames.iter().any(|u| u.is_empty()) {
                        out.extend_from_slice(b"-ERR Username cannot be empty\r\n");
                    } else if usernames.iter().any(|u| u == "default") {
                        out.extend_from_slice(b"-ERR The 'default' user cannot be removed\r\n");
                    } else {
                        let count = acl.write().del_user(&usernames);
                        let map = GLOBAL_CLIENTS.read();
                        for (&(p, cid), entry) in map.iter() {
                            if is_server_client_port(router, p) && usernames.iter().any(|u| u == &entry.auth_user) {
                                if cid == client_id {
                                    close_conn = true;
                                } else {
                                    kill_registered_client(router.port, cid, entry);
                                }
                            }
                        }
                        write_resp_integer(out, count as i64);
                    }
                }
                crate::resp::AclSubcommand::Cat(cat_opt) => {
                    if let Some(cat) = cat_opt {
                        match crate::acl::commands_in_category(&cat) {
                            Some(cmds) => {
                                out.extend_from_slice(format!("*{}\r\n", cmds.len()).as_bytes());
                                for c in cmds {
                                    write_resp_bulk(out, c.as_bytes());
                                }
                            }
                            None => {
                                out.extend_from_slice(
                                    format!("-ERR Unknown category '{}'\r\n", err_arg(&cat)).as_bytes(),
                                );
                            }
                        }
                    } else {
                        let cats = crate::acl_categories::CATEGORIES;
                        out.extend_from_slice(format!("*{}\r\n", cats.len()).as_bytes());
                        for cat in cats {
                            out.extend_from_slice(
                                format!("${}\r\n{}\r\n", cat.len(), cat).as_bytes(),
                            );
                        }
                    }
                }
                crate::resp::AclSubcommand::GenPass(bits) => match crate::acl::genpass_hex(bits) {
                    Ok(s) => write_resp_bulk(out, s.as_bytes()),
                    Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
                },
                crate::resp::AclSubcommand::DryRun { username, cmd_args } => {
                    let user_opt = acl.read().get_user(&username);
                    let Some(user) = user_opt else {
                        out.extend_from_slice(
                            format!("-ERR User '{}' not found\r\n", err_arg(&username)).as_bytes(),
                        );
                        return false;
                    };
                    let raw_name = String::from_utf8_lossy(&cmd_args[0]).to_ascii_lowercase();
                    match crate::resp::build_command(cmd_args.clone()) {
                        Ok(Some(Command::Unknown(_))) | Ok(None) => {
                            out.extend_from_slice(
                                format!("-ERR Command '{}' not found\r\n", err_arg(&raw_name)).as_bytes(),
                            );
                        }
                        Ok(Some(parsed_cmd)) => {
                            if let Some(err) = acl_check_user_cmd(&user, &parsed_cmd) {
                                let msg = String::from_utf8_lossy(&err);
                                let msg = msg
                                    .trim_start_matches("-NOPERM ")
                                    .trim_start_matches('-')
                                    .trim_end_matches("\r\n");
                                let full = format!("This user {}", msg.trim_start_matches("this user "));
                                write_resp_bulk(out, full.as_bytes());
                            } else {
                                out.extend_from_slice(b"+OK\r\n");
                            }
                        }
                        Err(_) => {
                            // Even when arguments fail syntax validation, Redis
                            // DRYRUN checks command permission first.
                            let cand = if cmd_args.len() >= 2 {
                                let sub = format!(
                                    "{}|{}",
                                    raw_name,
                                    String::from_utf8_lossy(&cmd_args[1]).to_ascii_lowercase()
                                );
                                if crate::acl::command_index(&sub).is_some() {
                                    sub
                                } else {
                                    raw_name.clone()
                                }
                            } else {
                                raw_name.clone()
                            };
                            if crate::acl::command_index(&cand).is_none() {
                                out.extend_from_slice(
                                    format!("-ERR Command '{}' not found\r\n", err_arg(&raw_name)).as_bytes(),
                                );
                            } else if !user.can_execute_command_with_first_arg(
                                &cand,
                                cmd_args.get(1).map(|b| b.as_ref()),
                            ) {
                                let full = format!(
                                    "This user has no permissions to run the '{}' command",
                                    cand
                                );
                                write_resp_bulk(out, full.as_bytes());
                            } else {
                                out.extend_from_slice(b"+OK\r\n");
                            }
                        }
                    }
                }
                crate::resp::AclSubcommand::Log(count_opt) => {
                    let guard = acl.read();
                    let limit = count_opt.unwrap_or(guard.acl_log.len());
                    let entries: Vec<_> = guard.acl_log.iter().take(limit).collect();
                    write_resp_array_header(out, entries.len());
                    for e in entries {
                        if CURRENT_CLIENT_RESP3.get() {
                            out.extend_from_slice(b"%10\r\n");
                        } else {
                            write_resp_array_header(out, 20);
                        }
                        write_resp_bulk(out, b"count");
                        write_resp_integer(out, e.count as i64);
                        write_resp_bulk(out, b"reason");
                        write_resp_bulk(out, e.reason.as_str().as_bytes());
                        write_resp_bulk(out, b"context");
                        write_resp_bulk(out, e.context.as_bytes());
                        write_resp_bulk(out, b"object");
                        write_resp_bulk(out, e.object.as_bytes());
                        write_resp_bulk(out, b"username");
                        write_resp_bulk(out, e.username.as_bytes());
                        write_resp_bulk(out, b"age-seconds");
                        let age_s = format!("{:.3}", e.age_created.elapsed().as_secs_f64());
                        write_resp_bulk(out, age_s.as_bytes());
                        write_resp_bulk(out, b"client-info");
                        write_resp_bulk(out, e.client_info.as_bytes());
                        write_resp_bulk(out, b"entry-id");
                        write_resp_integer(out, e.entry_id as i64);
                        write_resp_bulk(out, b"timestamp-created");
                        let ts_c = (e.timestamp_created * 1000.0) as i64;
                        write_resp_integer(out, ts_c);
                        write_resp_bulk(out, b"timestamp-last-updated");
                        let ts_u = (e.timestamp_last_updated * 1000.0) as i64;
                        write_resp_integer(out, ts_u);
                    }
                }
                crate::resp::AclSubcommand::LogReset => {
                    acl.write().acl_log.clear();
                    out.extend_from_slice(b"+OK\r\n");
                }
                crate::resp::AclSubcommand::Save => match acl.read().save_to_aclfile() {
                    Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
                },
                crate::resp::AclSubcommand::Load => {
                    let res = acl.write().load_from_aclfile();
                    match res {
                        Ok(()) => {
                            {
                                let guard = acl.read();
                                let map = GLOBAL_CLIENTS.read();
                                for (&(p, cid), entry) in map.iter() {
                                    let keep = guard
                                        .users
                                        .get(&entry.auth_user)
                                        .is_some_and(|u| u.enabled);
                                    if is_server_client_port(router, p) && !keep {
                                        if cid == client_id {
                                            close_conn = true;
                                        } else {
                                            kill_registered_client(router.port, cid, entry);
                                        }
                                    }
                                }
                            }
                            out.extend_from_slice(b"+OK\r\n");
                            if router.kill_revoked_subscribers(client_id).await {
                                close_conn = true;
                            }
                        }
                        Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
                    }
                }
                crate::resp::AclSubcommand::Help => {
                    const HELP: &[&str] = &[
                        "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                        "LOAD",
                        "    Reload users from the ACL file.",
                        "SAVE",
                        "    Save the current ACL rules to the ACL file.",
                        "LIST",
                        "    Show user details in config file format.",
                        "USERS",
                        "    List all the registered usernames.",
                        "GETUSER <username>",
                        "    Get the user details.",
                        "SETUSER <username> [attribs ...]",
                        "    Create or modify a user with the specified attributes.",
                        "DELUSER <username> [<username> ...]",
                        "    Delete a list of users.",
                        "CAT [<category>]",
                        "    List available categories, or commands inside a category.",
                        "GENPASS [<bits>]",
                        "    Generate a secure 256-bit user password.",
                        "WHOAMI",
                        "    Return the current connection username.",
                        "LOG [<count> | RESET]",
                        "    List latest events denied because of ACLs.",
                        "DRYRUN <username> <command> [<arg> ...]",
                        "    Returns whether the user can execute the given command without executing the command.",
                        "HELP",
                        "    Print this help.",
                    ];
                    write_resp_array_header(out, HELP.len());
                    for line in HELP {
                        write_resp_status(out, line);
                    }
                }
            }
            close_conn
        }
        Command::Blpop { keys, timeout } => {
            let mut popped: Option<(Bytes, Bytes)> = None;
            let mut popped_locally = false;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    let exists = db.exists(k);
                    if !exists {
                        continue;
                    }
                    match db.lpop(k, 1) {
                        Ok(mut vals) => {
                            if let Some(v) = vals.pop() {
                                popped = Some((k.clone(), v));
                                popped_locally = true;
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_res = router
                        .execute_remote(
                            target,
                            Command::Lpop {
                                key: k.clone(),
                                count: None,
                            },
                        )
                        .await;
                    if remote_res.starts_with(b"-") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if let Some(v) = parse_bulk_str_from_resp(&remote_res) {
                        popped = Some((k.clone(), v));
                        break;
                    }
                }
            }

            if let Some((k, v)) = popped {
                // A remote pop was already logged and replicated by the owner
                // shard's execute_local_command; only a local pop needs it here.
                if popped_locally {
                    let rep_cmd = Command::Lpop {
                        key: k.clone(),
                        count: None,
                    };
                    if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                        if let Some(aof_w) = &router.aof() {
                            aof_w.borrow_mut().append(&bytes);
                        }
                        if crate::replication::has_connected_replicas(router.base_port) {
                            crate::replication::propagate_shard_bytes(
                                router.base_port,
                                router.shard_id,
                                &bytes,
                            );
                        }
                    }
                }
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n$");
                out.extend_from_slice(v.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&v);
                out.extend_from_slice(b"\r\n");
                return false;
            }

            if in_tx(client_id) {
                write_resp_null_array(out);
                return false;
            }

            let _guard = BlockedClientGuard {
                port: router.port,
                client_id,
            };
            let (tx, rx) = flume::bounded(1);
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock();
                hub.register_blocked_client(client_id, tx.clone());
                for k in &keys {
                    hub.register_list_waiter(
                        client_id,
                        k.clone(),
                        crate::block::ListPopType::Left,
                        1,
                        tx.clone(),
                    );
                }
            }
            recheck_blocking_keys(router, keys.iter());

            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
            let (recv_res, client_disconnected) =
                wait_for_blocked_result(&rx, timeout, raw_fd).await;
            if client_disconnected {
                return true;
            }

            match recv_res {
                Some(crate::block::BlockedListResult::Popped(k, mut vals)) => {
                    if let Some(v) = vals.pop() {
                        out.extend_from_slice(b"*2\r\n$");
                        out.extend_from_slice(k.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n$");
                        out.extend_from_slice(v.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        write_resp_null_array(out);
                    }
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::WrongType,
                )) => {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::NoPerm,
                )) => {
                    out.extend_from_slice(BLOCKED_NOPERM_REPLY);
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Error,
                )) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Moved(slot),
                )) => {
                    blocked_moved_reply(router, slot, out);
                }
                _ => {
                    write_resp_null_array(out);
                }
            }
            false
        }
        Command::Brpop { keys, timeout } => {
            let mut popped: Option<(Bytes, Bytes)> = None;
            let mut popped_locally = false;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    if !db.exists(k) {
                        continue;
                    }
                    match db.rpop(k, 1) {
                        Ok(mut vals) => {
                            if let Some(v) = vals.pop() {
                                popped = Some((k.clone(), v));
                                popped_locally = true;
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_res = router
                        .execute_remote(
                            target,
                            Command::Rpop {
                                key: k.clone(),
                                count: None,
                            },
                        )
                        .await;
                    if remote_res.starts_with(b"-") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if let Some(v) = parse_bulk_str_from_resp(&remote_res) {
                        popped = Some((k.clone(), v));
                        break;
                    }
                }
            }

            if let Some((k, v)) = popped {
                // A remote pop was already logged and replicated by the owner
                // shard's execute_local_command; only a local pop needs it here.
                if popped_locally {
                    let rep_cmd = Command::Rpop {
                        key: k.clone(),
                        count: None,
                    };
                    if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                        if let Some(aof_w) = &router.aof() {
                            aof_w.borrow_mut().append(&bytes);
                        }
                        if crate::replication::has_connected_replicas(router.base_port) {
                            crate::replication::propagate_shard_bytes(
                                router.base_port,
                                router.shard_id,
                                &bytes,
                            );
                        }
                    }
                }
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n$");
                out.extend_from_slice(v.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&v);
                out.extend_from_slice(b"\r\n");
                return false;
            }

            if in_tx(client_id) {
                write_resp_null_array(out);
                return false;
            }

            let _guard = BlockedClientGuard {
                port: router.port,
                client_id,
            };
            let (tx, rx) = flume::bounded(1);
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock();
                hub.register_blocked_client(client_id, tx.clone());
                for k in &keys {
                    hub.register_list_waiter(
                        client_id,
                        k.clone(),
                        crate::block::ListPopType::Right,
                        1,
                        tx.clone(),
                    );
                }
            }
            recheck_blocking_keys(router, keys.iter());

            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
            let (recv_res, client_disconnected) =
                wait_for_blocked_result(&rx, timeout, raw_fd).await;
            if client_disconnected {
                return true;
            }

            match recv_res {
                Some(crate::block::BlockedListResult::Popped(k, mut vals)) => {
                    if let Some(v) = vals.pop() {
                        out.extend_from_slice(b"*2\r\n$");
                        out.extend_from_slice(k.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n$");
                        out.extend_from_slice(v.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        write_resp_null_array(out);
                    }
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::WrongType,
                )) => {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::NoPerm,
                )) => {
                    out.extend_from_slice(BLOCKED_NOPERM_REPLY);
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Error,
                )) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Moved(slot),
                )) => {
                    blocked_moved_reply(router, slot, out);
                }
                _ => {
                    write_resp_null_array(out);
                }
            }
            false
        }
        Command::Smove {
            ref source,
            ref destination,
            ref member,
        } => {
            let s_target = router.target_shard(source);
            let d_target = router.target_shard(destination);
            if s_target != d_target {
                if router.cluster_enabled {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                } else {
                    let op = CrossShardElementMove::Set {
                        member: member.clone(),
                    };
                    out.extend_from_slice(
                        &move_element_across_shards(router, source, destination, op, in_tx(client_id)).await,
                    );
                }
                return false;
            }
            if s_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof().as_deref(),
                );
            } else {
                let res = router.execute_remote(s_target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Sort {
            ref key,
            desc,
            alpha,
            ref store,
            limit,
            ref by,
            ref get,
            readonly: _,
        } => {
            // 1. Cluster checks
            if router.cluster_enabled {
                let key_slot = crate::router::key_slot(key);
                if let Some(dest) = store
                    && key_slot != crate::router::key_slot(dest)
                {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                    return false;
                }
                if let Some(by_pat) = by
                    && by_pat.contains(&b'*')
                {
                    let by_slot = crate::router::pattern_hash_slot(by_pat);
                    if by_slot != Some(key_slot) {
                        out.extend_from_slice(
                                b"-ERR BY option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.\r\n",
                            );
                        return false;
                    }
                }
                for get_pat in get {
                    if get_pat.as_ref() != b"#" && get_pat.contains(&b'*') {
                        let get_slot = crate::router::pattern_hash_slot(get_pat);
                        if get_slot != Some(key_slot) {
                            out.extend_from_slice(
                                b"-ERR GET option of SORT denied in Cluster mode when keys formed by the pattern may be in different slots.\r\n",
                            );
                            return false;
                        }
                    }
                }
            }

            // 2. Fetch collection elements
            record_client_read(router.port, client_id, key.as_ref());
            if let Some(dest) = store {
                notify_key_invalidation(router.port, dest.as_ref(), client_id);
            }
            let (key_type, mut items) = match router.get_collection_for_sort(key).await {
                Ok(res) => res,
                Err(err) => {
                    write_resp_err(out, err);
                    return false;
                }
            };

            // 3. Determine sorting mode
            let mut by_pattern = by.clone();
            let mut sort_alpha = alpha;
            let mut dontsort = match &by_pattern {
                Some(p) => !p.contains(&b'*'),
                None => false,
            };

            if dontsort && key_type == "set" {
                dontsort = false;
                sort_alpha = true;
                by_pattern = None;
            }

            // 4. Perform sorting if not dontsort
            if !dontsort {
                let mut sort_items = Vec::with_capacity(items.len());
                let mut int_conversion_error = false;

                for item in items {
                    let by_val: Option<Bytes> = if let Some(ref pat) = by_pattern {
                        let (lookup_key, field_opt) = pattern_subst_and_field(pat, &item);
                        if let Some(field) = field_opt {
                            router.hget(lookup_key, field).await
                        } else {
                            router.get(lookup_key).await
                        }
                    } else {
                        Some(item.clone())
                    };

                    if sort_alpha {
                        sort_items.push((0.0f64, by_val, item));
                    } else {
                        let score = match by_val {
                            Some(ref s_bytes) => {
                                match std::str::from_utf8(s_bytes.as_ref())
                                    .ok()
                                    .and_then(|s| s.parse::<f64>().ok())
                                {
                                    Some(val) if !val.is_nan() => val,
                                    _ => {
                                        int_conversion_error = true;
                                        0.0
                                    }
                                }
                            }
                            None => 0.0,
                        };
                        sort_items.push((score, None, item));
                    }
                }

                if int_conversion_error {
                    write_resp_err(out, "One or more scores can't be converted into double");
                    return false;
                }

                if sort_alpha {
                    sort_items.sort_by(|a, b| {
                        let cmp = match (&a.1, &b.1) {
                            (Some(v1), Some(v2)) => v1.cmp(v2),
                            (None, Some(_)) => std::cmp::Ordering::Less,
                            (Some(_), None) => std::cmp::Ordering::Greater,
                            (None, None) => std::cmp::Ordering::Equal,
                        };
                        let cmp = if cmp == std::cmp::Ordering::Equal {
                            a.2.cmp(&b.2)
                        } else {
                            cmp
                        };
                        if desc { cmp.reverse() } else { cmp }
                    });
                } else {
                    sort_items.sort_by(|a, b| {
                        let cmp = a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal);
                        let cmp = if cmp == std::cmp::Ordering::Equal {
                            a.2.cmp(&b.2)
                        } else {
                            cmp
                        };
                        if desc { cmp.reverse() } else { cmp }
                    });
                }

                items = sort_items.into_iter().map(|(_, _, item)| item).collect();
            } else if desc {
                items.reverse();
            }

            // 5. Apply LIMIT
            let (start, count) = match limit {
                Some((offset, count)) => {
                    let start = offset.max(0) as usize;
                    let count = if count < 0 {
                        items.len()
                    } else {
                        count as usize
                    };
                    (start, count)
                }
                None => (0, items.len()),
            };

            let sliced_items: Vec<Bytes> = if start >= items.len() {
                Vec::new()
            } else {
                items.into_iter().skip(start).take(count).collect()
            };

            // 6. Perform GET operations
            let mut results: Vec<Option<Bytes>> = Vec::new();
            if get.is_empty() {
                for item in sliced_items {
                    results.push(Some(item));
                }
            } else {
                for item in sliced_items {
                    for get_pat in get {
                        if get_pat.as_ref() == b"#" {
                            results.push(Some(item.clone()));
                        } else {
                            let (lookup_key, field_opt) = pattern_subst_and_field(get_pat, &item);
                            let val = if let Some(field) = field_opt {
                                router.hget(lookup_key, field).await
                            } else {
                                router.get(lookup_key).await
                            };
                            results.push(val);
                        }
                    }
                }
            }

            // 7. STORE or deliver to client
            if let Some(dest) = store {
                let prev_kind = fetch_key_type(router, dest).await;
                if results.is_empty() {
                    router.del(dest.clone()).await;
                    if prev_kind != "none" {
                        notify_keyspace_event_sync(router, NOTIFY_GENERIC, "del", dest.as_ref());
                    }
                    out.extend_from_slice(b":0\r\n");
                } else {
                    let count = results.len();
                    let list_items: Vec<Bytes> = results
                        .into_iter()
                        .map(|opt| opt.unwrap_or_else(|| Bytes::from_static(b"")))
                        .collect();
                    router.del(dest.clone()).await;
                    router.rpush(dest.clone(), list_items).await;
                    notify_set_key_events(router, &prev_kind, "list", dest.as_ref());
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
            } else {
                out.extend_from_slice(format!("*{}\r\n", results.len()).as_bytes());
                for res in results {
                    match res {
                        Some(b) => write_resp_bulk(out, &b),
                        None => write_resp_null(out),
                    }
                }
            }
            false
        }
        Command::Lmove {
            ref source,
            ref destination,
            where_from,
            where_to,
            is_rpoplpush,
        } => {
            let s_target = router.target_shard(source);
            let d_target = router.target_shard(destination);
            if s_target != d_target {
                if router.cluster_enabled {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                } else {
                    let op = CrossShardElementMove::List {
                        from: where_from,
                        to: where_to,
                        is_rpoplpush,
                    };
                    out.extend_from_slice(
                        &move_element_across_shards(router, source, destination, op, in_tx(client_id)).await,
                    );
                }
                return false;
            }
            if s_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof().as_deref(),
                );
            } else {
                let res = router.execute_remote(s_target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Blmove {
            ref source,
            ref destination,
            where_from,
            where_to,
            timeout,
            is_rpoplpush,
        } => {
            let s_target = router.target_shard(source);
            let d_target = router.target_shard(destination);
            if s_target != d_target {
                if router.cluster_enabled {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                } else {
                    return blmove_across_shards(
                        router,
                        client_registry,
                        client_id,
                        source,
                        destination,
                        where_from,
                        where_to,
                        timeout,
                        is_rpoplpush,
                        out,
                    )
                    .await;
                }
                return false;
            }
            let start_len = out.len();
            if s_target == router.shard_id {
                execute_local_command(
                    &Command::Lmove {
                        source: source.clone(),
                        destination: destination.clone(),
                        where_from,
                        where_to,
                        is_rpoplpush,
                    },
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof().as_deref(),
                );
            } else {
                let res = router
                    .execute_remote(
                        s_target,
                        Command::Lmove {
                            source: source.clone(),
                            destination: destination.clone(),
                            where_from,
                            where_to,
                            is_rpoplpush,
                        },
                    )
                    .await;
                out.extend_from_slice(&res);
            }

            if &out[start_len..] != b"$-1\r\n" && &out[start_len..] != b"_\r\n" {
                return false;
            }
            if in_tx(client_id) {
                return false;
            }
            out.truncate(start_len);

            let _guard = BlockedClientGuard {
                port: router.port,
                client_id,
            };
            let (tx, rx) = flume::bounded(1);
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock();
                hub.register_blocked_client(client_id, tx.clone());
                let from_type = match where_from {
                    crate::table::ListDirection::Left => crate::block::ListPopType::Left,
                    crate::table::ListDirection::Right => crate::block::ListPopType::Right,
                };
                let to_type = match where_to {
                    crate::table::ListDirection::Left => crate::block::ListPopType::Left,
                    crate::table::ListDirection::Right => crate::block::ListPopType::Right,
                };
                hub.register_move_waiter(
                    client_id,
                    source.clone(),
                    from_type,
                    to_type,
                    destination.clone(),
                    is_rpoplpush,
                    tx,
                );
            }
            recheck_blocking_keys(router, std::iter::once(source));

            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
            let (recv_res, client_disconnected) =
                wait_for_blocked_result(&rx, timeout, raw_fd).await;
            CURRENT_CLIENT_ID.set(client_id);
            EXECUTING_CLIENT_ID.set(client_id);
            if client_disconnected {
                return true;
            }

            match recv_res {
                Some(crate::block::BlockedListResult::Popped(_, mut vals)) => {
                    // The block hub logged and replicated the move on the
                    // key's shard.
                    if let Some(val) = vals.pop() {
                        write_resp_bulk(out, &val);
                        notify_key_invalidation(router.port, source.as_ref(), client_id);
                        if source != destination {
                            notify_key_invalidation(router.port, destination.as_ref(), client_id);
                        }
                    } else {
                        write_resp_null(out);
                    }
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::WrongType,
                )) => {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::NoPerm,
                )) => {
                    out.extend_from_slice(BLOCKED_NOPERM_REPLY);
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Error,
                )) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Moved(slot),
                )) => {
                    blocked_moved_reply(router, slot, out);
                }
                _ => {
                    write_resp_null(out);
                }
            }
            false
        }
        Command::Lmovem {
            ref source,
            ref destination,
            where_from,
            where_to,
            ..
        } => {
            let s_target = router.target_shard(source);
            let d_target = router.target_shard(destination);
            if s_target != d_target {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            let start_len = out.len();
            if s_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof().as_deref(),
                );
            } else {
                let res = router.execute_remote(s_target, cmd.clone()).await;
                out.extend_from_slice(&res);
            }
            let reply = &out[start_len..];
            if !reply.starts_with(b"-")
                && !reply.starts_with(b"*-1")
                && !reply.starts_with(b"_\r\n")
            {
                let samekey = source == destination;
                let push_event = if where_to == crate::table::ListDirection::Left {
                    "lpush"
                } else {
                    "rpush"
                };
                let pop_event = if where_from == crate::table::ListDirection::Left {
                    "lpop"
                } else {
                    "rpop"
                };
                notify_keyspace_event_sync(router, NOTIFY_LIST, push_event, destination.as_ref());
                notify_keyspace_event_sync(router, NOTIFY_LIST, pop_event, source.as_ref());
                let source_exists = if s_target == router.shard_id {
                    router.local_db.borrow_mut().exists(source.as_ref())
                } else {
                    router.exists(source.clone()).await
                };
                if !samekey && !source_exists {
                    notify_keyspace_event_sync(router, NOTIFY_GENERIC, "del", source.as_ref());
                }
            }
            false
        }
        Command::Blmovem {
            ref source,
            ref destination,
            where_from,
            where_to,
            timeout,
            mode,
            count,
            ordering,
        } => {
            let s_target = router.target_shard(source);
            let d_target = router.target_shard(destination);
            if s_target != d_target {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            let start_len = out.len();
            let non_blocking_cmd = Command::Lmovem {
                source: source.clone(),
                destination: destination.clone(),
                where_from,
                where_to,
                mode,
                count,
                ordering,
                raw_tokens: None,
            };
            if s_target == router.shard_id {
                execute_local_command(
                    &non_blocking_cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof().as_deref(),
                );
            } else {
                let res = router.execute_remote(s_target, non_blocking_cmd).await;
                out.extend_from_slice(&res);
            }

            let reply = &out[start_len..];
            if !reply.starts_with(b"*-1") && !reply.starts_with(b"_\r\n") {
                if !reply.starts_with(b"-") {
                    let samekey = source == destination;
                    let push_event = if where_to == crate::table::ListDirection::Left {
                        "lpush"
                    } else {
                        "rpush"
                    };
                    let pop_event = if where_from == crate::table::ListDirection::Left {
                        "lpop"
                    } else {
                        "rpop"
                    };
                    notify_keyspace_event_sync(
                        router,
                        NOTIFY_LIST,
                        push_event,
                        destination.as_ref(),
                    );
                    notify_keyspace_event_sync(router, NOTIFY_LIST, pop_event, source.as_ref());
                    let source_exists = if s_target == router.shard_id {
                        router.local_db.borrow_mut().exists(source.as_ref())
                    } else {
                        router.exists(source.clone()).await
                    };
                    if !samekey && !source_exists {
                        notify_keyspace_event_sync(router, NOTIFY_GENERIC, "del", source.as_ref());
                    }
                }
                return false;
            }
            if in_tx(client_id) {
                return false;
            }
            out.truncate(start_len);

            let _guard = BlockedClientGuard {
                port: router.port,
                client_id,
            };
            let (tx, rx) = flume::bounded(1);
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock();
                hub.register_blocked_client(client_id, tx.clone());
                let from_type = match where_from {
                    crate::table::ListDirection::Left => crate::block::ListPopType::Left,
                    crate::table::ListDirection::Right => crate::block::ListPopType::Right,
                };
                let to_type = match where_to {
                    crate::table::ListDirection::Left => crate::block::ListPopType::Left,
                    crate::table::ListDirection::Right => crate::block::ListPopType::Right,
                };
                hub.register_movem_waiter(
                    client_id,
                    source.clone(),
                    from_type,
                    to_type,
                    destination.clone(),
                    mode,
                    count,
                    ordering,
                    tx,
                );
            }
            recheck_blocking_keys(router, std::iter::once(source));

            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
            let (recv_res, client_disconnected) =
                wait_for_blocked_result(&rx, timeout, raw_fd).await;
            if client_disconnected {
                return true;
            }

            match recv_res {
                Some(crate::block::BlockedListResult::Popped(_, vals)) => {
                    // The block hub logged and replicated the move on the
                    // key's shard.
                    let samekey = source == destination;
                    let push_event = if where_to == crate::table::ListDirection::Left {
                        "lpush"
                    } else {
                        "rpush"
                    };
                    let pop_event = if where_from == crate::table::ListDirection::Left {
                        "lpop"
                    } else {
                        "rpop"
                    };
                    notify_keyspace_event_sync(
                        router,
                        NOTIFY_LIST,
                        push_event,
                        destination.as_ref(),
                    );
                    notify_keyspace_event_sync(router, NOTIFY_LIST, pop_event, source.as_ref());
                    let source_exists = if s_target == router.shard_id {
                        router.local_db.borrow_mut().exists(source.as_ref())
                    } else {
                        router.exists(source.clone()).await
                    };
                    if !samekey && !source_exists {
                        notify_keyspace_event_sync(router, NOTIFY_GENERIC, "del", source.as_ref());
                    }
                    write_resp_array_header(out, vals.len());
                    for v in &vals {
                        write_resp_bulk(out, v);
                    }
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::WrongType,
                )) => {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::NoPerm,
                )) => {
                    out.extend_from_slice(BLOCKED_NOPERM_REPLY);
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Error,
                )) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Moved(slot),
                )) => {
                    blocked_moved_reply(router, slot, out);
                }
                _ => {
                    write_resp_null_array(out);
                }
            }
            false
        }
        Command::Lmpop {
            keys,
            where_from,
            count,
        } => {
            let mut popped: Option<(Bytes, Vec<Bytes>)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    if !db.exists(k) {
                        continue;
                    }
                    let res = match where_from {
                        crate::table::ListDirection::Left => db.lpop(k, count),
                        crate::table::ListDirection::Right => db.rpop(k, count),
                    };
                    match res {
                        Ok(vals) => {
                            if !vals.is_empty() {
                                let rep_cmd = match where_from {
                                    crate::table::ListDirection::Left => Command::Lpop {
                                        key: k.clone(),
                                        count: Some(vals.len()),
                                    },
                                    crate::table::ListDirection::Right => Command::Rpop {
                                        key: k.clone(),
                                        count: Some(vals.len()),
                                    },
                                };
                                if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                                    if let Some(aof_w) = &router.aof() {
                                        aof_w.borrow_mut().append(&bytes);
                                    }
                                    if crate::replication::has_connected_replicas(router.base_port) {
                                        crate::replication::propagate_shard_bytes(
                                            router.base_port,
                                            router.shard_id,
                                            &bytes,
                                        );
                                    }
                                }
                                popped = Some((k.clone(), vals));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_cmd = match where_from {
                        crate::table::ListDirection::Left => Command::Lpop {
                            key: k.clone(),
                            count: Some(count),
                        },
                        crate::table::ListDirection::Right => Command::Rpop {
                            key: k.clone(),
                            count: Some(count),
                        },
                    };
                    let remote_res = router.execute_remote(target, remote_cmd).await;
                    if remote_res.starts_with(b"-WRONGTYPE") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if remote_res.starts_with(b"*")
                        && !remote_res.starts_with(b"*-1")
                        && !remote_res.starts_with(b"*0")
                        && let Some(vals) = parse_array_from_resp(&remote_res)
                        && !vals.is_empty()
                    {
                        popped = Some((k.clone(), vals));
                        break;
                    }
                }
            }

            if let Some((k, vals)) = popped {
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n*");
                out.extend_from_slice(vals.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for v in &vals {
                    out.extend_from_slice(b"$");
                    out.extend_from_slice(v.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(v);
                    out.extend_from_slice(b"\r\n");
                }
            } else {
                write_resp_null_array(out);
            }
            false
        }
        Command::Blmpop {
            timeout,
            keys,
            where_from,
            count,
        } => {
            let mut popped: Option<(Bytes, Vec<Bytes>)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    if !db.exists(k) {
                        continue;
                    }
                    let res = match where_from {
                        crate::table::ListDirection::Left => db.lpop(k, count),
                        crate::table::ListDirection::Right => db.rpop(k, count),
                    };
                    match res {
                        Ok(vals) => {
                            if !vals.is_empty() {
                                let rep_cmd = match where_from {
                                    crate::table::ListDirection::Left => Command::Lpop {
                                        key: k.clone(),
                                        count: Some(vals.len()),
                                    },
                                    crate::table::ListDirection::Right => Command::Rpop {
                                        key: k.clone(),
                                        count: Some(vals.len()),
                                    },
                                };
                                if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                                    if let Some(aof_w) = &router.aof() {
                                        aof_w.borrow_mut().append(&bytes);
                                    }
                                    if crate::replication::has_connected_replicas(router.base_port) {
                                        crate::replication::propagate_shard_bytes(
                                            router.base_port,
                                            router.shard_id,
                                            &bytes,
                                        );
                                    }
                                }
                                popped = Some((k.clone(), vals));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_cmd = match where_from {
                        crate::table::ListDirection::Left => Command::Lpop {
                            key: k.clone(),
                            count: Some(count),
                        },
                        crate::table::ListDirection::Right => Command::Rpop {
                            key: k.clone(),
                            count: Some(count),
                        },
                    };
                    let remote_res = router.execute_remote(target, remote_cmd).await;
                    if remote_res.starts_with(b"-WRONGTYPE") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if remote_res.starts_with(b"*")
                        && !remote_res.starts_with(b"*-1")
                        && !remote_res.starts_with(b"*0")
                        && let Some(vals) = parse_array_from_resp(&remote_res)
                        && !vals.is_empty()
                    {
                        popped = Some((k.clone(), vals));
                        break;
                    }
                }
            }

            if let Some((k, vals)) = popped {
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n*");
                out.extend_from_slice(vals.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for v in &vals {
                    out.extend_from_slice(b"$");
                    out.extend_from_slice(v.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(v);
                    out.extend_from_slice(b"\r\n");
                }
                return false;
            }

            if in_tx(client_id) {
                write_resp_null_array(out);
                return false;
            }

            let _guard = BlockedClientGuard {
                port: router.port,
                client_id,
            };
            let (tx, rx) = flume::bounded(1);
            let pop_type = match where_from {
                crate::table::ListDirection::Left => crate::block::ListPopType::Left,
                crate::table::ListDirection::Right => crate::block::ListPopType::Right,
            };
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock();
                hub.register_blocked_client(client_id, tx.clone());
                for k in &keys {
                    hub.register_list_waiter(client_id, k.clone(), pop_type, count, tx.clone());
                }
            }
            recheck_blocking_keys(router, keys.iter());

            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
            let (recv_res, client_disconnected) =
                wait_for_blocked_result(&rx, timeout, raw_fd).await;
            if client_disconnected {
                return true;
            }

            match recv_res {
                Some(crate::block::BlockedListResult::Popped(k, vals)) => {
                    // The block hub logged and replicated the pop on the
                    // key's shard.
                    out.extend_from_slice(b"*2\r\n$");
                    out.extend_from_slice(k.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(&k);
                    out.extend_from_slice(b"\r\n*");
                    out.extend_from_slice(vals.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    for v in &vals {
                        out.extend_from_slice(b"$");
                        out.extend_from_slice(v.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(v);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::NoPerm,
                )) => {
                    out.extend_from_slice(BLOCKED_NOPERM_REPLY);
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Error,
                )) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Moved(slot),
                )) => {
                    blocked_moved_reply(router, slot, out);
                }
                _ => {
                    write_resp_null_array(out);
                }
            }
            false
        }
        Command::Bzpopmin { keys, timeout } => {
            handle_bzpop(router, client_id, client_registry, out, keys, timeout, true).await
        }
        Command::Bzpopmax { keys, timeout } => {
            handle_bzpop(
                router,
                client_id,
                client_registry,
                out,
                keys,
                timeout,
                false,
            )
            .await
        }
        Command::Zmpop {
            keys,
            is_min,
            count,
        } => {
            let mut popped: Option<(Bytes, Vec<(Bytes, f64)>)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    if !db.exists(k) {
                        continue;
                    }
                    let res = if is_min {
                        db.zpopmin(k, count)
                    } else {
                        db.zpopmax(k, count)
                    };
                    match res {
                        Ok(items) => {
                            if !items.is_empty() {
                                let rep_cmd = if is_min {
                                    Command::Zpopmin {
                                        key: k.clone(),
                                        count: Some(items.len()),
                                    }
                                } else {
                                    Command::Zpopmax {
                                        key: k.clone(),
                                        count: Some(items.len()),
                                    }
                                };
                                if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                                    if let Some(aof_w) = &router.aof() {
                                        aof_w.borrow_mut().append(&bytes);
                                    }
                                    if crate::replication::has_connected_replicas(router.base_port) {
                                        crate::replication::propagate_shard_bytes(
                                            router.base_port,
                                            router.shard_id,
                                            &bytes,
                                        );
                                    }
                                }
                                popped = Some((k.clone(), items));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_cmd = if is_min {
                        Command::Zpopmin {
                            key: k.clone(),
                            count: Some(count),
                        }
                    } else {
                        Command::Zpopmax {
                            key: k.clone(),
                            count: Some(count),
                        }
                    };
                    let remote_res = router.execute_remote(target, remote_cmd).await;
                    if remote_res.starts_with(b"-WRONGTYPE") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if let Some(items) = parse_zpop_items(&remote_res)
                        && !items.is_empty()
                    {
                        popped = Some((k.clone(), items));
                        break;
                    }
                }
            }

            if let Some((k, items)) = popped {
                format_zmpop_response(out, &k, &items);
            } else {
                write_resp_null_array(out);
            }
            false
        }
        Command::Bzmpop {
            timeout,
            keys,
            is_min,
            count,
        } => {
            let mut popped: Option<(Bytes, Vec<(Bytes, f64)>)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    if !db.exists(k) {
                        continue;
                    }
                    let res = if is_min {
                        db.zpopmin(k, count)
                    } else {
                        db.zpopmax(k, count)
                    };
                    match res {
                        Ok(items) => {
                            if !items.is_empty() {
                                let rep_cmd = if is_min {
                                    Command::Zpopmin {
                                        key: k.clone(),
                                        count: Some(items.len()),
                                    }
                                } else {
                                    Command::Zpopmax {
                                        key: k.clone(),
                                        count: Some(items.len()),
                                    }
                                };
                                if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                                    if let Some(aof_w) = &router.aof() {
                                        aof_w.borrow_mut().append(&bytes);
                                    }
                                    if crate::replication::has_connected_replicas(router.base_port) {
                                        crate::replication::propagate_shard_bytes(
                                            router.base_port,
                                            router.shard_id,
                                            &bytes,
                                        );
                                    }
                                }
                                popped = Some((k.clone(), items));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_cmd = if is_min {
                        Command::Zpopmin {
                            key: k.clone(),
                            count: Some(count),
                        }
                    } else {
                        Command::Zpopmax {
                            key: k.clone(),
                            count: Some(count),
                        }
                    };
                    let remote_res = router.execute_remote(target, remote_cmd).await;
                    if remote_res.starts_with(b"-WRONGTYPE") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if let Some(items) = parse_zpop_items(&remote_res)
                        && !items.is_empty()
                    {
                        popped = Some((k.clone(), items));
                        break;
                    }
                }
            }

            if let Some((k, items)) = popped {
                format_zmpop_response(out, &k, &items);
                return false;
            }

            if in_tx(client_id) {
                write_resp_null_array(out);
                return false;
            }

            let _guard = BlockedClientGuard {
                port: router.port,
                client_id,
            };
            let (tx, rx) = flume::bounded(1);
            let pop_type = if is_min {
                crate::block::ZSetPopType::Min
            } else {
                crate::block::ZSetPopType::Max
            };
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock();
                hub.register_blocked_zset_client(client_id, tx.clone());
                for k in keys.iter() {
                    hub.register_zset_waiter(
                        client_id,
                        k.clone(),
                        pop_type,
                        count,
                        true,
                        tx.clone(),
                    );
                }
            }
            recheck_blocking_keys(router, keys.iter());

            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
            let (recv_res, client_disconnected) =
                wait_for_blocked_result(&rx, timeout, raw_fd).await;
            if client_disconnected {
                return true;
            }

            match recv_res {
                Some(crate::block::BlockedZSetResult::Popped { key, items, .. }) => {
                    // The block hub logged and replicated the pop on the
                    // key's shard.
                    format_zmpop_response(out, &key, &items);
                }
                Some(crate::block::BlockedZSetResult::Unblocked(
                    crate::block::ClientUnblockType::NoPerm,
                )) => {
                    out.extend_from_slice(BLOCKED_NOPERM_REPLY);
                }
                Some(crate::block::BlockedZSetResult::Unblocked(
                    crate::block::ClientUnblockType::Error,
                )) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                }
                Some(crate::block::BlockedZSetResult::Unblocked(
                    crate::block::ClientUnblockType::Moved(slot),
                )) => {
                    blocked_moved_reply(router, slot, out);
                }
                _ => {
                    write_resp_null_array(out);
                }
            }
            false
        }
        Command::Hello {
            proto,
            ref auth,
            ref setname,
        } => {
            if let Some(p) = proto
                && p != 2
                && p != 3
            {
                out.extend_from_slice(b"-NOPROTO unsupported protocol version\r\n");
                return false;
            }

            let acl = crate::acl::get_acl_for_port(router.port);
            let default_requires_auth = acl.read().is_auth_required_for_default();

            if let Some((uname, pass)) = auth {
                if let Ok(user) = acl.read().check_auth(Some(uname), pass) {
                    *authenticated = true;
                    *auth_user = user.clone();
                    update_global_client_auth(router.port, client_id, &user);
                } else {
                    record_acl_denial(
                        router.port,
                        crate::acl::AclDeniedReason::Auth,
                        "toplevel",
                        "AUTH",
                        uname,
                        "hello",
                    );
                    out.extend_from_slice(
                        b"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
                    );
                    return false;
                }
            } else if !*authenticated && default_requires_auth {
                out.extend_from_slice(
                    b"-NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and set the protocol.\r\n",
                );
                return false;
            }

            if let Some(name) = setname {
                if name.bytes().any(|b| !(b'!'..=b'~').contains(&b)) {
                    out.extend_from_slice(
                        b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n",
                    );
                    return false;
                }
                if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                    c.name = Some(name.clone());
                }
            }

            let cur_resp3 = client_registry
                .borrow()
                .get(&client_id)
                .map(|c| c.is_resp3)
                .unwrap_or(false);
            let proto_ver = proto.unwrap_or(if cur_resp3 { 3 } else { 2 });
            if proto_ver == 3 {
                if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                    c.is_resp3 = true;
                }
                CURRENT_CLIENT_RESP3.set(true);
                update_global_client_resp3(router.port, client_id, true);
                out.extend_from_slice(b"%7\r\n");
            } else {
                if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                    c.is_resp3 = false;
                }
                CURRENT_CLIENT_RESP3.set(false);
                update_global_client_resp3(router.port, client_id, false);
                out.extend_from_slice(b"*14\r\n");
            }
            out.extend_from_slice(b"$6\r\nserver\r\n$6\r\nvalkey\r\n");
            out.extend_from_slice(b"$7\r\nversion\r\n$5\r\n7.2.0\r\n");
            out.extend_from_slice(format!("$5\r\nproto\r\n:{}\r\n", proto_ver).as_bytes());
            out.extend_from_slice(format!("$2\r\nid\r\n:{}\r\n", client_id).as_bytes());

            let is_cluster = router.cluster_enabled
                || crate::cluster::get_cluster_hub(router.base_port)
                    .cluster_enabled
                    .load(std::sync::atomic::Ordering::Relaxed);
            if is_cluster {
                out.extend_from_slice(b"$4\r\nmode\r\n$7\r\ncluster\r\n");
            } else {
                out.extend_from_slice(b"$4\r\nmode\r\n$10\r\nstandalone\r\n");
            }

            let is_replica = crate::replication::HAS_SLAVE_INSTANCE
                .load(std::sync::atomic::Ordering::Relaxed)
                && crate::replication::cached_hub(router.base_port).is_slave();
            if is_replica {
                out.extend_from_slice(b"$4\r\nrole\r\n$7\r\nreplica\r\n");
            } else {
                out.extend_from_slice(b"$4\r\nrole\r\n$6\r\nmaster\r\n");
            }

            out.extend_from_slice(b"$7\r\nmodules\r\n*0\r\n");
            false
        }
        Command::Reset => {
            if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                c.himport_fieldsets.clear();
                c.name = None;
                c.is_resp3 = client_default_resp3();
                c.db = 0;
                c.is_monitor = false;
                c.reply_mode = crate::resp::ClientReplyMode::On;
            }
            CURRENT_CLIENT_RESP3.set(client_default_resp3());
            CURRENT_DB.set(0);
            router.local_db.borrow_mut().select_db(0);
            update_global_client_resp3(router.port, client_id, client_default_resp3());
            update_global_client_auth(router.port, client_id, "default");
            unregister_client_tracking(router.port, client_id);
            unregister_monitor_client(router.port, client_id);
            *asking = false;
            reset_client_pubsub(router, client_id);

            let acl = crate::acl::get_acl_for_port(router.port);
            let default_requires_auth = acl.read().is_auth_required_for_default();
            *authenticated = !default_requires_auth;
            *auth_user = "default".to_string();
            CLIENT_NO_TOUCH.set(false);
            out.extend_from_slice(b"+RESET\r\n");
            false
        }
        Command::Time => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let secs = now.as_secs();
            let micros = now.subsec_micros();
            out.extend_from_slice(
                format!(
                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    secs.to_string().len(),
                    secs,
                    micros.to_string().len(),
                    micros
                )
                .as_bytes(),
            );
            false
        }
        Command::Echo(ref msg) => {
            write_resp_bulk(out, msg);
            false
        }
        Command::Zrangestore {
            ref dst,
            ref src,
            ref opts,
        } => {
            let src_target = router.target_shard(src);
            let dst_target = router.target_shard(dst);
            if src_target == dst_target {
                if src_target == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof().as_deref(),
                    );
                } else {
                    let res = router.execute_remote(src_target, cmd).await;
                    out.extend_from_slice(&res);
                }
                return false;
            }
            if router.cluster_enabled
                && crate::router::key_slot(src) != crate::router::key_slot(dst)
            {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            let mut zrange_opts = opts.clone();
            zrange_opts.with_scores = true;
            let zrange_cmd = Command::Zrange {
                key: src.clone(),
                opts: *zrange_opts,
            };
            let raw = if src_target == router.shard_id {
                let mut tmp = Vec::new();
                execute_local_command(
                    &zrange_cmd,
                    &mut router.local_db.borrow_mut(),
                    &mut tmp,
                    None,
                );
                tmp
            } else {
                router.execute_remote(src_target, zrange_cmd).await
            };
            if raw.starts_with(b"-") {
                out.extend_from_slice(&raw);
                return false;
            }
            let mut items = Vec::new();
            let mut pos = 0usize;
            if pos < raw.len()
                && raw[pos] == b'*'
                && let Some(nl) = raw[pos..].iter().position(|&b| b == b'\n')
            {
                pos += nl + 1;
                while pos < raw.len() {
                    if raw[pos] != b'$' {
                        break;
                    }
                    let Some(len_nl) = raw[pos..].iter().position(|&b| b == b'\n') else {
                        break;
                    };
                    let len_str =
                        std::str::from_utf8(&raw[pos + 1..pos + len_nl - 1]).unwrap_or("0");
                    let bulk_len: usize = len_str.parse().unwrap_or(0);
                    pos += len_nl + 1;
                    if pos + bulk_len + 2 > raw.len() {
                        break;
                    }
                    let val = bytes::Bytes::copy_from_slice(&raw[pos..pos + bulk_len]);
                    pos += bulk_len + 2;
                    items.push(val);
                }
            }
            let del_cmd = Command::Del(smallvec::smallvec![dst.clone()]);
            if dst_target == router.shard_id {
                let mut tmp = Vec::new();
                execute_local_command(
                    &del_cmd,
                    &mut router.local_db.borrow_mut(),
                    &mut tmp,
                    router.aof().as_deref(),
                );
            } else {
                let _ = router.execute_remote(dst_target, del_cmd).await;
            }
            let count = items.len() / 2;
            if count > 0 {
                let mut elements = Vec::with_capacity(count);
                for pair in items.as_chunks::<2>().0 {
                    let score: f64 = std::str::from_utf8(&pair[1])
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    elements.push((score, pair[0].clone()));
                }
                let zadd_cmd = Command::Zadd {
                    key: dst.clone(),
                    elements: elements.into(),
                    flags: crate::table::ZAddFlags::default(),
                };
                if dst_target == router.shard_id {
                    let mut tmp = Vec::new();
                    execute_local_command(
                        &zadd_cmd,
                        &mut router.local_db.borrow_mut(),
                        &mut tmp,
                        router.aof().as_deref(),
                    );
                } else {
                    let _ = router.execute_remote(dst_target, zadd_cmd).await;
                }
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::Sintercard { ref keys, limit } | Command::Zintercard { ref keys, limit } => {
            if keys.is_empty() {
                write_resp_integer(out, 0);
                return false;
            }
            let first_target = router.target_shard(&keys[0]);
            let all_same_shard = keys[1..]
                .iter()
                .all(|k| router.target_shard(k) == first_target);
            if all_same_shard {
                if first_target == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof().as_deref(),
                    );
                } else {
                    let res = router.execute_remote(first_target, cmd).await;
                    out.extend_from_slice(&res);
                }
                return false;
            }
            if router.cluster_enabled
                && keys[1..]
                    .iter()
                    .any(|k| crate::router::key_slot(k) != crate::router::key_slot(&keys[0]))
            {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if matches!(cmd, Command::Zintercard { .. }) {
                let mut member_sets: Vec<hashbrown::HashSet<Bytes>> =
                    Vec::with_capacity(keys.len());
                for k in keys {
                    match fetch_remote_zset(router, k).await {
                        Ok(map) => member_sets.push(map.into_keys().collect()),
                        Err(e) => {
                            out.extend_from_slice(&e);
                            return false;
                        }
                    }
                }
                if member_sets.iter().any(|s| s.is_empty()) {
                    write_resp_integer(out, 0);
                    return false;
                }
                member_sets.sort_unstable_by_key(|s| s.len());
                let first = &member_sets[0];
                let rest = &member_sets[1..];
                let mut count = 0usize;
                for m in first {
                    if rest.iter().all(|s| s.contains(m)) {
                        count += 1;
                        if limit > 0 && count >= limit {
                            count = limit;
                            break;
                        }
                    }
                }
                write_resp_integer(out, count as i64);
            } else {
                let mut member_sets: Vec<hashbrown::HashSet<Bytes>> =
                    Vec::with_capacity(keys.len());
                for k in keys {
                    match fetch_remote_set(router, k).await {
                        Ok(s) => member_sets.push(s),
                        Err(e) => {
                            out.extend_from_slice(&e);
                            return false;
                        }
                    }
                }
                if member_sets.iter().any(|s| s.is_empty()) {
                    write_resp_integer(out, 0);
                    return false;
                }
                member_sets.sort_unstable_by_key(|s| s.len());
                let first = &member_sets[0];
                let rest = &member_sets[1..];
                let mut count = 0usize;
                for m in first {
                    if rest.iter().all(|s| s.contains(m)) {
                        count += 1;
                        if limit > 0 && count >= limit {
                            count = limit;
                            break;
                        }
                    }
                }
                write_resp_integer(out, count as i64);
            }
            false
        }
        Command::Sinter(ref keys)
        | Command::Sunion(ref keys)
        | Command::Sdiff(ref keys)
        | Command::Sunioncard { ref keys, .. }
        | Command::Sdiffcard { ref keys, .. } => {
            if keys.is_empty() {
                if matches!(cmd, Command::Sunioncard { .. } | Command::Sdiffcard { .. }) {
                    write_resp_integer(out, 0);
                } else {
                    out.extend_from_slice(b"*0\r\n");
                }
                return false;
            }
            let first_target = router.target_shard(&keys[0]);
            let all_same = keys[1..]
                .iter()
                .all(|k| router.target_shard(k) == first_target);
            if all_same {
                if first_target == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof().as_deref(),
                    );
                } else {
                    let res = router.execute_remote(first_target, cmd).await;
                    out.extend_from_slice(&res);
                }
                return false;
            }
            if router.cluster_enabled
                && keys[1..]
                    .iter()
                    .any(|k| crate::router::key_slot(k) != crate::router::key_slot(&keys[0]))
            {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            let mut sets = Vec::with_capacity(keys.len());
            for k in keys {
                match fetch_remote_set(router, k).await {
                    Ok(s) => sets.push(s),
                    Err(e) => {
                        out.extend_from_slice(&e);
                        return false;
                    }
                }
            }
            match cmd {
                Command::Sinter(_) => {
                    if sets.iter().any(|s| s.is_empty()) {
                        write_resp_set_len(out, 0);
                        return false;
                    }
                    sets.sort_unstable_by_key(|s| s.len());
                    let first = &sets[0];
                    let rest = &sets[1..];
                    let mut res = Vec::new();
                    for m in first {
                        if rest.iter().all(|s| s.contains(m)) {
                            res.push(m.clone());
                        }
                    }
                    write_resp_set_len(out, res.len());
                    for m in res {
                        write_resp_bulk(out, &m);
                    }
                }
                Command::Sunion(_) => {
                    let mut union_set = hashbrown::HashSet::new();
                    for s in sets {
                        for m in s {
                            union_set.insert(m);
                        }
                    }
                    write_resp_set_len(out, union_set.len());
                    for m in union_set {
                        write_resp_bulk(out, &m);
                    }
                }
                Command::Sdiff(_) => {
                    let first = sets.remove(0);
                    let mut diff = Vec::new();
                    for m in first {
                        if !sets.iter().any(|s| s.contains(&m)) {
                            diff.push(m);
                        }
                    }
                    write_resp_set_len(out, diff.len());
                    for m in diff {
                        write_resp_bulk(out, &m);
                    }
                }
                Command::Sunioncard { limit, .. } => {
                    let mut union_set = hashbrown::HashSet::new();
                    for s in sets {
                        for m in s {
                            union_set.insert(m);
                            if limit > 0 && union_set.len() >= limit {
                                break;
                            }
                        }
                        if limit > 0 && union_set.len() >= limit {
                            break;
                        }
                    }
                    let count = if limit > 0 {
                        union_set.len().min(limit)
                    } else {
                        union_set.len()
                    };
                    write_resp_integer(out, count as i64);
                }
                Command::Sdiffcard { limit, .. } => {
                    let first = sets.remove(0);
                    let mut count = 0;
                    for m in first {
                        if !sets.iter().any(|s| s.contains(&m)) {
                            count += 1;
                            if limit > 0 && count >= limit {
                                break;
                            }
                        }
                    }
                    write_resp_integer(out, count as i64);
                }
                _ => {}
            }
            false
        }
        Command::Sinterstore {
            ref destination,
            ref keys,
        }
        | Command::Sunionstore {
            ref destination,
            ref keys,
        }
        | Command::Sdiffstore {
            ref destination,
            ref keys,
        } => {
            let dest_target = router.target_shard(destination);
            let all_same = keys.iter().all(|k| router.target_shard(k) == dest_target);
            if all_same {
                if dest_target == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof().as_deref(),
                    );
                } else {
                    let res = router.execute_remote(dest_target, cmd).await;
                    out.extend_from_slice(&res);
                }
                return false;
            }
            if router.cluster_enabled
                && keys
                    .iter()
                    .any(|k| crate::router::key_slot(k) != crate::router::key_slot(destination))
            {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            let mut sets = Vec::with_capacity(keys.len());
            for k in keys {
                match fetch_remote_set(router, k).await {
                    Ok(s) => sets.push(s),
                    Err(e) => {
                        out.extend_from_slice(&e);
                        return false;
                    }
                }
            }
            let members: Vec<Bytes> = if sets.is_empty() {
                Vec::new()
            } else {
                match cmd {
                    Command::Sinterstore { .. } => {
                        if sets.iter().any(|s| s.is_empty()) {
                            Vec::new()
                        } else {
                            sets.sort_unstable_by_key(|s| s.len());
                            let first = &sets[0];
                            let rest = &sets[1..];
                            first
                                .iter()
                                .filter(|m| rest.iter().all(|s| s.contains(*m)))
                                .cloned()
                                .collect()
                        }
                    }
                    Command::Sunionstore { .. } => {
                        let mut union_set = hashbrown::HashSet::new();
                        for s in sets {
                            for m in s {
                                union_set.insert(m);
                            }
                        }
                        union_set.into_iter().collect()
                    }
                    Command::Sdiffstore { .. } => {
                        let first = sets.remove(0);
                        first
                            .into_iter()
                            .filter(|m| !sets.iter().any(|s| s.contains(m)))
                            .collect()
                    }
                    _ => Vec::new(),
                }
            };
            let del_cmd = Command::Del(smallvec::smallvec![destination.clone()]);
            if dest_target == router.shard_id {
                let mut tmp = Vec::new();
                execute_local_command(
                    &del_cmd,
                    &mut router.local_db.borrow_mut(),
                    &mut tmp,
                    router.aof().as_deref(),
                );
            } else {
                let _ = router.execute_remote(dest_target, del_cmd).await;
            }
            let count = members.len();
            if count > 0 {
                let sadd_cmd = Command::Sadd {
                    key: destination.clone(),
                    members: members.into(),
                };
                if dest_target == router.shard_id {
                    let mut tmp = Vec::new();
                    execute_local_command(
                        &sadd_cmd,
                        &mut router.local_db.borrow_mut(),
                        &mut tmp,
                        router.aof().as_deref(),
                    );
                } else {
                    let _ = router.execute_remote(dest_target, sadd_cmd).await;
                }
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::Zdiff {
            ref keys,
            with_scores,
        }
        | Command::Zinter {
            ref keys,
            with_scores,
            ..
        }
        | Command::Zunion {
            ref keys,
            with_scores,
            ..
        } => {
            if keys.is_empty() {
                out.extend_from_slice(b"*0\r\n");
                return false;
            }
            let first_target = router.target_shard(&keys[0]);
            let all_same = keys[1..]
                .iter()
                .all(|k| router.target_shard(k) == first_target);
            if all_same {
                if first_target == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof().as_deref(),
                    );
                } else {
                    let res = router.execute_remote(first_target, cmd).await;
                    out.extend_from_slice(&res);
                }
                return false;
            }
            if router.cluster_enabled
                && keys[1..]
                    .iter()
                    .any(|k| crate::router::key_slot(k) != crate::router::key_slot(&keys[0]))
            {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            let mut maps = Vec::with_capacity(keys.len());
            for k in keys {
                match fetch_remote_zset(router, k).await {
                    Ok(m) => maps.push(m),
                    Err(e) => {
                        out.extend_from_slice(&e);
                        return false;
                    }
                }
            }
            let mut results: Vec<(Bytes, f64)> = match &cmd {
                Command::Zdiff { .. } => {
                    let first = maps.remove(0);
                    first
                        .into_iter()
                        .filter(|(m, _)| !maps.iter().any(|other| other.contains_key(m)))
                        .collect()
                }
                Command::Zinter {
                    weights, aggregate, ..
                } => {
                    if maps.iter().any(|m| m.is_empty()) {
                        Vec::new()
                    } else {
                        let mut acc: hashbrown::HashMap<Bytes, f64> = hashbrown::HashMap::new();
                        for (i, m) in maps.into_iter().enumerate() {
                            let w = weights.get(i).copied().unwrap_or(1.0);
                            if i == 0 {
                                for (k, s) in m {
                                    let val = s * w;
                                    acc.insert(k, if val.is_nan() { 0.0 } else { val });
                                }
                            } else {
                                acc.retain(|k, cur| {
                                    if let Some(other_score) = m.get(k) {
                                        let val = *other_score * w;
                                        *cur = match aggregate {
                                            crate::table::Aggregate::Sum
                                            | crate::table::Aggregate::Count => {
                                                let r = *cur + val;
                                                if r.is_nan() { 0.0 } else { r }
                                            }
                                            crate::table::Aggregate::Min => cur.min(val),
                                            crate::table::Aggregate::Max => cur.max(val),
                                        };
                                        true
                                    } else {
                                        false
                                    }
                                });
                            }
                        }
                        acc.into_iter().collect()
                    }
                }
                Command::Zunion {
                    weights, aggregate, ..
                } => {
                    let mut acc: hashbrown::HashMap<Bytes, f64> = hashbrown::HashMap::new();
                    for (i, m) in maps.into_iter().enumerate() {
                        let w = weights.get(i).copied().unwrap_or(1.0);
                        for (k, s) in m {
                            let val = s * w;
                            acc.entry(k)
                                .and_modify(|cur| {
                                    *cur = match aggregate {
                                        crate::table::Aggregate::Sum
                                        | crate::table::Aggregate::Count => {
                                            let r = *cur + val;
                                            if r.is_nan() { 0.0 } else { r }
                                        }
                                        crate::table::Aggregate::Min => cur.min(val),
                                        crate::table::Aggregate::Max => cur.max(val),
                                    };
                                })
                                .or_insert(if val.is_nan() { 0.0 } else { val });
                        }
                    }
                    acc.into_iter().collect()
                }
                _ => Vec::new(),
            };
            results.sort_by(|(m1, s1), (m2, s2)| {
                s1.partial_cmp(s2)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| m1.cmp(m2))
            });
            if with_scores && CURRENT_CLIENT_RESP3.get() {
                // RESP3: [member, score] pairs, as ZRANGE WITHSCORES.
                write_resp_array_header(out, results.len());
                for (m, s) in results {
                    out.extend_from_slice(b"*2\r\n");
                    write_resp_bulk(out, &m);
                    write_resp_score(out, s);
                }
            } else if with_scores {
                write_resp_array_header(out, results.len() * 2);
                for (m, s) in results {
                    write_resp_bulk(out, &m);
                    write_resp_score(out, s);
                }
            } else {
                write_resp_array_header(out, results.len());
                for (m, _) in results {
                    write_resp_bulk(out, &m);
                }
            }
            false
        }
        Command::Zunionstore { .. } | Command::Zinterstore { .. } | Command::Zdiffstore { .. } => {
            let (destination, keys) = match &cmd {
                Command::Zunionstore {
                    destination, keys, ..
                }
                | Command::Zinterstore {
                    destination, keys, ..
                }
                | Command::Zdiffstore { destination, keys } => (destination, keys),
                _ => unreachable!(),
            };
            let dest_target = router.target_shard(destination);
            let all_same = keys.iter().all(|k| router.target_shard(k) == dest_target);
            if all_same {
                if dest_target == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof().as_deref(),
                    );
                } else {
                    let res = router.execute_remote(dest_target, cmd).await;
                    out.extend_from_slice(&res);
                }
                return false;
            }
            if router.cluster_enabled
                && keys
                    .iter()
                    .any(|k| crate::router::key_slot(k) != crate::router::key_slot(destination))
            {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            let mut maps = Vec::with_capacity(keys.len());
            for k in keys {
                match fetch_remote_zset(router, k).await {
                    Ok(m) => maps.push(m),
                    Err(e) => {
                        out.extend_from_slice(&e);
                        return false;
                    }
                }
            }
            let elements: Vec<(f64, Bytes)> = match &cmd {
                Command::Zdiffstore { .. } => {
                    let first = maps.remove(0);
                    first
                        .into_iter()
                        .filter(|(m, _)| !maps.iter().any(|other| other.contains_key(m)))
                        .map(|(m, s)| (s, m))
                        .collect()
                }
                Command::Zinterstore {
                    weights, aggregate, ..
                } => {
                    if maps.iter().any(|m| m.is_empty()) {
                        Vec::new()
                    } else {
                        let mut acc: hashbrown::HashMap<Bytes, f64> = hashbrown::HashMap::new();
                        for (i, m) in maps.into_iter().enumerate() {
                            let w = weights.get(i).copied().unwrap_or(1.0);
                            if i == 0 {
                                for (k, s) in m {
                                    let val = s * w;
                                    acc.insert(k, if val.is_nan() { 0.0 } else { val });
                                }
                            } else {
                                acc.retain(|k, cur| {
                                    if let Some(other_score) = m.get(k) {
                                        let val = *other_score * w;
                                        *cur = match aggregate {
                                            crate::table::Aggregate::Sum
                                            | crate::table::Aggregate::Count => {
                                                let r = *cur + val;
                                                if r.is_nan() { 0.0 } else { r }
                                            }
                                            crate::table::Aggregate::Min => cur.min(val),
                                            crate::table::Aggregate::Max => cur.max(val),
                                        };
                                        true
                                    } else {
                                        false
                                    }
                                });
                            }
                        }
                        acc.into_iter().map(|(m, s)| (s, m)).collect()
                    }
                }
                Command::Zunionstore {
                    weights, aggregate, ..
                } => {
                    let mut acc: hashbrown::HashMap<Bytes, f64> = hashbrown::HashMap::new();
                    for (i, m) in maps.into_iter().enumerate() {
                        let w = weights.get(i).copied().unwrap_or(1.0);
                        for (k, s) in m {
                            let val = s * w;
                            acc.entry(k)
                                .and_modify(|cur| {
                                    *cur = match aggregate {
                                        crate::table::Aggregate::Sum
                                        | crate::table::Aggregate::Count => {
                                            let r = *cur + val;
                                            if r.is_nan() { 0.0 } else { r }
                                        }
                                        crate::table::Aggregate::Min => cur.min(val),
                                        crate::table::Aggregate::Max => cur.max(val),
                                    };
                                })
                                .or_insert(if val.is_nan() { 0.0 } else { val });
                        }
                    }
                    acc.into_iter().map(|(m, s)| (s, m)).collect()
                }
                _ => Vec::new(),
            };
            let del_cmd = Command::Del(smallvec::smallvec![destination.clone()]);
            if dest_target == router.shard_id {
                let mut tmp = Vec::new();
                execute_local_command(
                    &del_cmd,
                    &mut router.local_db.borrow_mut(),
                    &mut tmp,
                    router.aof().as_deref(),
                );
            } else {
                let _ = router.execute_remote(dest_target, del_cmd).await;
            }
            let count = elements.len();
            if count > 0 {
                let zadd_cmd = Command::Zadd {
                    key: destination.clone(),
                    elements: elements.into(),
                    flags: crate::table::ZAddFlags::default(),
                };
                if dest_target == router.shard_id {
                    let mut tmp = Vec::new();
                    execute_local_command(
                        &zadd_cmd,
                        &mut router.local_db.borrow_mut(),
                        &mut tmp,
                        router.aof().as_deref(),
                    );
                } else {
                    let _ = router.execute_remote(dest_target, zadd_cmd).await;
                }
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::KeyspaceStats => {
            let (keys, expires) = router.keyspace_stats().await;
            out.extend_from_slice(format!("*2\r\n:{}\r\n:{}\r\n", keys, expires).as_bytes());
            false
        }
        Command::Dbsize => {
            let count = router.dbsize().await;
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::Select(idx) => {
            let is_cluster = router.cluster_enabled
                || crate::cluster::get_cluster_hub(router.base_port)
                    .cluster_enabled
                    .load(std::sync::atomic::Ordering::Relaxed);
            if is_cluster && idx != 0 {
                out.extend_from_slice(b"-ERR SELECT is not allowed in cluster mode\r\n");
            } else if idx as usize >= crate::config::databases_count() {
                out.extend_from_slice(b"-ERR DB index is out of range\r\n");
            } else {
                let db_idx = idx as usize;
                CURRENT_DB.set(db_idx);
                router.local_db.borrow_mut().select_db(db_idx);
                if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                    c.db = db_idx;
                }
                out.extend_from_slice(b"+OK\r\n");
            }
            false
        }
        Command::Slowlog(args) => {
            let sub = if !args.is_empty() {
                args[0].to_ascii_lowercase()
            } else {
                b"get".to_vec()
            };
            if sub == b"reset" {
                crate::slowlog::slowlog_reset();
                out.extend_from_slice(b"+OK\r\n");
            } else if sub == b"len" {
                let len = crate::slowlog::slowlog_len();
                out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
            } else if sub == b"help" {
                let help_lines = [
                    "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "GET [<count>]",
                    "    Return top <count> entries from the slowlog (default: 10, -1 for all).",
                    "LEN",
                    "    Return the length of the slowlog.",
                    "RESET",
                    "    Reset the slowlog, clearing all entries.",
                    "HELP",
                    "    Return top-level help about SLOWLOG subcommands.",
                ];
                out.extend_from_slice(format!("*{}\r\n", help_lines.len()).as_bytes());
                for line in help_lines {
                    out.extend_from_slice(format!("${}\r\n{}\r\n", line.len(), line).as_bytes());
                }
            } else {
                let count = if args.len() > 1 {
                    match std::str::from_utf8(&args[1])
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        Some(c) if c < -1 => {
                            out.extend_from_slice(
                                b"-ERR count should be greater than or equal to -1\r\n",
                            );
                            return false;
                        }
                        Some(c) => Some(c),
                        None => None,
                    }
                } else {
                    None
                };
                let entries = crate::slowlog::slowlog_get(count);
                crate::slowlog::write_slowlog_entries_resp(&entries, out);
            }
            false
        }
        Command::Commandlog(args) => {
            crate::slowlog::handle_commandlog(&args, out);
            false
        }
        Command::Lolwut(args) => {
            crate::slowlog::handle_lolwut(&args, out);
            false
        }
        Command::Sentinel(args) => {
            crate::slowlog::handle_sentinel(&args, out);
            false
        }
        Command::Flushdb | Command::Flushall => {
            let is_all = matches!(cmd, Command::Flushall);
            let cur_db = CURRENT_DB.get();
            // The slots a migration moves lose their keys: it stops.
            crate::asm::cancel_on_flush(router.base_port);
            if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                let watched_keys_for_port: Vec<(usize, Bytes)> = {
                    let map = WATCHED_KEYS.read();
                    map.get(&router.port)
                        .map(|m| {
                            m.keys()
                                .filter(|(w_db, _)| is_all || *w_db == cur_db)
                                .cloned()
                                .collect()
                        })
                        .unwrap_or_default()
                };
                for (w_db, k) in watched_keys_for_port {
                    CURRENT_DB.set(w_db);
                    router.local_db.borrow_mut().select_db(w_db);
                    let ex = router.exists(k.clone()).await;
                    CURRENT_DB.set(cur_db);
                    router.local_db.borrow_mut().select_db(cur_db);
                    if ex {
                        touch_watched_key_in_db(router.port, w_db, k.as_ref());
                    }
                }
            }
            if !in_tx(client_id) {
                let n = if is_all {
                    router
                        .keyspace_stats_by_db()
                        .await
                        .into_values()
                        .map(|(k, _, _)| k)
                        .sum()
                } else {
                    router.dbsize().await
                };
                if n > 0 {
                    crate::table::add_lazyfreed_objects(n as u64);
                }
            }
            if is_all {
                router.flushall().await;
                // Like Redis killing its BGSAVE child, FLUSHALL aborts any
                // in-progress BGSAVE and clears the synthetic delay flag.
                RDB_BGSAVE_ABORTED.store(true, std::sync::atomic::Ordering::SeqCst);
                RDB_BGSAVE_IN_PROGRESS.store(false, std::sync::atomic::Ordering::Relaxed);
                save_empty_rdb_after_flushall(router).await;
            } else {
                router.flushdb().await;
            }
            notify_flush_invalidation(router.port, client_id);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Swapdb { db1, db2 } => {
            let is_cluster = router.cluster_enabled
                || crate::cluster::get_cluster_hub(router.base_port)
                    .cluster_enabled
                    .load(std::sync::atomic::Ordering::Relaxed);
            let max_db = crate::config::databases_count();
            if is_cluster {
                out.extend_from_slice(b"-ERR SWAPDB is not allowed in cluster mode\r\n");
            } else if db1 < 0 || db2 < 0 || db1 as usize >= max_db || db2 as usize >= max_db {
                out.extend_from_slice(b"-ERR DB index is out of range\r\n");
            } else {
                let (d1, d2) = (db1 as usize, db2 as usize);
                touch_live_watched_keys_on_swapdb(router, d1, d2).await;
                router.swapdb(d1, d2).await;
                notify_flush_invalidation(router.port, client_id);
                out.extend_from_slice(b"+OK\r\n");
            }
            false
        }
        Command::Move { ref key, .. } => {
            let is_cluster = router.cluster_enabled
                || crate::cluster::get_cluster_hub(router.base_port)
                    .cluster_enabled
                    .load(std::sync::atomic::Ordering::Relaxed);
            if is_cluster {
                out.extend_from_slice(b"-ERR MOVE is not allowed in cluster mode\r\n");
                return false;
            }
            let target = router.target_shard(key);
            if target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof().as_deref(),
                );
            } else {
                let res = router.execute_remote(target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Touch(keys) => {
            let (local, remote) = router.group_keys_by_shard(&keys);
            let mut count = if local.is_empty() {
                0
            } else {
                router.local_db.borrow_mut().touch(&local)
            };
            let remote = remote
                .into_iter()
                .map(|(shard, ks)| (shard, Command::Touch(smallvec::SmallVec::from_vec(ks))))
                .collect();
            count += sum_remote_counts(router, remote).await;
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::Rename {
            ref key,
            ref newkey,
            nx,
        } => {
            let target_src = router.target_shard(key);
            let target_dst = router.target_shard(newkey);
            if target_src != target_dst {
                if router.cluster_enabled {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                } else {
                    let mode = if nx {
                        CrossShardMove::RenameNx
                    } else {
                        CrossShardMove::Rename
                    };
                    out.extend_from_slice(
                        &move_key_across_shards(
                            router,
                            key,
                            newkey,
                            CURRENT_DB.get(),
                            mode,
                            in_tx(client_id),
                        )
                        .await,
                    );
                }
                return false;
            }
            if target_src == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof().as_deref(),
                );
            } else {
                let res = router.execute_remote(target_src, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Copy {
            ref source,
            ref destination,
            destination_db,
            replace,
        } => {
            let is_cluster = router.cluster_enabled
                || crate::cluster::get_cluster_hub(router.base_port)
                    .cluster_enabled
                    .load(std::sync::atomic::Ordering::Relaxed);
            if is_cluster
                && (CURRENT_DB.get() != 0 || destination_db.is_some_and(|d| d != 0))
            {
                out.extend_from_slice(
                    b"-ERR Copying to another database is not allowed in cluster mode\r\n",
                );
                return false;
            }
            if let Some(d) = destination_db
                && d as usize >= crate::config::databases_count()
            {
                out.extend_from_slice(b"-ERR DB index is out of range\r\n");
                return false;
            }
            let target_src = router.target_shard(source);
            let target_dst = router.target_shard(destination);
            if target_src != target_dst {
                if router.cluster_enabled {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                } else {
                    let dst_db = destination_db
                        .map(|d| d as usize)
                        .unwrap_or_else(|| CURRENT_DB.get());
                    let mode = if replace {
                        CrossShardMove::CopyReplace
                    } else {
                        CrossShardMove::Copy
                    };
                    out.extend_from_slice(
                        &move_key_across_shards(
                            router,
                            source,
                            destination,
                            dst_db,
                            mode,
                            in_tx(client_id),
                        )
                        .await,
                    );
                }
                return false;
            }
            if target_src == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof().as_deref(),
                );
            } else {
                let res = router.execute_remote(target_src, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Msetnx(ref pairs) => {
            if pairs.is_empty() {
                out.extend_from_slice(b":0\r\n");
                return false;
            }
            let first_shard = router.target_shard(&pairs[0].0);
            let all_same_shard = pairs
                .iter()
                .all(|(k, _)| router.target_shard(k) == first_shard);
            if !all_same_shard {
                if router.cluster_enabled {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                } else {
                    out.extend_from_slice(&msetnx_across_shards(router, pairs, in_tx(client_id)).await);
                }
                return false;
            }
            if first_shard == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof().as_deref(),
                );
            } else {
                let res = router.execute_remote(first_shard, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Pfcount { ref keys } => {
            if keys.is_empty() {
                out.extend_from_slice(b":0\r\n");
                return false;
            }
            let first_shard = router.target_shard(&keys[0]);
            let all_same = keys.iter().all(|k| router.target_shard(k) == first_shard);
            if !all_same && router.cluster_enabled {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if all_same {
                if first_shard == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof().as_deref(),
                    );
                } else {
                    let res = router.execute_remote(first_shard, cmd).await;
                    out.extend_from_slice(&res);
                }
            } else {
                let mut max_regs = [0u8; 16384];
                let mut has_hll = false;
                for k in keys {
                    let val_opt = router.get(k.clone()).await;
                    if let Some(val) = val_opt {
                        match crate::hll::hll_decode_registers(&val) {
                            Ok(regs) => {
                                has_hll = true;
                                for i in 0..16384 {
                                    if regs[i] > max_regs[i] {
                                        max_regs[i] = regs[i];
                                    }
                                }
                            }
                            Err(err) => {
                                write_resp_err(out, err);
                                return false;
                            }
                        }
                    }
                }
                if !has_hll {
                    out.extend_from_slice(b":0\r\n");
                } else {
                    let count = crate::hll::hll_compute_card(&max_regs);
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
            }
            false
        }
        Command::Bitop {
            ref op,
            ref destkey,
            ref srckeys,
        } => {
            let first_shard = router.target_shard(destkey);
            let all_same = srckeys
                .iter()
                .all(|k| router.target_shard(k) == first_shard);
            if !all_same {
                if router.cluster_enabled {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                } else {
                    out.extend_from_slice(&bitop_across_shards(router, op, destkey, srckeys, in_tx(client_id)).await);
                }
                return false;
            }
            if first_shard == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof().as_deref(),
                );
            } else {
                let res = router.execute_remote(first_shard, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Pfmerge {
            ref destkey,
            ref srckeys,
        } => {
            let first_shard = router.target_shard(destkey);
            let all_same = srckeys
                .iter()
                .all(|k| router.target_shard(k) == first_shard);
            if !all_same && router.cluster_enabled {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if all_same {
                if first_shard == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof().as_deref(),
                    );
                } else {
                    let res = router.execute_remote(first_shard, cmd).await;
                    out.extend_from_slice(&res);
                }
            } else {
                let mut max_regs = [0u8; 16384];
                let dest_val = router.get(destkey.clone()).await;
                if let Some(val) = dest_val {
                    match crate::hll::hll_decode_registers(&val) {
                        Ok(regs) => {
                            for i in 0..16384 {
                                if regs[i] > max_regs[i] {
                                    max_regs[i] = regs[i];
                                }
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                }
                for k in srckeys {
                    let val_opt = router.get(k.clone()).await;
                    if let Some(val) = val_opt {
                        match crate::hll::hll_decode_registers(&val) {
                            Ok(regs) => {
                                for i in 0..16384 {
                                    if regs[i] > max_regs[i] {
                                        max_regs[i] = regs[i];
                                    }
                                }
                            }
                            Err(err) => {
                                write_resp_err(out, err);
                                return false;
                            }
                        }
                    }
                }
                let new_hll = crate::hll::hll_create_from_regs(&max_regs, None);
                router
                    .set(destkey.clone(), Bytes::from(new_hll), None)
                    .await;
                out.extend_from_slice(b"+OK\r\n");
            }
            false
        }
        Command::PfdebugSimd(_) => {
            out.extend_from_slice(b"+enabled\r\n");
            false
        }
        Command::Pfselftest => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Hexpire { ref key, .. }
        | Command::Httl { ref key, .. }
        | Command::Hpersist { ref key, .. }
        | Command::Hgetex { ref key, .. }
        | Command::Hsetex { ref key, .. }
        | Command::Xclaim { ref key, .. }
        | Command::Xautoclaim { ref key, .. } => {
            let target = router.target_shard(key);
            if target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof().as_deref(),
                );
            } else {
                let res = router.execute_remote(target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Save => {
            match router.save_rdb().await {
                Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => {
                    let err = format!("-ERR {}\r\n", e);
                    out.extend_from_slice(err.as_bytes());
                }
            }
            false
        }
        Command::Bgsave { schedule } => {
            if RDB_KEY_SAVE_DELAY.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                RDB_BGSAVE_IN_PROGRESS.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            match router.bgsave_command(schedule).await {
                Ok(status) => {
                    out.push(b'+');
                    out.extend_from_slice(status.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                Err(e) => {
                    let err = format!("-ERR {}\r\n", e);
                    out.extend_from_slice(err.as_bytes());
                }
            }
            false
        }
        Command::Bgrewriteaof => {
            match router.bgrewriteaof().await {
                Ok(status) => {
                    out.extend_from_slice(format!("+{status}\r\n").as_bytes());
                }
                Err(e) => {
                    let err = format!("-ERR {}\r\n", e);
                    out.extend_from_slice(err.as_bytes());
                }
            }
            false
        }
        Command::Lastsave => {
            let ts = router.lastsave();
            out.extend_from_slice(format!(":{}\r\n", ts).as_bytes());
            false
        }
        Command::Asking => {
            // Already handled at start of execute_command
            false
        }
        Command::Migrate {
            host,
            port,
            key,
            keys,
            destination_db,
            timeout_ms,
            copy,
            replace,
            auth,
        } => {
            let mut migrate_keys = Vec::new();
            if let Some(k) = key
                && !k.is_empty()
            {
                migrate_keys.push(k);
            }
            for k in keys {
                if !migrate_keys.contains(&k) {
                    migrate_keys.push(k);
                }
            }
            if migrate_keys.is_empty() {
                out.extend_from_slice(b"+NOKEY\r\n");
                return false;
            }

            // Redis treats a zero timeout as one second.
            let timeout =
                std::time::Duration::from_millis(if timeout_ms == 0 { 1000 } else { timeout_ms });
            let opts = MigrateOpts {
                db: Some(destination_db),
                auth,
                copy,
                replace,
                timeout,
            };
            // Like Redis, connections to MIGRATE targets are cached (see
            // `migrate_cache_take`); a cached one that turns out to be dead
            // is replaced once. Unlike Redis, by credentials too: a
            // connection authenticated for one MIGRATE ... AUTH is never
            // reused by a MIGRATE with other (or no) credentials.
            let mut target = format!("{host}:{port}").into_bytes();
            if let Some((user, pass)) = &opts.auth {
                target.push(0);
                target.extend_from_slice(user.as_deref().unwrap_or(b"default"));
                target.push(0);
                target.extend_from_slice(pass);
            }
            let mut conn = migrate_cache_take(&target);
            let was_cached = conn.is_some();
            let mut res =
                migrate_keys_to_node(router, &migrate_keys, &host, port, opts.clone(), &mut conn)
                    .await;
            if was_cached && matches!(&res, Err(e) if e.starts_with("IOERR")) {
                conn = None;
                res = migrate_keys_to_node(router, &migrate_keys, &host, port, opts, &mut conn)
                    .await;
            }
            if let Some(c) = conn {
                migrate_cache_put(target, c);
            }
            match res {
                Ok(0) => out.extend_from_slice(b"+NOKEY\r\n"),
                Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
            }
            false
        }
        Command::Subscribe(_) | Command::Psubscribe(_) | Command::Ssubscribe(_) => false,
        Command::Unsubscribe(channels) => {
            if channels.is_empty() {
                out.extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n");
            } else {
                for ch in channels {
                    out.extend_from_slice(&unsubscribe_reply("unsubscribe", &ch, 0));
                }
            }
            false
        }
        Command::Sunsubscribe(channels) => {
            if channels.is_empty() {
                out.extend_from_slice(b"*3\r\n$12\r\nsunsubscribe\r\n$-1\r\n:0\r\n");
            } else {
                for ch in channels {
                    out.extend_from_slice(&unsubscribe_reply("sunsubscribe", &ch, 0));
                }
            }
            false
        }
        Command::Punsubscribe(patterns) => {
            if patterns.is_empty() {
                out.extend_from_slice(b"*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n");
            } else {
                for pat in patterns {
                    out.extend_from_slice(&unsubscribe_reply("punsubscribe", &pat, 0));
                }
            }
            false
        }
        Command::Publish { channel, message } => {
            propagate_publish(b"PUBLISH", &channel, &message);
            let count = router.publish(channel, message).await;
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::Spublish { channel, message } => {
            propagate_publish(b"SPUBLISH", &channel, &message);
            let count = router.spublish(channel, message).await;
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::PubsubChannels(pattern) => {
            let channels = router.pubsub_channels(pattern).await;
            out.extend_from_slice(format!("*{}\r\n", channels.len()).as_bytes());
            for ch in channels {
                out.extend_from_slice(format!("${}\r\n", ch.len()).as_bytes());
                out.extend_from_slice(&ch);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::PubsubShardchannels(pattern) => {
            let channels = router.pubsub_shardchannels(pattern).await;
            out.extend_from_slice(format!("*{}\r\n", channels.len()).as_bytes());
            for ch in channels {
                out.extend_from_slice(format!("${}\r\n", ch.len()).as_bytes());
                out.extend_from_slice(&ch);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::PubsubNumsub(channels) => {
            let counts = router.pubsub_numsub(channels).await;
            out.extend_from_slice(format!("*{}\r\n", counts.len() * 2).as_bytes());
            for (ch, cnt) in counts {
                out.extend_from_slice(format!("${}\r\n", ch.len()).as_bytes());
                out.extend_from_slice(&ch);
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(format!(":{}\r\n", cnt).as_bytes());
            }
            false
        }
        Command::PubsubShardnumsub(channels) => {
            let counts = router.pubsub_shardnumsub(channels).await;
            out.extend_from_slice(format!("*{}\r\n", counts.len() * 2).as_bytes());
            for (ch, cnt) in counts {
                out.extend_from_slice(format!("${}\r\n", ch.len()).as_bytes());
                out.extend_from_slice(&ch);
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(format!(":{}\r\n", cnt).as_bytes());
            }
            false
        }
        Command::PubsubNumpat => {
            let count = router.pubsub_numpat().await;
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::PubsubHelp => {
            let help_lines = [
                "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "CHANNELS [<pattern>]",
                "    Return the currently active channels matching a <pattern> (default: '*').",
                "NUMPAT",
                "    Return number of subscriptions to patterns (that are performed using",
                "    the PSUBSCRIBE command).",
                "NUMSUB [<channel> ...]",
                "    Return the number of subscribers for the specified channels, excluding",
                "    pattern subscriptions(default: no channels).",
                "SHARDCHANNELS [<pattern>]",
                "    Return the currently active shard level channels matching a <pattern> (default: '*').",
                "SHARDNUMSUB [<shardchannel> ...]",
                "    Return the number of subscribers for the specified shard level channels.",
                "HELP",
                "    Print this help.",
            ];
            out.extend_from_slice(format!("*{}\r\n", help_lines.len()).as_bytes());
            for line in help_lines {
                write_resp_status(out, line);
            }
            false
        }
        Command::Keys(pattern) => {
            let keys = router.keys(&pattern).await;
            out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
            for k in keys {
                out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::Scan {
            cursor,
            pattern,
            count,
            key_type,
        } => {
            let cnt = count.unwrap_or(10);
            let (next_cursor, keys) = router
                .scan(cursor, pattern.as_deref(), cnt, key_type.as_deref())
                .await;
            let cursor_str = next_cursor.to_string();
            out.extend_from_slice(b"*2\r\n$");
            out.extend_from_slice(cursor_str.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(cursor_str.as_bytes());
            out.extend_from_slice(b"\r\n*");
            out.extend_from_slice(keys.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for k in keys {
                out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::Randomkey => {
            match router.random_key().await {
                Some(k) => {
                    out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                    out.extend_from_slice(&k);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    write_resp_null(out);
                }
            }
            false
        }
        Command::Expiretime(key, in_millis) => {
            let ts = router.expiretime(key, in_millis).await;
            crate::server_stats::note_key_lookup(ts != -2);
            out.extend_from_slice(format!(":{}\r\n", ts).as_bytes());
            false
        }
        Command::Multi => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Exec => {
            out.extend_from_slice(b"-ERR EXEC without MULTI\r\n");
            false
        }
        Command::Discard => {
            out.extend_from_slice(b"-ERR DISCARD without MULTI\r\n");
            false
        }
        Command::Watch(keys) => {
            watch_command(router, client_id, &keys).await;
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Unwatch => {
            unwatch_keys(router.port, client_id);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Eval {
            script,
            keys,
            args,
            read_only,
            auth_user: cmd_auth_user,
        } => {
            let effective_user = if !cmd_auth_user.is_empty() {
                cmd_auth_user
            } else {
                auth_user.clone()
            };
            CURRENT_AUTH_USER.with(|u| *u.borrow_mut() = effective_user.clone());
            if keys_span_shards(&keys, |k| router.target_shard(k)) {
                out.extend_from_slice(SCRIPT_CROSS_SHARD_ERR);
                return false;
            }
            if let Some(first_key) = keys.first() {
                let target = router.target_shard(first_key);
                if target != router.shard_id {
                    let res = router
                        .run_script_remote(
                            target,
                            Command::Eval {
                                script,
                                keys,
                                args,
                                read_only,
                                auth_user: effective_user,
                            },
                        )
                        .await;
                    out.extend_from_slice(&res);
                    return false;
                }
            }
            let script_str = String::from_utf8_lossy(&script);
            crate::scripting::load_script(&script);
            if keys.is_empty() {
                crate::scripting::arm_script_redirect();
            }
            let saved_db = CURRENT_DB.get();
            let result = crate::scripting::eval_script_yielding(
                &script_str,
                &keys,
                &args,
                &router.local_db,
                router.aof(),
                read_only,
            )
            .await;
            CURRENT_DB.set(saved_db);
            router.local_db.borrow_mut().select_db(saved_db);
            if let Some(target) = crate::scripting::take_script_redirect() {
                let cmd = Command::Eval {
                    script,
                    keys,
                    args,
                    read_only,
                    auth_user: effective_user,
                };
                out.extend_from_slice(&router.run_script_remote(target, cmd).await);
                return false;
            }
            match result {
                Ok(resp) => out.extend_from_slice(&resp),
                Err(e) => {
                    write_resp_err(out, e);
                }
            }
            false
        }
        Command::Evalsha {
            sha,
            keys,
            args,
            read_only,
            auth_user: cmd_auth_user,
        } => {
            let effective_user = if !cmd_auth_user.is_empty() {
                cmd_auth_user
            } else {
                auth_user.clone()
            };
            CURRENT_AUTH_USER.with(|u| *u.borrow_mut() = effective_user.clone());
            if keys_span_shards(&keys, |k| router.target_shard(k)) {
                out.extend_from_slice(SCRIPT_CROSS_SHARD_ERR);
                return false;
            }
            if let Some(first_key) = keys.first() {
                let target = router.target_shard(first_key);
                if target != router.shard_id {
                    let res = router
                        .run_script_remote(
                            target,
                            Command::Evalsha {
                                sha,
                                keys,
                                args,
                                read_only,
                                auth_user: effective_user,
                            },
                        )
                        .await;
                    out.extend_from_slice(&res);
                    return false;
                }
            }
            let sha_str = String::from_utf8_lossy(&sha);
            if let Some(script) = crate::scripting::get_script(&sha_str) {
                if keys.is_empty() {
                    crate::scripting::arm_script_redirect();
                }
                let saved_db = CURRENT_DB.get();
                let result = crate::scripting::eval_script_yielding(
                    &script,
                    &keys,
                    &args,
                    &router.local_db,
                    router.aof(),
                    read_only,
                )
                .await;
                CURRENT_DB.set(saved_db);
                router.local_db.borrow_mut().select_db(saved_db);
                if let Some(target) = crate::scripting::take_script_redirect() {
                    let cmd = Command::Evalsha {
                        sha,
                        keys,
                        args,
                        read_only,
                        auth_user: effective_user,
                    };
                    out.extend_from_slice(&router.run_script_remote(target, cmd).await);
                    return false;
                }
                match result {
                    Ok(resp) => out.extend_from_slice(&resp),
                    Err(e) => {
                        write_resp_err(out, e);
                    }
                }
            } else {
                out.extend_from_slice(b"-NOSCRIPT No matching script. Please use EVAL.\r\n");
            }
            false
        }
        Command::ScriptLoad(script) => {
            let sha = crate::scripting::load_script(&script);
            write_resp_bulk(out, sha.as_bytes());
            false
        }
        Command::ScriptExists(shas) => {
            let exists = crate::scripting::script_exists(&shas);
            out.extend_from_slice(format!("*{}\r\n", exists.len()).as_bytes());
            for b in exists {
                if b {
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
            }
            false
        }
        Command::ScriptKill => {
            out.extend_from_slice(crate::scripting::kill_running_script(router.port, true));
            false
        }
        Command::ScriptFlush => {
            crate::scripting::flush_scripts();
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::CrdtDump => {
            let mut payload = router.local_db.borrow().crdt_dump();
            for sid in 0..router.num_shards {
                if sid != router.shard_id {
                    let res = router.execute_remote(sid, Command::CrdtDump).await;
                    if let Some(first_nl) = res.iter().position(|&b| b == b'\n')
                        && res.len() > first_nl + 2
                    {
                        let chunk = &res[first_nl + 1..res.len() - 2];
                        payload.extend_from_slice(chunk);
                    }
                }
            }
            write_resp_bulk(out, &payload);
            false
        }
        Command::CrdtMerge(payload) => {
            // Each shard merges, and logs, only the keys it owns.
            let parts = match router.split_crdt_payload(&payload) {
                Ok(parts) => parts,
                Err(e) => {
                    let err_resp = format!("-ERR {}\r\n", e);
                    out.extend_from_slice(err_resp.as_bytes());
                    return false;
                }
            };
            let mut total_merged = 0;
            for (sid, part) in parts {
                let res = router.execute_on_shard(sid, Command::CrdtMerge(part)).await;
                if let Ok(s) = std::str::from_utf8(&res)
                    && let Some(num_str) = s.strip_prefix(':').and_then(|x| x.split("\r\n").next())
                    && let Ok(n) = num_str.parse::<usize>()
                {
                    total_merged += n;
                }
            }
            write_resp_integer(out, total_merged as i64);
            false
        }
        Command::CrdtGc(horizon) => {
            // One absolute cutoff for every shard, so they all log (and a
            // replica applies) the same GC.
            let before = Command::CrdtGc(crate::crdt::GcHorizon::Before(horizon.cutoff_ms()));
            let (mut total_regs, mut total_sets) = (0, 0);
            for sid in 0..router.num_shards {
                let res = router.execute_on_shard(sid, before.clone()).await;
                if let Ok(s) = std::str::from_utf8(&res) {
                    let parts: Vec<&str> = s.split("\r\n").collect();
                    if parts.len() >= 7 {
                        if let Ok(r) = parts[3].trim_start_matches(':').parse::<usize>() {
                            total_regs += r;
                        }
                        if let Ok(st) = parts[6].trim_start_matches(':').parse::<usize>() {
                            total_sets += st;
                        }
                    }
                }
            }
            out.extend_from_slice(b"*4\r\n");
            write_resp_bulk(out, b"registers_pruned");
            write_resp_integer(out, total_regs as i64);
            write_resp_bulk(out, b"set_tombstones_pruned");
            write_resp_integer(out, total_sets as i64);
            false
        }
        Command::FunctionLoad { replace, code } => {
            let code_str = String::from_utf8_lossy(&code);
            match crate::scripting::load_function(&code_str, replace) {
                Ok(lib_name) => {
                    router.log_mutation(|| Command::FunctionLoad {
                        replace,
                        code: code.clone(),
                    });
                    write_resp_bulk(out, lib_name.as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Fcall {
            function,
            keys,
            args,
            read_only,
            auth_user: cmd_auth_user,
        } => {
            let effective_user = if !cmd_auth_user.is_empty() {
                cmd_auth_user
            } else {
                auth_user.clone()
            };
            CURRENT_AUTH_USER.with(|u| *u.borrow_mut() = effective_user.clone());
            if keys_span_shards(&keys, |k| router.target_shard(k)) {
                out.extend_from_slice(SCRIPT_CROSS_SHARD_ERR);
                return false;
            }
            if let Some(first_key) = keys.first() {
                let target = router.target_shard(first_key);
                if target != router.shard_id {
                    let res = router
                        .run_script_remote(
                            target,
                            Command::Fcall {
                                function,
                                keys,
                                args,
                                read_only,
                                auth_user: effective_user,
                            },
                        )
                        .await;
                    out.extend_from_slice(&res);
                    return false;
                }
            }
            if keys.is_empty() {
                crate::scripting::arm_script_redirect();
            }
            let saved_db = CURRENT_DB.get();
            let result = crate::scripting::call_function_yielding(
                &function,
                &keys,
                &args,
                &router.local_db,
                router.aof(),
                read_only,
            )
            .await;
            CURRENT_DB.set(saved_db);
            router.local_db.borrow_mut().select_db(saved_db);
            if let Some(target) = crate::scripting::take_script_redirect() {
                let cmd = Command::Fcall {
                    function,
                    keys,
                    args,
                    read_only,
                    auth_user: effective_user,
                };
                out.extend_from_slice(&router.run_script_remote(target, cmd).await);
                return false;
            }
            match result {
                Ok(res) => {
                    for k in &keys {
                        notify_key_invalidation(router.port, k.as_ref(), client_id);
                    }
                    out.extend_from_slice(&res);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::FunctionList {
            library_name_pattern,
            with_code,
        } => {
            let mut libs = crate::scripting::list_functions();
            if let Some(pat) = library_name_pattern {
                libs.retain(|lib| crate::pubsub::glob_match(pat.as_bytes(), lib.name.as_bytes()));
            }
            out.extend_from_slice(format!("*{}\r\n", libs.len()).as_bytes());
            for lib in libs {
                if with_code {
                    out.extend_from_slice(b"*8\r\n");
                } else {
                    out.extend_from_slice(b"*6\r\n");
                }
                write_resp_bulk(out, b"library_name");
                write_resp_bulk(out, lib.name.as_bytes());
                write_resp_bulk(out, b"engine");
                write_resp_bulk(out, lib.engine.as_bytes());
                write_resp_bulk(out, b"functions");
                out.extend_from_slice(format!("*{}\r\n", lib.functions.len()).as_bytes());
                for f in &lib.functions {
                    out.extend_from_slice(b"*6\r\n");
                    write_resp_bulk(out, b"name");
                    write_resp_bulk(out, f.name.as_bytes());
                    write_resp_bulk(out, b"description");
                    // Null when the function has none, as Redis.
                    if f.description.is_empty() {
                        write_resp_null(out);
                    } else {
                        write_resp_bulk(out, f.description.as_bytes());
                    }
                    write_resp_bulk(out, b"flags");
                    out.extend_from_slice(format!("*{}\r\n", f.flags.len()).as_bytes());
                    for flag in &f.flags {
                        write_resp_bulk(out, flag.as_bytes());
                    }
                }
                if with_code {
                    write_resp_bulk(out, b"library_code");
                    write_resp_bulk(out, lib.original_code.as_bytes());
                }
            }
            false
        }
        Command::FunctionDelete(lib) => {
            if crate::scripting::delete_function(&lib) {
                router.log_mutation(|| Command::FunctionDelete(lib.clone()));
                out.extend_from_slice(b"+OK\r\n");
            } else {
                write_resp_err(out, "ERR Library not found");
            }
            false
        }
        Command::FunctionDump => {
            let dump = crate::scripting::dump_functions();
            write_resp_bulk(out, &dump);
            false
        }
        Command::FunctionRestore { payload, policy } => {
            match crate::scripting::restore_functions(&payload, &policy) {
                Ok(()) => {
                    router.log_mutation(|| Command::FunctionRestore {
                        payload: payload.clone(),
                        policy: policy.clone(),
                    });
                    out.extend_from_slice(b"+OK\r\n")
                }
                Err(e) => write_resp_err(out, e),
            }
            false
        }
        Command::FunctionFlush => {
            crate::scripting::flush_functions();
            router.log_mutation(|| Command::FunctionFlush);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::FunctionKill => {
            out.extend_from_slice(crate::scripting::kill_running_script(router.port, false));
            false
        }
        Command::FunctionStats => {
            let libs = crate::scripting::list_functions();
            let lib_count = libs.len();
            let func_count: usize = libs.iter().map(|l| l.functions.len()).sum();
            if CURRENT_CLIENT_RESP3.get() {
                out.extend_from_slice(b"%2\r\n");
            } else {
                out.extend_from_slice(b"*4\r\n");
            }
            write_resp_bulk(out, b"running_script");
            write_function_stats_running(out, router.port);
            write_resp_bulk(out, b"engines");
            if CURRENT_CLIENT_RESP3.get() {
                out.extend_from_slice(b"%1\r\n");
            } else {
                out.extend_from_slice(b"*2\r\n");
            }
            write_resp_bulk(out, b"LUA");
            if CURRENT_CLIENT_RESP3.get() {
                out.extend_from_slice(b"%2\r\n");
            } else {
                out.extend_from_slice(b"*4\r\n");
            }
            write_resp_bulk(out, b"libraries_count");
            write_resp_integer(out, lib_count as i64);
            write_resp_bulk(out, b"functions_count");
            write_resp_integer(out, func_count as i64);
            false
        }
        Command::FtCreate {
            index,
            on_type,
            prefixes,
            fields,
            schema_fields,
        } => {
            let schema = crate::search::IndexSchema {
                name: index,
                on_type,
                prefixes,
                fields,
                schema_fields,
            };
            match router.create_search_index(schema).await {
                Ok(()) => {
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
            }
            false
        }
        Command::FtSearch {
            index,
            query,
            options,
        } => {
            if router.has_search_index(&index) {
                let ast = crate::search::parse_query(&query);
                let (total, hits) = router.ft_search(&index, &ast, &options).await;

                if options.nocontent {
                    let per_hit = if options.withscores { 2 } else { 1 };
                    out.extend_from_slice(
                        format!("*{}\r\n:{}\r\n", 1 + hits.len() * per_hit, total).as_bytes(),
                    );
                    for hit in hits {
                        write_resp_bulk(out, hit.doc_id.as_bytes());
                        if options.withscores {
                            let s = format!("{:.6}", hit.score);
                            write_resp_bulk(out, s.as_bytes());
                        }
                    }
                } else {
                    let per_hit = if options.withscores { 3 } else { 2 };
                    let num_elems = 1 + hits.len() * per_hit;
                    out.extend_from_slice(format!("*{}\r\n:{}\r\n", num_elems, total).as_bytes());
                    for hit in hits {
                        write_resp_bulk(out, hit.doc_id.as_bytes());
                        if options.withscores {
                            let s = format!("{:.6}", hit.score);
                            write_resp_bulk(out, s.as_bytes());
                        }
                        out.extend_from_slice(format!("*{}\r\n", hit.fields.len() * 2).as_bytes());
                        for (k, v) in hit.fields {
                            write_resp_bulk(out, k.as_bytes());
                            write_resp_bulk(out, v.as_bytes());
                        }
                    }
                }
            } else {
                out.extend_from_slice(format!("-ERR Unknown Index name: {}\r\n", index).as_bytes());
            }
            false
        }
        Command::FtAggregate {
            index,
            query,
            options,
        } => {
            if router.has_search_index(&index) {
                let rows = router.ft_aggregate(&index, &query, options).await;
                out.extend_from_slice(
                    format!("*{}\r\n:{}\r\n", 1 + rows.len(), rows.len()).as_bytes(),
                );
                for row in rows {
                    out.extend_from_slice(format!("*{}\r\n", row.len() * 2).as_bytes());
                    for (k, v) in row {
                        write_resp_bulk(out, k.as_bytes());
                        write_resp_bulk(out, v.as_bytes());
                    }
                }
            } else {
                out.extend_from_slice(format!("-ERR Unknown Index name: {}\r\n", index).as_bytes());
            }
            false
        }
        Command::FtInfo(index) => {
            if let Some(idx_arc) = crate::search::get_search_index(&index) {
                let idx = idx_arc.read();
                if let Some(schema) = &idx.schema {
                    let total_vec_bytes: usize =
                        idx.vector_indices.values().map(|v| v.memory_usage()).sum();
                    let vec_mb = format!("{:.6}", total_vec_bytes as f64 / (1024.0 * 1024.0));

                    out.extend_from_slice(b"*20\r\n");
                    write_resp_bulk(out, b"index_name");
                    write_resp_bulk(out, schema.name.as_bytes());
                    write_resp_bulk(out, b"index_options");
                    out.extend_from_slice(b"*0\r\n");
                    write_resp_bulk(out, b"index_definition");
                    out.extend_from_slice(b"*4\r\n");
                    write_resp_bulk(out, b"key_type");
                    write_resp_bulk(out, schema.on_type.as_bytes());
                    write_resp_bulk(out, b"prefixes");
                    out.extend_from_slice(format!("*{}\r\n", schema.prefixes.len()).as_bytes());
                    for p in &schema.prefixes {
                        write_resp_bulk(out, p.as_bytes());
                    }
                    write_resp_bulk(out, b"attributes");
                    out.extend_from_slice(
                        format!("*{}\r\n", schema.schema_fields.len()).as_bytes(),
                    );
                    for sf in &schema.schema_fields {
                        match &sf.field_type {
                            crate::search::FieldType::Vector {
                                dim,
                                distance_metric,
                                algorithm,
                                attrs,
                            } => {
                                let v_idx = idx
                                    .vector_indices
                                    .get(&sf.alias)
                                    .or_else(|| idx.vector_indices.get(&sf.identifier));
                                let actual_dim = v_idx.map(|v| v.dim()).unwrap_or(*dim);
                                let num_vecs = v_idx.map(|v| v.len()).unwrap_or(0);
                                out.extend_from_slice(b"*22\r\n");
                                write_resp_bulk(out, b"identifier");
                                write_resp_bulk(out, sf.identifier.as_bytes());
                                write_resp_bulk(out, b"attribute");
                                write_resp_bulk(out, sf.alias.as_bytes());
                                write_resp_bulk(out, b"type");
                                write_resp_bulk(out, b"VECTOR");
                                write_resp_bulk(out, b"algorithm");
                                write_resp_bulk(out, algorithm.to_ascii_uppercase().as_bytes());
                                write_resp_bulk(out, b"data_type");
                                write_resp_bulk(out, attrs.data_type.as_str().as_bytes());
                                write_resp_bulk(out, b"dim");
                                write_resp_integer(out, actual_dim as i64);
                                write_resp_bulk(out, b"distance_metric");
                                write_resp_bulk(
                                    out,
                                    distance_metric.to_ascii_uppercase().as_bytes(),
                                );
                                write_resp_bulk(out, b"M");
                                write_resp_integer(out, attrs.m as i64);
                                write_resp_bulk(out, b"ef_construction");
                                write_resp_integer(out, attrs.ef_construction as i64);
                                write_resp_bulk(out, b"ef_runtime");
                                write_resp_integer(out, attrs.ef_runtime as i64);
                                write_resp_bulk(out, b"num_vectors");
                                write_resp_integer(out, num_vecs as i64);
                            }
                            crate::search::FieldType::Text { weight, .. } => {
                                out.extend_from_slice(b"*8\r\n");
                                write_resp_bulk(out, b"identifier");
                                write_resp_bulk(out, sf.identifier.as_bytes());
                                write_resp_bulk(out, b"attribute");
                                write_resp_bulk(out, sf.alias.as_bytes());
                                write_resp_bulk(out, b"type");
                                write_resp_bulk(out, b"TEXT");
                                write_resp_bulk(out, b"WEIGHT");
                                let w = format!("{}", weight);
                                write_resp_bulk(out, w.as_bytes());
                            }
                            crate::search::FieldType::Numeric { .. } => {
                                out.extend_from_slice(b"*6\r\n");
                                write_resp_bulk(out, b"identifier");
                                write_resp_bulk(out, sf.identifier.as_bytes());
                                write_resp_bulk(out, b"attribute");
                                write_resp_bulk(out, sf.alias.as_bytes());
                                write_resp_bulk(out, b"type");
                                write_resp_bulk(out, b"NUMERIC");
                            }
                            crate::search::FieldType::Tag { separator, .. } => {
                                out.extend_from_slice(b"*8\r\n");
                                write_resp_bulk(out, b"identifier");
                                write_resp_bulk(out, sf.identifier.as_bytes());
                                write_resp_bulk(out, b"attribute");
                                write_resp_bulk(out, sf.alias.as_bytes());
                                write_resp_bulk(out, b"type");
                                write_resp_bulk(out, b"TAG");
                                write_resp_bulk(out, b"SEPARATOR");
                                let sep = separator.to_string();
                                write_resp_bulk(out, sep.as_bytes());
                            }
                        }
                    }
                    write_resp_bulk(out, b"num_docs");
                    write_resp_integer(out, idx.total_docs as i64);
                    write_resp_bulk(out, b"num_terms");
                    write_resp_integer(out, idx.inverted.len() as i64);
                    write_resp_bulk(out, b"total_inverted_index_blocks");
                    write_resp_integer(out, idx.total_terms as i64);
                    write_resp_bulk(out, b"vector_index_sz_mb");
                    write_resp_bulk(out, vec_mb.as_bytes());
                    write_resp_bulk(out, b"indexing");
                    write_resp_bulk(out, b"0");
                    write_resp_bulk(out, b"hash_indexing_failures");
                    write_resp_integer(out, idx.indexing_failures as i64);
                } else {
                    write_resp_null(out);
                }
            } else {
                out.extend_from_slice(format!("-ERR Unknown Index name: {}\r\n", index).as_bytes());
            }
            false
        }
        Command::FtDropIndex { index, dd } => {
            match router.drop_search_index(&index, dd).await {
                Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
            }
            false
        }
        Command::FtExplain { index: _, query } => {
            let ast = crate::search::parse_query(&query);
            let repr = format!("{:?}", ast);
            write_resp_bulk(out, repr.as_bytes());
            false
        }
        Command::FtAdd {
            index,
            doc_id,
            score: _,
            fields,
        } => {
            if let Some(idx_arc) = crate::search::get_search_index(&index) {
                let mut idx = idx_arc.write();
                let map: std::collections::HashMap<String, String> = fields.into_iter().collect();
                idx.add_document(&doc_id, map, None);
                out.extend_from_slice(b"+OK\r\n");
            } else {
                out.extend_from_slice(format!("-ERR Unknown Index name: {}\r\n", index).as_bytes());
            }
            false
        }
        Command::FtList => {
            let names = crate::search::list_search_indices();
            write_resp_array_header(out, names.len());
            for name in names {
                write_resp_bulk(out, name.as_bytes());
            }
            false
        }
        Command::FtAlter {
            index,
            fields,
            schema_fields,
        } => {
            match router
                .alter_search_index(&index, fields, schema_fields)
                .await
            {
                Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
            }
            false
        }
        Command::FtProfile {
            index,
            query,
            options,
        } => {
            if router.has_search_index(&index) {
                let t0 = std::time::Instant::now();
                let ast = crate::search::parse_query(&query);
                let parse_us = t0.elapsed().as_micros();
                let (total, hits) = router.ft_search(&index, &ast, &options).await;
                let total_us = t0.elapsed().as_micros();
                let total_sec = format!("{:.6}", total_us as f64 / 1_000_000.0);
                let parse_sec = format!("{:.6}", parse_us as f64 / 1_000_000.0);
                let repr = format!("{:?}", ast);

                // Top-level *2 array: [search_results, profile_details]
                out.extend_from_slice(b"*2\r\n");
                if options.nocontent {
                    let per_hit = if options.withscores { 2 } else { 1 };
                    out.extend_from_slice(
                        format!("*{}\r\n:{}\r\n", 1 + hits.len() * per_hit, total).as_bytes(),
                    );
                    for hit in hits {
                        write_resp_bulk(out, hit.doc_id.as_bytes());
                        if options.withscores {
                            let s = format!("{:.6}", hit.score);
                            write_resp_bulk(out, s.as_bytes());
                        }
                    }
                } else {
                    let per_hit = if options.withscores { 3 } else { 2 };
                    let num_elems = 1 + hits.len() * per_hit;
                    out.extend_from_slice(format!("*{}\r\n:{}\r\n", num_elems, total).as_bytes());
                    for hit in hits {
                        write_resp_bulk(out, hit.doc_id.as_bytes());
                        if options.withscores {
                            let s = format!("{:.6}", hit.score);
                            write_resp_bulk(out, s.as_bytes());
                        }
                        out.extend_from_slice(format!("*{}\r\n", hit.fields.len() * 2).as_bytes());
                        for (k, v) in hit.fields {
                            write_resp_bulk(out, k.as_bytes());
                            write_resp_bulk(out, v.as_bytes());
                        }
                    }
                }

                // Profile details array
                out.extend_from_slice(b"*8\r\n");
                write_resp_bulk(out, b"Total profile time");
                write_resp_bulk(out, total_sec.as_bytes());
                write_resp_bulk(out, b"Parsing time");
                write_resp_bulk(out, parse_sec.as_bytes());
                write_resp_bulk(out, b"Query plan");
                write_resp_bulk(out, repr.as_bytes());
                write_resp_bulk(out, b"Shards");
                write_resp_integer(out, router.num_shards as i64);
            } else {
                out.extend_from_slice(format!("-ERR Unknown Index name: {}\r\n", index).as_bytes());
            }
            false
        }
        #[cfg(not(feature = "xdp"))]
        Command::XdpInfo
        | Command::XdpRuleAdd { .. }
        | Command::XdpRuleDel(_)
        | Command::XdpRuleList
        | Command::XdpStats
        | Command::XdpSocket(_)
        | Command::XdpInject { .. }
        | Command::XdpPacket(_) => {
            out.extend_from_slice(b"-ERR XDP is not compiled into this build (cargo feature xdp)\r\n");
            false
        }
        #[cfg(feature = "xdp")]
        Command::XdpInfo => {
            let engine = crate::xdp::get_xdp_engine();
            for qid in 0..router.num_shards as u32 {
                let _ = engine.get_or_create_socket(router.port, qid);
            }
            let info = engine.info();
            write_resp_bulk(out, info.as_bytes());
            false
        }
        #[cfg(feature = "xdp")]
        Command::XdpRuleAdd { action, cidr } => {
            match crate::xdp::get_xdp_engine().add_rule(action, &cidr) {
                Ok(id) => write_resp_integer(out, id as i64),
                Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
            }
            false
        }
        #[cfg(feature = "xdp")]
        Command::XdpRuleDel(id) => {
            match crate::xdp::get_xdp_engine().del_rule(id) {
                Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
            }
            false
        }
        #[cfg(feature = "xdp")]
        Command::XdpRuleList => {
            let rules = crate::xdp::get_xdp_engine().list_rules();
            out.extend_from_slice(format!("*{}\r\n", rules.len()).as_bytes());
            for r in rules {
                let line = format!("id:{} action:{} cidr:{}", r.id, r.action, r.cidr);
                write_resp_bulk(out, line.as_bytes());
            }
            false
        }
        #[cfg(feature = "xdp")]
        Command::XdpStats => {
            use std::sync::atomic::Ordering;
            let engine = crate::xdp::get_xdp_engine();
            out.extend_from_slice(b"*12\r\n");
            write_resp_bulk(out, b"rx_packets");
            write_resp_integer(out, engine.rx_packets.load(Ordering::Relaxed) as i64);
            write_resp_bulk(out, b"rx_bytes");
            write_resp_integer(out, engine.rx_bytes.load(Ordering::Relaxed) as i64);
            write_resp_bulk(out, b"dropped_packets");
            write_resp_integer(out, engine.dropped_packets.load(Ordering::Relaxed) as i64);
            write_resp_bulk(out, b"redirected_packets");
            write_resp_integer(
                out,
                engine.redirected_packets.load(Ordering::Relaxed) as i64,
            );
            write_resp_bulk(out, b"pass_packets");
            write_resp_integer(out, engine.pass_packets.load(Ordering::Relaxed) as i64);
            write_resp_bulk(out, b"rate_limit_drops");
            write_resp_integer(out, engine.rate_limit_drops.load(Ordering::Relaxed) as i64);
            false
        }
        #[cfg(feature = "xdp")]
        Command::XdpPacket(payload) => {
            let action = crate::xdp::get_xdp_engine().process_packet(&payload);
            let s = format!("+{}\r\n", action);
            out.extend_from_slice(s.as_bytes());
            false
        }
        #[cfg(feature = "xdp")]
        Command::XdpSocket(qid) => {
            let engine = crate::xdp::get_xdp_engine();
            let sock = engine.get_or_create_socket(router.port, qid);
            out.extend_from_slice(b"*8\r\n");
            write_resp_bulk(out, b"queue_id");
            write_resp_integer(out, qid as i64);
            write_resp_bulk(out, b"rx_len");
            write_resp_integer(out, sock.rx_ring.len() as i64);
            write_resp_bulk(out, b"fill_len");
            write_resp_integer(out, sock.fill_ring.len() as i64);
            write_resp_bulk(out, b"tx_len");
            write_resp_integer(out, sock.tx_ring.len() as i64);
            false
        }
        #[cfg(feature = "xdp")]
        Command::XdpInject { queue_id, payload } => {
            let engine = crate::xdp::get_xdp_engine();
            let sock = engine.get_or_create_socket(router.port, queue_id);
            if sock.inject_rx(&payload) {
                out.extend_from_slice(b"+OK\r\n");
            } else {
                out.extend_from_slice(b"-ERR ring full\r\n");
            }
            false
        }
        Command::DflyCluster(sub) => {
            match sub {
                crate::resp::DflyClusterSubcommand::MyId => {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    let id = hub.my_id();
                    write_resp_bulk(out, id.as_bytes());
                }
                crate::resp::DflyClusterSubcommand::Config(json) => {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    match hub.dfly_cluster_config(&json) {
                        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
                    }
                }
                crate::resp::DflyClusterSubcommand::GetSlotInfo(slots) => {
                    out.extend_from_slice(format!("*{}\r\n", slots.len()).as_bytes());
                    for s in slots {
                        let key_count = router.count_keys_in_slot(s).await;
                        let mem_bytes = key_count * 128;
                        out.extend_from_slice(b"*3\r\n");
                        write_resp_integer(out, s as i64);
                        write_resp_integer(out, key_count as i64);
                        write_resp_integer(out, mem_bytes as i64);
                    }
                }
                crate::resp::DflyClusterSubcommand::FlushSlots(ranges) => {
                    let _ = router.flush_slots(&ranges).await;
                    out.extend_from_slice(b"+OK\r\n");
                }
                crate::resp::DflyClusterSubcommand::SlotMigrationStatus => {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    hub.dfly_slot_migration_status(out);
                }
            }
            false
        }
        Command::DflyMigrate(sub) => {
            let hub = crate::cluster::get_cluster_hub(router.base_port);
            match sub {
                crate::resp::DflyMigrateSubcommand::Init {
                    source_id,
                    num_shards,
                    slots,
                } => {
                    hub.dfly_migrate_init(&source_id, num_shards, &slots);
                    out.extend_from_slice(b"+OK\r\n");
                }
                crate::resp::DflyMigrateSubcommand::Flow { source_id, flow_id } => {
                    hub.dfly_migrate_flow(&source_id, flow_id);
                    out.extend_from_slice(b"+OK\r\n");
                }
                crate::resp::DflyMigrateSubcommand::Ack { flow_id } => {
                    hub.dfly_migrate_ack(flow_id);
                    out.extend_from_slice(b"+OK\r\n");
                }
            }
            false
        }
        Command::Stick(keys) => {
            let count = router.stick(&keys).await;
            write_resp_integer(out, count as i64);
            false
        }
        Command::Unstick(keys) => {
            let count = router.unstick(&keys).await;
            write_resp_integer(out, count as i64);
            false
        }
        Command::Sticky(key) => {
            let sticky = router.is_sticky(&key).await;
            write_resp_integer(out, if sticky { 1 } else { 0 });
            false
        }
        Command::MemcachedSet {
            key,
            flags: _,
            exptime,
            bytes: _,
            noreply,
            data,
        } => {
            let dur = if exptime > 0 {
                Some(std::time::Duration::from_secs(exptime as u64))
            } else {
                None
            };
            router.set(key, data, dur).await;
            if !noreply {
                out.extend_from_slice(b"STORED\r\n");
            }
            false
        }
        Command::MemcachedAdd {
            key,
            flags: _,
            exptime,
            bytes: _,
            noreply,
            data,
        } => {
            if router.exists(key.clone()).await {
                if !noreply {
                    out.extend_from_slice(b"NOT_STORED\r\n");
                }
            } else {
                let dur = if exptime > 0 {
                    Some(std::time::Duration::from_secs(exptime as u64))
                } else {
                    None
                };
                router.set(key, data, dur).await;
                if !noreply {
                    out.extend_from_slice(b"STORED\r\n");
                }
            }
            false
        }
        Command::MemcachedReplace {
            key,
            flags: _,
            exptime,
            bytes: _,
            noreply,
            data,
        } => {
            if router.exists(key.clone()).await {
                let dur = if exptime > 0 {
                    Some(std::time::Duration::from_secs(exptime as u64))
                } else {
                    None
                };
                router.set(key, data, dur).await;
                if !noreply {
                    out.extend_from_slice(b"STORED\r\n");
                }
            } else {
                if !noreply {
                    out.extend_from_slice(b"NOT_STORED\r\n");
                }
            }
            false
        }
        Command::MemcachedGet { keys } => {
            for k in keys {
                if let Some(val) = router.get(k.clone()).await {
                    out.extend_from_slice(
                        format!("VALUE {} 0 {}\r\n", String::from_utf8_lossy(&k), val.len())
                            .as_bytes(),
                    );
                    out.extend_from_slice(&val);
                    out.extend_from_slice(b"\r\n");
                }
            }
            out.extend_from_slice(b"END\r\n");
            false
        }
        Command::MemcachedDelete { key, noreply } => {
            let deleted = router.del(key).await;
            if !noreply {
                if deleted {
                    out.extend_from_slice(b"DELETED\r\n");
                } else {
                    out.extend_from_slice(b"NOT_FOUND\r\n");
                }
            }
            false
        }
        Command::MemcachedIncr {
            key,
            value,
            noreply,
        } => {
            match router
                .incr_by(key, value as i64, crate::resp::IncrName::IncrBy)
                .await
            {
                Ok(new_val) => {
                    if !noreply {
                        out.extend_from_slice(format!("{}\r\n", new_val).as_bytes());
                    }
                }
                Err(_) => {
                    if !noreply {
                        out.extend_from_slice(b"NOT_FOUND\r\n");
                    }
                }
            }
            false
        }
        Command::MemcachedDecr {
            key,
            value,
            noreply,
        } => {
            match router
                .incr_by(key, -(value as i64), crate::resp::IncrName::IncrBy)
                .await
            {
                Ok(new_val) => {
                    let clamped = new_val.max(0);
                    if !noreply {
                        out.extend_from_slice(format!("{}\r\n", clamped).as_bytes());
                    }
                }
                Err(_) => {
                    if !noreply {
                        out.extend_from_slice(b"NOT_FOUND\r\n");
                    }
                }
            }
            false
        }
        Command::MemcachedStats => {
            let dbsize = router.dbsize().await;
            out.extend_from_slice(format!("STAT pid {}\r\nSTAT uptime 3600\r\nSTAT version 1.6.0-rudis-dragonfly\r\nSTAT curr_items {}\r\nEND\r\n", std::process::id(), dbsize).as_bytes());
            false
        }
        Command::MemcachedVersion => {
            out.extend_from_slice(b"VERSION 1.6.0-rudis-dragonfly\r\n");
            false
        }
        Command::MemcachedQuit => true,
        Command::Shutdown { abort: true, .. } => {
            if crate::replication::abort_shutdown() {
                out.extend_from_slice(b"+OK\r\n");
            } else {
                out.extend_from_slice(b"-ERR No shutdown in progress.\r\n");
            }
            false
        }
        Command::Shutdown {
            save, now, force, ..
        } => {
            crate::log_warning!("User requested shutdown...");
            if crate::shutdown::loading_before_serving() {
                // Like Redis: no save while loading (it would overwrite the
                // files being loaded with part of them), and nothing to
                // flush, since every write was refused.
                crate::log_warning!("Shutting down during loading, exiting now without saving.");
                std::process::exit(0);
            }
            let _running = crate::shutdown::ShutdownCommand::begin();
            if !now && !crate::replication::wait_replicas_before_shutdown(router.base_port, router.num_shards).await {
                // SHUTDOWN ABORT (already logged).
                out.extend_from_slice(b"-ERR Errors trying to SHUTDOWN. Check logs.\r\n");
                return false;
            }
            if crate::aof::aof_enabling(router.base_port) && !force {
                // Its AOF files don't hold the dataset yet (Redis too).
                crate::log_warning!("Writing initial AOF, can't exit.");
                crate::log_warning!(
                    "Errors trying to shut down the server. Check the logs for more information."
                );
                out.extend_from_slice(b"-ERR Errors trying to SHUTDOWN. Check logs.\r\n");
                return false;
            }
            let saved = router.save_before_shutdown(save, 1).await;
            if let Err(e) = &saved {
                if force {
                    crate::log_warning!("Error trying to save the DB, exiting anyway (FORCE): {}", e);
                } else {
                    crate::log_warning!("Error trying to save the DB, can't exit: {}", e);
                }
            }
            if saved.is_err() && !force {
                crate::log_warning!(
                    "Errors trying to shut down the server. Check the logs for more information."
                );
                out.extend_from_slice(b"-ERR Errors trying to SHUTDOWN. Check logs.\r\n");
                false
            } else {
                crate::shutdown::request_shutdown();
                crate::shutdown::stop_listening();
                out.extend_from_slice(b"+OK\r\n");
                true
            }
        }
        Command::Readonly => {
            if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                c.readonly = true;
            }
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Readwrite => {
            if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                c.readonly = false;
            }
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Wait {
            numreplicas,
            timeout,
        } => {
            // Writes pipelined before WAIT may still be held by a shard.
            crate::replication::wait_streams_flushed(router.base_port, router.num_shards).await;
            match crate::replication::wait_replicas_for(client_id, router.base_port, numreplicas, timeout)
                .await
            {
                Ok(count) => write_resp_integer(out, count as i64),
                Err(crate::replication::WaitError::Demoted) => {
                    out.extend_from_slice(crate::replication::DEMOTED_UNBLOCK_REPLY);
                    return true;
                }
                Err(_) => out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n"),
            }
            false
        }
        Command::WaitAof {
            numlocal,
            numreplicas,
            timeout,
        } => {
            if crate::replication::cached_hub(router.base_port).is_slave() {
                out.extend_from_slice(b"-ERR WAITAOF cannot be used with replica instances. Please also note that writes to replicas are just local and are not propagated.\r\n");
                return false;
            }
            if numlocal > 0 && router.aof().is_none() {
                out.extend_from_slice(
                    b"-ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.\r\n",
                );
                return false;
            }
            // The local copy counts once the AOF bytes appended so far are
            // fsynced (at once under appendfsync always, by the next
            // everysec fsync, or never under appendfsync no).
            let local = router
                .aof()
                .is_some()
                .then(|| crate::replication::LocalAofTarget::now(router.base_port, router.num_shards));
            crate::replication::wait_streams_flushed(router.base_port, router.num_shards).await;
            match crate::replication::wait_aof(
                client_id,
                router.base_port,
                local,
                numlocal,
                numreplicas,
                timeout,
                || crate::aof::aof_on(router.base_port),
            )
            .await
            {
                Ok((local_ok, count)) => out.extend_from_slice(
                    format!("*2\r\n:{}\r\n:{}\r\n", u8::from(local_ok), count).as_bytes(),
                ),
                Err(crate::replication::WaitError::Unblocked) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n")
                }
                Err(crate::replication::WaitError::AofOff) => out.extend_from_slice(
                    b"-ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.\r\n",
                ),
                Err(crate::replication::WaitError::Demoted) => {
                    out.extend_from_slice(crate::replication::DEMOTED_UNBLOCK_REPLY);
                    return true;
                }
            }
            false
        }
        Command::Object(crate::resp::ObjectSubcommand::Help) => {
            let help_items = [
                "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "ENCODING <key> -- Return the kind of internal representation used in the object stored at <key>.",
                "FREQ <key> -- Return the logarithmic access frequency counter of the object stored at <key>.",
                "IDLETIME <key> -- Return the idle time of the object stored at <key>, in seconds.",
                "REFCOUNT <key> -- Return the number of references of the value at <key>.",
                "HELP -- Print this help.",
            ];
            write_resp_array_header(out, help_items.len());
            for item in help_items {
                write_resp_status(out, item);
            }
            false
        }
        Command::Xinfo(crate::resp::XinfoSubcommand::Help) => {
            let help_items = [
                "CONSUMERS <key> <group> -- Show consumers of <group>.",
                "GROUPS <key> -- Show consumer groups of <key>.",
                "STREAM <key> -- Show information about stream <key>.",
                "HELP -- Print this help.",
            ];
            write_resp_array_header(out, help_items.len());
            for item in help_items {
                write_resp_status(out, item);
            }
            false
        }
        Command::XgroupHelp => {
            let help_items = [
                "CREATE <key> <groupname> <id|$> [option]",
                "    Create a new consumer group. Options are:",
                "    * MKSTREAM",
                "      Create the empty stream if it does not exist.",
                "    * ENTRIESREAD entries_read",
                "      Set the group's entries_read counter (internal use).",
                "CREATECONSUMER <key> <groupname> <consumer>",
                "    Create a new consumer in the specified group.",
                "DELCONSUMER <key> <groupname> <consumer>",
                "    Remove the specified consumer.",
                "DESTROY <key> <groupname>",
                "    Destroy the specified consumer group.",
                "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
                "    Set the group last delivered ID to <id|$>.",
                "HELP",
                "    Print this help.",
            ];
            write_resp_array_header(out, help_items.len());
            for item in help_items {
                write_resp_status(out, item);
            }
            false
        }
        Command::CommandCount => {
            crate::command_info::write_count(out);
            false
        }
        Command::CommandList => {
            crate::command_info::write_list(out, None);
            false
        }
        Command::CommandListFiltered {
            ref filter_type,
            ref filter_val,
        } => {
            crate::command_info::write_list(out, Some((filter_type.as_str(), filter_val.as_str())));
            false
        }
        Command::CommandGetkeys(ref cmd_args) => {
            write_command_getkeys(out, cmd_args);
            false
        }
        Command::CommandGetkeysAndFlags(ref cmd_args) => {
            write_command_getkeys_and_flags(out, cmd_args);
            false
        }
        Command::CommandInfo(ref cmds) => {
            crate::command_info::write_info(out, cmds, CURRENT_CLIENT_RESP3.get());
            false
        }
        Command::Quit => {
            out.extend_from_slice(b"+OK\r\n");
            true
        }
        Command::Unknown(cmd_name) => {
            // No modules can be loaded: the list is always empty.
            if cmd_name == "MODULE LIST" {
                write_resp_array_header(out, 0);
                return false;
            }
            if let Some(cmd_prefix) = cmd_name.strip_suffix(" HELP") {
                let first_line = format!(
                    "{} <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    cmd_prefix
                );
                let help_lines = [first_line.as_str(), "HELP -- Print this help."];
                write_resp_array_header(out, help_lines.len());
                for line in help_lines {
                    write_resp_status(out, line);
                }
                return false;
            }
            let resp = format!("-ERR unknown command '{}'\r\n", err_arg(&cmd_name));
            out.extend_from_slice(resp.as_bytes());
            false
        }
        Command::Memory(sub) => {
            match sub {
                MemorySubcommand::Usage { key } => {
                    out.extend_from_slice(&memory_usage_reply(router, key).await);
                }
                MemorySubcommand::Stats | MemorySubcommand::Doctor => {
                    let report = crate::allocator::MemoryReport {
                        breakdown: router.memory_breakdown().await,
                        used: router.get_total_used_memory().await,
                        maxmemory: crate::tiering::get_max_memory(router.base_port),
                        alloc: crate::allocator::get_allocator_stats(),
                    };
                    if matches!(sub, MemorySubcommand::Stats) {
                        crate::allocator::format_memory_stats(&report, out);
                    } else {
                        write_resp_bulk(out, crate::allocator::memory_doctor(&report).as_bytes());
                    }
                }
                MemorySubcommand::Purge | MemorySubcommand::Defrag => {
                    let _ = router.active_defrag().await;
                    out.extend_from_slice(b"+OK\r\n");
                }
                MemorySubcommand::MallocStats => {
                    write_resp_bulk(out, crate::allocator::malloc_stats().as_bytes());
                }
            }
            false
        }
        Command::Latency(sub) => {
            if matches!(sub, LatencySubcommand::Histogram(_)) {
                router.flush_all_command_stats().await;
            }
            write_latency_response(&sub, out);
            false
        }
        Command::Debug(ref args) => {
            if let Some(sub) = args.first() {
                if sub.eq_ignore_ascii_case(b"object") {
                    if let Some(key) = args.get(1) {
                        let shard_id = router.target_shard(key);
                        if shard_id == router.shard_id {
                            let mut db = router.local_db.borrow_mut();
                            // Like Redis, DEBUG OBJECT looks at the keyspace
                            // as stored: an expired key not yet deleted shows.
                            let _raw = ReplayingWrites::new();
                            if let Some(enc) = db.object_encoding(key) {
                                let (lru, idle) = db.lru_and_idletime(key).unwrap_or((0, 0));
                                out.extend_from_slice(format!("+Value at:0x12345678 refcount:1 encoding:{} serializedlength:10 lru:{} lru_seconds_idle:{}\r\n", enc, lru, idle).as_bytes());
                            } else {
                                out.extend_from_slice(b"-ERR no such key\r\n");
                            }
                        } else {
                            let res = router.execute_remote(shard_id, cmd).await;
                            out.extend_from_slice(&res);
                        }
                        return false;
                    }
                } else if sub.eq_ignore_ascii_case(b"pause-cron") {
                    let flag = args.get(1).map(|v| v.as_ref() == b"1").unwrap_or(false);
                    PAUSE_CRON.store(flag, std::sync::atomic::Ordering::Relaxed);
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"set-allow-access-expired") {
                    let flag = args.get(1).map(|v| v.as_ref() == b"1").unwrap_or(false);
                    ALLOW_ACCESS_EXPIRED.store(flag, std::sync::atomic::Ordering::Relaxed);
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"set-skip-checksum-validation") {
                    let flag = args.get(1).map(|v| v.as_ref() == b"1").unwrap_or(false);
                    crate::table::SKIP_DUMP_CHECKSUM.store(flag, std::sync::atomic::Ordering::Relaxed);
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"change-repl-id") {
                    crate::replication::cached_hub(router.base_port).change_replid();
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"cross-shard-push-delay")
                    || sub.eq_ignore_ascii_case(b"cross-shard-move-timeout")
                {
                    let knob = if sub.eq_ignore_ascii_case(b"cross-shard-push-delay") {
                        &CROSS_SHARD_PUSH_DELAY_US
                    } else {
                        &CROSS_SHARD_MOVE_TIMEOUT_MS
                    };
                    match args.get(1).and_then(|v| std::str::from_utf8(v).ok()?.parse::<u64>().ok()) {
                        Some(v) => {
                            knob.store(v, std::sync::atomic::Ordering::Relaxed);
                            out.extend_from_slice(b"+OK\r\n");
                        }
                        None => out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n"),
                    }
                    return false;
                } else if sub.eq_ignore_ascii_case(b"cross-shard-move-delay") {
                    match args.get(1).and_then(|v| std::str::from_utf8(v).ok()?.parse::<u64>().ok()) {
                        Some(us) => {
                            CROSS_SHARD_MOVE_DELAY_US.store(us, std::sync::atomic::Ordering::Relaxed);
                            out.extend_from_slice(b"+OK\r\n");
                        }
                        None => out.extend_from_slice(b"-ERR value is not an integer or out of range\r\n"),
                    }
                    return false;
                } else if sub.eq_ignore_ascii_case(b"segfault") {
                    // As in Redis: crash, to test the crash report
                    // (`crate::crash`). DEBUG is refused unless
                    // enable-debug-command allows it.
                    crate::log_warning!("DEBUG SEGFAULT: crashing the server on request");
                    // SAFETY: raise(3) with a valid signal; the crash
                    // handler reports it and the process dies by it.
                    unsafe {
                        libc::raise(libc::SIGSEGV);
                    }
                    return true;
                } else if sub.eq_ignore_ascii_case(b"asm-failpoint") {
                    crate::asm::set_fail_point(
                        args.get(1).map_or(&b""[..], |a| a),
                        args.get(2).map_or(&b""[..], |a| a),
                    );
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"repl-pause") {
                    let point = args.get(1).map_or(&b""[..], |a| a);
                    crate::asm::set_repl_pause(if point.eq_ignore_ascii_case(b"clear") {
                        b""
                    } else {
                        point
                    });
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"asm-trim-method") {
                    match crate::asm::set_trim_method(
                        args.get(1).map_or(&b""[..], |a| a),
                        args.get(2).map(|a| a.as_ref()),
                    ) {
                        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => out.extend_from_slice(format!("-{e}\r\n").as_bytes()),
                    }
                    return false;
                } else if sub.eq_ignore_ascii_case(b"set-active-expire") {
                    let enabled = args.get(1).map(|v| v.as_ref() != b"0").unwrap_or(true);
                    PAUSE_CRON.store(!enabled, std::sync::atomic::Ordering::Relaxed);
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"sleep") && args.len() == 2 {
                    // Like Redis' single thread, every shard stalls.
                    if let Ok(s) = std::str::from_utf8(&args[1])
                        && let Ok(secs) = s.parse::<f64>()
                        && secs.is_finite()
                        && secs > 0.0
                    {
                        router
                            .debug_sleep_all(
                                std::time::Duration::from_secs_f64(secs),
                                args[1].clone(),
                            )
                            .await;
                    }
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"panic") {
                    panic!("DEBUG PANIC requested by client");
                } else if sub.eq_ignore_ascii_case(b"digest") && args.len() == 1 {
                    let d = router.debug_digest().await;
                    crate::debug_cmd::write_digest(out, &d);
                    return false;
                } else if sub.eq_ignore_ascii_case(b"digest-value") {
                    let digests = router.debug_digest_values(&args[1..]).await;
                    crate::debug_cmd::write_digest_array(out, &digests);
                    return false;
                } else if sub.eq_ignore_ascii_case(b"reload") {
                    match crate::debug_cmd::parse_reload_options(&args[1..]) {
                        Err(e) => write_resp_err(out, e),
                        Ok(save) => match router.debug_reload(save).await {
                            Ok(()) => {
                                println!("DB reloaded by DEBUG RELOAD");
                                out.extend_from_slice(b"+OK\r\n");
                            }
                            Err(e) => {
                                eprintln!("DEBUG RELOAD failed: {}", e);
                                out.extend_from_slice(
                                    b"-ERR Error trying to load the RDB dump, check server logs.\r\n",
                                );
                            }
                        },
                    }
                    return false;
                } else if sub.eq_ignore_ascii_case(b"loadaof") && args.len() == 1 {
                    match router.debug_loadaof().await {
                        Ok(()) => {
                            println!("Append Only File loaded by DEBUG LOADAOF");
                            out.extend_from_slice(b"+OK\r\n");
                        }
                        Err(e) => write_resp_err(out, e),
                    }
                    return false;
                } else if sub.eq_ignore_ascii_case(b"populate") && (2..=4).contains(&args.len()) {
                    let count = crate::debug_cmd::parse_non_negative(&args[1]);
                    let size = args
                        .get(3)
                        .map(|s| crate::debug_cmd::parse_non_negative(s))
                        .transpose();
                    match (count, size) {
                        (Err(e), _) | (_, Err(e)) => write_resp_err(out, e),
                        (Ok(count), Ok(size)) => {
                            let prefix = args
                                .get(2)
                                .cloned()
                                .unwrap_or_else(|| Bytes::from_static(b"key"));
                            router.debug_populate(count, prefix, size).await;
                            out.extend_from_slice(b"+OK\r\n");
                        }
                    }
                    return false;
                } else if sub.eq_ignore_ascii_case(b"replicate") && args.len() >= 2 {
                    // Sent to the replicas only, never executed or logged
                    // to the AOF (Redis uses it to test replica error
                    // handling and data divergence).
                    let bytes = crate::debug_cmd::encode_resp_array(&args[1..]);
                    crate::replication::propagate_shard_bytes(router.base_port, router.shard_id, &bytes);
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if (sub.eq_ignore_ascii_case(b"stringmatch-len")
                    || sub.eq_ignore_ascii_case(b"stringmatch-test"))
                    && args.len() == 1
                {
                    crate::debug_cmd::stringmatch_fuzz();
                    out.extend_from_slice(b"+Apparently Rudis did not crash: test passed\r\n");
                    return false;
                } else if sub.as_ref() == crate::router::DEBUG_DIGEST_SHARD
                    || sub.as_ref() == crate::router::DEBUG_KEYSIZES_SHARD
                    || sub.as_ref() == crate::router::DEBUG_SLOTSTATS_SHARD
                    || sub.as_ref() == crate::router::DEBUG_LOADAOF_SHARD
                    || sub.as_ref() == crate::router::DEBUG_POPULATE_SHARD
                    || sub.as_ref() == crate::router::DEBUG_FLUSH_SHARD
                {
                    // Internal shard messages, not client subcommands.
                    crate::debug_cmd::write_unknown_subcommand(out, sub);
                    return false;
                }
            } else {
                out.extend_from_slice(b"-ERR wrong number of arguments for 'debug' command\r\n");
                return false;
            }
            // HELP, PROTOCOL, ERROR, LOG, the accepted no-op subcommands and
            // the unknown-subcommand error need no shard data beyond this one.
            crate::debug_cmd::execute_shard_debug(
                args,
                &mut router.local_db.borrow_mut(),
                out,
                router.aof().as_deref(),
            );
            false
        }
        Command::DflyFlow { .. } => false,
    }
    }
    .await;

    if let Some(start_len) = reply_mode_start
        && !is_client_reply_cmd
        && initial_reply_mode != crate::resp::ClientReplyMode::On
    {
        out.truncate(start_len);
        if initial_reply_mode == crate::resp::ClientReplyMode::Skip
            && let Some(c) = client_registry.borrow_mut().get_mut(&client_id)
        {
            c.reply_mode = crate::resp::ClientReplyMode::On;
        }
    }
    _slowlog_guard.reply_bytes = out.len().saturating_sub(error_stat_start);
    if cmd_stat_guard.rejected.get() || blocked_rejected_pending(router.port, client_id) {
        // Refused, not failed: `CmdStatGuard` counts it as rejected.
        if let Some(prefix) = out[error_stat_start..]
            .strip_prefix(b"-")
            .and_then(|e| e.split(|&b| b == b' ').next())
            .and_then(|p| std::str::from_utf8(p).ok())
        {
            record_error_stat(prefix, None);
        }
    } else {
        record_reply_error_stat(out, error_stat_start, cmd_stat_guard.name);
    }
    ret
}
