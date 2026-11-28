use super::*;

/// MEMORY USAGE, sized on the shard that owns `key`: a GET from another
/// shard only saw strings, so hashes, lists... there answered nil.
pub(super) async fn memory_usage_reply(router: &Router, key: Bytes) -> Vec<u8> {
    let target = router.target_shard(&key);
    let usage = Command::Memory(crate::resp::MemorySubcommand::Usage { key });
    router.execute_on_shard(target, usage).await
}

/// INFO's reply for `section` (all sections if None): asks the other
/// shards for what only they know (memory, pub/sub, keyspace counts).
/// Also run for INFO from a script (see `scripting::script_info`).
#[inline(never)]
pub(crate) async fn info_reply(router: &Router, section: Option<Bytes>, out: &mut Vec<u8>) {
    let hub = crate::replication::cached_hub(router.base_port);
    let stats = crate::tiering::get_tier_stats(router.base_port);
    let max_mem = crate::tiering::get_max_memory(router.base_port);
    // As in Redis, used_memory includes the memory maxmemory leaves out,
    // and used_memory - mem_not_counted_for_evict is what it is held to.
    let not_counted = stats.not_counted_for_evict().max(0) as usize;
    let used_mem = router.get_total_used_memory().await + not_counted;
    let memory_str = crate::allocator::format_memory_info(
        used_mem,
        not_counted,
        (
            stats.peak_used.load(std::sync::atomic::Ordering::Relaxed),
            stats.peak_time.load(std::sync::atomic::Ordering::Relaxed),
        ),
        max_mem,
        stats.cooled_keys.load(std::sync::atomic::Ordering::Relaxed),
        stats.tiered_keys.load(std::sync::atomic::Ordering::Relaxed),
    ) + &format!(
        "mem_total_replication_buffers:{}\r\n",
        hub.replication_buffer_bytes()
    );
    let storage_str = format!(
        "# Storage\r\ntier_enabled:{}\r\ntier_shards_open:{}\r\nmaxmemory:{}\r\nmaxmemory_human:{}\r\nused_memory:{}\r\nused_memory_human:{}\r\ncooled_keys:{}\r\ntiered_keys:{}\r\ntiered_bytes:{}\r\nram_saved_bytes:{}\r\ndisk_reads:{}\r\ndisk_writes:{}\r\ndead_bytes:{}\r\ndecommit_count:{}\r\ncoalesced_reads:{}\r\nbin_pages:{}\r\ntotal_stashes:{}\r\ntotal_fetches:{}\r\ntotal_deletes:{}\r\nram_hits:{}\r\nram_misses:{}\r\nstreaming_reads:{}\r\noffload_threshold_pct:{}\r\nupload_threshold_pct:{}\r\n",
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
    let slowlog_count =
        crate::slowlog::SLOWLOG_COMMANDS_COUNT.load(std::sync::atomic::Ordering::Relaxed);
    let slowlog_sum_us =
        crate::slowlog::SLOWLOG_COMMANDS_TIME_US_SUM.load(std::sync::atomic::Ordering::Relaxed);
    let slowlog_max_us =
        crate::slowlog::SLOWLOG_COMMANDS_TIME_US_MAX.load(std::sync::atomic::Ordering::Relaxed);
    let (tracking_clients, tracking_total_items, tracking_total_keys, tracking_total_prefixes) =
        get_tracking_info_stats(router.port);
    use crate::server_stats::{Stat, total};
    let (ops_per_sec, in_bps, out_bps) = crate::server_stats::instantaneous();
    let pubsub_channels = router.pubsub_channels(None).await.len();
    let pubsub_patterns = router.pubsub_numpat().await;
    let pubsubshard_channels = router.pubsub_shardchannels(None).await.len();
    let stats_str = format!(
        "# Stats\r\ntotal_connections_received:{}\r\ntotal_commands_processed:{}\r\ninstantaneous_ops_per_sec:{}\r\ntotal_net_input_bytes:{}\r\ntotal_net_output_bytes:{}\r\ninstantaneous_input_kbps:{:.2}\r\ninstantaneous_output_kbps:{:.2}\r\nrejected_connections:{}\r\nsync_full:{}\r\nsync_partial_ok:{}\r\nsync_partial_err:{}\r\nexpired_subkeys:{}\r\nexpired_keys:{}\r\nexpired_keys_active:{}\r\nevicted_keys:{}\r\nevicted_clients:{evicted_clients}\r\nkeyspace_hits:{}\r\nkeyspace_misses:{}\r\npubsub_channels:{}\r\npubsub_patterns:{}\r\npubsubshard_channels:{}\r\nlatest_fork_usec:0\r\ntotal_error_replies:{}\r\nslowlog_commands_count:{}\r\nslowlog_commands_time_ms_sum:{:.2}\r\nslowlog_commands_time_ms_max:{:.2}\r\nmigrate_cached_sockets:{migrate_cached_sockets}\r\ntracking_total_items:{}\r\ntracking_total_keys:{}\r\ntracking_total_prefixes:{}\r\nclient_query_buffer_limit_disconnections:{}\r\nclient_output_buffer_limit_disconnections:{}\r\nacl_access_denied_auth:{}\r\nacl_access_denied_cmd:{}\r\nacl_access_denied_key:{}\r\nacl_access_denied_channel:{}\r\n",
        total(Stat::ConnectionsReceived),
        total(Stat::Commands),
        ops_per_sec,
        total(Stat::NetInputBytes),
        total(Stat::NetOutputBytes),
        in_bps as f64 / 1024.0,
        out_bps as f64 / 1024.0,
        total(Stat::RejectedConnections),
        total(Stat::SyncFull),
        total(Stat::SyncPartialOk),
        total(Stat::SyncPartialErr),
        crate::table::get_expired_subkeys(),
        crate::table::get_expired_keys(),
        crate::table::get_expired_keys_active(),
        crate::table::get_evicted_keys(),
        total(Stat::KeyspaceHits),
        total(Stat::KeyspaceMisses),
        pubsub_channels,
        pubsub_patterns,
        pubsubshard_channels,
        TOTAL_ERROR_REPLIES.load(std::sync::atomic::Ordering::Relaxed),
        slowlog_count,
        slowlog_sum_us as f64 / 1000.0,
        slowlog_max_us as f64 / 1000.0,
        tracking_total_items,
        tracking_total_keys,
        tracking_total_prefixes,
        total(Stat::ClientQueryBufferLimitDisconnections),
        total(Stat::ClientOutputBufferLimitDisconnections),
        total(Stat::AclAccessDeniedAuth),
        total(Stat::AclAccessDeniedCmd),
        total(Stat::AclAccessDeniedKey),
        total(Stat::AclAccessDeniedChannel),
        migrate_cached_sockets = migrate_cached_sockets(),
        evicted_clients = total(Stat::EvictedClients),
    );
    let (blocked_clients_count, total_blocking_keys, total_blocking_keys_on_nokey) = {
        let hub_arc = crate::block::get_block_hub_for_port(router.port);
        let hub = hub_arc.lock();
        (
            hub.blocked_clients_count()
                + paused_clients_count()
                + crate::replication::WAIT_BLOCKED_CLIENTS
                    .load(std::sync::atomic::Ordering::Relaxed),
            hub.blocking_keys_count(),
            hub.blocking_keys_on_nokey_count(),
        )
    };
    // Per-shard connection census. Exposes whether connections are
    // evenly spread across shards; a skewed distribution caps
    // throughput at the busiest shard. See crate::conn_balance.
    let shard_conns: Vec<String> = (0..router.num_shards)
        .map(|s| crate::conn_balance::conn_count(s).to_string())
        .collect();
    let pubsub_clients = router.pubsub_clients().await;
    let (watching_clients, total_watched_keys) = watch_counts(router.port);
    let clients_str = format!(
        "# Clients\r\nconnected_clients:{}\r\nmaxclients:{}\r\nblocked_clients:{}\r\ntracking_clients:{}\r\npubsub_clients:{}\r\nwatching_clients:{}\r\ntotal_watched_keys:{}\r\nactive_clients:{}\r\ntotal_blocking_keys:{}\r\ntotal_blocking_keys_on_nokey:{}\r\nisolated_panics:{}\r\nshard_connections:{}\r\n",
        connected_clients(router.base_port),
        get_max_clients(),
        blocked_clients_count,
        tracking_clients,
        pubsub_clients,
        watching_clients,
        total_watched_keys,
        active_clients(router),
        total_blocking_keys,
        total_blocking_keys_on_nokey,
        get_isolated_panics(),
        shard_conns.join(",")
    );
    let snap = crate::snapshot::state(router.base_port);
    let aof_rw = crate::snapshot::aof_rewrite_state(router.base_port);
    let persistence_str = format!(
        // The fork/COW fields are always 0: snapshots are taken in
        // place, without a child process.
        "# Persistence\r\nloading:{}\r\nasync_loading:{}\r\ncurrent_cow_peak:0\r\ncurrent_cow_size:0\r\ncurrent_cow_size_age:0\r\ncurrent_fork_perc:0.00\r\ncurrent_save_keys_processed:0\r\ncurrent_save_keys_total:0\r\nrdb_changes_since_last_save:{}\r\nrdb_bgsave_in_progress:{}\r\nrdb_last_save_time:{}\r\nrdb_last_bgsave_status:{}\r\nrdb_last_cow_size:0\r\nrdb_last_load_keys_expired:{}\r\nrdb_last_load_keys_loaded:{}\r\naof_enabled:{}\r\naof_rewrite_in_progress:{}\r\naof_rewrite_scheduled:{}\r\naof_last_bgrewrite_status:{}\r\naof_last_write_status:{}\r\naof_last_cow_size:0\r\nmodule_fork_in_progress:0\r\nmodule_fork_last_cow_size:0\r\naof_rewrites:{}\r\naof_rewrites_consecutive_failures:{}\r\naof_rewrite_fallbacks:{}\r\n{}unpropagated_changes:{}\r\n",
        u8::from(crate::replication::is_loading(router.base_port)),
        u8::from(crate::replication::is_async_loading(router.base_port)),
        snap.changes_since_last_save(),
        u8::from(
            RDB_BGSAVE_IN_PROGRESS.load(std::sync::atomic::Ordering::Relaxed)
                || snap.in_progress()
                || crate::replication::sending_snapshot(router.base_port)
        ),
        snap.last_save_unix(),
        if snap.last_save_ok() { "ok" } else { "err" },
        crate::server::rdb_last_load_keys_expired(router.base_port),
        crate::server::rdb_last_load_keys_loaded(router.base_port),
        u8::from(router.aof().is_some()),
        u8::from(aof_rw.in_progress()),
        // The initial rewrite of AOF turned on at runtime, or a
        // BGREWRITEAOF, waiting for a save to end.
        u8::from(
            (crate::aof::aof_enabling(router.base_port) && !aof_rw.in_progress())
                || crate::snapshot::aof_rewrite_scheduled(router.base_port)
                    .load(std::sync::atomic::Ordering::Relaxed)
        ),
        if aof_rw.last_ok() { "ok" } else { "err" },
        if crate::aof::aof_write_misconf(router.port).is_some() {
            "err"
        } else {
            "ok"
        },
        aof_rw.rewrites(),
        aof_rw.consecutive_failures(),
        // Live rewrites that had to fall back to a blocking pass.
        crate::aof::LIVE_REWRITE_FALLBACKS.load(std::sync::atomic::Ordering::Relaxed),
        // Redis lists these only while the AOF is on. The current size
        // counts what the shards appended but haven't flushed yet: Redis
        // writes a command to the AOF before it replies, so a client sees
        // its write in aof_current_size at once.
        if router.aof().is_some() {
            format!(
                "aof_current_size:{}\r\naof_base_size:{}\r\n",
                crate::aof::aof_files_size(&router.db_dir, router.num_shards)
                    + crate::aof::aof_unwritten_bytes(router.base_port, router.num_shards),
                aof_rw.base_size()
            )
        } else {
            String::new()
        },
        crate::aof::unpropagated_changes()
    );
    let cmdstat_str = {
        router.flush_all_command_stats().await;
        let mut s = String::from("# Commandstats\r\n");
        let mut all_cmds = hashbrown::HashSet::new();
        {
            let map = CMD_STATS.read();
            for k in map.keys() {
                all_cmds.insert(k.clone());
            }
        }
        {
            let map = FAILED_CMD_STATS.read();
            for k in map.keys() {
                all_cmds.insert(k.clone());
            }
        }
        {
            let map = REJECTED_CMD_STATS.read();
            for k in map.keys() {
                all_cmds.insert(k.clone());
            }
        }
        let mut entries: Vec<_> = all_cmds.into_iter().collect();
        entries.sort();
        for cmd in entries {
            let stat = CMD_STATS.read().get(&cmd).copied().unwrap_or_default();
            let calls = stat.calls;
            let usec = stat.nanos / 1000;
            let usec_per_call = if calls == 0 {
                0.0
            } else {
                stat.nanos as f64 / 1000.0 / calls as f64
            };
            let failed = FAILED_CMD_STATS.read().get(&cmd).copied().unwrap_or(0);
            let rejected = REJECTED_CMD_STATS.read().get(&cmd).copied().unwrap_or(0);
            if let Some(slow) = crate::slowlog::get_cmd_slow_stat(&cmd) {
                s.push_str(&format!(
                    "cmdstat_{}:calls={},usec={},usec_per_call={:.2},rejected_calls={},failed_calls={},slowlog_count={},slowlog_time_ms_sum={:.2},slowlog_time_ms_max={:.2}\r\n",
                    cmd, calls, usec, usec_per_call, rejected, failed, slow.count, slow.time_ms_sum, slow.time_ms_max
                ));
            } else {
                s.push_str(&format!(
                    "cmdstat_{}:calls={},usec={},usec_per_call={:.2},rejected_calls={},failed_calls={}\r\n",
                    cmd, calls, usec, usec_per_call, rejected, failed
                ));
            }
        }
        s
    };
    let latencystat_str = {
        // Built from the same per-command histograms as LATENCY
        // HISTOGRAM (flushed by the commandstats step above).
        let mut s = String::from("# Latencystats\r\n");
        if LATENCY_TRACKING.load(std::sync::atomic::Ordering::Relaxed) {
            let ps = latency_percentiles();
            let map = CMD_STATS.read();
            let mut entries: Vec<_> = map.iter().filter(|(_, st)| st.calls > 0).collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            for (cmd, st) in entries {
                let fields: Vec<String> = ps
                    .iter()
                    .map(|&p| {
                        format!(
                            "p{}={:.3}",
                            format_percentile(p),
                            latency_percentile_usec(&st.hist, st.calls, p)
                        )
                    })
                    .collect();
                s.push_str(&format!(
                    "latency_percentiles_usec_{}:{}\r\n",
                    cmd,
                    fields.join(",")
                ));
            }
        }
        s
    };
    let errorstat_str = {
        let mut s = String::from("# Errorstats\r\n");
        {
            let map = ERROR_STATS.read();
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by_key(|(k, _)| *k);
            for (prefix, count) in entries {
                s.push_str(&format!("errorstat_{}:count={}\r\n", prefix, count));
            }
        }
        s
    };
    let server_str = format!(
        "# Server\r\nredis_version:7.2.4\r\nredis_git_sha1:{}\r\nredis_git_dirty:{}\r\nredis_build_id:0\r\nredis_mode:{}\r\nos:Linux\r\narch_bits:64\r\nmultiplexing_api:{}\r\n{}rudis_version:{}\r\narch:shared-nothing-{}\r\nshard_id:{}\r\nnum_shards:{}\r\ntcp_port:{}\r\n",
        crate::version::GIT_SHA,
        u8::from(crate::version::GIT_DIRTY),
        if router.cluster_enabled {
            "cluster"
        } else {
            "standalone"
        },
        io_driver_name(),
        server_info_process_fields(),
        env!("CARGO_PKG_VERSION"),
        io_driver_name(),
        router.shard_id,
        router.num_shards,
        router.port,
    );
    let cpu_str = info_cpu_section();
    // `format_info_replication` starts with its own header.
    let repl_str = hub.format_info_replication();
    let sec_raw = section.as_deref().unwrap_or(b"default");
    let sec_text = String::from_utf8_lossy(sec_raw);
    let tokens: Vec<String> = sec_text
        .split_whitespace()
        .map(|t| t.to_lowercase())
        .collect();
    let tokens = if tokens.is_empty() {
        vec!["default".to_string()]
    } else {
        tokens
    };
    let info_str = if tokens.len() == 1 && matches!(tokens[0].as_str(), "metrics" | "prometheus") {
        crate::telemetry::format_prometheus_metrics(router.base_port, used_mem)
    } else {
        let mut expanded: Vec<&str> = Vec::new();
        let mut push_unique = |name: &'static str| {
            if !expanded.contains(&name) {
                expanded.push(name);
            }
        };
        for tok in &tokens {
            match tok.as_str() {
                "default" => {
                    for s in [
                        "server",
                        "clients",
                        "memory",
                        "persistence",
                        "stats",
                        "replication",
                        "cpu",
                        "storage",
                        "errorstats",
                        "cluster",
                        "keyspace",
                    ] {
                        push_unique(s);
                    }
                }
                "all" | "everything" => {
                    for s in [
                        "server",
                        "clients",
                        "memory",
                        "persistence",
                        "stats",
                        "replication",
                        "cpu",
                        "storage",
                        "commandstats",
                        "latencystats",
                        "errorstats",
                        "cluster",
                        "keyspace",
                        "keysizes",
                    ] {
                        push_unique(s);
                    }
                }
                "server" => push_unique("server"),
                "clients" => push_unique("clients"),
                "memory" => push_unique("memory"),
                "persistence" => push_unique("persistence"),
                "stats" => push_unique("stats"),
                "replication" => push_unique("replication"),
                "cpu" => push_unique("cpu"),
                "storage" | "tiered" => push_unique("storage"),
                "commandstats" => push_unique("commandstats"),
                "latencystats" => push_unique("latencystats"),
                "errorstats" => push_unique("errorstats"),
                "cluster" => push_unique("cluster"),
                "keyspace" => push_unique("keyspace"),
                "keysizes" => push_unique("keysizes"),
                _ => {}
            }
        }
        let mut acc = String::new();
        for sec_name in expanded {
            match sec_name {
                "server" => acc.push_str(&server_str),
                "clients" => acc.push_str(&clients_str),
                "memory" => acc.push_str(&memory_str),
                "persistence" => acc.push_str(&persistence_str),
                "stats" => acc.push_str(&stats_str),
                "replication" => acc.push_str(&repl_str),
                "cpu" => acc.push_str(&cpu_str),
                "storage" => acc.push_str(&storage_str),
                "commandstats" => acc.push_str(&cmdstat_str),
                "latencystats" => acc.push_str(&latencystat_str),
                "errorstats" => acc.push_str(&errorstat_str),
                // As in Redis: redis-cli --cluster (and client libraries)
                // check cluster_enabled before treating a node as a cluster
                // node.
                "cluster" => acc.push_str(&format!(
                    "# Cluster\r\ncluster_enabled:{}\r\n",
                    u8::from(router.cluster_enabled)
                )),
                "keysizes" => {
                    // A scan of every shard's keys: only when asked for.
                    acc.push_str(&crate::shard::format_keysizes(&router.keysizes().await));
                }
                "keyspace" => {
                    // Only fan out when the section is requested.
                    let stats_by_db = router.keyspace_stats_by_db().await;
                    acc.push_str("# Keyspace\r\n");
                    for (&db_idx, &(keys, expires, subexpiry)) in stats_by_db.iter() {
                        if keys > 0 {
                            acc.push_str(&format!(
                                "db{}:keys={},expires={},avg_ttl=0,subexpiry={}\r\n",
                                db_idx, keys, expires, subexpiry
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
        acc
    };
    out.extend_from_slice(format!("${}\r\n", info_str.len()).as_bytes());
    out.extend_from_slice(info_str.as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// `CLUSTER SLOT-STATS`: for each slot this node serves that the query
/// selects, `[slot, {metric: value}]`, with `key-count` and the metrics
/// `cluster-slot-stats-enabled` turns on.
pub(super) async fn write_cluster_slot_stats(
    router: &Router,
    query: &crate::slot_stats::Query,
    out: &mut Vec<u8>,
) {
    use crate::slot_stats::{METRICS, Query};
    let flags = crate::slot_stats::flags();
    if let Query::OrderBy { metric, .. } = query
        && METRICS[*metric].1 & flags != METRICS[*metric].1
    {
        out.extend_from_slice(
            format!(
                "-ERR {} is not enabled: set cluster-slot-stats-enabled to report it\r\n",
                METRICS[*metric].0
            )
            .as_bytes(),
        );
        return;
    }
    let owned: Vec<u16> = {
        let hub = crate::cluster::get_cluster_hub(router.base_port);
        // A replica reports its master's slots, as Valkey does.
        let mut ranges = hub.my_slots.read().clone();
        if ranges.is_empty() && *hub.role.read() == "slave" {
            let master = hub.master_id.read().clone();
            if let Some(m) = hub.nodes.read().get(&master) {
                ranges = m.slots.clone();
            }
        }
        let mut v: Vec<u16> = ranges.iter().flat_map(|&(a, b)| a..=b).collect();
        v.sort_unstable();
        v.dedup();
        v
    };
    crate::slot_stats::reset_changed_owners(&owned);
    let stats = router.slot_stats().await;
    // [key-count, cpu, in, out, mem], in METRICS order.
    let values = |s: u16| {
        let v = stats.get(&s).copied().unwrap_or_default();
        [v[0], v[1], v[2], v[3], v[4]]
    };
    let slots = crate::slot_stats::select(query, &owned, &values);
    let shown: Vec<usize> = (0..METRICS.len())
        .filter(|&i| METRICS[i].1 & flags == METRICS[i].1)
        .collect();
    write_resp_array_header(out, slots.len());
    for s in slots {
        let v = values(s);
        write_resp_array_header(out, 2);
        write_resp_integer(out, s as i64);
        write_resp_map_len(out, shown.len());
        for &i in &shown {
            write_resp_bulk(out, METRICS[i].0.as_bytes());
            write_resp_integer(out, v[i] as i64);
        }
    }
}
