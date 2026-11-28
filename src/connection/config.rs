use super::*;

pub(super) fn parse_config_num<T: std::str::FromStr>(name: &str, val: &str) -> Result<T, String> {
    val.trim()
        .parse::<T>()
        .map_err(|_| format!("argument '{}' for '{}' is not a valid number", val, name))
}

/// A `proto-max-bulk-len` value: a memory size, units allowed (`512mb`), as
/// in Redis. CONFIG SET took digits only, though the setting applied below
/// and the config file accept units.
pub(super) fn proto_max_bulk_len_arg(val: &str) -> Option<u64> {
    crate::tiering::parse_memory_bytes(val)
}

pub(super) fn parse_config_bytes(name: &str, val: &str) -> Result<u64, String> {
    crate::tiering::parse_memory_bytes(val).ok_or_else(|| {
        format!(
            "argument '{}' for '{}' is not a valid memory size",
            val, name
        )
    })
}

pub(super) fn parse_config_bool(name: &str, val: &str) -> Result<bool, String> {
    match val.to_ascii_lowercase().as_str() {
        "yes" => Ok(true),
        "no" => Ok(false),
        _ => Err(format!("argument for '{}' must be 'yes' or 'no'", name)),
    }
}

/// Why a `cluster-announce-ip` was refused: it must be an IP address or,
/// as in Redis 8, a hostname.
pub(super) const ANNOUNCE_IP_ERR: &str =
    "must be an IP address or a hostname of alphanumeric characters, hyphens or dots";

/// Applies one runtime-settable config parameter. Shared by `CONFIG SET`
/// (after its own validation pass) and by config-file loading at startup.
///
/// Returns `Ok(true)` when the parameter was applied, `Ok(false)` when rudis
/// has no runtime setting for it, and `Err` when the value is invalid.
pub fn apply_config_value(
    port: u16,
    base_port: u16,
    name: &str,
    val_str: &str,
) -> Result<bool, String> {
    use std::sync::atomic::Ordering;
    match name {
        "proto-max-bulk-len" => {
            crate::resp::set_proto_max_bulk_len(parse_config_bytes(name, val_str)? as usize)
        }
        "maxmemory" => crate::tiering::set_max_memory(port, parse_config_bytes(name, val_str)?),
        "tiered-offload-threshold" => {
            crate::tiering::set_offload_threshold_pct(port, parse_config_num(name, val_str)?)
        }
        "tiered-upload-threshold" => {
            crate::tiering::set_upload_threshold_pct(port, parse_config_num(name, val_str)?)
        }
        "hash-max-listpack-entries" | "hash-max-ziplist-entries" => {
            HASH_MAX_ENTRIES.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "list-max-listpack-size" | "list-max-ziplist-size" => {
            LIST_MAX_LISTPACK_SIZE.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "hash-max-listpack-value" | "hash-max-ziplist-value" => {
            HASH_MAX_VALUE.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "stream-node-max-entries" => crate::table::STREAM_NODE_MAX_ENTRIES
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "stream-idmp-duration" => crate::table::STREAM_IDMP_DURATION.store(
            parse_config_num::<i64>(name, val_str)? as u64,
            Ordering::Relaxed,
        ),
        "stream-idmp-maxsize" => crate::table::STREAM_IDMP_MAXSIZE.store(
            parse_config_num::<i64>(name, val_str)? as usize,
            Ordering::Relaxed,
        ),
        "min-replicas-to-write" | "min-slaves-to-write" => {
            MIN_REPLICAS_TO_WRITE.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "shutdown-timeout" => crate::replication::SHUTDOWN_TIMEOUT_SECS
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "cluster-slot-migration-write-pause-timeout" => crate::asm::WRITE_PAUSE_TIMEOUT_MS
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "cluster-slot-migration-handoff-max-lag-bytes" => crate::asm::HANDOFF_MAX_LAG_BYTES
            .store(parse_config_bytes(name, val_str)?, Ordering::Relaxed),
        "replica-full-sync-buffer-limit" => crate::asm::FULL_SYNC_BUFFER_LIMIT
            .store(parse_config_bytes(name, val_str)?, Ordering::Relaxed),
        "cluster-slot-migration-sync-buffer-drain-timeout" => {
            crate::asm::SYNC_BUFFER_DRAIN_TIMEOUT_MS
                .store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "repl-diskless-sync-delay" => crate::replication::REPL_DISKLESS_SYNC_DELAY
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "repl-diskless-load" => crate::replication::REPL_DISKLESS_LOAD.store(
            crate::replication::parse_repl_diskless_load(val_str)?,
            Ordering::Relaxed,
        ),
        "repl-timeout" => {
            let secs: u64 = parse_config_num(name, val_str)?;
            if secs == 0 {
                return Err(format!("argument for '{}' must be greater than 0", name));
            }
            crate::replication::REPL_TIMEOUT_SECS.store(secs, Ordering::Relaxed)
        }
        "repl-backlog-ttl" => crate::replication::REPL_BACKLOG_TTL
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "repl-backpressure-threshold" => crate::replication::REPL_BACKPRESSURE_THRESHOLD
            .store(parse_config_bytes(name, val_str)?, Ordering::Relaxed),
        "repl-backpressure-max-delay" => crate::replication::REPL_BACKPRESSURE_MAX_DELAY_MS
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "repl-ping-replica-period" | "repl-ping-slave-period" => {
            let secs: u64 = parse_config_num(name, val_str)?;
            if secs == 0 {
                return Err(format!("argument for '{}' must be greater than 0", name));
            }
            crate::replication::REPL_PING_PERIOD.store(secs, Ordering::Relaxed)
        }
        "min-replicas-max-lag" | "min-slaves-max-lag" => {
            MIN_REPLICAS_MAX_LAG.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "slowlog-log-slower-than" | "commandlog-execution-slower-than" => {
            crate::slowlog::SLOWLOG_LOG_SLOWER_THAN
                .store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "slowlog-max-len" | "commandlog-slow-execution-max-len" => {
            crate::slowlog::SLOWLOG_MAX_LEN
                .store(parse_config_num(name, val_str)?, Ordering::Relaxed);
            crate::slowlog::trim_slowlog_buffer();
        }
        "commandlog-request-larger-than" => crate::slowlog::COMMANDLOG_REQUEST_LARGER_THAN
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "commandlog-large-request-max-len" => {
            crate::slowlog::COMMANDLOG_LARGE_REQUEST_MAX_LEN
                .store(parse_config_num(name, val_str)?, Ordering::Relaxed);
            crate::slowlog::trim_large_request_buffer();
        }
        "commandlog-reply-larger-than" => crate::slowlog::COMMANDLOG_REPLY_LARGER_THAN
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "commandlog-large-reply-max-len" => {
            crate::slowlog::COMMANDLOG_LARGE_REPLY_MAX_LEN
                .store(parse_config_num(name, val_str)?, Ordering::Relaxed);
            crate::slowlog::trim_large_reply_buffer();
        }
        "slowlog-entry-max-argc" => crate::slowlog::SLOWLOG_ENTRY_MAX_ARGC
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "slowlog-entry-max-string-len" => crate::slowlog::SLOWLOG_ENTRY_MAX_STRING_LEN
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "client-output-buffer-limit" => {
            set_client_output_buffer_limit_str(val_str).map_err(|e| {
                e.trim_start_matches("-ERR ")
                    .trim_end_matches("\r\n")
                    .to_string()
            })?
        }
        "latency-monitor-threshold" => {
            LATENCY_MONITOR_THRESHOLD.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "maxclients" => set_max_clients(parse_config_num(name, val_str)?),
        "protected-mode" => {
            crate::netsec::set_protected_mode(base_port, parse_config_bool(name, val_str)?)
        }
        "maxmemory-policy" => {
            let p = crate::config::validate_maxmemory_policy(val_str)?;
            set_max_memory_policy(&p);
        }
        "requirepass" => {
            let acl = crate::acl::get_acl_for_port(port);
            acl.write().set_requirepass(val_str);
        }
        "acllog-max-len" => {
            let max_len: usize = parse_config_num(name, val_str)?;
            crate::acl::get_acl_for_port(port).write().acllog_max_len = max_len;
        }
        "acl-pubsub-default" => {
            let acl_arc = crate::acl::get_acl_for_port(port);
            let mut acl = acl_arc.write();
            if val_str.eq_ignore_ascii_case("allchannels") {
                acl.acl_pubsub_default_all = true;
            } else if val_str.eq_ignore_ascii_case("resetchannels") {
                acl.acl_pubsub_default_all = false;
            } else {
                return Err(
                    "acl-pubsub-default must be 'allchannels' or 'resetchannels'".to_string(),
                );
            }
        }
        "aclfile" => {
            let acl_arc = crate::acl::get_acl_for_port(port);
            let mut acl = acl_arc.write();
            if val_str.is_empty() {
                acl.aclfile = None;
            } else {
                acl.aclfile = Some(std::path::PathBuf::from(val_str));
            }
        }
        "notify-keyspace-events" => set_notify_keyspace_events_str(val_str),
        "aof-load-truncated" => {
            crate::aof::set_aof_load_truncated(parse_config_bool(name, val_str)?)
        }
        "lazyfree-lazy-expire" => {
            crate::table::set_lazyfree_lazy_expire(parse_config_bool(name, val_str)?)
        }
        "lazyfree-lazy-server-del" => crate::table::LAZYFREE_LAZY_SERVER_DEL
            .store(parse_config_bool(name, val_str)?, Ordering::Relaxed),
        "replica-lazy-flush" | "slave-lazy-flush" => crate::router::REPLICA_LAZY_FLUSH
            .store(parse_config_bool(name, val_str)?, Ordering::Relaxed),
        "tracking-table-max-keys" => {
            TRACKING_TABLE_MAX_KEYS.store(parse_config_num(name, val_str)?, Ordering::Relaxed);
            if !PAUSE_CRON.load(Ordering::Relaxed) {
                enforce_tracking_max_keys(port);
            }
        }
        "rdb-key-save-delay" => {
            let v: u64 = parse_config_num(name, val_str)?;
            RDB_KEY_SAVE_DELAY.store(v, Ordering::Relaxed);
            if v == 0 {
                RDB_BGSAVE_IN_PROGRESS.store(false, Ordering::Relaxed);
            }
        }
        "repl-backlog-size" => {
            crate::replication::set_repl_backlog_size(parse_config_bytes(name, val_str)? as usize)
        }
        "backup-sealed-ttl" => {
            CONFIG_BACKUP_SEALED_TTL.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "maxmemory-samples" => {
            CONFIG_MAXMEMORY_SAMPLES.store(maxmemory_samples_arg(val_str)?, Ordering::Relaxed)
        }
        "auto-aof-rewrite-percentage" => crate::aof::auto_rewrite_config(base_port)
            .percentage
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "auto-aof-rewrite-min-size" => crate::aof::auto_rewrite_config(base_port)
            .min_size
            .store(parse_config_bytes(name, val_str)?, Ordering::Relaxed),
        "lfu-log-factor" => {
            crate::table::LFU_LOG_FACTOR.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "lfu-decay-time" => {
            crate::table::LFU_DECAY_TIME.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "client-query-buffer-limit" => {
            let limit = parse_config_bytes(name, val_str)?;
            if limit < MIN_CLIENT_QUERY_BUFFER_LIMIT {
                return Err(format!(
                    "argument must be between {} and {} inclusive",
                    MIN_CLIENT_QUERY_BUFFER_LIMIT,
                    i64::MAX
                ));
            }
            CONFIG_CLIENT_QUERY_BUFFER_LIMIT.store(limit, Ordering::Relaxed)
        }
        "key-load-delay" => {
            CONFIG_KEY_LOAD_DELAY.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "lua-time-limit" | "busy-reply-threshold" => {
            CONFIG_LUA_TIME_LIMIT.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "oom-score-adj" | "oom-score-adj-values" => {
            let (mode, values) = if name == "oom-score-adj" {
                (
                    val_str.to_ascii_lowercase(),
                    CONFIG_OOM_SCORE_ADJ_VALUES.read().clone(),
                )
            } else {
                (CONFIG_OOM_SCORE_ADJ.read().clone(), val_str.to_string())
            };
            oom_score_adj_target(&mode, &parse_oom_score_adj_values(&values)?, 0, false)?;
            let old = if name == "oom-score-adj" {
                std::mem::replace(&mut *CONFIG_OOM_SCORE_ADJ.write(), mode)
            } else {
                std::mem::replace(&mut *CONFIG_OOM_SCORE_ADJ_VALUES.write(), values)
            };
            let is_replica = crate::replication::cached_hub(base_port).is_slave();
            if let Err(e) = apply_oom_score_adj(is_replica) {
                // Keep the setting in effect.
                if name == "oom-score-adj" {
                    *CONFIG_OOM_SCORE_ADJ.write() = old;
                } else {
                    *CONFIG_OOM_SCORE_ADJ_VALUES.write() = old;
                }
                return Err(e);
            }
        }
        "cross-shard-spin" => {
            crate::mailbox::set_cross_shard_spin(parse_config_num(name, val_str)?)
        }
        "idle-poll-us" => monoio::set_idle_poll_us(parse_config_num(name, val_str)?),
        "latency-tracking" => {
            LATENCY_TRACKING.store(parse_config_bool(name, val_str)?, Ordering::Relaxed)
        }
        "cross-shard-strict" => set_cross_shard_strict(parse_config_bool(name, val_str)?),
        "dual-channel-replication-enabled" | "repl-rdb-channel" => {
            crate::replication::set_dual_channel_replication(parse_config_bool(name, val_str)?)
        }
        "repl-diskless-sync" => crate::replication::REPL_DISKLESS_SYNC
            .store(parse_config_bool(name, val_str)?, Ordering::Relaxed),
        "latency-tracking-info-percentiles" => {
            let mut ps = Vec::new();
            for t in val_str.split_whitespace() {
                match t.parse::<f64>() {
                    Ok(v) if (0.0..=100.0).contains(&v) => ps.push(v),
                    _ => {
                        return Err(format!(
                            "ERR CONFIG SET failed (possibly related to argument '{}') - percentile should be between 0.0 and 100.0",
                            name
                        ));
                    }
                }
            }
            *LATENCY_PERCENTILES.write() = ps;
        }
        "appendfsync" => crate::aof::set_appendfsync(base_port, val_str)?,
        "save" => {
            crate::config::set_save_points(base_port, crate::config::parse_save_points(val_str)?)
        }
        "maxmemory-clients" => {
            let (bytes, pct) = parse_maxmemory_clients(val_str)?;
            MAXMEMORY_CLIENTS_BYTES.store(bytes, Ordering::Relaxed);
            MAXMEMORY_CLIENTS_PCT.store(pct, Ordering::Relaxed);
            // As CONFIG GET shows it: a percentage, or bytes.
            *CONFIG_MAXMEMORY_CLIENTS.write() = if pct > 0 {
                format!("{pct}%")
            } else {
                bytes.to_string()
            }
        }
        "backupdirname" => *CONFIG_BACKUPDIRNAME.write() = val_str.to_string(),
        "slaveof" | "replicaof" => *CONFIG_SLAVEOF.write() = val_str.to_string(),
        "dbfilename" => crate::config::set_dbfilename(base_port, val_str)?,
        "timeout" => {
            CLIENT_IDLE_TIMEOUT_SECS.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "tcp-keepalive" => {
            TCP_KEEPALIVE_SECS.store(parse_config_num(name, val_str)?, Ordering::Relaxed)
        }
        "replica-serve-stale-data" | "slave-serve-stale-data" => {
            REPLICA_SERVE_STALE_DATA.store(parse_config_bool(name, val_str)?, Ordering::Relaxed)
        }
        "replica-read-only" | "slave-read-only" => {
            REPLICA_READ_ONLY.store(parse_config_bool(name, val_str)?, Ordering::Relaxed)
        }
        "stop-writes-on-bgsave-error" => {
            STOP_WRITES_ON_BGSAVE_ERROR.store(parse_config_bool(name, val_str)?, Ordering::Relaxed)
        }
        "replica-ignore-maxmemory" | "slave-ignore-maxmemory" => {
            REPLICA_IGNORE_MAXMEMORY.store(parse_config_bool(name, val_str)?, Ordering::Relaxed)
        }
        "replica-priority" | "slave-priority" => {
            crate::replication::cached_hub(base_port)
                .replica_priority
                .store(parse_config_num(name, val_str)?, Ordering::Relaxed);
        }
        "replica-announce-ip" | "slave-announce-ip" => {
            let hub = crate::replication::cached_hub(base_port);
            *hub.announce_ip.write() = if val_str.is_empty() {
                None
            } else {
                Some(val_str.to_string())
            };
        }
        "replica-announce-port" | "slave-announce-port" => {
            crate::replication::cached_hub(base_port)
                .announce_port
                .store(parse_config_num(name, val_str)?, Ordering::Relaxed);
        }
        "client-default-resp" => {
            CLIENT_DEFAULT_RESP3.store(parse_client_default_resp(val_str)?, Ordering::Relaxed);
        }
        "cluster-allow-reads-when-down" => {
            crate::cluster::get_cluster_hub(base_port)
                .allow_reads_when_down
                .store(parse_config_bool(name, val_str)?, Ordering::Relaxed);
        }
        "cluster-announce-human-nodename" => {
            if !crate::cluster::valid_human_nodename(val_str) {
                return Err("Human nodename contains an invalid character".to_string());
            }
            *crate::cluster::get_cluster_hub(base_port)
                .human_nodename
                .write() = val_str.to_string();
        }
        "cluster-announce-hostname" => {
            if !crate::cluster::valid_hostname(val_str) {
                return Err(if val_str.len() >= 256 {
                    "Hostnames must be less than 256 characters".to_string()
                } else {
                    "Hostnames may only contain alphanumeric characters, hyphens or dots"
                        .to_string()
                });
            }
            *crate::cluster::get_cluster_hub(base_port)
                .announce_hostname
                .write() = val_str.to_string();
        }
        "cluster-preferred-endpoint-type" => {
            let e = crate::cluster::Endpoint::parse(val_str).ok_or_else(|| {
                "argument must be one of: ip, hostname, unknown-endpoint".to_string()
            })?;
            crate::cluster::get_cluster_hub(base_port)
                .preferred_endpoint
                .store(e as u8, Ordering::Relaxed);
        }
        "cluster-replica-validity-factor" | "cluster-slave-validity-factor" => {
            crate::cluster::get_cluster_hub(base_port)
                .replica_validity_factor
                .store(parse_config_num(name, val_str)?, Ordering::Relaxed);
        }
        "cluster-allow-replica-migration" => {
            crate::cluster::get_cluster_hub(base_port)
                .allow_replica_migration
                .store(parse_config_bool(name, val_str)?, Ordering::Relaxed);
        }
        "cluster-slot-stats-enabled" => crate::slot_stats::set(val_str)?,
        "cluster-require-full-coverage" => {
            crate::cluster::get_cluster_hub(base_port)
                .require_full_coverage
                .store(parse_config_bool(name, val_str)?, Ordering::Relaxed);
        }
        "cluster-node-timeout" => {
            let ms: u64 = parse_config_num(name, val_str)?;
            if ms == 0 {
                return Err("cluster-node-timeout must be positive".to_string());
            }
            crate::cluster::get_cluster_hub(base_port)
                .node_timeout_ms
                .store(ms, Ordering::Relaxed);
        }
        "cluster-announce-ip" => {
            if !val_str.is_empty() && !crate::cluster::valid_announced_ip(val_str) {
                return Err(ANNOUNCE_IP_ERR.to_string());
            }
            *crate::cluster::get_cluster_hub(base_port)
                .announce_ip
                .write() = (!val_str.is_empty()).then(|| val_str.to_string());
        }
        "tls-cluster" => crate::cluster::get_cluster_hub(base_port)
            .tls
            .store(parse_config_bool(name, val_str)?, Ordering::Relaxed),
        "cluster-announce-port" => crate::cluster::get_cluster_hub(base_port)
            .announce_port
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "cluster-announce-bus-port" => crate::cluster::get_cluster_hub(base_port)
            .announce_bus_port
            .store(parse_config_num(name, val_str)?, Ordering::Relaxed),
        "cluster-config-file" => {
            if val_str.is_empty() {
                return Err("Invalid cluster-config-file".to_string());
            }
            let hub = crate::cluster::get_cluster_hub(base_port);
            let new_path = hub
                .config_file
                .read()
                .as_ref()
                .and_then(|p| p.parent().map(|dir| dir.join(val_str)))
                .unwrap_or_else(|| std::path::PathBuf::from(val_str));
            hub.set_config_file(new_path);
        }
        "tls-auth-clients" => {
            let mode = crate::tls::TlsAuthClients::parse(val_str)?;
            let tls_rt = crate::tls::get_tls_runtime(base_port);
            let mut rt = tls_rt.write();
            let prev = rt.auth_clients;
            rt.auth_clients = mode;
            if let Err(e) = rt.rebuild_server_config() {
                rt.auth_clients = prev;
                return Err(format!("Unable to update TLS configuration: {}", e));
            }
        }
        "tls-ca-cert-file" => {
            let tls_rt = crate::tls::get_tls_runtime(base_port);
            let mut rt = tls_rt.write();
            let prev = rt.ca_cert_file.clone();
            rt.ca_cert_file = if val_str.is_empty() {
                None
            } else {
                Some(std::path::PathBuf::from(val_str))
            };
            if let Err(e) = rt.rebuild_server_config() {
                rt.ca_cert_file = prev;
                return Err(format!("Unable to update TLS configuration: {}", e));
            }
        }
        "tls-cert-file" => {
            let tls_rt = crate::tls::get_tls_runtime(base_port);
            let mut rt = tls_rt.write();
            let prev = rt.cert_file.clone();
            rt.cert_file = if val_str.is_empty() {
                None
            } else {
                Some(std::path::PathBuf::from(val_str))
            };
            if let Err(e) = rt.rebuild_server_config() {
                rt.cert_file = prev;
                return Err(format!("Unable to update TLS configuration: {}", e));
            }
        }
        "tls-key-file" => {
            let tls_rt = crate::tls::get_tls_runtime(base_port);
            let mut rt = tls_rt.write();
            let prev = rt.key_file.clone();
            rt.key_file = if val_str.is_empty() {
                None
            } else {
                Some(std::path::PathBuf::from(val_str))
            };
            if let Err(e) = rt.rebuild_server_config() {
                rt.key_file = prev;
                return Err(format!("Unable to update TLS configuration: {}", e));
            }
        }
        "masterauth" => crate::replication::set_masterauth(port, val_str),
        "masteruser" => crate::replication::set_masteruser(port, val_str),
        "jemalloc-bg-thread" => {
            crate::allocator::set_background_thread(parse_config_bool(name, val_str)?);
        }
        "loglevel" => crate::log::set_loglevel(val_str)?,
        "log-format" => crate::log::set_log_format(val_str)?,
        "log-timestamp-format" => crate::log::set_log_timestamp_format(val_str)?,
        // Config file only (immutable in CONFIG SET, as in Redis).
        "logfile" => crate::log::set_logfile(val_str)?,
        "enable-debug-command" => crate::netsec::set_enable_debug_command(
            base_port,
            crate::netsec::ProtectedAction::parse(val_str)?,
        ),
        "enable-module-command" => crate::netsec::set_enable_module_command(
            base_port,
            crate::netsec::ProtectedAction::parse(val_str)?,
        ),
        "databases" => {
            let n: usize = parse_config_num(name, val_str)?;
            if n == 0 {
                return Err("Invalid number of databases".to_string());
            }
            crate::config::set_databases_count(n);
        }
        _ => return Ok(false),
    }
    Ok(true)
}

#[inline(never)]
pub(super) fn write_config_get_reply(router: &Router, patterns: &[Bytes], out: &mut Vec<u8>) {
    let max_mem = crate::tiering::get_max_memory(router.base_port).to_string();
    let offload = crate::tiering::get_offload_threshold_pct(router.base_port).to_string();
    let upload = crate::tiering::get_upload_threshold_pct(router.base_port).to_string();
    let hash_entries = HASH_MAX_ENTRIES
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let hash_value = HASH_MAX_VALUE
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let stream_entries = crate::table::STREAM_NODE_MAX_ENTRIES
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let stream_idmp_dur = crate::table::STREAM_IDMP_DURATION
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let stream_idmp_max = crate::table::STREAM_IDMP_MAXSIZE
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let max_c = get_max_clients().to_string();
    let policy = get_max_memory_policy();
    let slow_than = crate::slowlog::SLOWLOG_LOG_SLOWER_THAN
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let slow_len = crate::slowlog::SLOWLOG_MAX_LEN
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let slow_argc = crate::slowlog::SLOWLOG_ENTRY_MAX_ARGC
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let slow_str = crate::slowlog::SLOWLOG_ENTRY_MAX_STRING_LEN
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let cmdlog_req_than = crate::slowlog::COMMANDLOG_REQUEST_LARGER_THAN
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let cmdlog_req_len = crate::slowlog::COMMANDLOG_LARGE_REQUEST_MAX_LEN
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let cmdlog_rep_than = crate::slowlog::COMMANDLOG_REPLY_LARGER_THAN
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let cmdlog_rep_len = crate::slowlog::COMMANDLOG_LARGE_REPLY_MAX_LEN
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let obuf = format_client_output_buffer_limit_config();
    let acl = crate::acl::get_acl_for_port(router.port);
    let (pass, acllog_max_len, acl_pubsub_default, aclfile_str) = {
        let g = acl.read();
        (
            g.requirepass.clone().unwrap_or_default(),
            g.acllog_max_len.to_string(),
            if g.acl_pubsub_default_all {
                "allchannels".to_string()
            } else {
                "resetchannels".to_string()
            },
            g.aclfile
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
        )
    };
    let layout = crate::aof_manifest::layout_for(&router.db_dir);
    let app = if router.aof().is_some() {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let proto_bulk = crate::resp::get_proto_max_bulk_len().to_string();
    let notify_ev = get_notify_keyspace_events_str();
    let lazy_exp = if crate::table::is_lazyfree_lazy_expire() {
        "yes".to_string()
    } else {
        "no".to_string()
    };
    let latency_thresh = LATENCY_MONITOR_THRESHOLD
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let tracking_max = TRACKING_TABLE_MAX_KEYS
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let port_str = router.port.to_string();
    let repl_backlog = crate::replication::repl_backlog_size().to_string();
    let backup_ttl = CONFIG_BACKUP_SEALED_TTL
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let maxmem_samples = CONFIG_MAXMEMORY_SAMPLES
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let qbuf_limit = CONFIG_CLIENT_QUERY_BUFFER_LIMIT
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let key_load_delay = CONFIG_KEY_LOAD_DELAY
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let rdb_save_delay = RDB_KEY_SAVE_DELAY
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let lua_time_limit = CONFIG_LUA_TIME_LIMIT
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let oom_adj = CONFIG_OOM_SCORE_ADJ.read().clone();
    let oom_adj_vals = CONFIG_OOM_SCORE_ADJ_VALUES.read().clone();
    let save_cfg =
        crate::snapshot::format_save_points(&crate::config::save_points(router.base_port));
    let maxmem_clients = CONFIG_MAXMEMORY_CLIENTS.read().clone();
    let bind_cfg = crate::netsec::format_bind_spec(&crate::netsec::bind_addrs(router.base_port));
    let backup_dir = CONFIG_BACKUPDIRNAME.read().clone();
    let slaveof_cfg = CONFIG_SLAVEOF.read().clone();

    let ro = if REPLICA_READ_ONLY.load(std::sync::atomic::Ordering::Relaxed) {
        "yes"
    } else {
        "no"
    };
    let ignore_maxmem = if REPLICA_IGNORE_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed) {
        "yes"
    } else {
        "no"
    };
    let min_replicas = MIN_REPLICAS_TO_WRITE
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let min_replicas_lag = MIN_REPLICAS_MAX_LAG
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let repl_ping_period = crate::replication::REPL_PING_PERIOD
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let hub = crate::replication::cached_hub(router.base_port);
    let replica_priority = hub
        .replica_priority
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let announce_ip = hub.announce_ip.read().clone().unwrap_or_default();
    let announce_port = hub
        .announce_port
        .load(std::sync::atomic::Ordering::Relaxed)
        .to_string();
    let (tls_auth_clients, tls_ca_cert_file, tls_cert_file, tls_key_file) = {
        let tls_rt = crate::tls::get_tls_runtime(router.base_port);
        let rt = tls_rt.read();
        (
            rt.auth_clients.as_str().to_string(),
            rt.ca_cert_file
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            rt.cert_file
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
            rt.key_file
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default(),
        )
    };
    let cluster_hub = crate::cluster::get_cluster_hub(router.base_port);
    let all_configs: [(&str, String); 129] = [
        ("port", port_str),
        (
            "client-default-resp",
            if client_default_resp3() { "3" } else { "2" }.to_string(),
        ),
        (
            "cluster-allow-reads-when-down",
            if cluster_hub
                .allow_reads_when_down
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        ("cluster-announce-hostname", cluster_hub.my_hostname()),
        (
            "cluster-announce-human-nodename",
            cluster_hub.human_nodename.read().clone(),
        ),
        (
            "cluster-preferred-endpoint-type",
            cluster_hub.preferred_endpoint().name().to_string(),
        ),
        (
            "cluster-replica-validity-factor",
            cluster_hub
                .replica_validity_factor
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "cluster-allow-replica-migration",
            if cluster_hub
                .allow_replica_migration
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        (
            "cluster-slot-stats-enabled",
            crate::slot_stats::config_value(),
        ),
        (
            "cluster-require-full-coverage",
            if cluster_hub
                .require_full_coverage
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        ("tls-auth-clients", tls_auth_clients),
        ("tls-ca-cert-file", tls_ca_cert_file),
        ("tls-cert-file", tls_cert_file),
        ("tls-key-file", tls_key_file),
        ("cluster-config-file", cluster_hub.config_file_name()),
        (
            "cluster-node-timeout",
            cluster_hub
                .node_timeout_ms
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "cluster-announce-ip",
            cluster_hub.announce_ip.read().clone().unwrap_or_default(),
        ),
        (
            "tls-cluster",
            if cluster_hub.tls.load(std::sync::atomic::Ordering::Relaxed) {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        (
            "cluster-announce-port",
            cluster_hub
                .announce_port
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "cluster-announce-bus-port",
            cluster_hub
                .announce_bus_port
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        ("acllog-max-len", acllog_max_len),
        ("acl-pubsub-default", acl_pubsub_default),
        ("aclfile", aclfile_str),
        ("databases", crate::config::databases_count().to_string()),
        ("loglevel", crate::log::loglevel().to_string()),
        ("logfile", crate::log::logfile()),
        ("log-format", crate::log::log_format().to_string()),
        (
            "log-timestamp-format",
            crate::log::log_timestamp_format().to_string(),
        ),
        ("idle-poll-us", monoio::idle_poll_us().to_string()),
        (
            "dual-channel-replication-enabled",
            if crate::replication::dual_channel_replication() {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        (
            "cross-shard-strict",
            if cross_shard_strict() { "yes" } else { "no" }.to_string(),
        ),
        (
            "latency-tracking",
            if LATENCY_TRACKING.load(std::sync::atomic::Ordering::Relaxed) {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        (
            "latency-tracking-info-percentiles",
            latency_percentiles()
                .iter()
                .map(|p| format_percentile(*p))
                .collect::<Vec<_>>()
                .join(" "),
        ),
        (
            "protected-mode",
            if crate::netsec::protected_mode(router.base_port) {
                "yes".to_string()
            } else {
                "no".to_string()
            },
        ),
        ("daemonize", "no".to_string()),
        (
            "enable-debug-command",
            crate::netsec::enable_debug_command(router.base_port)
                .name()
                .to_string(),
        ),
        (
            "enable-module-command",
            crate::netsec::enable_module_command(router.base_port)
                .name()
                .to_string(),
        ),
        ("maxmemory", max_mem),
        ("maxmemory-samples", maxmem_samples),
        (
            "auto-aof-rewrite-percentage",
            crate::aof::auto_rewrite_config(router.base_port)
                .percentage
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "auto-aof-rewrite-min-size",
            crate::aof::auto_rewrite_config(router.base_port)
                .min_size
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "lfu-log-factor",
            crate::table::LFU_LOG_FACTOR
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "lfu-decay-time",
            crate::table::LFU_DECAY_TIME
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        ("maxmemory-clients", maxmem_clients),
        ("maxmemory-policy", policy),
        ("client-query-buffer-limit", qbuf_limit),
        ("repl-backlog-size", repl_backlog),
        ("save", save_cfg),
        ("backup-sealed-ttl", backup_ttl),
        ("backupdirname", backup_dir),
        ("bind", bind_cfg),
        ("slaveof", slaveof_cfg.clone()),
        ("replicaof", slaveof_cfg),
        ("oom-score-adj", oom_adj),
        ("oom-score-adj-values", oom_adj_vals),
        ("busy-reply-threshold", lua_time_limit.clone()),
        ("lua-time-limit", lua_time_limit),
        ("tiered-offload-threshold", offload),
        ("tiered-upload-threshold", upload),
        (
            "list-max-listpack-size",
            LIST_MAX_LISTPACK_SIZE
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "list-max-ziplist-size",
            LIST_MAX_LISTPACK_SIZE
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        ("hash-max-listpack-entries", hash_entries.clone()),
        ("hash-max-ziplist-entries", hash_entries),
        ("hash-max-listpack-value", hash_value.clone()),
        ("hash-max-ziplist-value", hash_value),
        ("stream-node-max-entries", stream_entries),
        ("stream-idmp-duration", stream_idmp_dur),
        ("stream-idmp-maxsize", stream_idmp_max),
        ("maxclients", max_c),
        ("slowlog-log-slower-than", slow_than.clone()),
        ("slowlog-max-len", slow_len.clone()),
        ("commandlog-execution-slower-than", slow_than),
        ("commandlog-slow-execution-max-len", slow_len),
        ("commandlog-request-larger-than", cmdlog_req_than),
        ("commandlog-large-request-max-len", cmdlog_req_len),
        ("commandlog-reply-larger-than", cmdlog_rep_than),
        ("commandlog-large-reply-max-len", cmdlog_rep_len),
        ("slowlog-entry-max-argc", slow_argc),
        ("slowlog-entry-max-string-len", slow_str),
        ("client-output-buffer-limit", obuf),
        ("requirepass", pass),
        ("appendonly", app),
        (
            "aof-multipart",
            if layout.is_some() { "yes" } else { "no" }.to_string(),
        ),
        (
            "appenddirname",
            layout
                .as_ref()
                .and_then(|l| l.dir.file_name())
                .map_or("appendonlydir".to_string(), |n| {
                    n.to_string_lossy().into_owned()
                }),
        ),
        (
            "appendfilename",
            layout
                .as_ref()
                .map_or("appendonly.aof".to_string(), |l| l.name.clone()),
        ),
        (
            "appendfsync",
            crate::aof::appendfsync_name(router.base_port).to_string(),
        ),
        (
            "cross-shard-spin",
            crate::mailbox::cross_shard_spin().to_string(),
        ),
        (
            "aof-load-truncated",
            if crate::aof::aof_load_truncated() {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        ("proto-max-bulk-len", proto_bulk),
        ("notify-keyspace-events", notify_ev),
        ("lazyfree-lazy-expire", lazy_exp),
        (
            "replica-lazy-flush",
            if crate::router::REPLICA_LAZY_FLUSH.load(std::sync::atomic::Ordering::Relaxed) {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        (
            "lazyfree-lazy-server-del",
            if crate::table::LAZYFREE_LAZY_SERVER_DEL.load(std::sync::atomic::Ordering::Relaxed) {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        ("latency-monitor-threshold", latency_thresh),
        ("tracking-table-max-keys", tracking_max),
        (
            "dir",
            std::fs::canonicalize(&router.db_dir)
                .unwrap_or_else(|_| router.db_dir.clone())
                .display()
                .to_string(),
        ),
        ("dbfilename", crate::config::dbfilename(router.base_port)),
        (
            "timeout",
            CLIENT_IDLE_TIMEOUT_SECS
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "tcp-keepalive",
            TCP_KEEPALIVE_SECS
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "stop-writes-on-bgsave-error",
            if STOP_WRITES_ON_BGSAVE_ERROR.load(std::sync::atomic::Ordering::Relaxed) {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        ("replica-read-only", ro.to_string()),
        ("slave-read-only", ro.to_string()),
        (
            "replica-serve-stale-data",
            if REPLICA_SERVE_STALE_DATA.load(std::sync::atomic::Ordering::Relaxed) {
                "yes"
            } else {
                "no"
            }
            .to_string(),
        ),
        ("replica-ignore-maxmemory", ignore_maxmem.to_string()),
        ("slave-ignore-maxmemory", ignore_maxmem.to_string()),
        ("replica-priority", replica_priority.clone()),
        ("slave-priority", replica_priority),
        ("replica-announce-ip", announce_ip.clone()),
        ("slave-announce-ip", announce_ip),
        ("replica-announce-port", announce_port.clone()),
        ("slave-announce-port", announce_port),
        ("min-replicas-to-write", min_replicas.clone()),
        ("min-slaves-to-write", min_replicas),
        ("min-replicas-max-lag", min_replicas_lag.clone()),
        ("min-slaves-max-lag", min_replicas_lag),
        (
            "repl-backlog-ttl",
            crate::replication::REPL_BACKLOG_TTL
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        ("repl-ping-replica-period", repl_ping_period.clone()),
        ("repl-ping-slave-period", repl_ping_period),
        (
            "repl-timeout",
            crate::replication::REPL_TIMEOUT_SECS
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "repl-backpressure-threshold",
            crate::replication::REPL_BACKPRESSURE_THRESHOLD
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "repl-backpressure-max-delay",
            crate::replication::REPL_BACKPRESSURE_MAX_DELAY_MS
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "shutdown-timeout",
            crate::replication::SHUTDOWN_TIMEOUT_SECS
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "repl-diskless-load",
            crate::replication::repl_diskless_load_name().to_string(),
        ),
        (
            "cluster-slot-migration-write-pause-timeout",
            crate::asm::WRITE_PAUSE_TIMEOUT_MS
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "cluster-slot-migration-handoff-max-lag-bytes",
            crate::asm::HANDOFF_MAX_LAG_BYTES
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "cluster-slot-migration-sync-buffer-drain-timeout",
            crate::asm::SYNC_BUFFER_DRAIN_TIMEOUT_MS
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "replica-full-sync-buffer-limit",
            crate::asm::FULL_SYNC_BUFFER_LIMIT
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "repl-diskless-sync-delay",
            crate::replication::REPL_DISKLESS_SYNC_DELAY
                .load(std::sync::atomic::Ordering::Relaxed)
                .to_string(),
        ),
        (
            "repl-diskless-sync",
            yes_no(
                crate::replication::REPL_DISKLESS_SYNC.load(std::sync::atomic::Ordering::Relaxed),
            ),
        ),
    ];
    // As in Redis, which hides repl-rdb-channel from CONFIG GET *.
    let hidden_configs: [(&str, String); 3] = [
        ("key-load-delay", key_load_delay),
        ("rdb-key-save-delay", rdb_save_delay),
        (
            "repl-rdb-channel",
            yes_no(crate::replication::dual_channel_replication()),
        ),
    ];

    let mut matched: Vec<(&str, &str)> = Vec::new();
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();

    for pat in patterns {
        let pat_str = String::from_utf8_lossy(pat).to_lowercase();
        let is_exact = !pat_str.contains('*') && !pat_str.contains('?') && !pat_str.contains('[');
        for (name, val) in &all_configs {
            if ((is_exact && pat_str == *name)
                || (!is_exact && crate::pubsub::glob_match(pat_str.as_bytes(), name.as_bytes())))
                && seen.insert(*name)
            {
                matched.push((*name, val.as_str()));
            }
        }
        if is_exact {
            for (name, val) in &hidden_configs {
                if pat_str == *name && seen.insert(*name) {
                    matched.push((*name, val.as_str()));
                }
            }
        }
    }

    write_resp_map_len(out, matched.len());
    for (k, v) in matched {
        out.extend_from_slice(format!("${}\r\n{}\r\n", k.len(), k).as_bytes());
        out.extend_from_slice(format!("${}\r\n{}\r\n", v.len(), v).as_bytes());
    }
}

pub(super) fn apply_config_set_pairs(router: &Router, pairs: &[(Bytes, Bytes)], out: &mut Vec<u8>) {
    // Phase 1: Validate all pairs and check for duplicates before applying any change
    let mut seen_canonical: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (param, val) in pairs {
        let p_str = String::from_utf8_lossy(param).to_lowercase();
        let val_str = String::from_utf8_lossy(val);
        let canonical = match p_str.as_str() {
            "busy-reply-threshold" | "lua-time-limit" => "lua-time-limit",
            "slaveof" | "replicaof" => "replicaof",
            "slave-read-only" | "replica-read-only" => "replica-read-only",
            "slave-serve-stale-data" | "replica-serve-stale-data" => "replica-serve-stale-data",
            "slave-ignore-maxmemory" | "replica-ignore-maxmemory" => "replica-ignore-maxmemory",
            "slave-priority" | "replica-priority" => "replica-priority",
            "slave-announce-ip" | "replica-announce-ip" => "replica-announce-ip",
            "slave-announce-port" | "replica-announce-port" => "replica-announce-port",
            "min-slaves-to-write" | "min-replicas-to-write" => "min-replicas-to-write",
            "min-slaves-max-lag" | "min-replicas-max-lag" => "min-replicas-max-lag",
            "hash-max-ziplist-entries" | "hash-max-listpack-entries" => "hash-max-listpack-entries",
            "list-max-ziplist-size" | "list-max-listpack-size" => "list-max-listpack-size",
            "hash-max-ziplist-value" | "hash-max-listpack-value" => "hash-max-listpack-value",
            other => other,
        };
        if !seen_canonical.insert(canonical.to_string()) {
            out.extend_from_slice(
                b"-ERR Cannot set duplicate config parameters in a single CONFIG SET call\r\n",
            );
            return;
        }

        if matches!(
            p_str.as_str(),
            "daemonize"
                | "logfile"
                | "databases"
                | "enable-debug-command"
                | "enable-module-command"
        ) {
            out.extend_from_slice(
                format!(
                    "-ERR CONFIG SET failed (possibly related to argument '{}') - can't set immutable config\r\n",
                    err_arg(&p_str)
                )
                .as_bytes(),
            );
            return;
        } else if let Some(Err(e)) = match p_str.as_str() {
            "loglevel" => Some(crate::log::validate_loglevel(&val_str)),
            "log-format" => Some(crate::log::validate_log_format(&val_str)),
            "log-timestamp-format" => Some(crate::log::validate_log_timestamp_format(&val_str)),
            _ => None,
        } {
            out.extend_from_slice(
                format!(
                    "-ERR CONFIG SET failed (possibly related to argument '{}') - {}\r\n",
                    err_arg(&p_str),
                    err_arg(&e)
                )
                .as_bytes(),
            );
            return;
        } else if matches!(
            p_str.as_str(),
            "repl-timeout" | "repl-ping-replica-period" | "repl-ping-slave-period"
        ) && !val_str.parse::<u64>().is_ok_and(|v| v > 0)
        {
            out.extend_from_slice(
                format!(
                    "-ERR CONFIG SET failed (possibly related to argument '{}') - argument must be greater than 0\r\n",
                    err_arg(&p_str)
                )
                .as_bytes(),
            );
            return;
        } else if p_str == "aof-load-truncated" {
            if !matches!(val_str.to_ascii_lowercase().as_str(), "yes" | "no") {
                out.extend_from_slice(
                    b"-ERR CONFIG SET failed (possibly related to argument 'aof-load-truncated') - argument must be 'yes' or 'no'\r\n",
                );
                return;
            }
        } else if p_str == "appendonly" {
            let requested = match val_str.to_ascii_lowercase().as_str() {
                "yes" => true,
                "no" => false,
                _ => {
                    out.extend_from_slice(
                        b"-ERR CONFIG SET failed (possibly related to argument 'appendonly') - argument must be 'yes' or 'no'\r\n",
                    );
                    return;
                }
            };
            // Applied by the caller once every pair is valid (it is async:
            // see `Router::enable_aof`).
            let _ = requested;
        } else if p_str == "cross-shard-spin" {
            if val_str.parse::<usize>().is_err() {
                out.extend_from_slice(
                    b"-ERR CONFIG SET failed (possibly related to argument 'cross-shard-spin') - argument must be a non-negative integer\r\n",
                );
                return;
            }
        } else if p_str == "appendfsync" {
            if let Err(e) = crate::aof::parse_appendfsync(&val_str) {
                out.extend_from_slice(
                    format!(
                        "-ERR CONFIG SET failed (possibly related to argument 'appendfsync') - {}\r\n",
                        e
                    )
                    .as_bytes(),
                );
                return;
            }
        } else if matches!(
            p_str.as_str(),
            "timeout" | "tcp-keepalive" | "replica-priority" | "slave-priority"
        ) {
            if val_str.parse::<u64>().is_err() {
                out.extend_from_slice(
                    format!(
                        "-ERR CONFIG SET failed (possibly related to argument '{}') - argument couldn't be parsed into an integer\r\n",
                        err_arg(&p_str)
                    )
                    .as_bytes(),
                );
                return;
            }
        } else if p_str == "cluster-announce-ip" {
            if !val_str.is_empty() && !crate::cluster::valid_announced_ip(&val_str) {
                out.extend_from_slice(
                    format!(
                        "-ERR CONFIG SET failed (possibly related to argument 'cluster-announce-ip') - {ANNOUNCE_IP_ERR}\r\n"
                    )
                    .as_bytes(),
                );
                return;
            }
        } else if matches!(
            p_str.as_str(),
            "replica-announce-port"
                | "slave-announce-port"
                | "cluster-announce-port"
                | "cluster-announce-bus-port"
        ) {
            if val_str.parse::<u16>().is_err() {
                out.extend_from_slice(
                    format!(
                        "-ERR CONFIG SET failed (possibly related to argument '{}') - argument couldn't be parsed into an integer\r\n",
                        err_arg(&p_str)
                    )
                    .as_bytes(),
                );
                return;
            }
        } else if matches!(
            p_str.as_str(),
            "replica-read-only"
                | "slave-read-only"
                | "replica-serve-stale-data"
                | "slave-serve-stale-data"
                | "replica-ignore-maxmemory"
                | "slave-ignore-maxmemory"
                | "stop-writes-on-bgsave-error"
                | "tls-cluster"
        ) {
            if !matches!(val_str.to_ascii_lowercase().as_str(), "yes" | "no") {
                out.extend_from_slice(
                    format!(
                        "-ERR CONFIG SET failed (possibly related to argument '{}') - argument must be 'yes' or 'no'\r\n",
                        err_arg(&p_str)
                    )
                    .as_bytes(),
                );
                return;
            }
        } else if p_str == "dbfilename" {
            if let Err(e) = crate::config::validate_dbfilename(&val_str) {
                out.extend_from_slice(
                    format!(
                        "-ERR CONFIG SET failed (possibly related to argument 'dbfilename') - {}\r\n",
                        e
                    )
                    .as_bytes(),
                );
                return;
            }
        } else if p_str == "save" {
            if crate::config::parse_save_points(&val_str).is_err() {
                out.extend_from_slice(
                    b"-ERR CONFIG SET failed (possibly related to argument 'save') - Invalid save parameters\r\n",
                );
                return;
            }
        } else if p_str == "protected-mode" {
            if !matches!(val_str.to_ascii_lowercase().as_str(), "yes" | "no") {
                out.extend_from_slice(
                    b"-ERR CONFIG SET failed (possibly related to argument 'protected-mode') - argument must be 'yes' or 'no'\r\n",
                );
                return;
            }
        } else if p_str == "bind" {
            let current = crate::netsec::bind_addrs(router.base_port);
            match crate::netsec::parse_bind_spec(&val_str) {
                Ok(requested) if requested == current => {}
                Ok(_) => {
                    out.extend_from_slice(
                        b"-ERR CONFIG SET failed (possibly related to argument 'bind') - changing bind at runtime is not supported; set it in the config file and restart\r\n",
                    );
                    return;
                }
                Err(_) => {
                    out.extend_from_slice(
                        b"-ERR CONFIG SET failed (possibly related to argument 'bind') - Failed to bind to specified addresses.\r\n",
                    );
                    return;
                }
            }
        } else if p_str == "maxmemory-clients" {
            if let Some(pct_str) = val_str.strip_suffix('%') {
                match pct_str.parse::<i64>() {
                    Ok(pct) if (0..=100).contains(&pct) => {}
                    _ => {
                        out.extend_from_slice(
                            b"-ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - percentage argument must be less or equal to 100\r\n",
                        );
                        return;
                    }
                }
            } else if crate::tiering::parse_memory_bytes(&val_str).is_none() {
                out.extend_from_slice(
                    b"-ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - invalid maxmemory-clients\r\n",
                );
                return;
            }
        } else if p_str == "client-query-buffer-limit" {
            match crate::tiering::parse_memory_bytes(&val_str) {
                None => {
                    out.extend_from_slice(
                        b"-ERR CONFIG SET failed (possibly related to argument 'client-query-buffer-limit') - invalid client-query-buffer-limit\r\n",
                    );
                    return;
                }
                Some(v) if v < MIN_CLIENT_QUERY_BUFFER_LIMIT => {
                    out.extend_from_slice(
                        format!(
                            "-ERR CONFIG SET failed (possibly related to argument 'client-query-buffer-limit') - argument must be between {} and {} inclusive\r\n",
                            MIN_CLIENT_QUERY_BUFFER_LIMIT,
                            i64::MAX
                        )
                        .as_bytes(),
                    );
                    return;
                }
                Some(_) => {}
            }
        } else if p_str == "port" {
            match val_str.parse::<u16>() {
                Ok(new_port) => {
                    if new_port != 0
                        && new_port != router.port
                        && let Err(e) = std::net::TcpListener::bind(("127.0.0.1", new_port))
                    {
                        out.extend_from_slice(
                            format!(
                                "-ERR CONFIG SET failed (possibly related to argument 'port') - Unable to listen on this port: {}\r\n",
                                e
                            )
                            .as_bytes(),
                        );
                        return;
                    }
                }
                Err(_) => {
                    out.extend_from_slice(
                        b"-ERR CONFIG SET failed (possibly related to argument 'port') - Invalid argument\r\n",
                    );
                    return;
                }
            }
        } else if p_str == "proto-max-bulk-len" {
            if proto_max_bulk_len_arg(&val_str).is_none() {
                out.extend_from_slice(
                    b"-ERR Invalid argument for CONFIG SET proto-max-bulk-len\r\n",
                );
                return;
            }
        } else if p_str == "maxmemory" {
            if crate::tiering::parse_memory_bytes(&val_str).is_none() {
                out.extend_from_slice(
                    b"-ERR CONFIG SET failed (Invalid argument for 'maxmemory')\r\n",
                );
                return;
            }
        } else if p_str == "maxmemory-policy" {
            if let Err(e) = crate::config::validate_maxmemory_policy(&val_str) {
                out.extend_from_slice(
                    format!(
                        "-ERR CONFIG SET failed (possibly related to argument 'maxmemory-policy') - {}\r\n",
                        e
                    )
                    .as_bytes(),
                );
                return;
            }
        } else if p_str == "tiered-offload-threshold" {
            if val_str.parse::<u64>().is_err() {
                out.extend_from_slice(
                    b"-ERR Invalid argument for CONFIG SET tiered-offload-threshold\r\n",
                );
                return;
            }
        } else if p_str == "tiered-upload-threshold" {
            if val_str.parse::<u64>().is_err() {
                out.extend_from_slice(
                    b"-ERR Invalid argument for CONFIG SET tiered-upload-threshold\r\n",
                );
                return;
            }
        } else if p_str == "list-max-listpack-size" || p_str == "list-max-ziplist-size" {
            if val_str.parse::<i64>().is_err() {
                out.extend_from_slice(b"-ERR Invalid argument for CONFIG SET\r\n");
                return;
            }
        } else if p_str == "hash-max-listpack-entries"
            || p_str == "hash-max-ziplist-entries"
            || p_str == "hash-max-listpack-value"
            || p_str == "hash-max-ziplist-value"
        {
            if val_str.parse::<usize>().is_err() {
                out.extend_from_slice(b"-ERR Invalid argument for CONFIG SET\r\n");
                return;
            }
        } else if p_str == "stream-node-max-entries" {
            match val_str.parse::<usize>() {
                Ok(n) if n > 0 => {}
                Ok(_) => {
                    out.extend_from_slice(
                        b"-ERR stream-node-max-entries must be a positive integer\r\n",
                    );
                    return;
                }
                Err(_) => {
                    out.extend_from_slice(b"-ERR Invalid argument for CONFIG SET\r\n");
                    return;
                }
            }
        } else if p_str == "stream-idmp-duration" {
            match val_str.parse::<i64>() {
                Ok(n) if (1..=86400).contains(&n) => {}
                _ => {
                    out.extend_from_slice(
                        b"-ERR stream-idmp-duration must be between 1 and 86400\r\n",
                    );
                    return;
                }
            }
        } else if p_str == "stream-idmp-maxsize" {
            match val_str.parse::<i64>() {
                Ok(n) if (1..=10000).contains(&n) => {}
                _ => {
                    out.extend_from_slice(
                        b"-ERR stream-idmp-maxsize must be between 1 and 10000\r\n",
                    );
                    return;
                }
            }
        } else if matches!(
            p_str.as_str(),
            "min-replicas-to-write"
                | "min-slaves-to-write"
                | "min-replicas-max-lag"
                | "min-slaves-max-lag"
        ) {
            if val_str.parse::<u32>().is_err() {
                out.extend_from_slice(
                    format!("-ERR Invalid argument for CONFIG SET {p_str}\r\n").as_bytes(),
                );
                return;
            }
        } else if matches!(
            p_str.as_str(),
            "slowlog-log-slower-than"
                | "commandlog-execution-slower-than"
                | "commandlog-request-larger-than"
                | "commandlog-reply-larger-than"
        ) {
            if val_str.parse::<i64>().is_err() {
                out.extend_from_slice(
                    format!("-ERR Invalid argument for CONFIG SET {p_str}\r\n").as_bytes(),
                );
                return;
            }
        } else if matches!(
            p_str.as_str(),
            "slowlog-max-len"
                | "commandlog-slow-execution-max-len"
                | "commandlog-large-request-max-len"
                | "commandlog-large-reply-max-len"
        ) {
            if val_str.parse::<usize>().is_err() {
                out.extend_from_slice(
                    format!("-ERR Invalid argument for CONFIG SET {p_str}\r\n").as_bytes(),
                );
                return;
            }
        } else if p_str == "slowlog-entry-max-argc" {
            match val_str.parse::<usize>() {
                Ok(v) if v >= 2 => {}
                _ => {
                    out.extend_from_slice(b"-ERR argument must be between 2 and 2147483647\r\n");
                    return;
                }
            }
        } else if p_str == "slowlog-entry-max-string-len" {
            match val_str.parse::<usize>() {
                Ok(v) if v >= 1 => {}
                _ => {
                    out.extend_from_slice(b"-ERR argument must be between 1 and 2147483647\r\n");
                    return;
                }
            }
        } else if p_str == "client-output-buffer-limit" {
            if let Err(err) = set_client_output_buffer_limit_str(&val_str) {
                out.extend_from_slice(err.as_bytes());
                return;
            }
        } else if let Some(Err(e)) = match p_str.as_str() {
            "oom-score-adj" => Some(oom_score_adj_target(&val_str, &[0; 3], 0, false).map(|_| ())),
            "oom-score-adj-values" => Some(parse_oom_score_adj_values(&val_str).map(|_| ())),
            _ => None,
        } {
            out.extend_from_slice(
                format!(
                    "-ERR CONFIG SET failed (possibly related to argument '{}') - {}\r\n",
                    err_arg(&p_str),
                    err_arg(&e)
                )
                .as_bytes(),
            );
            return;
        } else if p_str == "maxmemory-samples" {
            if let Err(e) = maxmemory_samples_arg(&val_str) {
                out.extend_from_slice(
                    format!(
                        "-ERR CONFIG SET failed (possibly related to argument 'maxmemory-samples') - {}\r\n",
                        e
                    )
                    .as_bytes(),
                );
                return;
            }
        } else if p_str == "latency-monitor-threshold" {
            if val_str.parse::<i64>().is_err() {
                out.extend_from_slice(
                    b"-ERR Invalid argument for CONFIG SET latency-monitor-threshold\r\n",
                );
                return;
            }
        } else if p_str == "maxclients" {
            if val_str.parse::<usize>().is_err() {
                out.extend_from_slice(b"-ERR Invalid argument for CONFIG SET maxclients\r\n");
                return;
            }
        } else if p_str == "client-default-resp" && parse_client_default_resp(&val_str).is_err() {
            out.extend_from_slice(
                b"-ERR Invalid argument for CONFIG SET client-default-resp (must be 2 or 3)\r\n",
            );
            return;
        } else if p_str == "cluster-allow-reads-when-down"
            && parse_config_bool(&p_str, &val_str).is_err()
        {
            out.extend_from_slice(
                b"-ERR Invalid argument for CONFIG SET cluster-allow-reads-when-down\r\n",
            );
            return;
        } else if p_str == "tracking-table-max-keys" && val_str.parse::<usize>().is_err() {
            out.extend_from_slice(
                b"-ERR Invalid argument for CONFIG SET tracking-table-max-keys\r\n",
            );
            return;
        }
    }

    // Phase 2: Apply all validated config parameters. Some values are only
    // checked by `apply_config_value`; on such an error the parameters
    // already applied get their previous values back and the command
    // fails, so CONFIG SET is all-or-nothing as in Redis.
    let mut applied: Vec<(String, Option<String>)> = Vec::new();
    for (param, val) in pairs {
        let p_str = String::from_utf8_lossy(param).to_lowercase();
        let val_str = String::from_utf8_lossy(val);
        let old = config_value_string(router, &p_str);
        if let Err(e) = apply_config_value(router.port, router.base_port, &p_str, &val_str) {
            for (name, old) in applied.iter().rev() {
                if let Some(old) = old {
                    let _ = apply_config_value(router.port, router.base_port, name, old);
                }
            }
            let msg = if e.starts_with("ERR ") {
                e
            } else {
                format!(
                    "ERR CONFIG SET failed (possibly related to argument '{}') - {}",
                    err_arg(&p_str),
                    e
                )
            };
            out.extend_from_slice(b"-");
            out.extend_from_slice(msg.replace(['\r', '\n'], " ").as_bytes());
            out.extend_from_slice(b"\r\n");
            return;
        }
        applied.push((p_str, old));
    }
    // A lower client memory limit evicts before CONFIG SET replies, as in
    // Redis, from fresh readings of this shard's clients (the others'
    // come from their shards' periodic measurement).
    if applied
        .iter()
        .any(|(n, _)| n == "maxmemory-clients" || n == "maxmemory")
    {
        let limit = maxmemory_clients_limit(router.base_port);
        if limit > 0 {
            SHARD_CLIENTS.with(|c| {
                if let Some(reg) = c.borrow().as_ref() {
                    measure_client_memory(reg, router.port, &router.pubsub.borrow());
                }
            });
            evict_clients(router.base_port, limit);
        }
    }
    out.extend_from_slice(b"+OK\r\n");
}

/// The current value of config parameter `name`, as CONFIG GET reports it.
pub(super) fn yes_no(on: bool) -> String {
    if on { "yes" } else { "no" }.to_string()
}

pub(super) fn config_value_string(router: &Router, name: &str) -> Option<String> {
    let mut buf = Vec::new();
    write_config_get_reply(router, &[Bytes::copy_from_slice(name.as_bytes())], &mut buf);
    let mut lines = buf
        .split(|&b| b == b'\n')
        .map(|l| l.strip_suffix(b"\r").unwrap_or(l));
    let header = lines.next()?;
    if header != b"*2" && header != b"%1" {
        return None;
    }
    let _name_len = lines.next()?;
    let _name = lines.next()?;
    let val_len: usize = std::str::from_utf8(lines.next()?.strip_prefix(b"$")?)
        .ok()?
        .parse()
        .ok()?;
    // The value starts right after its `$len\r\n` line.
    let header_len: usize = buf
        .split_inclusive(|&b| b == b'\n')
        .take(4)
        .map(|l| l.len())
        .sum();
    let val = buf.get(header_len..header_len + val_len)?;
    Some(String::from_utf8_lossy(val).into_owned())
}
