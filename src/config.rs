use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Production configuration for Rudis, compatible with redis.conf / rudis.conf
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RudisConfig {
    pub bind: String,
    pub protected_mode: bool,
    pub port: u16,
    pub threads: Option<usize>,
    pub maxclients: usize,
    pub maxmemory: Option<String>,
    pub maxmemory_bytes: Option<u64>,
    pub maxmemory_policy: String,
    pub appendonly: bool,
    /// `appendfsync`: true = everysec (default), false = no. `always` is
    /// rejected at parse time.
    pub appendfsync_every_sec: bool,
    /// `cross-shard-spin`: polls before parking on a cross-shard reply.
    pub cross_shard_spin: usize,
    /// `idle-poll-us`: how long a shard thread busy-polls for work before it
    /// sleeps (default 50; 0 = sleep at once). Cuts cross-shard hop latency on a partly
    /// idle server at the cost of CPU spent polling.
    pub idle_poll_us: u64,
    /// `enable-experimental-commands`: accept the non-Redis command families
    /// (JSON., BF., FT., CRDT., ...; see `resp::is_experimental_command_name`).
    pub enable_experimental_commands: bool,
    pub aof_load_truncated: bool,
    pub dir: PathBuf,
    pub requirepass: Option<String>,
    pub tls_port: Option<u16>,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    pub cluster_enabled: bool,
    pub tiered_offload_threshold: u64,
    pub tiered_upload_threshold: u64,
    /// `metrics-port`: serve Prometheus metrics over HTTP (`GET /metrics`)
    /// on this port. Off by default.
    pub metrics_port: Option<u16>,
    /// `metrics-bind`: address the metrics port listens on. Defaults to
    /// loopback because the endpoint has no authentication.
    pub metrics_bind: std::net::IpAddr,
    /// `save <seconds> <changes>` points. Empty (the default) means no
    /// snapshots unless asked for, unlike Redis's built-in defaults.
    pub save_points: Vec<(u64, u64)>,
    /// `user <name> <rules...>` lines, applied to the ACL at startup.
    pub users: Vec<(String, Vec<String>)>,
    /// Other recognised directives, in file order (a directive such as
    /// `client-output-buffer-limit` may legitimately appear several times).
    /// Applied at startup through `connection::apply_config_value`; the ones
    /// rudis has no setting for are reported and ignored.
    pub extra_directives: Vec<(String, String)>,
}

impl Default for RudisConfig {
    fn default() -> Self {
        Self {
            bind: crate::netsec::DEFAULT_BIND.to_string(),
            protected_mode: true,
            port: 6379,
            threads: None,
            maxclients: 10000,
            maxmemory: None,
            maxmemory_bytes: None,
            maxmemory_policy: "noeviction".to_string(),
            appendonly: false,
            appendfsync_every_sec: true,
            cross_shard_spin: 0,
            idle_poll_us: 50,
            enable_experimental_commands: false,
            aof_load_truncated: true,
            dir: PathBuf::from("."),
            requirepass: None,
            tls_port: None,
            tls_cert_file: None,
            tls_key_file: None,
            cluster_enabled: false,
            tiered_offload_threshold: 60,
            tiered_upload_threshold: 80,
            metrics_port: None,
            metrics_bind: std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            save_points: Vec::new(),
            users: Vec::new(),
            extra_directives: Vec::new(),
        }
    }
}

/// Parses a `save` value: `<seconds> <changes>` pairs, or `""` for none.
pub fn parse_save_points(spec: &str) -> Result<Vec<(u64, u64)>, String> {
    let spec = spec.trim();
    if spec.is_empty() || spec == "\"\"" || spec == "''" {
        return Ok(Vec::new());
    }
    let nums = spec
        .split_whitespace()
        .map(|n| {
            n.parse::<u64>()
                .map_err(|_| format!("'{}' is not a number", n))
        })
        .collect::<Result<Vec<u64>, String>>()?;
    if nums.len() % 2 != 0 {
        return Err("expected <seconds> <changes> pairs".to_string());
    }
    Ok(nums.chunks(2).map(|p| (p[0], p[1])).collect())
}

const MAX_INCLUDE_DEPTH: usize = 16;

/// Directive names accepted by Redis 7 / Valkey 8 config files (generated
/// from valkey/src/config.c, plus its deprecated names and a few Redis 7.4+
/// additions). Used to tell directives rudis does not implement, which are
/// ignored with a warning, from typos, which are fatal like in Redis.
const REDIS_CONFIG_NAMES: &[&str] = &[
    "aclfile",
    "acllog-max-len",
    "acl-pubsub-default",
    "activedefrag",
    "active-defrag-cycle-max",
    "active-defrag-cycle-min",
    "active-defrag-cycle-us",
    "active-defrag-ignore-bytes",
    "active-defrag-max-scan-fields",
    "active-defrag-threshold-lower",
    "active-defrag-threshold-upper",
    "active-expire-effort",
    "activerehashing",
    "always-show-logo",
    "aof-disable-auto-gc",
    "aof-load-truncated",
    "aof-rewrite-cpulist",
    "aof_rewrite_cpulist",
    "aof-rewrite-incremental-fsync",
    "aof-timestamp-enabled",
    "aof-use-rdb-preamble",
    "appenddirname",
    "appendfilename",
    "appendfsync",
    "appendonly",
    "auto-aof-rewrite-min-size",
    "auto-aof-rewrite-percentage",
    "availability-zone",
    "bgsave-cpulist",
    "bgsave_cpulist",
    "bind",
    "bind-source-addr",
    "bio-cpulist",
    "bio_cpulist",
    "busy-reply-threshold",
    "client-default-resp",
    "client-output-buffer-limit",
    "client-query-buffer-limit",
    "cluster-allow-pubsubshard-when-down",
    "cluster-allow-reads-when-down",
    "cluster-allow-replica-migration",
    "cluster-announce-bus-port",
    "cluster-announce-client-ipv4",
    "cluster-announce-client-ipv6",
    "cluster-announce-client-port",
    "cluster-announce-client-tls-port",
    "cluster-announce-hostname",
    "cluster-announce-human-nodename",
    "cluster-announce-ip",
    "cluster-announce-port",
    "cluster-announce-tls-port",
    "cluster-blacklist-ttl",
    "cluster-config-file",
    "cluster-config-save-behavior",
    "cluster-databases",
    "cluster-enabled",
    "cluster-link-sendbuf-limit",
    "cluster-manual-failover-timeout",
    "cluster-message-gossip-perc",
    "cluster-migration-barrier",
    "cluster-node-timeout",
    "cluster-ping-interval",
    "cluster-port",
    "cluster-preferred-endpoint-type",
    "cluster-replica-no-failover",
    "cluster-replica-validity-factor",
    "cluster-require-full-coverage",
    "cluster-slave-no-failover",
    "cluster-slave-validity-factor",
    "cluster-slot-migration-log-max-len",
    "cluster-slot-stats-enabled",
    "commandlog-execution-slower-than",
    "commandlog-large-reply-max-len",
    "commandlog-large-request-max-len",
    "commandlog-reply-larger-than",
    "commandlog-request-larger-than",
    "commandlog-slow-execution-max-len",
    "crash-log-enabled",
    "crash-memcheck-enabled",
    "daemonize",
    "databases",
    "dbfilename",
    "debug-context",
    "dir",
    "disable-thp",
    "dual-channel-replication-enabled",
    "dynamic-hz",
    "enable-debug-assert",
    "enable-debug-command",
    "enable-module-command",
    "enable-protected-configs",
    "events-per-io-thread",
    "extended-redis-compatibility",
    "hash-max-listpack-entries",
    "hash-max-listpack-value",
    "hash-max-ziplist-entries",
    "hash-max-ziplist-value",
    "hash-seed",
    "hide-user-data-from-log",
    "hll-sparse-max-bytes",
    "hz",
    "ignore-warnings",
    "import-mode",
    "io-threads",
    "io-threads-always-active",
    "io-threads-do-reads",
    "jemalloc-bg-thread",
    "key-load-delay",
    "latency-monitor-threshold",
    "latency-tracking",
    "latency-tracking-info-percentiles",
    "lazyfree-lazy-eviction",
    "lazyfree-lazy-expire",
    "lazyfree-lazy-server-del",
    "lazyfree-lazy-user-del",
    "lazyfree-lazy-user-flush",
    "lfu-decay-time",
    "lfu-log-factor",
    "list-compress-depth",
    "list-max-listpack-size",
    "list-max-ziplist-entries",
    "list-max-ziplist-size",
    "list-max-ziplist-value",
    "loading-process-events-interval-bytes",
    "locale-collate",
    "logfile",
    "log-format",
    "loglevel",
    "log-timestamp-format",
    "lua-enable-deprecated-api",
    "lua-enable-insecure-api",
    "lua-replicate-commands",
    "lua-time-limit",
    "masterauth",
    "masteruser",
    "maxclients",
    "maxmemory",
    "maxmemory-clients",
    "maxmemory-eviction-tenacity",
    "maxmemory-policy",
    "maxmemory-samples",
    "max-new-connections-per-cycle",
    "max-new-tls-connections-per-cycle",
    "min-io-threads-avoid-copy-reply",
    "min-replicas-max-lag",
    "min-replicas-to-write",
    "min-slaves-max-lag",
    "min-slaves-to-write",
    "min-string-size-avoid-copy-reply",
    "min-string-size-avoid-copy-reply-threaded",
    "mptcp",
    "no-appendfsync-on-rewrite",
    "notify-keyspace-events",
    "oom-score-adj",
    "oom-score-adj-values",
    "pidfile",
    "port",
    "prefetch-batch-max-size",
    "primaryauth",
    "primaryuser",
    "proc-title-template",
    "propagation-error-behavior",
    "protected-mode",
    "proto-max-bulk-len",
    "rdbchecksum",
    "rdbcompression",
    "rdb-del-sync-files",
    "rdb-key-save-delay",
    "rdb-save-incremental-fsync",
    "rdb-version-check",
    "rdma-bind",
    "rdma-completion-vector",
    "rdma-port",
    "rdma-rx-size",
    "repl-backlog-size",
    "repl-backlog-ttl",
    "repl-disable-tcp-nodelay",
    "repl-diskless-load",
    "repl-diskless-sync",
    "repl-diskless-sync-delay",
    "repl-diskless-sync-max-replicas",
    "replica-announced",
    "replica-announce-ip",
    "replica-announce-port",
    "replica-ignore-disk-write-errors",
    "replica-ignore-maxmemory",
    "replica-lazy-flush",
    "replicaof",
    "replica-priority",
    "replica-read-only",
    "replica-serve-stale-data",
    "repl-mptcp",
    "repl-ping-replica-period",
    "repl-ping-slave-period",
    "repl-timeout",
    "req-res-logfile",
    "requirepass",
    "sanitize-dump-payload",
    "save",
    "server-cpulist",
    "server_cpulist",
    "set-max-intset-entries",
    "set-max-listpack-entries",
    "set-max-listpack-value",
    "set-proc-title",
    "shard-threads",
    "shutdown-on-sigint",
    "shutdown-on-sigterm",
    "shutdown-timeout",
    "slave-announce-ip",
    "slave-announce-port",
    "slave-ignore-maxmemory",
    "slave-lazy-flush",
    "slaveof",
    "slave-priority",
    "slave-read-only",
    "slave-serve-stale-data",
    "slot-migration-max-failover-repl-bytes",
    "slowlog-log-slower-than",
    "slowlog-max-len",
    "socket-mark-id",
    "stop-writes-on-bgsave-error",
    "stream-node-max-bytes",
    "stream-node-max-entries",
    "supervised",
    "syslog-enabled",
    "syslog-facility",
    "syslog-ident",
    "tcp-backlog",
    "tcp-keepalive",
    "timeout",
    "tls-auth-clients",
    "tls-auth-clients-user",
    "tls-auto-reload-interval",
    "tls-ca-cert-dir",
    "tls-ca-cert-file",
    "tls-cert-file",
    "tls-ciphers",
    "tls-ciphersuites",
    "tls-client-cert-file",
    "tls-client-key-file",
    "tls-client-key-file-pass",
    "tls-cluster",
    "tls-dh-params-file",
    "tls-key-file",
    "tls-key-file-pass",
    "tls-port",
    "tls-prefer-server-ciphers",
    "tls-protocols",
    "tls-replication",
    "tls-session-cache-size",
    "tls-session-cache-timeout",
    "tls-session-caching",
    "tracking-table-max-keys",
    "unixsocket",
    "unixsocketgroup",
    "unixsocketperm",
    "use-exit-on-panic",
    "watchdog-period",
    "zset-max-listpack-entries",
    "zset-max-listpack-value",
    "zset-max-ziplist-entries",
    "zset-max-ziplist-value",
];

/// rudis-specific directives that are not handled by `RudisConfig::parse_str`
/// itself but by `connection::apply_config_value`.
const RUDIS_CONFIG_NAMES: &[&str] = &[
    "backup-sealed-ttl",
    "backupdirname",
    "key-load-delay",
    "stream-idmp-duration",
    "stream-idmp-maxsize",
];

/// Whether `name` is a directive Redis/Valkey or rudis accept. Module
/// configs (`module.option`) are accepted too; they are reported as ignored.
fn is_known_directive(name: &str) -> bool {
    REDIS_CONFIG_NAMES.contains(&name) || RUDIS_CONFIG_NAMES.contains(&name) || name.contains('.')
}

/// Strips one pair of matching surrounding quotes, so `notify-keyspace-events ""`
/// means the empty string as it does in Redis.
fn unquote(v: &str) -> &str {
    let b = v.as_bytes();
    if b.len() >= 2 && (b[0] == b'"' || b[0] == b'\'') && b[b.len() - 1] == b[0] {
        &v[1..v.len() - 1]
    } else {
        v
    }
}

static SAVE_POINTS: parking_lot::Mutex<Option<HashMap<u16, Vec<(u64, u64)>>>> =
    parking_lot::Mutex::new(None);

pub fn set_save_points(port: u16, points: Vec<(u64, u64)>) {
    let mut map = SAVE_POINTS.lock();
    map.get_or_insert_with(HashMap::new).insert(port, points);
}

pub fn save_points(port: u16) -> Vec<(u64, u64)> {
    let map = SAVE_POINTS.lock();
    map.as_ref()
        .and_then(|m| m.get(&port).cloned())
        .unwrap_or_default()
}

static DB_FILENAMES: parking_lot::Mutex<Option<HashMap<u16, String>>> =
    parking_lot::Mutex::new(None);

pub const DEFAULT_DBFILENAME: &str = "dump.rdb";

/// Sets `dbfilename`, the RDB file name inside `dir`. Like Redis, it must be
/// a plain file name, not a path.
pub fn set_dbfilename(port: u16, name: &str) -> Result<(), String> {
    validate_dbfilename(name)?;
    let mut map = DB_FILENAMES.lock();
    map.get_or_insert_with(HashMap::new)
        .insert(port, name.to_string());
    Ok(())
}

pub fn validate_dbfilename(name: &str) -> Result<(), String> {
    if name.is_empty() || name.contains('/') || name == "." || name == ".." {
        return Err("dbfilename can't be a path, just a filename".to_string());
    }
    Ok(())
}

pub fn dbfilename(port: u16) -> String {
    let map = DB_FILENAMES.lock();
    map.as_ref()
        .and_then(|m| m.get(&port).cloned())
        .unwrap_or_else(|| DEFAULT_DBFILENAME.to_string())
}

/// Redis rule: `SHUTDOWN SAVE` always snapshots, `NOSAVE` never does, and
/// plain `SHUTDOWN` (or SIGTERM) snapshots when save points are configured.
pub fn should_save_on_shutdown(save: Option<bool>, has_save_points: bool) -> bool {
    save.unwrap_or(has_save_points)
}

impl RudisConfig {
    /// Loads configuration from a string formatted like redis.conf
    pub fn parse_str(content: &str) -> Result<Self, String> {
        let mut config = Self::default();
        config.parse_into(content, 0)?;
        Ok(config)
    }

    fn parse_into(&mut self, content: &str, depth: usize) -> Result<(), String> {
        let config = self;
        for (line_num, raw_line) in content.lines().enumerate() {
            let line = raw_line.trim();
            // Ignore empty lines and comment lines starting with #
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            // Directives are whitespace-separated
            let mut parts = line.split_whitespace();
            let directive = match parts.next() {
                Some(d) => d.to_lowercase(),
                None => continue,
            };

            let rest: Vec<&str> = parts.collect();
            if rest.is_empty() {
                continue;
            }

            match directive.as_str() {
                "bind" => {
                    let spec = rest.join(" ");
                    crate::netsec::parse_bind_spec(&spec)
                        .map_err(|e| format!("Invalid bind at line {}: {}", line_num + 1, e))?;
                    config.bind = spec;
                }
                "protected-mode" => {
                    config.protected_mode = match rest[0].to_lowercase().as_str() {
                        "yes" => true,
                        "no" => false,
                        other => {
                            return Err(format!(
                                "Invalid protected-mode at line {}: '{}' (expected yes|no)",
                                line_num + 1,
                                other
                            ));
                        }
                    };
                }
                "port" => {
                    config.port = rest[0]
                        .parse::<u16>()
                        .map_err(|e| format!("Invalid port at line {}: {}", line_num + 1, e))?;
                }
                "threads" | "io-threads" => {
                    let n = rest[0]
                        .parse::<usize>()
                        .map_err(|e| format!("Invalid threads at line {}: {}", line_num + 1, e))?;
                    config.threads = Some(n);
                }
                "maxclients" => {
                    config.maxclients = rest[0].parse::<usize>().map_err(|e| {
                        format!("Invalid maxclients at line {}: {}", line_num + 1, e)
                    })?;
                }
                "maxmemory" => {
                    let raw_mem = rest[0].to_string();
                    let bytes = crate::tiering::parse_memory_bytes(&raw_mem);
                    config.maxmemory = Some(raw_mem);
                    config.maxmemory_bytes = bytes;
                }
                "maxmemory-policy" => {
                    config.maxmemory_policy = rest[0].to_lowercase();
                }
                "appendonly" => {
                    config.appendonly = match rest[0].to_lowercase().as_str() {
                        "yes" => true,
                        "no" => false,
                        other => {
                            return Err(format!(
                                "Invalid appendonly at line {}: '{}' (expected yes|no)",
                                line_num + 1,
                                other
                            ));
                        }
                    };
                }
                "metrics-port" => {
                    config.metrics_port = Some(rest[0].parse::<u16>().map_err(|e| {
                        format!("Invalid metrics-port at line {}: {}", line_num + 1, e)
                    })?);
                }
                "metrics-bind" => {
                    config.metrics_bind = rest[0].parse().map_err(|e| {
                        format!("Invalid metrics-bind at line {}: {}", line_num + 1, e)
                    })?;
                }
                "cross-shard-spin" => {
                    config.cross_shard_spin = rest[0].parse::<usize>().map_err(|e| {
                        format!("Invalid cross-shard-spin at line {}: {}", line_num + 1, e)
                    })?;
                }
                "idle-poll-us" => {
                    config.idle_poll_us = rest[0].parse::<u64>().map_err(|e| {
                        format!("Invalid idle-poll-us at line {}: {}", line_num + 1, e)
                    })?;
                }
                "enable-experimental-commands" => {
                    config.enable_experimental_commands = match rest[0].to_lowercase().as_str() {
                        "yes" => true,
                        "no" => false,
                        other => {
                            return Err(format!(
                                "Invalid enable-experimental-commands at line {}: '{}' (expected yes|no)",
                                line_num + 1,
                                other
                            ));
                        }
                    };
                }
                "appendfsync" => {
                    config.appendfsync_every_sec =
                        crate::aof::parse_appendfsync(rest[0]).map_err(|e| {
                            format!("Invalid appendfsync at line {}: {}", line_num + 1, e)
                        })?;
                }
                "aof-load-truncated" => {
                    config.aof_load_truncated = match rest[0].to_lowercase().as_str() {
                        "yes" => true,
                        "no" => false,
                        other => {
                            return Err(format!(
                                "Invalid aof-load-truncated at line {}: '{}' (expected yes|no)",
                                line_num + 1,
                                other
                            ));
                        }
                    };
                }
                "dir" => {
                    // Strip quotes if present
                    let mut p = rest.join(" ");
                    if (p.starts_with('"') && p.ends_with('"'))
                        || (p.starts_with('\'') && p.ends_with('\''))
                    {
                        p = p[1..p.len() - 1].to_string();
                    }
                    config.dir = PathBuf::from(p);
                }
                "requirepass" => {
                    let mut pass = rest.join(" ");
                    if (pass.starts_with('"') && pass.ends_with('"'))
                        || (pass.starts_with('\'') && pass.ends_with('\''))
                    {
                        pass = pass[1..pass.len() - 1].to_string();
                    }
                    config.requirepass = Some(pass);
                }
                "tls-port" => {
                    let p = rest[0]
                        .parse::<u16>()
                        .map_err(|e| format!("Invalid tls-port at line {}: {}", line_num + 1, e))?;
                    config.tls_port = Some(p);
                }
                "tls-cert-file" => {
                    config.tls_cert_file = Some(PathBuf::from(rest.join(" ")));
                }
                "tls-key-file" => {
                    config.tls_key_file = Some(PathBuf::from(rest.join(" ")));
                }
                "cluster-enabled" => {
                    config.cluster_enabled =
                        matches!(rest[0].to_lowercase().as_str(), "yes" | "true" | "1");
                }
                "tiered-offload-threshold" => {
                    let val = rest[0].parse::<u64>().map_err(|e| {
                        format!(
                            "Invalid tiered-offload-threshold at line {}: {}",
                            line_num + 1,
                            e
                        )
                    })?;
                    config.tiered_offload_threshold = val;
                }
                "tiered-upload-threshold" => {
                    let val = rest[0].parse::<u64>().map_err(|e| {
                        format!(
                            "Invalid tiered-upload-threshold at line {}: {}",
                            line_num + 1,
                            e
                        )
                    })?;
                    config.tiered_upload_threshold = val;
                }
                "save" => {
                    // Like Redis: each line adds points; `save ""` clears them.
                    let points = parse_save_points(&rest.join(" "))
                        .map_err(|e| format!("Invalid save at line {}: {}", line_num + 1, e))?;
                    if points.is_empty() {
                        config.save_points.clear();
                    } else {
                        config.save_points.extend(points);
                    }
                }
                "include" => {
                    // Like Redis, a relative path is resolved against the
                    // working directory, and later lines override earlier ones.
                    if rest.len() != 1 || rest[0].contains(['*', '?', '[']) {
                        return Err(format!(
                            "Invalid include at line {}: expected a single file path (glob patterns are not supported)",
                            line_num + 1
                        ));
                    }
                    if depth >= MAX_INCLUDE_DEPTH {
                        return Err(format!(
                            "Invalid include at line {}: includes nested more than {} levels deep",
                            line_num + 1,
                            MAX_INCLUDE_DEPTH
                        ));
                    }
                    let path = unquote(rest[0]);
                    let content = fs::read_to_string(path).map_err(|e| {
                        format!(
                            "Failed to read included config file {:?} (line {}): {}",
                            path,
                            line_num + 1,
                            e
                        )
                    })?;
                    config
                        .parse_into(&content, depth + 1)
                        .map_err(|e| format!("In included file {:?}: {}", path, e))?;
                }
                "user" => {
                    config.users.push((
                        rest[0].to_string(),
                        rest[1..].iter().map(|r| r.to_string()).collect(),
                    ));
                }
                "rename-command" => {
                    // Silently keeping a command that the operator meant to
                    // rename or disable would be a security hole.
                    return Err(format!(
                        "Unsupported directive at line {}: rename-command is not supported by rudis; \
                         restrict commands with ACL rules instead (e.g. `user default on nopass ~* &* +@all -flushall`)",
                        line_num + 1
                    ));
                }
                "loadmodule" => {
                    return Err(format!(
                        "Unsupported directive at line {}: rudis cannot load Redis modules",
                        line_num + 1
                    ));
                }
                other => {
                    if !is_known_directive(other) {
                        return Err(format!(
                            "Bad directive or wrong number of arguments at line {}: '{}'",
                            line_num + 1,
                            other
                        ));
                    }
                    config
                        .extra_directives
                        .push((other.to_string(), unquote(&rest.join(" ")).to_string()));
                }
            }
        }

        Ok(())
    }

    /// Loads configuration from a file path
    pub fn load_file<P: AsRef<Path>>(path: P) -> Result<Self, String> {
        let content = fs::read_to_string(path.as_ref())
            .map_err(|e| format!("Failed to read config file {:?}: {}", path.as_ref(), e))?;
        Self::parse_str(&content)
    }

    /// Overrides configuration settings with explicitly provided CLI arguments
    pub fn merge_cli(
        &mut self,
        port: Option<u16>,
        threads: Option<usize>,
        aof: Option<bool>,
        aof_dir: Option<PathBuf>,
        maxmemory: Option<String>,
        tiered_offload_threshold: Option<u64>,
        tiered_upload_threshold: Option<u64>,
        tls_port: Option<u16>,
        tls_cert_file: Option<PathBuf>,
        tls_key_file: Option<PathBuf>,
        cluster_enabled: Option<bool>,
    ) {
        if let Some(p) = port {
            self.port = p;
        }
        if let Some(t) = threads {
            self.threads = Some(t);
        }
        if let Some(a) = aof {
            self.appendonly = a;
        }
        if let Some(d) = aof_dir {
            self.dir = d;
        }
        if let Some(m) = maxmemory {
            let bytes = crate::tiering::parse_memory_bytes(&m);
            self.maxmemory = Some(m);
            self.maxmemory_bytes = bytes;
        }
        if let Some(o) = tiered_offload_threshold {
            self.tiered_offload_threshold = o;
        }
        if let Some(u) = tiered_upload_threshold {
            self.tiered_upload_threshold = u;
        }
        if let Some(tp) = tls_port {
            self.tls_port = Some(tp);
        }
        if let Some(tc) = tls_cert_file {
            self.tls_cert_file = Some(tc);
        }
        if let Some(tk) = tls_key_file {
            self.tls_key_file = Some(tk);
        }
        if let Some(c) = cluster_enabled {
            self.cluster_enabled = c;
        }
    }
}

/// The file the server was started with (`--config`), stored as an absolute
/// path so that CONFIG REWRITE does not depend on the working directory.
pub static ACTIVE_CONFIG_FILE: parking_lot::RwLock<Option<PathBuf>> =
    parking_lot::RwLock::new(None);

/// Records `path` as the file CONFIG REWRITE updates.
pub fn set_active_config_file(path: &Path) -> Result<(), String> {
    let abs = std::path::absolute(path)
        .map_err(|e| format!("Failed to resolve config file {:?}: {}", path, e))?;
    *ACTIVE_CONFIG_FILE.write() = Some(abs);
    Ok(())
}

/// Rewrites the active configuration file atomically with current runtime settings
pub fn rewrite_config_file(port: u16) -> Result<(), String> {
    // Like Redis, there is nothing to rewrite without a config file; never
    // invent one in the working directory.
    let path = ACTIVE_CONFIG_FILE
        .read()
        .clone()
        .ok_or_else(|| "The server is running without a config file".to_string())?;

    // 1. Read existing file if it exists, or create a new template
    let mut lines: Vec<String> = if path.exists() {
        fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read config file: {}", e))?
            .lines()
            .map(|s| s.to_string())
            .collect()
    } else {
        vec!["# Rudis configuration file (auto-generated by CONFIG REWRITE)".to_string()]
    };

    // 2. Prepare current dynamic directives
    let mut directives: HashMap<String, String> = HashMap::new();
    directives.insert("port".to_string(), port.to_string());
    directives.insert(
        "maxclients".to_string(),
        crate::connection::get_max_clients().to_string(),
    );
    directives.insert(
        "maxmemory-policy".to_string(),
        crate::connection::get_max_memory_policy(),
    );
    let max_mem = crate::tiering::get_max_memory(port);
    if max_mem > 0 {
        directives.insert("maxmemory".to_string(), max_mem.to_string());
    }
    directives.insert(
        "tiered-offload-threshold".to_string(),
        crate::tiering::get_offload_threshold_pct(port).to_string(),
    );
    directives.insert(
        "tiered-upload-threshold".to_string(),
        crate::tiering::get_upload_threshold_pct(port).to_string(),
    );
    directives.insert(
        "slowlog-log-slower-than".to_string(),
        crate::slowlog::SLOWLOG_LOG_SLOWER_THAN
            .load(std::sync::atomic::Ordering::Relaxed)
            .to_string(),
    );
    directives.insert(
        "slowlog-max-len".to_string(),
        crate::slowlog::SLOWLOG_MAX_LEN
            .load(std::sync::atomic::Ordering::Relaxed)
            .to_string(),
    );
    directives.insert(
        "slowlog-entry-max-argc".to_string(),
        crate::slowlog::SLOWLOG_ENTRY_MAX_ARGC
            .load(std::sync::atomic::Ordering::Relaxed)
            .to_string(),
    );
    directives.insert(
        "slowlog-entry-max-string-len".to_string(),
        crate::slowlog::SLOWLOG_ENTRY_MAX_STRING_LEN
            .load(std::sync::atomic::Ordering::Relaxed)
            .to_string(),
    );
    directives.insert(
        "client-output-buffer-limit".to_string(),
        crate::connection::format_client_output_buffer_limit_config(),
    );
    directives.insert("loglevel".to_string(), crate::log::loglevel().to_string());

    // If requirepass is set
    let acl = crate::acl::get_acl_for_port(port);
    if let Some(pass) = acl.read().requirepass.clone() {
        directives.insert("requirepass".to_string(), pass);
    }

    // 3. Update existing lines or track which ones were updated
    let mut updated_keys = std::collections::HashSet::new();
    for line in &mut lines {
        let trimmed = line.trim();
        if trimmed.starts_with('#') || trimmed.is_empty() {
            continue;
        }
        if let Some(key) = trimmed.split_whitespace().next() {
            let key_lower = key.to_lowercase();
            if let Some(val) = directives.get(&key_lower) {
                *line = format!("{} {}", key_lower, val);
                updated_keys.insert(key_lower);
            }
        }
    }

    // 4. Append remaining directives that were not present in the original file
    for (k, v) in directives {
        if !updated_keys.contains(&k) {
            lines.push(format!("{} {}", k, v));
        }
    }

    // 5. Write to temp file in the same directory, sync, rename, and sync parent dir!
    let tmp_path = format!("{}.tmp.{}", path.display(), std::process::id());
    {
        use std::io::Write;
        let mut tmp_file = fs::File::create(&tmp_path)
            .map_err(|e| format!("Failed to create temp config file: {}", e))?;
        for l in lines {
            writeln!(tmp_file, "{}", l)
                .map_err(|e| format!("Failed to write config line: {}", e))?;
        }
        tmp_file.flush().map_err(|e| e.to_string())?;
        tmp_file.sync_all().map_err(|e| e.to_string())?;
    }

    fs::rename(&tmp_path, &path)
        .map_err(|e| format!("Failed to atomically rename config file: {}", e))?;
    crate::log_notice!("CONFIG REWRITE executed with success.");

    // Directory fsync for crash-durability!
    if let Some(parent) = path.parent() {
        let dir_path = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        if let Ok(dir_file) = fs::File::open(dir_path) {
            let _ = dir_file.sync_all();
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty_and_comments() {
        let text = r#"
        # This is a sample comment
        # Another line

        "#;
        let cfg = RudisConfig::parse_str(text).unwrap();
        assert_eq!(cfg.port, 6379);
        assert_eq!(cfg.maxclients, 10000);
        assert!(!cfg.appendonly);
    }

    #[test]
    fn test_parse_aof_load_truncated() {
        assert!(RudisConfig::parse_str("").unwrap().aof_load_truncated);
        assert!(
            !RudisConfig::parse_str("aof-load-truncated no")
                .unwrap()
                .aof_load_truncated
        );
        assert!(
            RudisConfig::parse_str("aof-load-truncated YES")
                .unwrap()
                .aof_load_truncated
        );
        assert!(RudisConfig::parse_str("aof-load-truncated maybe").is_err());
    }

    #[test]
    fn test_parse_metrics_port_and_bind() {
        let c = RudisConfig::parse_str("").unwrap();
        assert_eq!(c.metrics_port, None);
        assert_eq!(c.metrics_bind.to_string(), "127.0.0.1");
        let c = RudisConfig::parse_str("metrics-port 9121\nmetrics-bind 0.0.0.0").unwrap();
        assert_eq!(c.metrics_port, Some(9121));
        assert_eq!(c.metrics_bind.to_string(), "0.0.0.0");
        assert!(RudisConfig::parse_str("metrics-port 70000").is_err());
        assert!(RudisConfig::parse_str("metrics-bind localhost:1").is_err());
    }

    #[test]
    fn test_parse_idle_poll_us() {
        assert_eq!(RudisConfig::parse_str("").unwrap().idle_poll_us, 50);
        assert_eq!(
            RudisConfig::parse_str("idle-poll-us 0")
                .unwrap()
                .idle_poll_us,
            0
        );
        assert_eq!(
            RudisConfig::parse_str("idle-poll-us 50")
                .unwrap()
                .idle_poll_us,
            50
        );
        assert!(RudisConfig::parse_str("idle-poll-us -1").is_err());
    }

    #[test]
    fn test_parse_cross_shard_spin() {
        assert_eq!(RudisConfig::parse_str("").unwrap().cross_shard_spin, 0);
        assert_eq!(
            RudisConfig::parse_str("cross-shard-spin 64")
                .unwrap()
                .cross_shard_spin,
            64
        );
        assert!(RudisConfig::parse_str("cross-shard-spin -1").is_err());
        assert!(RudisConfig::parse_str("cross-shard-spin lots").is_err());
    }

    #[test]
    fn test_parse_enable_experimental_commands() {
        assert!(
            !RudisConfig::parse_str("")
                .unwrap()
                .enable_experimental_commands
        );
        assert!(
            RudisConfig::parse_str("enable-experimental-commands yes")
                .unwrap()
                .enable_experimental_commands
        );
        assert!(RudisConfig::parse_str("enable-experimental-commands maybe").is_err());
    }

    #[test]
    fn test_parse_appendonly_and_appendfsync() {
        let cfg = RudisConfig::parse_str("").unwrap();
        assert!(!cfg.appendonly);
        assert!(cfg.appendfsync_every_sec);
        let cfg = RudisConfig::parse_str("appendonly yes\nappendfsync no").unwrap();
        assert!(cfg.appendonly);
        assert!(!cfg.appendfsync_every_sec);
        let cfg = RudisConfig::parse_str("appendfsync EVERYSEC").unwrap();
        assert!(cfg.appendfsync_every_sec);
        // `always` can't be honoured, so it must not load as something weaker.
        let err = RudisConfig::parse_str("appendfsync always").unwrap_err();
        assert!(err.contains("always is not supported"), "{}", err);
        assert!(RudisConfig::parse_str("appendfsync sometimes").is_err());
        assert!(RudisConfig::parse_str("appendonly on").is_err());
    }

    #[test]
    fn test_parse_standard_redis_conf_options() {
        let text = r#"
        port 7777
        bind 0.0.0.0
        threads 4
        maxclients 5000
        maxmemory 2gb
        maxmemory-policy allkeys-lru
        appendonly yes
        dir /tmp/rudis-data
        requirepass "secret_pwd_123"
        tls-port 7778
        tls-cert-file /etc/ssl/cert.pem
        tls-key-file /etc/ssl/key.pem
        cluster-enabled yes
        tiered-offload-threshold 65
        tiered-upload-threshold 85
        save 900 1
        "#;
        let cfg = RudisConfig::parse_str(text).unwrap();
        assert_eq!(cfg.port, 7777);
        assert_eq!(cfg.bind, "0.0.0.0");
        assert_eq!(cfg.threads, Some(4));
        assert_eq!(cfg.maxclients, 5000);
        assert_eq!(cfg.maxmemory.as_deref(), Some("2gb"));
        assert_eq!(cfg.maxmemory_bytes, Some(2 * 1024 * 1024 * 1024));
        assert_eq!(cfg.maxmemory_policy, "allkeys-lru");
        assert!(cfg.appendonly);
        assert_eq!(cfg.dir, PathBuf::from("/tmp/rudis-data"));
        assert_eq!(cfg.requirepass.as_deref(), Some("secret_pwd_123"));
        assert_eq!(cfg.tls_port, Some(7778));
        assert_eq!(cfg.tls_cert_file, Some(PathBuf::from("/etc/ssl/cert.pem")));
        assert_eq!(cfg.tls_key_file, Some(PathBuf::from("/etc/ssl/key.pem")));
        assert!(cfg.cluster_enabled);
        assert_eq!(cfg.tiered_offload_threshold, 65);
        assert_eq!(cfg.tiered_upload_threshold, 85);
        assert_eq!(cfg.save_points, vec![(900, 1)]);
    }

    #[test]
    fn test_parse_save_points_and_shutdown_rule() {
        assert!(RudisConfig::parse_str("").unwrap().save_points.is_empty());
        let cfg = RudisConfig::parse_str("save 900 1\nsave 300 10 60 10000\n").unwrap();
        assert_eq!(cfg.save_points, vec![(900, 1), (300, 10), (60, 10000)]);
        let cfg = RudisConfig::parse_str("save 900 1\nsave \"\"\n").unwrap();
        assert!(cfg.save_points.is_empty());
        assert!(RudisConfig::parse_str("save 900\n").is_err());
        assert!(RudisConfig::parse_str("save 900 x\n").is_err());
        assert_eq!(parse_save_points("''").unwrap(), vec![]);

        set_save_points(1, vec![(1, 1)]);
        assert_eq!(save_points(1), vec![(1, 1)]);
        assert!(save_points(2).is_empty());

        assert_eq!(dbfilename(3), "dump.rdb");
        set_dbfilename(3, "cache.rdb").unwrap();
        assert_eq!(dbfilename(3), "cache.rdb");
        assert!(set_dbfilename(3, "../x.rdb").is_err());
        assert!(set_dbfilename(3, "").is_err());
        assert_eq!(dbfilename(3), "cache.rdb");

        assert!(should_save_on_shutdown(Some(true), false));
        assert!(!should_save_on_shutdown(Some(false), true));
        assert!(should_save_on_shutdown(None, true));
        assert!(!should_save_on_shutdown(None, false));
    }

    #[test]
    fn test_bind_default_and_validation() {
        assert_eq!(RudisConfig::default().bind, "* -::*");
        let cfg = RudisConfig::parse_str("bind 127.0.0.1 -::1\n").unwrap();
        assert_eq!(cfg.bind, "127.0.0.1 -::1");
        assert!(RudisConfig::parse_str("bind 999.1.1.1\n").is_err());
    }

    #[test]
    fn test_protected_mode_directive() {
        assert!(RudisConfig::default().protected_mode);
        assert!(
            !RudisConfig::parse_str("protected-mode no\n")
                .unwrap()
                .protected_mode
        );
        assert!(
            RudisConfig::parse_str("protected-mode yes\n")
                .unwrap()
                .protected_mode
        );
        assert!(RudisConfig::parse_str("protected-mode maybe\n").is_err());
    }

    #[test]
    fn test_unknown_and_unsupported_directives() {
        // Typos are fatal, like in Redis.
        let err = RudisConfig::parse_str("maxmemroy 1gb\n").unwrap_err();
        assert!(err.contains("Bad directive"), "{}", err);
        assert!(RudisConfig::parse_str("rename-command FLUSHALL \"\"\n").is_err());
        assert!(RudisConfig::parse_str("loadmodule /x.so\n").is_err());
        // Known Redis directives are kept, in order and with repeats, for
        // startup to apply or report; one pair of quotes is stripped.
        let cfg = RudisConfig::parse_str(
            "client-output-buffer-limit normal 0 0 0\n\
             client-output-buffer-limit pubsub 32mb 8mb 60\n\
             notify-keyspace-events \"\"\n\
             databases 16\n\
             search.timeout 5\n\
             user bob on >pw +@all\n",
        )
        .unwrap();
        let names: Vec<&str> = cfg
            .extra_directives
            .iter()
            .map(|(k, _)| k.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "client-output-buffer-limit",
                "client-output-buffer-limit",
                "notify-keyspace-events",
                "databases",
                "search.timeout"
            ]
        );
        assert_eq!(cfg.extra_directives[1].1, "pubsub 32mb 8mb 60");
        assert_eq!(cfg.extra_directives[2].1, "");
        assert_eq!(
            cfg.users,
            vec![(
                "bob".to_string(),
                vec!["on".to_string(), ">pw".to_string(), "+@all".to_string()]
            )]
        );
    }

    #[test]
    fn test_include_directive() {
        let dir = std::env::temp_dir().join(format!("rudis-include-test-{}", std::process::id()));
        let _ = fs::create_dir_all(&dir);
        let inc = dir.join("inc.conf");
        fs::write(&inc, "maxclients 42\nslowlog-max-len 7\n").unwrap();
        let cfg = RudisConfig::parse_str(&format!(
            "maxclients 1\ninclude {}\nport 7001\n",
            inc.display()
        ))
        .unwrap();
        assert_eq!(cfg.maxclients, 42);
        assert_eq!(cfg.port, 7001);
        assert_eq!(
            cfg.extra_directives,
            vec![("slowlog-max-len".to_string(), "7".to_string())]
        );
        // Errors inside the included file and missing files are reported.
        fs::write(&inc, "port nope\n").unwrap();
        assert!(RudisConfig::parse_str(&format!("include {}\n", inc.display())).is_err());
        assert!(RudisConfig::parse_str("include /nonexistent/rudis.conf\n").is_err());
        // A self-include is cut off instead of recursing forever.
        fs::write(&inc, format!("include {}\n", inc.display())).unwrap();
        assert!(RudisConfig::parse_str(&format!("include {}\n", inc.display())).is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_parse_invalid_number_error() {
        let text = "port not_a_number\n";
        assert!(RudisConfig::parse_str(text).is_err());
    }

    #[test]
    fn test_config_merge_cli() {
        let mut cfg = RudisConfig::default();
        assert_eq!(cfg.port, 6379);
        assert!(!cfg.appendonly);

        cfg.merge_cli(
            Some(8888),
            Some(2),
            Some(true),
            Some(PathBuf::from("/data/aof")),
            Some("1gb".to_string()),
            Some(75),
            Some(90),
            Some(8889),
            None,
            None,
            Some(true),
        );

        assert_eq!(cfg.port, 8888);
        assert_eq!(cfg.threads, Some(2));
        assert!(cfg.appendonly);
        assert_eq!(cfg.dir, PathBuf::from("/data/aof"));
        assert_eq!(cfg.maxmemory.as_deref(), Some("1gb"));
        assert_eq!(cfg.maxmemory_bytes, Some(1024 * 1024 * 1024));
        assert_eq!(cfg.tiered_offload_threshold, 75);
        assert_eq!(cfg.tiered_upload_threshold, 90);
        assert_eq!(cfg.tls_port, Some(8889));
        assert!(cfg.cluster_enabled);
    }

    /// Serializes the tests that set the process-wide `ACTIVE_CONFIG_FILE`.
    static ACTIVE_CONFIG_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

    #[test]
    fn test_rewrite_requires_a_config_file_and_resolves_it_absolutely() {
        let _guard = ACTIVE_CONFIG_LOCK.lock();
        let saved = ACTIVE_CONFIG_FILE.write().take();

        let err = rewrite_config_file(9999).unwrap_err();
        assert_eq!(err, "The server is running without a config file");
        assert!(ACTIVE_CONFIG_FILE.read().is_none());

        set_active_config_file(Path::new("conf/rudis.conf")).unwrap();
        let active = ACTIVE_CONFIG_FILE.read().clone().unwrap();
        assert!(active.is_absolute(), "{active:?}");
        assert_eq!(
            active,
            std::env::current_dir().unwrap().join("conf/rudis.conf")
        );

        *ACTIVE_CONFIG_FILE.write() = saved;
    }

    #[test]
    fn test_rewrite_config_file_atomic_and_durable() {
        let _guard = ACTIVE_CONFIG_LOCK.lock();
        let temp_dir = std::env::temp_dir().join(format!("rudis-cfg-test-{}", std::process::id()));
        let _ = fs::create_dir_all(&temp_dir);
        let cfg_path = temp_dir.join("test_rudis.conf");

        // Initial config file
        fs::write(
            &cfg_path,
            "# Sample config\nport 6379\nmaxclients 1000\n# Custom comment\n",
        )
        .unwrap();

        *ACTIVE_CONFIG_FILE.write() = Some(cfg_path.clone());

        // Rewrite with port 9999
        let res = rewrite_config_file(9999);
        assert!(res.is_ok());

        // Verify content
        let content = fs::read_to_string(&cfg_path).unwrap();
        assert!(content.contains("port 9999"));
        assert!(content.contains("# Custom comment"));
        assert!(content.contains("slowlog-log-slower-than"));
        assert!(content.contains("client-output-buffer-limit"));

        let _ = fs::remove_file(&cfg_path);
        let _ = fs::remove_dir_all(&temp_dir);
    }
}
