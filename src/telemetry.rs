use parking_lot::Mutex;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing_subscriber::{EnvFilter, fmt};

static TELEMETRY_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Initializes structured logging with `tracing` and `EnvFilter` (default: INFO level).
pub fn init_telemetry() {
    if !TELEMETRY_INITIALIZED.swap(true, Ordering::SeqCst) {
        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,rudis=info"));

        let _ = fmt()
            .with_env_filter(env_filter)
            .with_target(false)
            .with_thread_ids(false)
            .with_thread_names(false)
            .compact()
            .try_init();
    }
}

/// Formats Prometheus openmetrics format for server statistics
pub fn format_prometheus_metrics(port: u16, total_used_memory: usize) -> String {
    let stats = crate::tiering::get_tier_stats(port);
    let max_mem = crate::tiering::get_max_memory(port);
    let expired = crate::table::get_expired_keys();
    let evicted = crate::table::get_evicted_keys();
    let active_clients = crate::connection::get_active_clients();
    let max_clients = crate::connection::get_max_clients();
    let isolated_panics = crate::connection::get_isolated_panics();

    format!(
        "# HELP rudis_connected_clients Current active client connections\n\
         # TYPE rudis_connected_clients gauge\n\
         rudis_connected_clients {}\n\
         # HELP rudis_max_clients Configured max client connections\n\
         # TYPE rudis_max_clients gauge\n\
         rudis_max_clients {}\n\
         # HELP rudis_isolated_panics_total Total count of panics caught and isolated\n\
         # TYPE rudis_isolated_panics_total counter\n\
         rudis_isolated_panics_total {}\n\
         # HELP rudis_used_memory_bytes Total RAM memory in bytes used by keyspace\n\
         # TYPE rudis_used_memory_bytes gauge\n\
         rudis_used_memory_bytes {}\n\
         # HELP rudis_max_memory_bytes Configured maximum memory limit in bytes\n\
         # TYPE rudis_max_memory_bytes gauge\n\
         rudis_max_memory_bytes {}\n\
         # HELP rudis_expired_keys_total Total number of key expiration events\n\
         # TYPE rudis_expired_keys_total counter\n\
         rudis_expired_keys_total {}\n\
         # HELP rudis_evicted_keys_total Total number of keys evicted due to maxmemory limit\n\
         # TYPE rudis_evicted_keys_total counter\n\
         rudis_evicted_keys_total {}\n\
         # HELP rudis_tiered_keys Current number of keys stored on NVMe disk\n\
         # TYPE rudis_tiered_keys gauge\n\
         rudis_tiered_keys {}\n\
         # HELP rudis_ram_hits_total Number of keyspace read hits in RAM\n\
         # TYPE rudis_ram_hits_total counter\n\
         rudis_ram_hits_total {}\n\
         # HELP rudis_ram_misses_total Number of keyspace read misses in RAM needing disk fetch\n\
         # TYPE rudis_ram_misses_total counter\n\
         rudis_ram_misses_total {}\n",
        active_clients,
        max_clients,
        isolated_panics,
        total_used_memory,
        max_mem,
        expired,
        evicted,
        stats.tiered_keys.load(Ordering::Relaxed),
        stats.ram_hits.load(Ordering::Relaxed),
        stats.ram_misses.load(Ordering::Relaxed),
    ) + &format_server_stats()
}

/// The INFO `# Stats` counters as Prometheus counters.
fn format_server_stats() -> String {
    use crate::server_stats::{Stat, total};
    let counters = [
        ("commands_processed", "Commands processed", Stat::Commands),
        (
            "connections_received",
            "Client connections accepted",
            Stat::ConnectionsReceived,
        ),
        (
            "rejected_connections",
            "Client connections rejected because of maxclients",
            Stat::RejectedConnections,
        ),
        (
            "net_input_bytes",
            "Bytes read from clients",
            Stat::NetInputBytes,
        ),
        (
            "net_output_bytes",
            "Bytes written to clients",
            Stat::NetOutputBytes,
        ),
        (
            "keyspace_hits",
            "Key lookups that found the key",
            Stat::KeyspaceHits,
        ),
        (
            "keyspace_misses",
            "Key lookups that did not find the key",
            Stat::KeyspaceMisses,
        ),
    ];
    let mut out = String::new();
    for (name, help, stat) in counters {
        out += &format!(
            "# HELP rudis_{name}_total {help}\n\
             # TYPE rudis_{name}_total counter\n\
             rudis_{name}_total {}\n",
            total(stat)
        );
    }
    out
}

/// Escapes a Prometheus label value.
fn label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Per-command calls, time, failures and latency histograms (from the
/// same data as INFO commandstats / LATENCY HISTOGRAM), error counts by
/// prefix, and persistence and replication state.
pub fn format_server_state_metrics(router: &crate::router::Router) -> String {
    use crate::connection::{CMD_STATS, ERROR_STATS, FAILED_CMD_STATS, REJECTED_CMD_STATS};
    let mut out = String::new();

    let stats: Vec<(String, crate::connection::CmdStat)> = {
        let map = CMD_STATS.read();
        let mut v: Vec<_> = map.iter().map(|(k, s)| (k.clone(), *s)).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    };
    out += "# HELP rudis_command_calls_total Calls per command\n# TYPE rudis_command_calls_total counter\n";
    for (cmd, s) in &stats {
        out += &format!(
            "rudis_command_calls_total{{cmd=\"{}\"}} {}\n",
            label(cmd),
            s.calls
        );
    }
    out += "# HELP rudis_command_duration_seconds_total Time spent executing each command\n# TYPE rudis_command_duration_seconds_total counter\n";
    for (cmd, s) in &stats {
        out += &format!(
            "rudis_command_duration_seconds_total{{cmd=\"{}\"}} {:.9}\n",
            label(cmd),
            s.nanos as f64 / 1e9
        );
    }
    for (name, help, map) in [
        ("failed", "Calls that returned an error", &FAILED_CMD_STATS),
        (
            "rejected",
            "Calls rejected before execution",
            &REJECTED_CMD_STATS,
        ),
    ] {
        out += &format!(
            "# HELP rudis_command_{name}_total {help}\n# TYPE rudis_command_{name}_total counter\n"
        );
        let map = map.read();
        let mut v: Vec<_> = map.iter().collect();
        v.sort();
        for (cmd, n) in v {
            out += &format!(
                "rudis_command_{name}_total{{cmd=\"{}\"}} {}\n",
                label(cmd),
                n
            );
        }
    }
    // Histogram buckets are powers of two microseconds (1 us .. ~16.8 s).
    out += "# HELP rudis_command_latency_seconds Command execution latency\n# TYPE rudis_command_latency_seconds histogram\n";
    for (cmd, s) in &stats {
        let c = label(cmd);
        let mut cum = 0u64;
        for i in 0..=24 {
            cum += s.hist[i];
            out += &format!(
                "rudis_command_latency_seconds_bucket{{cmd=\"{c}\",le=\"{}\"}} {cum}\n",
                (1u64 << i) as f64 / 1e6
            );
        }
        out += &format!(
            "rudis_command_latency_seconds_bucket{{cmd=\"{c}\",le=\"+Inf\"}} {}\n\
             rudis_command_latency_seconds_sum{{cmd=\"{c}\"}} {:.9}\n\
             rudis_command_latency_seconds_count{{cmd=\"{c}\"}} {}\n",
            s.calls,
            s.nanos as f64 / 1e9,
            s.calls
        );
    }
    out += "# HELP rudis_errors_total Error replies by error prefix\n# TYPE rudis_errors_total counter\n";
    {
        let map = ERROR_STATS.read();
        let mut v: Vec<_> = map.iter().collect();
        v.sort();
        for (prefix, n) in v {
            out += &format!("rudis_errors_total{{prefix=\"{}\"}} {}\n", label(prefix), n);
        }
    }

    let snap = crate::snapshot::state(router.base_port);
    let aof_rw = crate::snapshot::aof_rewrite_state(router.base_port);
    for (name, help, v) in [
        (
            "rdb_changes_since_last_save",
            "Writes since the last successful save",
            snap.changes_since_last_save(),
        ),
        (
            "rdb_last_save_timestamp_seconds",
            "Unix time of the last successful save",
            snap.last_save_unix(),
        ),
        (
            "rdb_bgsave_in_progress",
            "1 while a save runs",
            u64::from(snap.in_progress()),
        ),
        (
            "rdb_last_bgsave_ok",
            "1 if the last save succeeded",
            u64::from(snap.last_save_ok()),
        ),
        (
            "aof_enabled",
            "1 if AOF is on",
            u64::from(router.aof.is_some()),
        ),
        (
            "aof_rewrite_in_progress",
            "1 while an AOF rewrite runs",
            u64::from(aof_rw.in_progress()),
        ),
        (
            "aof_last_rewrite_ok",
            "1 if the last AOF rewrite succeeded",
            u64::from(aof_rw.last_ok()),
        ),
        (
            "loading",
            "1 while loading data at startup or from a master",
            u64::from(crate::replication::is_loading(router.port)),
        ),
    ] {
        out +=
            &format!("# HELP rudis_{name} {help}\n# TYPE rudis_{name} gauge\nrudis_{name} {v}\n");
    }

    // INFO replication fields that are plain numbers become gauges; the
    // role becomes a labelled 1.
    let repl = crate::replication::get_replication_hub(router.port).format_info_replication();
    for line in repl.lines() {
        let Some((k, v)) = line.trim_end().split_once(':') else {
            continue;
        };
        if k == "role" {
            out += &format!(
                "# HELP rudis_replication_role Replication role\n# TYPE rudis_replication_role gauge\nrudis_replication_role{{role=\"{}\"}} 1\n",
                label(v)
            );
        } else if !k.is_empty()
            && k.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            && let Ok(n) = v.parse::<i64>()
        {
            out += &format!(
                "# HELP rudis_replication_{k} INFO replication {k}\n# TYPE rudis_replication_{k} gauge\nrudis_replication_{k} {n}\n"
            );
        }
    }
    out
}

/// `--metrics-port` listeners, bound at startup so a bad address fails
/// fast, then served by shard 0. Keyed by the server's base port.
static METRICS_LISTENERS: Mutex<Option<HashMap<u16, std::net::TcpListener>>> = Mutex::new(None);

/// How long a scraper may take to send its request.
const METRICS_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Largest request head accepted on the metrics port.
const METRICS_MAX_REQUEST: usize = 8192;

/// Binds the HTTP metrics listener for the server on `port`.
pub fn bind_metrics_listener(port: u16, addr: SocketAddr) -> std::io::Result<SocketAddr> {
    let listener = std::net::TcpListener::bind(addr)?;
    listener.set_nonblocking(true)?;
    let local = listener.local_addr()?;
    METRICS_LISTENERS
        .lock()
        .get_or_insert_with(HashMap::new)
        .insert(port, listener);
    Ok(local)
}

/// Takes the listener bound by [`bind_metrics_listener`], if any.
pub fn take_metrics_listener(port: u16) -> Option<std::net::TcpListener> {
    METRICS_LISTENERS.lock().as_mut()?.remove(&port)
}

/// The HTTP response for a request head: the metrics for `GET /metrics`,
/// 404 for other paths, 405 for other methods. `metrics` is only called
/// for a metrics request.
pub fn metrics_http_response(request: &[u8], metrics: impl FnOnce() -> String) -> Vec<u8> {
    let line = request.split(|&b| b == b'\r' || b == b'\n').next();
    let mut parts = line.unwrap_or_default().split(|&b| b == b' ');
    let (method, path) = (parts.next(), parts.next());
    let path = path.map(|p| p.split(|&b| b == b'?').next().unwrap_or_default());
    let (status, content_type, body) = match (method, path) {
        (Some(b"GET"), Some(b"/metrics")) => (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            metrics(),
        ),
        (Some(b"GET"), _) => ("404 Not Found", "text/plain", "not found\n".to_string()),
        _ => (
            "405 Method Not Allowed",
            "text/plain",
            "method not allowed\n".to_string(),
        ),
    };
    let mut out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body.as_bytes());
    out
}

/// Serves `GET /metrics` on `listener` (Prometheus text format) until the
/// shard stops. Runs on shard 0; each scrape costs one INFO-style memory
/// fan-out across the shards.
pub async fn serve_metrics(router: Rc<crate::router::Router>, listener: monoio::net::TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            // e.g. out of file descriptors: back off instead of spinning.
            monoio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let router = router.clone();
        monoio::spawn(async move {
            let _ =
                monoio::time::timeout(METRICS_REQUEST_TIMEOUT, serve_metrics_conn(&router, stream))
                    .await;
        });
    }
}

async fn serve_metrics_conn(router: &crate::router::Router, mut stream: monoio::net::TcpStream) {
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
    let mut request = Vec::new();
    let mut buf = vec![0u8; 1024];
    while !request.windows(4).any(|w| w == b"\r\n\r\n") {
        let (res, b) = stream.read(buf).await;
        buf = b;
        match res {
            Ok(0) | Err(_) => return,
            Ok(n) => request.extend_from_slice(&buf[..n]),
        }
        if request.len() > METRICS_MAX_REQUEST {
            return;
        }
    }
    let wants_metrics = request.starts_with(b"GET /metrics");
    let used = if wants_metrics {
        // Per-command stats are buffered per shard; merge them first.
        router.flush_all_command_stats().await;
        router.get_total_used_memory().await
    } else {
        0
    };
    let response = metrics_http_response(&request, || {
        format_prometheus_metrics(router.port, used) + &format_server_state_metrics(router)
    });
    let _ = stream.write_all(response).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_telemetry_idempotency() {
        init_telemetry();
        init_telemetry(); // safe multiple invocations
        assert!(TELEMETRY_INITIALIZED.load(Ordering::Relaxed));
    }

    #[test]
    fn test_format_prometheus_metrics() {
        let metrics = format_prometheus_metrics(0, 1024 * 1024);
        assert!(metrics.contains("rudis_connected_clients"));
        assert!(metrics.contains("rudis_used_memory_bytes 1048576"));
        assert!(metrics.contains("rudis_expired_keys_total"));
        assert!(metrics.contains("rudis_evicted_keys_total"));
        assert!(metrics.contains("rudis_isolated_panics_total"));
        assert!(metrics.contains("# TYPE rudis_commands_processed_total counter\n"));
        assert!(metrics.contains("rudis_keyspace_misses_total "));
    }

    #[test]
    fn test_metrics_http_response_routes_requests() {
        let ok = metrics_http_response(b"GET /metrics?x=1 HTTP/1.1\r\nHost: a\r\n\r\n", || {
            "m 1\n".to_string()
        });
        let ok = String::from_utf8(ok).unwrap();
        assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"), "{ok}");
        assert!(ok.contains("Content-Type: text/plain; version=0.0.4"));
        assert!(ok.ends_with("Content-Length: 4\r\nConnection: close\r\n\r\nm 1\n"));

        let not_metrics = |_: &str| -> String { panic!("metrics built for a non-metrics request") };
        let missing = metrics_http_response(b"GET / HTTP/1.1\r\n\r\n", || not_metrics(""));
        assert!(missing.starts_with(b"HTTP/1.1 404 "));
        let post = metrics_http_response(b"POST /metrics HTTP/1.1\r\n\r\n", || not_metrics(""));
        assert!(post.starts_with(b"HTTP/1.1 405 "));
        let junk = metrics_http_response(b"\r\n\r\n", || not_metrics(""));
        assert!(junk.starts_with(b"HTTP/1.1 405 "));
    }
}
