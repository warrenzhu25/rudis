use std::sync::atomic::{AtomicBool, Ordering};
use tracing_subscriber::{EnvFilter, fmt};

static TELEMETRY_INITIALIZED: AtomicBool = AtomicBool::new(false);

/// Initializes structured logging with `tracing` and `EnvFilter` (default: INFO level).
pub fn init_telemetry() {
    if !TELEMETRY_INITIALIZED.swap(true, Ordering::SeqCst) {
        let env_filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("info,rudis=info"));

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
    )
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
    }
}
