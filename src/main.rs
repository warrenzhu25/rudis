use clap::Parser;
use std::thread;

use rudis::server::run_shard_worker;

#[derive(Parser, Debug)]
#[command(
    name = "rudis",
    version = "0.1.0",
    about = "Multi-threaded Shared-Nothing Redis in Rust based on io_uring"
)]
struct Args {
    /// Path to configuration file (e.g. rudis.conf or redis.conf)
    #[arg(short = 'c', long)]
    config: Option<std::path::PathBuf>,

    /// Port to listen on
    #[arg(short, long)]
    port: Option<u16>,

    /// Number of worker threads / shards (defaults to number of CPU cores)
    #[arg(short, long)]
    threads: Option<usize>,

    /// Enable Append-Only File (AOF) persistence
    #[arg(long)]
    aof: Option<bool>,

    /// Directory to store AOF files
    #[arg(long)]
    aof_dir: Option<std::path::PathBuf>,

    /// Max memory limit for tiered storage auto-tiering (e.g. 512mb, 1gb)
    #[arg(long)]
    maxmemory: Option<String>,

    /// Offload memory threshold percentage (default: 60)
    #[arg(long)]
    tiered_offload_threshold: Option<u64>,

    /// Upload streaming memory threshold percentage (default: 80)
    #[arg(long)]
    tiered_upload_threshold: Option<u64>,

    /// Disable CPU core affinity pinning
    #[arg(long, default_value_t = false)]
    no_pin: bool,

    /// Optional TLS port to listen for secure TLS connections
    #[arg(long)]
    tls_port: Option<u16>,

    /// Path to TLS certificate PEM file
    #[arg(long)]
    tls_cert_file: Option<std::path::PathBuf>,

    /// Path to TLS private key PEM file
    #[arg(long)]
    tls_key_file: Option<std::path::PathBuf>,

    /// Enable Redis Cluster mode with per-shard direct routing (e.g. --cluster-enabled yes)
    #[arg(long)]
    cluster_enabled: Option<String>,
}

fn main() {
    rudis::telemetry::init_telemetry();
    rudis::shutdown::install_signal_handlers();
    let args = Args::parse();

    // 1. Load initial config from file if provided, else use defaults
    let mut server_config = if let Some(ref config_path) = args.config {
        rudis::config::RudisConfig::load_file(config_path)
            .unwrap_or_else(|e| panic!("Failed to load config file {:?}: {}", config_path, e))
    } else {
        rudis::config::RudisConfig::default()
    };

    // 2. Merge CLI overrides
    let cluster_opt = args
        .cluster_enabled
        .as_ref()
        .map(|s| matches!(s.to_lowercase().as_str(), "yes" | "true" | "1"));
    server_config.merge_cli(
        args.port,
        args.threads,
        args.aof,
        args.aof_dir,
        args.maxmemory,
        args.tiered_offload_threshold,
        args.tiered_upload_threshold,
        args.tls_port,
        args.tls_cert_file,
        args.tls_key_file,
        cluster_opt,
    );

    let port = server_config.port;
    if let Some(bytes) = server_config.maxmemory_bytes {
        rudis::tiering::set_max_memory(port, bytes);
    }
    rudis::tiering::set_offload_threshold_pct(port, server_config.tiered_offload_threshold);
    rudis::tiering::set_upload_threshold_pct(port, server_config.tiered_upload_threshold);
    rudis::connection::set_max_clients(server_config.maxclients);
    rudis::connection::set_max_memory_policy(&server_config.maxmemory_policy);

    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let num_cores = if !core_ids.is_empty() {
        core_ids.len()
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    };

    let num_shards = server_config.threads.unwrap_or_else(|| num_cores.min(8));

    let cluster_enabled = server_config.cluster_enabled;
    if cluster_enabled {
        let hub = rudis::cluster::get_cluster_hub(port);
        hub.cluster_enabled
            .store(true, std::sync::atomic::Ordering::Release);
        rudis::cluster::HAS_ACTIVE_CLUSTER
            .store(true, std::sync::atomic::Ordering::Release);
        hub.num_shards
            .store(num_shards, std::sync::atomic::Ordering::Release);
    }

    let aof_config = rudis::aof::AofConfig {
        enabled: server_config.appendonly,
        dir: server_config.dir.clone(),
        fsync_every_sec: true,
    };

    let tls_config = if let Some(tls_port) = server_config.tls_port {
        let server_config_tls = match (&server_config.tls_cert_file, &server_config.tls_key_file) {
            (Some(cert_path), Some(key_path)) => {
                rudis::tls::load_certs_and_key_from_files(cert_path, key_path)
                    .expect("Failed to load TLS cert/key files")
            }
            _ => {
                let (cert_der, key_der) = rudis::tls::generate_self_signed_cert(vec![
                    "localhost".to_string(),
                    "127.0.0.1".to_string(),
                ])
                .expect("Failed to generate in-memory self-signed TLS cert");
                rudis::tls::create_server_config(&cert_der, &key_der)
                    .expect("Failed to create TLS server config")
            }
        };
        Some(rudis::tls::TlsWorkerConfig {
            tls_port,
            server_config: server_config_tls,
        })
    } else {
        None
    };

    println!("============================================================");
    println!("  rudis v0.1.0 (Redis in Rust)");
    println!("  Architecture: Multi-threaded Shared-Nothing (Thread-per-Core)");
    println!("  I/O Backend:  Linux io_uring (Monoio)");
    println!("  Listening:    0.0.0.0:{}", port);
    if let Some(ref tls_cfg) = tls_config {
        println!("  TLS Port:     0.0.0.0:{}", tls_cfg.tls_port);
    }
    println!(
        "  Shards:       {} worker threads (pinned to CPU cores)",
        num_shards
    );
    if cluster_enabled {
        println!(
            "  Cluster Mode: ENABLED (Per-shard ports: {}-{})",
            port,
            port + num_shards as u16 - 1
        );
    }
    println!(
        "  AOF Persist:  {}",
        if aof_config.enabled {
            "ENABLED"
        } else {
            "disabled"
        }
    );
    let sanity_report = rudis::syscheck::run_system_sanity_checks();
    rudis::syscheck::print_sanity_warnings(&sanity_report);
    println!("============================================================");

    // Create lock-free cross-shard communication mesh
    let (senders_mesh, receivers) = rudis::mailbox::create_shard_mesh(num_shards);

    let mut handles = Vec::with_capacity(num_shards);

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let shard_senders = senders_mesh[shard_id].clone();
        let shard_aof_config = aof_config.clone();
        let shard_tls_config = tls_config.clone();
        let core_id = if !args.no_pin && shard_id < core_ids.len() {
            Some(core_ids[shard_id])
        } else {
            None
        };

        let handle = thread::Builder::new()
            .name(format!("rudis-shard-{}", shard_id))
            .spawn(move || {
                run_shard_worker(
                    shard_id,
                    num_shards,
                    port,
                    shard_senders,
                    rx,
                    core_id,
                    shard_aof_config,
                    shard_tls_config,
                    cluster_enabled,
                );
            })
            .expect("Failed to spawn shard worker thread");

        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }
    println!("rudis server gracefully stopped. Goodbye!");
}
