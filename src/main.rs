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
    /// Port to listen on
    #[arg(short, long, default_value_t = 6379)]
    port: u16,

    /// Number of worker threads / shards (defaults to number of CPU cores)
    #[arg(short, long)]
    threads: Option<usize>,

    /// Enable Append-Only File (AOF) persistence
    #[arg(long, default_value_t = false)]
    aof: bool,

    /// Directory to store AOF files
    #[arg(long, default_value = ".")]
    aof_dir: std::path::PathBuf,

    /// Max memory limit for tiered storage auto-tiering (e.g. 512mb, 1gb)
    #[arg(long)]
    maxmemory: Option<String>,

    /// Offload memory threshold percentage (default: 60)
    #[arg(long, default_value_t = 60)]
    tiered_offload_threshold: u64,

    /// Upload streaming memory threshold percentage (default: 80)
    #[arg(long, default_value_t = 80)]
    tiered_upload_threshold: u64,

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
    #[arg(long, default_value = "no")]
    cluster_enabled: String,
}

fn main() {
    let args = Args::parse();

    if let Some(ref m) = args.maxmemory
        && let Some(bytes) = rudis::tiering::parse_memory_bytes(m)
    {
        rudis::tiering::set_max_memory(args.port, bytes);
    }
    rudis::tiering::set_offload_threshold_pct(args.port, args.tiered_offload_threshold);
    rudis::tiering::set_upload_threshold_pct(args.port, args.tiered_upload_threshold);

    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let num_cores = if !core_ids.is_empty() {
        core_ids.len()
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    };

    let num_shards = args.threads.unwrap_or_else(|| num_cores.min(8));

    let cluster_enabled = matches!(
        args.cluster_enabled.to_lowercase().as_str(),
        "yes" | "true" | "1"
    );

    if cluster_enabled {
        let hub = rudis::cluster::get_cluster_hub(args.port);
        hub.cluster_enabled
            .store(true, std::sync::atomic::Ordering::Release);
        hub.num_shards
            .store(num_shards, std::sync::atomic::Ordering::Release);
    }

    let aof_config = rudis::aof::AofConfig {
        enabled: args.aof,
        dir: args.aof_dir,
        fsync_every_sec: true,
    };

    let tls_config = if let Some(tls_port) = args.tls_port {
        let server_config = match (&args.tls_cert_file, &args.tls_key_file) {
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
            server_config,
        })
    } else {
        None
    };

    println!("============================================================");
    println!("  rudis v0.1.0 (Redis in Rust)");
    println!("  Architecture: Multi-threaded Shared-Nothing (Thread-per-Core)");
    println!("  I/O Backend:  Linux io_uring (Monoio)");
    println!("  Listening:    0.0.0.0:{}", args.port);
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
            args.port,
            args.port + num_shards as u16 - 1
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
    println!("============================================================");

    // Create lock-free cross-shard communication mesh
    let (senders_mesh, receivers) = rudis::mailbox::create_shard_mesh(num_shards);

    let mut handles = Vec::with_capacity(num_shards);

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let port = args.port;
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
}
