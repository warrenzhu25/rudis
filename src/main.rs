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

    /// Interfaces to listen on, Redis `bind` syntax (e.g. "127.0.0.1 -::1"; default "* -::*")
    #[arg(long)]
    bind: Option<String>,

    /// Protected mode: with no default-user password, only accept loopback clients (yes|no, default yes)
    #[arg(long)]
    protected_mode: Option<String>,

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

    /// Accept the experimental, non-Redis command families (JSON., BF., CF.,
    /// CMS., TOPK., FT., SEMANTIC., CRDT., LLM., MCP., XDP.). Apart from JSON.,
    /// SEMANTIC., BF., CF., CMS. and TOPK., most of their writes are not
    /// persisted to the AOF or replicated (yes|no, default no)
    #[arg(long)]
    enable_experimental_commands: Option<String>,

    /// Serve Prometheus metrics over HTTP (GET /metrics) on this port
    #[arg(long)]
    metrics_port: Option<u16>,

    /// Address the metrics port listens on (default 127.0.0.1; the
    /// endpoint has no authentication)
    #[arg(long)]
    metrics_bind: Option<std::net::IpAddr>,
}

fn get_process_affinity_cores() -> Vec<usize> {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) == 0 {
            let mut cores = Vec::new();
            for i in 0..libc::CPU_SETSIZE as usize {
                if libc::CPU_ISSET(i, &set) {
                    cores.push(i);
                }
            }
            if !cores.is_empty() {
                return cores;
            }
        }
    }
    core_affinity::get_core_ids()
        .unwrap_or_default()
        .into_iter()
        .map(|c| c.id)
        .collect()
}

fn main() {
    #[cfg(target_os = "linux")]
    unsafe {
        // Disable Linux Transparent Huge Pages (THP) for this process and all its worker threads.
        // Prevents the kernel from promoting allocations to 2MB physical pages and eliminates 512x COW amplification.
        libc::prctl(libc::PR_SET_THP_DISABLE, 1, 0, 0, 0);
    }

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

    if let Some(b) = args.bind {
        server_config.bind = b;
    }

    let port = server_config.port;
    let bind_addrs = rudis::netsec::parse_bind_spec(&server_config.bind).unwrap_or_else(|e| {
        eprintln!("FATAL CONFIG: {}", e);
        std::process::exit(1);
    });
    let bind_display = rudis::netsec::format_bind_spec(&bind_addrs);
    rudis::netsec::set_bind_addrs(port, bind_addrs);
    if let Some(pm) = args.protected_mode {
        server_config.protected_mode = match pm.to_lowercase().as_str() {
            "yes" => true,
            "no" => false,
            other => {
                eprintln!(
                    "FATAL CONFIG: --protected-mode expects yes|no, got '{}'",
                    other
                );
                std::process::exit(1);
            }
        };
    }
    rudis::netsec::set_protected_mode(port, server_config.protected_mode);
    if let Some(bytes) = server_config.maxmemory_bytes {
        rudis::tiering::set_max_memory(port, bytes);
    }
    rudis::tiering::set_offload_threshold_pct(port, server_config.tiered_offload_threshold);
    rudis::tiering::set_upload_threshold_pct(port, server_config.tiered_upload_threshold);
    rudis::connection::set_max_clients(server_config.maxclients);
    rudis::connection::set_max_memory_policy(&server_config.maxmemory_policy);

    if let Some(ref pass) = server_config.requirepass {
        rudis::acl::get_acl_for_port(port)
            .write()
            .unwrap()
            .set_requirepass(pass);
    }

    let allowed_cores = get_process_affinity_cores();
    let num_cores = if !allowed_cores.is_empty() {
        allowed_cores.len()
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
        rudis::cluster::HAS_ACTIVE_CLUSTER.store(true, std::sync::atomic::Ordering::Release);
        hub.num_shards
            .store(num_shards, std::sync::atomic::Ordering::Release);
    }

    rudis::aof::set_aof_load_truncated(server_config.aof_load_truncated);
    rudis::mailbox::set_cross_shard_spin(server_config.cross_shard_spin);
    if let Some(v) = args.enable_experimental_commands {
        server_config.enable_experimental_commands = match v.to_lowercase().as_str() {
            "yes" => true,
            "no" => false,
            other => {
                eprintln!(
                    "FATAL CONFIG: --enable-experimental-commands expects yes|no, got '{}'",
                    other
                );
                std::process::exit(1);
            }
        };
    }
    rudis::resp::set_experimental_commands(server_config.enable_experimental_commands);
    rudis::config::set_save_points(port, server_config.save_points.clone());
    if let Some(p) = args.metrics_port {
        server_config.metrics_port = Some(p);
    }
    if let Some(b) = args.metrics_bind {
        server_config.metrics_bind = b;
    }
    let metrics_addr = server_config.metrics_port.map(|metrics_port| {
        let addr = std::net::SocketAddr::new(server_config.metrics_bind, metrics_port);
        rudis::telemetry::bind_metrics_listener(port, addr).unwrap_or_else(|e| {
            eprintln!("FATAL CONFIG: cannot listen for metrics on {}: {}", addr, e);
            std::process::exit(1);
        })
    });
    let mut ignored_directives: Vec<&str> = Vec::new();
    for (name, value) in &server_config.extra_directives {
        if (name == "replicaof" || name == "slaveof")
            && let Err(e) = rudis::replication::set_startup_replicaof(port, value)
        {
            eprintln!("FATAL CONFIG: invalid '{}' directive: {}", name, e);
            std::process::exit(1);
        }
        match rudis::connection::apply_config_value(port, port, name, value) {
            Ok(true) => {}
            Ok(false) => {
                if !ignored_directives.contains(&name.as_str()) {
                    ignored_directives.push(name);
                }
            }
            Err(e) => {
                eprintln!("FATAL CONFIG: invalid '{}' directive: {}", name, e);
                std::process::exit(1);
            }
        }
    }
    if !ignored_directives.is_empty() {
        eprintln!(
            "WARNING: config directives not supported by rudis were ignored: {}",
            ignored_directives.join(", ")
        );
    }
    for (user, rules) in &server_config.users {
        if let Err(e) = rudis::acl::get_acl_for_port(port)
            .write()
            .unwrap()
            .set_user(user, rules)
        {
            eprintln!("FATAL CONFIG: error in user declaration '{}': {}", user, e);
            std::process::exit(1);
        }
    }
    let aof_config = rudis::aof::AofConfig {
        enabled: server_config.appendonly,
        dir: server_config.dir.clone(),
        fsync_every_sec: server_config.appendfsync_every_sec,
    };
    if aof_config.enabled {
        match rudis::aof::reshard_aof_dir(&aof_config.dir, num_shards, port) {
            Ok(Some(old)) => println!(
                "AOF files in {:?} were written with a different shard layout ({} shards); \
                 resharded them for {} shards (originals kept in an aof-reshard-backup-* dir)",
                aof_config.dir, old, num_shards
            ),
            Ok(None) => {}
            Err(e) => {
                eprintln!(
                    "FATAL: cannot reshard the AOF files in {:?}: {}",
                    aof_config.dir, e
                );
                std::process::exit(1);
            }
        }
    }

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
    println!("  Listening:    port {} (bind {})", port, bind_display);
    if let Some(ref tls_cfg) = tls_config {
        println!(
            "  TLS Port:     {} (bind {})",
            tls_cfg.tls_port, bind_display
        );
    }
    println!(
        "  Shards:       {} worker threads (pinned to CPU cores)",
        num_shards
    );
    if let Some(addr) = metrics_addr {
        println!("  Metrics:      http://{}/metrics", addr);
    }
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
        let core_id = if !args.no_pin && shard_id < allowed_cores.len() {
            Some(core_affinity::CoreId {
                id: allowed_cores[shard_id],
            })
        } else {
            None
        };

        let handle = thread::Builder::new()
            .name(format!("rudis-shard-{}", shard_id))
            .stack_size(rudis::server::SHARD_THREAD_STACK_SIZE)
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
