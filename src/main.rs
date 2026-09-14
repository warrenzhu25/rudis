use clap::Parser;
use std::thread;

use rudis::server::run_shard_worker;
use rudis::shard::ShardMessage;

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
}

fn main() {
    let args = Args::parse();

    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    let num_cores = if !core_ids.is_empty() {
        core_ids.len()
    } else {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    };

    let num_shards = args.threads.unwrap_or_else(|| num_cores.min(8));

    let aof_config = rudis::aof::AofConfig {
        enabled: args.aof,
        dir: args.aof_dir,
        fsync_every_sec: true,
    };

    println!("============================================================");
    println!("  rudis v0.1.0 (Redis in Rust)");
    println!("  Architecture: Multi-threaded Shared-Nothing (Thread-per-Core)");
    println!("  I/O Backend:  Linux io_uring (Monoio)");
    println!("  Listening:    0.0.0.0:{}", args.port);
    println!(
        "  Shards:       {} worker threads (pinned to CPU cores)",
        num_shards
    );
    println!(
        "  AOF Persist:  {}",
        if aof_config.enabled {
            "ENABLED"
        } else {
            "disabled"
        }
    );
    println!("============================================================");

    // Create cross-shard communication mesh
    let mut senders = Vec::with_capacity(num_shards);
    let mut receivers = Vec::with_capacity(num_shards);

    for _ in 0..num_shards {
        let (tx, rx) = flume::unbounded::<ShardMessage>();
        senders.push(tx);
        receivers.push(rx);
    }

    let mut handles = Vec::with_capacity(num_shards);

    for (shard_id, rx) in receivers.into_iter().enumerate() {
        let port = args.port;
        let shard_senders = senders.clone();
        let shard_aof_config = aof_config.clone();
        let core_id = if shard_id < core_ids.len() {
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
                );
            })
            .expect("Failed to spawn shard worker thread");

        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }
}
