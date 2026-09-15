# Component 01: Reactor Runtime & Server Lifecycle (`src/main.rs`, `src/server.rs`)

> ⚠️ **Unverified against the real source.** This document's §2.4 "Signal-Safe Shutdown"
> (trapping `SIGINT`/`SIGTERM`, draining requests) does not exist anywhere in `src/server.rs`
> — there is no signal handling in the codebase. Treat the rest of this file's claims as
> unverified too. See [`docs/designs/components.md`](../designs/components.md#part-2-server-runtime-and-entrypoint)
> (Part 2) for a version checked against the actual code.

## 1. Architectural Purpose & Scope

The **Reactor Runtime & Server Lifecycle** subsystem is responsible for bootstrapping the Rudis server process, pinning worker threads to physical CPU cores, setting up Linux `io_uring` instances via the `monoio` asynchronous runtime, and orchestrating thread-local event loops.

Unlike Redis (single-threaded event loop) or KeyDB/Memcached (multi-threaded with global lock synchronization), Rudis uses a **Shared-Nothing Multi-Reactor** pattern. Every worker core runs an independent `monoio` event loop driving an isolated Linux `io_uring` ring.

---

## 2. Key Invariants & Concurrency Constraints

1. **Thread-per-Core Pinning**: Every worker thread is pinned to an exclusive CPU core using `core_affinity`. No worker thread is ever migrated by the OS scheduler, avoiding L1/L2 cache invalidations.
2. **Zero Global Locks**: The reactor thread owns its database partition, block hub, and tiering controller. It never acquires mutexes or enters atomic CAS loops in the request path.
3. **`SO_REUSEPORT` Ingress Balancing**: Every worker thread opens its own listening socket bound to the same port. The Linux kernel's network stack distributes new incoming connections directly across all worker sockets via a 4-tuple hash (`src_ip, src_port, dst_ip, dst_port`), bypassing any userspace dispatch thread.
4. **Signal-Safe Shutdown**: Graceful termination traps `SIGINT` and `SIGTERM`, drains in-flight requests, flushes dirty AOF buffers, and cleanly drops socket file descriptors.

---

## 3. Component Architecture & Data Structures

```
                             Process Startup (main.rs)
                                        │
                                        ▼ (CLI Parsing & Config Validation)
                     Detect Available Hardware Cores (N)
                                        │
             ┌──────────────────────────┴──────────────────────────┐
             ▼                                                     ▼
    Spawn Worker Thread 0                                 Spawn Worker Thread 1..N-1
             │                                                     │
    core_affinity::set(0)                                 core_affinity::set(i)
             │                                                     │
  Monoio Runtime Init (io_uring)                        Monoio Runtime Init (io_uring)
             │                                                     │
  Bind Socket (SO_REUSEPORT)                            Bind Socket (SO_REUSEPORT)
             │                                                     │
   Shard 0 Event Loop                                    Shard i Event Loop
```

### Core Structures in `src/server.rs`

```rust
pub struct ServerConfig {
    pub port: u16,
    pub threads: usize,
    pub no_pin: bool,
    pub max_memory: Option<usize>,
    pub tiering_path: Option<PathBuf>,
    pub requirepass: Option<String>,
    pub tls_port: Option<u16>,
}

pub struct ShardContext {
    pub shard_id: usize,
    pub num_shards: usize,
    pub db: RudisDb,
    pub router: RouterHandle,
    pub hub: BlockHub,
    pub tiering: Option<TieredStorage>,
}
```

---

## 4. Execution Algorithms & Code Logic

### 4.1 Server Initialization Sequence

```rust
pub fn main() {
    let args = parse_cli_args();
    let num_threads = args.threads.unwrap_or_else(|| num_cpus::get());
    let cores = core_affinity::get_core_ids().expect("Failed to retrieve core IDs");

    // Initialize cross-shard channel mesh
    let (router_mesh, shard_receivers) = RouterMesh::new(num_threads);

    let mut handles = Vec::new();

    for (shard_id, rx) in shard_receivers.into_iter().enumerate() {
        let core_id = cores[shard_id % cores.len()];
        let config = args.clone();
        let router = router_mesh.get_handle(shard_id);

        let handle = std::thread::spawn(move || {
            // 1. Pin thread to designated CPU core
            if !config.no_pin {
                core_affinity::set_for_current(core_id);
            }

            // 2. Initialize monoio io_uring async runtime
            let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
                .enable_timer()
                .build()
                .expect("Failed to initialize monoio runtime");

            // 3. Run shard event loop inside io_uring proactor
            rt.block_on(async move {
                run_shard_event_loop(shard_id, num_threads, config, router, rx).await;
            });
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }
}
```

### 4.2 The Shard Event Loop (`run_shard_event_loop`)

The event loop handles three concurrent asynchronous streams on each core:
1. **New Ingress Connections**: Accepts client sockets from the `SO_REUSEPORT` listener.
2. **Cross-Shard Inter-Thread Messages**: Processes forwarded requests or notifications from peer shards arriving on `rx` (`flume::Receiver`).
3. **Active Eviction & Periodic Timers**: Performs passive expiration sampling, memory watermark checks, and AOF sync.

```rust
async fn run_shard_event_loop(
    shard_id: usize,
    num_shards: usize,
    config: ServerConfig,
    router: RouterHandle,
    shard_rx: flume::Receiver<ShardMessage>,
) {
    // Open thread-local SO_REUSEPORT TCP listener
    let listener = create_reuseport_listener(config.port).expect("Listener bind failed");
    let mut db = RudisDb::new(shard_id, config.max_memory);
    let mut block_hub = BlockHub::new();

    loop {
        monoio::select! {
            // Stream 1: Incoming TCP connection
            conn = listener.accept() => {
                if let Ok((stream, _peer_addr)) = conn {
                    let client_router = router.clone();
                    monoio::spawn(async move {
                        run_connection_handler(stream, client_router).await;
                    });
                }
            }

            // Stream 2: Message from another shard (Cross-shard request / wakeup)
            msg = shard_rx.recv_async() => {
                if let Ok(shard_msg) = msg {
                    handle_shard_message(shard_msg, &mut db, &mut block_hub).await;
                }
            }

            // Stream 3: Periodic 100ms maintenance timer
            _ = monoio::time::sleep(Duration::from_millis(100)) => {
                db.table.active_expire_sample(10);
                if let Some(ref mut tier) = db.tiering {
                    tier.check_memory_pressure(&mut db.table);
                }
            }
        }
    }
}
```

---

## 5. Cross-Component Interactions

- **`src/connection.rs`**: When a new connection is accepted, a connection task is spawned as a lightweight cooperative future within the shard's `monoio` executor.
- **`src/router.rs`**: Passes outgoing requests across shards through lock-free channels if a key is not owned by the current shard.
- **`src/block.rs`**: Wakes up blocked clients when cross-shard list/zset push notifications are received.

---

## 6. Performance Characteristics

- **Zero-Syscall Ingress**: Under heavy load, `io_uring` batching consumes multiple completed connection events per `enter` syscall.
- **Context Switch Elimination**: Pinning prevents CPU migration, drastically reducing L1/L2 data cache misses.
- **Scalability**: Throughput scales linearly with physical CPU cores without lock degradation.
