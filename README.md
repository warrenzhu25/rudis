# rudis (Redis in Rust)

A high-performance, **multi-threaded**, **shared-nothing** Redis implementation in Rust built on **`io_uring`** via **Monoio**.

---

## Architecture Overview

```
                      Client Connections
                              │
               (SO_REUSEPORT Kernel Balancing)
                 ┌────────────┴────────────┐
                 ▼                         ▼
         ┌───────────────┐         ┌───────────────┐
         │    Core 0     │         │    Core 1     │
         │ Monoio Runtime│         │ Monoio Runtime│
         │ (io_uring)    │         │ (io_uring)    │
         ├───────────────┤         ├───────────────┤
         │    Shard 0    │         │    Shard 1    │
         │ Local HashMap │         │ Local HashMap │
         └───────┬───────┘         └───────┬───────┘
                 │     Cross-Shard Mesh    │
                 └────────◄ Channels ►─────┘
```

`rudis` employs a **Thread-Per-Core (Shared-Nothing)** model inspired by modern high-throughput architectures like Dragonfly and ScyllaDB/Seastar:

1. **Thread-per-Core Pinning**: Worker threads are pinned to physical CPU cores using `core_affinity`. Each thread runs its own isolated `monoio` event loop driving an independent Linux `io_uring` instance.
2. **Ingress with `SO_REUSEPORT`**: Every worker thread binds its own TCP listener to the same port. The Linux kernel distributes incoming client connections across the worker threads with zero user-space coordination.
3. **Partitioned In-Memory Storage**: State is strictly thread-local (`ShardDb`). Local operations execute in nanoseconds against a thread-local `HashMap` with **zero mutexes, zero atomic operations, and zero cross-core cache invalidation**.
4. **CRC16 Key Routing & Cross-Shard Mesh**:
   - Keys are mapped to shards using CRC16: `target_shard = crc16(key) % num_shards`.
   - If a connection receives a command for a key on its local shard, it executes immediately.
   - If the key resides on another shard, it dispatches the request through a lock-free cross-core channel mesh, where Monoio utilizes an `eventfd` waker to resume the peer core's `io_uring` ring.
5. **RESP & Inline Protocol**: Supports standard Redis protocol (RESP arrays) as well as inline commands (`GET`, `SET`, `PUT`, `PING`, `INFO`, `QUIT`).

---

## Quick Start

### 1. Build
```bash
cargo build --release
```

### 2. Run
By default, `rudis` detects CPU cores and runs up to 8 threads on port 6379:
```bash
./target/release/rudis --port 6379 --threads 4
```

### 3. Connect with `redis-cli`
```bash
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET user:1 alice
OK
127.0.0.1:6379> GET user:1
"alice"
127.0.0.1:6379> PUT user:2 bob
OK
127.0.0.1:6379> GET user:2
"bob"
```

### 4. Connect with `nc` / `telnet` (Inline Protocol)
```bash
$ nc 127.0.0.1 6379
SET foo bar
+OK
GET foo
$3
bar
PUT hello world
+OK
GET hello
$5
world
QUIT
+OK
```

---

## Testing

Run unit tests and end-to-end multi-threaded integration tests:
```bash
cargo test
```

---

## Benchmarks

### Baseline Scaling (1 to 32 Threads, 100% SET, 1KB Payload, Pipeline 100)

Detailed baseline benchmark document: [docs/benchmarks/baseline.md](docs/benchmarks/baseline.md)

| Server Threads | Throughput (Ops/sec) | Bandwidth (MB/s) | Avg Latency (ms) | p50 (ms) | p99 (ms) |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 60,487.24 | 63.26 | 52.87 | 53.50 | 80.90 |
| **2** | 88,727.36 | 92.83 | 36.05 | 37.12 | 66.56 |
| **4** | 159,224.13 | 166.63 | 20.09 | 20.86 | 41.47 |
| **8** | 295,665.11 | 309.47 | 10.82 | 9.86 | 26.75 |
| **16** | 297,048.94 | 310.92 | 10.77 | 10.18 | 30.72 |
| **32** | 273,831.18 | 286.61 | 11.69 | 10.56 | 34.82 |

---

## Project Structure

```
rudis/
├── Cargo.toml
├── docs/
│   └── benchmarks/
│       └── baseline.md # Detailed 1-32 thread baseline results
├── src/
│   ├── main.rs         # CLI argument parsing, thread spawning, mesh setup
│   ├── lib.rs          # Library root exporting modules
│   ├── server.rs       # SO_REUSEPORT socket setup, Monoio io_uring accept loop
│   ├── connection.rs   # TCP connection handler and command dispatcher
│   ├── resp.rs         # RESP2 & inline frame parser and serializer
│   ├── router.rs       # CRC16 key partitioner and cross-core message dispatcher
│   └── shard.rs        # Thread-local in-memory key-value database and message types
└── tests/
    ├── test_cross_thread.rs # Validates cross-core eventfd waker with Monoio
    └── test_server_e2e.rs   # Multi-shard end-to-end integration tests
```

