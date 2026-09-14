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

### 5. NVMe Cold-Storage Tiering (`io_uring`)
Rudis features a thread-per-core asynchronous storage tiering engine built natively on `io_uring`:
- **Three-State Value Lifecycle**: Values transition through `Hot` (in DRAM) $\to$ `Cooled` (persisted to NVMe, cached in DRAM) $\to$ `Cold` (persisted to NVMe, pointer in DRAM).
- **Zero-I/O Decommit**: `TIER DECOMMIT` instantly drops in-memory copies of cooled keys without I/O.
- **SmallBins 4KB Page Packing**: Values $<2$ KB are packed into aligned 4KB disk bins to eliminate NVMe space amplification.
- **Direct I/O (`O_DIRECT`)**: Bypass Linux Page Cache overhead with hardware-aligned DMA writes and reads (`RUDIS_DIRECT_IO=1`).
- **Zero-Copy Online GC / Hole-Punching**: Reclaims NVMe storage on deletion via Linux `fallocate(FALLOC_FL_PUNCH_HOLE)` (`TIER GC`).
- **Transparent Async Retrieval**: Any access (`GET`, `DUMP`, etc.) to a tiered key transparently reads from disk via `io_uring` without blocking the event loop or other connections.
- **Bulk Operations & Metrics**: `TIER SPILLALL`, `TIER COOLALL`, and `TIER INFO` / `INFO storage` report live telemetry on disk footprint, RAM saved, and coalesced I/O.

---

## Testing

Run unit tests and end-to-end multi-threaded integration tests:
```bash
cargo test
```

---

## Benchmarks

### NVMe Tiered Storage: Rudis vs. Dragonfly (4 Worker Cores, 1KB Payloads)

Detailed Tiered Storage benchmark report: [docs/benchmarks/tiered_storage.md](docs/benchmarks/tiered_storage.md)

Rudis was benchmarked against **Dragonfly v1.39.0** on 4 physical worker cores (`taskset -c 0-3`) with `--maxmemory 1024mb` on NVMe storage using `memtier_benchmark` (4 threads, 4 connections/thread, pipeline depth 50, 1.5M keys):

| Workload | Payload | Rudis (Ops/sec) | Dragonfly (Ops/sec) | Rudis Speedup | Winner |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **SET** | 1KB | **1,279,856** | 286,351 | **+347.0% (4.47x)** | **Rudis** |
| **GET** | 1KB | **670,695** | 202,615 | **+231.0% (3.31x)** | **Rudis** |
| **SET/GET 1:1** | 1KB | **543,150** | 254,241 | **+113.6% (2.14x)** | **Rudis** |

### Multi-Core NVMe Tiered Storage Scaling (4, 8, 16 Cores)

| Worker Cores | SET Throughput | GET Throughput | SET/GET 1:1 Throughput | Peak Bandwidth | Median Latency (p50) |
| :---: | :---: | :---: | :---: | :---: | :---: |
| **4 Cores** | 1,351,053 ops/s | 528,145 ops/s | 948,435 ops/s | 1.41 GB/s | **0.54 ms** |
| **8 Cores** | **2,303,685 ops/s** | **1,099,510 ops/s** | **1,750,906 ops/s** | **2.41 GB/s** | **0.61 ms** |
| **16 Cores** | 1,933,960 ops/s | 763,735 ops/s | 1,236,764 ops/s | 2.02 GB/s | **0.71 ms** |

---

### Write-Batched Optimization (1 to 32 Threads, 100% SET, 1KB Payload, Pipeline 100)

Detailed write-batching benchmark document: [docs/benchmarks/write_batching.md](docs/benchmarks/write_batching.md)

| Server Threads | Baseline Ops/sec | **Batched Ops/sec** | Baseline Bandwidth | **Batched Bandwidth** | Baseline Avg Lat | **Batched Avg Lat** | Speedup |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 60,487.24 | **816,936.81** | 63.3 MB/s | **855.1 MB/s** | 52.87 ms | **3.90 ms** | **13.5x** |
| **2** | 88,727.36 | **593,237.78** | 92.8 MB/s | **620.9 MB/s** | 36.05 ms | **5.38 ms** | **6.7x** |
| **4** | 159,224.13 | **857,550.64** | 166.6 MB/s | **897.6 MB/s** | 20.09 ms | **3.71 ms** | **5.4x** |
| **8** | 295,665.11 | **794,211.64** | 309.5 MB/s | **831.3 MB/s** | 10.82 ms | **4.01 ms** | **2.7x** |
| **16** | 297,048.94 | **705,995.32** | 310.9 MB/s | **739.0 MB/s** | 10.77 ms | **4.51 ms** | **2.4x** |
| **32** | 273,831.18 | **704,537.20** | 286.6 MB/s | **737.4 MB/s** | 11.69 ms | **4.52 ms** | **2.6x** |

### Baseline Scaling (Pre-Optimization)

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

