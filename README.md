# rudis

<p align="center">
  <b>The Ultra-High-Performance, Multi-Threaded, Shared-Nothing In-Memory & NVMe-Tiered Datastore in Rust</b><br>
  Built natively on Linux <code>io_uring</code> via Monoio
</p>

<p align="center">
  <a href="https://github.com/warrenzhu25/rudis/actions"><img src="https://img.shields.io/badge/build-passing-brightgreen.svg" alt="Build Status"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-1.82+-blue.svg" alt="Rust Version"></a>
  <a href="https://kernel.dk/io_uring.pdf"><img src="https://img.shields.io/badge/Linux-io__uring-orange.svg" alt="Linux io_uring"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
</p>

> **Before moving on, please consider giving us a GitHub star ⭐️. Thank you!**

[Architecture Overview](#architecture-overview) • [Benchmarks](#benchmarks) • [Quick Start](#quick-start) • [Configuration](#configuration) • [Design Decisions](#design-decisions) • [Feature Matrix](#subsystem--feature-matrix) • [Documentation](docs/)

---

## The World's Most Efficient Rust In-Memory Datastore

**Rudis** is an ultra-high-performance in-memory and NVMe-tiered datastore built for modern multi-core, high-throughput cloud workloads.

Fully wire-compatible with **Redis (RESP2 and RESP3)** and **Memcached** text protocol APIs, Rudis requires **no application code changes** to adopt. Compared to legacy single-threaded in-memory datastores, Rudis delivers:
- **Up to 25X more throughput** (crossing **4.17M QPS** on a single node and scaling linearly with CPU cores)
- **Sub-millisecond tail latency** ($p99 < 1.5\text{ ms}$) under multi-million concurrent operations
- **Fork-less Linux `io_uring` streaming snapshots** and instant `ioctl(FICLONE)` reflink checkpoints with **zero memory spike** (eliminating Redis copy-on-write memory ballooning)
- **Redis 7 Slot-Bound Sharded Pub/Sub** (`SPUBLISH`, `SSUBSCRIBE`) with 16-stripe atomic presence filtering to eliminate cross-shard broadcast overhead
- **RediSearch Engine with Balanced `RangeTree` & `FT.AGGREGATE`**: $O(1)$ term-directed deletion, $O(\log N + K)$ numeric search, and multi-stage aggregation pipeline
- **Per-Shard Parallel TCP Replication Streams (`DFLY FLOW`)**: Dedicated streaming connections direct to primary worker shard threads
- **Hardware-accelerated Linux kernel bypass**: AF_XDP (XSK) wire-speed filtering, `io_uring` fixed registered buffer pools, Linux `SO_ZEROCOPY`, and hardware kernel TLS (`kTLS`)

---

## Contents

- [Architecture Overview](#architecture-overview)
- [Benchmarks](#benchmarks)
  - [1. Multi-Engine Throughput (16 Cores, AMD EPYC)](#1-multi-engine-throughput-16-cores-amd-epyc)
  - [2. Multi-Core Vertical Scaling (1 to 32 Cores)](#2-multi-core-vertical-scaling-1-to-32-cores)
  - [3. NVMe Tiered Storage: Rudis vs. Dragonfly](#3-nvme-tiered-storage-rudis-vs-dragonfly)
  - [4. Memory Efficiency during Snapshots (BGSAVE)](#4-memory-efficiency-during-snapshots-bgsave)
  - [5. Vector Search & Quantization (HNSW, SQ8, PQ)](#5-vector-search--quantization-hnsw-sq8-pq)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Design Decisions](#design-decisions)
  - [1. Shared-Nothing Thread-Per-Core on Linux io_uring](#1-shared-nothing-thread-per-core-on-linux-io_uring)
  - [2. Fork-less Streaming Snapshots & ioctl(FICLONE) Reflinks](#2-fork-less-streaming-snapshots--ioctlficlone-reflinks)
  - [3. Redis 7 Sharded Pub/Sub & Striped Shard Presence Bitmask](#3-redis-7-sharded-pubsub--striped-shard-presence-bitmask)
  - [4. Dense DocId Search Index & Balanced RangeTree with FT.AGGREGATE](#4-dense-docid-search-index--balanced-rangetree-with-ftaggregate)
  - [5. Per-Shard Parallel TCP Replication Streams (DFLY FLOW)](#5-per-shard-parallel-tcp-replication-streams-dfly-flow)
  - [6. NVMe Tiered Storage (SmallBins & Direct I/O)](#6-nvme-tiered-storage-smallbins--direct-io)
  - [7. Hardware Zero-Copy & Kernel TLS (kTLS)](#7-hardware-zero-copy--kernel-tls-ktls)
  - [8. Dual-Protocol Engine: Redis + Memcached](#8-dual-protocol-engine-redis--memcached)
- [Subsystem & Feature Matrix](#subsystem--feature-matrix)
- [Documentation & Contributor Guides](#documentation--contributor-guides)

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

Rudis employs a **Thread-Per-Core (Shared-Nothing)** architecture inspired by modern high-throughput engines like Dragonfly and ScyllaDB/Seastar:

1. **Thread-per-Core Pinning**: Worker threads are pinned to physical CPU cores using `core_affinity`. Each thread runs its own isolated `monoio` event loop driving an independent Linux `io_uring` instance.
2. **Ingress with `SO_REUSEPORT`**: Every worker thread binds its own TCP listener to the same port. The Linux kernel distributes incoming client connections across the worker threads with zero user-space coordination.
3. **Partitioned In-Memory Storage**: State is strictly thread-local (`ShardDb`). Local operations execute in nanoseconds against a thread-local table with **zero mutexes, zero atomic operations, and zero cross-core cache invalidation**.
4. **CRC16 Key Routing & Cross-Shard Mesh**:
   - Keys are mapped to 16,384 cluster slots using CRC16: `slot = crc16(tag) % 16384`.
   - If a connection receives a command for a key on its local shard, it executes immediately without hop.
   - If the key resides on another shard, it dispatches the request through a lock-free cross-core channel mesh, where Monoio utilizes an `eventfd` waker to resume the peer core's `io_uring` ring.
5. **Multi-Protocol Gateway**: Supports standard RESP2, RESP3 (`HELLO 3`), inline text commands, and a built-in **Dual-Protocol Memcached Gateway** sharing database 0 with zero configuration.

---

## Benchmarks

> Detailed test configurations, methodology, and reproduction scripts are documented in [docs/benchmarks/comprehensive_performance_guide.md](docs/benchmarks/comprehensive_performance_guide.md) and [docs/benchmarks/README.md](docs/benchmarks/README.md).

### 1. Multi-Engine Throughput (16 Cores, AMD EPYC)

Benchmarked on **AMD EPYC 7B13 (64 vCPUs, 117 GiB RAM)** with the server pinned to 16 physical cores (`taskset -c 0-15`) and `memtier_benchmark` driven from client cores `32-63` (32 client threads, 1KB payload, pipeline depth 50–100):

| Workload / Engine | Command | Rudis Throughput | Peak Bandwidth | p50 Latency | p99 Latency | vs. Dragonfly v1.39 |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **Counter Primitives** | `INCR` | **4,175,571 ops/s** | 157.8 MB/s | **0.66 ms** | **1.58 ms** | **+4.4% (1.04x)** |
| **Key-Value Read** | `GET (1KB)` | **3,698,356 ops/s** | 2,364.3 MB/s | **0.60 ms** | **1.71 ms** | **+534.2% (6.34x)** |
| **Sorted Sets** | `ZADD` | **3,468,035 ops/s** | 194.6 MB/s | **0.81 ms** | **1.96 ms** | 0.90x |
| **Key-Value Write** | `SET (1KB)` | **3,415,601 ops/s** | 3,491.4 MB/s | **0.69 ms** | **2.01 ms** | **+72.4% (1.72x)** |
| **Hash Table Read** | `HGET (1KB)` | **2,892,417 ops/s** | 1,354.6 MB/s | **0.93 ms** | **3.07 ms** | **+450.2% (5.50x)** |
| **Probabilistic Bloom** | `BF.ADD` | **2,806,122 ops/s** | 178.7 MB/s | **0.51 ms** | **1.18 ms** | *Native Rudis Engine* |
| **Lists** | `LPUSH (1KB)` | **2,774,462 ops/s** | 2,840.5 MB/s | **1.02 ms** | **2.58 ms** | **+67.3% (1.67x)** |
| **RedisJSON Read** | `JSON.GET` | **2,690,696 ops/s** | 170.2 MB/s | **0.54 ms** | **1.18 ms** | *Native Rudis Engine* |
| **RedisJSON Write** | `JSON.SET` | **2,656,600 ops/s** | 186.1 MB/s | **0.54 ms** | **1.26 ms** | *Native Rudis Engine* |
| **Hash Table Write** | `HSET (1KB)` | **2,636,209 ops/s** | 2,722.4 MB/s | **0.99 ms** | **2.93 ms** | **+20.9% (1.21x)** |
| **Probabilistic Cuckoo**| `CF.EXISTS` | **2,606,122 ops/s** | 346.9 MB/s | **0.52 ms** | **1.40 ms** | *Native Rudis Engine* |
| **Geospatial Distance**| `GEODIST` | **2,533,758 ops/s** | 214.3 MB/s | **0.55 ms** | **1.29 ms** | *Native Rudis Engine* |
| **Count-Min Sketch** | `CMS.QUERY` | **2,492,478 ops/s** | 146.9 MB/s | **0.58 ms** | **1.25 ms** | *Native Rudis Engine* |
| **Top-K Heavy Hitters**| `TOPK.ADD` | **2,551,940 ops/s** | 153.1 MB/s | **0.54 ms** | **1.33 ms** | *Native Rudis Engine* |
| **Geospatial Indexing**| `GEOADD` | **2,402,303 ops/s** | 297.3 MB/s | **0.57 ms** | **1.46 ms** | *Native Rudis Engine* |
| **Vector Search (HNSW)**| `VQUERY` | **1,687,465 ops/s** | 139.7 MB/s | **0.74 ms** | **2.13 ms** | *Native Rudis Engine* |
| **Streams Ingestion** | `XADD` | **1,560,679 ops/s** | 161.9 MB/s | **0.90 ms** | **2.35 ms** | *Native Rudis Engine* |
| **Socket Saturation** | `SET (64KB)` | 134,326 ops/s | **8,401.5 MB/s (67.2 Gbps)** | 10.18 ms | 34.05 ms | *Line-Rate Bound* |

---

### 2. Multi-Core Vertical Scaling (1 to 32 Cores)

Rudis throughput scales vertically as physical cores are added, without the synchronization bottlenecks that stall single-threaded architectures:

| Cores | 100% SET (1KB) | SET Bandwidth | 100% GET (1KB) | 50/50 SET/GET | p50 Latency | p99 Latency |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1 Core** | 867,251 ops/s | 886.29 MB/s | 1,707,230 ops/s | 448,088 ops/s | 1.48 ms | 4.25 ms |
| **2 Cores** | 1,181,227 ops/s | 1,207.27 MB/s | 2,410,932 ops/s | 578,853 ops/s | 1.04 ms | 4.32 ms |
| **4 Cores** | 1,689,026 ops/s | 1,726.42 MB/s | 3,546,696 ops/s | 757,423 ops/s | 0.71 ms | 2.81 ms |
| **8 Cores** | 2,190,518 ops/s | 2,239.09 MB/s | **4,251,151 ops/s** | 1,057,310 ops/s | **0.59 ms** | **2.24 ms** |
| **16 Cores** | **2,795,856 ops/s** | **2,857.88 MB/s** | 3,260,984 ops/s | **1,401,317 ops/s** | 0.86 ms | 2.67 ms |
| **32 Cores** | 2,534,120 ops/s | 2,590.36 MB/s | 3,278,615 ops/s | 1,341,148 ops/s | 0.83 ms | 2.64 ms |

---

### 3. NVMe Tiered Storage: Rudis vs. Dragonfly

Benchmarked on 4 physical worker cores (`taskset -c 0-3`) with `--maxmemory 1024mb` on NVMe SSD storage using `memtier_benchmark` (1.5M keys, 1KB payload, pipeline depth 50):

| Workload | Payload | Rudis (Ops/sec) | Dragonfly v1.39 (Ops/sec) | Speedup vs. Dragonfly | Winner |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **SET** | 1KB | **1,279,856** | 286,351 | **+347.0% (4.47x)** | **Rudis** |
| **GET** | 1KB | **670,695** | 202,615 | **+231.0% (3.31x)** | **Rudis** |
| **SET/GET 1:1** | 1KB | **543,150** | 254,241 | **+113.6% (2.14x)** | **Rudis** |

---

### 4. Memory Efficiency during Snapshots (BGSAVE)

Traditional Redis executes `fork()` to serialize snapshots. When active write workloads hit Redis during snapshotting, Linux copy-on-write (CoW) page duplication can increase memory usage by up to **3X**, frequently triggering out-of-memory (OOM) process termination.

Rudis utilizes **fork-less `io_uring` streaming snapshotting**:
- **0% Memory Spike**: Worker threads serialize snapshots incrementally without `fork()`. No duplicate page table allocations or CoW spikes occur.
- **Sub-Millisecond Reflink Snapshots**: On copy-on-write filesystems (XFS, Btrfs, ZFS), `TIER SNAPSHOT` uses kernel `ioctl(FICLONE)` to produce an instantaneous point-in-time snapshot in $< 1\text{ ms}$.

---

### 5. Vector Search & Quantization (HNSW, SQ8, PQ)

Evaluated on 10,000 vectors of 128 dimensions using cosine similarity distance:

| Index Mode | Vector Payload RAM | RAM Savings | Ingestion Rate | Search QPS | Latency p50 | Latency p99 | Recall@10 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Float32 HNSW (AVX2)** | 5.12 MB | Baseline (0%) | **3,556 vec/s** | **5,880 QPS** | **147 µs** | **338 µs** | **54.8%** |
| **SQ8 Quantized (AVX2)** | **1.28 MB** | **-75.0%** | 2,080 vec/s | 3,752 QPS | 254 µs | 427 µs | 53.0% |
| **SQ8 + Exact Rerank** | 1.28 MB | **-75.0%** | 2,080 vec/s | 3,901 QPS | 243 µs | 448 µs | 53.0% |

---

## Quick Start

### 1. Build from Source
Requires Rust 1.82+ and Linux kernel 5.10+ (for `io_uring` support):
```bash
git clone https://github.com/warrenzhu25/rudis.git
cd rudis
cargo build --release
```

### 2. Run Rudis
By default, Rudis detects CPU cores and runs worker threads on port 6379:
```bash
# Auto-detect cores, listen on port 6379
./target/release/rudis --port 6379

# Specific core count and memory limit
./target/release/rudis --port 6379 --threads 8 --maxmemory 16gb
```

### 3. Connect with `redis-cli` (RESP2 / RESP3)
```bash
redis-cli -p 6379
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET user:1 alice
OK
127.0.0.1:6379> GET user:1
"alice"
```

### 4. Connect with Memcached Clients (Dual-Protocol Gateway)
Rudis natively accepts Memcached text protocol connections on the same port:
```bash
$ nc 127.0.0.1 6379
set session:1 0 3600 5
admin
STORED
get session:1
VALUE session:1 0 5
admin
END
quit
```

---

## Configuration

Rudis supports standard Redis command-line flags and configuration options alongside advanced performance arguments:

### Common Redis Arguments
* `--port <port>`: TCP port to listen on (default: `6379`).
* `--bind <ip>`: Bind address (`0.0.0.0` for all interfaces, `127.0.0.1` for localhost only).
* `--requirepass <password>`: Authentication password for `AUTH`.
* `--maxmemory <bytes>`: Maximum RAM limit (e.g. `12gb`, `4096mb`). Default `0` means unbounded.
* `--dir <path>`: Working directory for persistence and snapshots (default: current directory).
* `--dbfilename <name>`: RDB snapshot filename (default: `dump.rdb`).
* `--appendonly <yes|no>`: Enable Append-Only File (AOF) persistence.

### Rudis & Dragonfly-Specific Arguments
* `--threads <count>`: Number of worker threads pinned to CPU cores (default: CPU core count).
* `--cache_mode <bool>`: Enable adaptive cache eviction when approaching `maxmemory`.
* `--tiered_prefix <prefix>`: Prefix for keys to automatically offload to NVMe storage.
* `--memcached_port <port>`: Optional separate port for Memcached-only traffic (default: unified port).
* `--tls_port <port>`: Port for Hardware Kernel TLS (`kTLS`) encrypted traffic.
* `--tls_cert <path>`, `--tls_key <path>`: TLS certificate and private key file paths.
* `--cluster_enabled <bool>`: Enable Redis Cluster gossip bus on `port + 10000`.

### Example Production Start Script:
```bash
./target/release/rudis \
  --bind 0.0.0.0 \
  --port 6379 \
  --threads 16 \
  --maxmemory 32gb \
  --cache_mode true \
  --dir /var/lib/rudis \
  --dbfilename dump.rdb
```

---

## Design Decisions

### 1. Shared-Nothing Thread-Per-Core on Linux `io_uring`
Rudis avoids the global lock contention of single-threaded stores and the mutex bottlenecks of traditional multi-threaded architectures:
- **Thread Pinning**: Worker threads are pinned to physical CPU cores via `core_affinity`.
- **Linux `io_uring` via Monoio**: Ingress network I/O, storage persistence, and inter-thread notifications execute asynchronously via Linux kernel submission/completion queues (`sqe`/`cqe`).
- **`SO_REUSEPORT` Ingress**: The Linux kernel distributes incoming client connections across all worker threads without user-space connection routers.
- **Lock-Free Routing**: Commands targeting foreign keys are forwarded through bounded cross-core channel rings, resumed by `eventfd` wakers with zero spinlock burn.

### 2. Fork-less Streaming Snapshots & `ioctl(FICLONE)` Reflinks
- Traditional Redis uses `fork()` during `bgsave`. During heavy write workloads, Linux copy-on-write (CoW) page duplication can double or triple server RSS.
- Rudis serializes RDB snapshots incrementally across shards over `io_uring` with **0% memory spike**.
- For NVMe tiered storage on Btrfs/XFS, Rudis executes `ioctl(FICLONE)` reflink cloning, creating instant point-in-time checkpoints in $< 1\text{ ms}$.

### 3. Redis 7 Sharded Pub/Sub & Striped Shard Presence Bitmask
- **Slot-Bound Sharded Pub/Sub (`SPUBLISH`, `SSUBSCRIBE`)**: Channels map to hash slots $S = \text{crc16}(\text{channel}) \pmod{16384}$. `SPUBLISH` routes point-to-point directly to the owning shard, eliminating cross-shard broadcasts.
- **16-Stripe Presence Bitmask (`ShardedPresenceTable`)**: Standard `PUBLISH` checks an atomic 16-stripe bitmask before broadcasting, only dispatching to shards containing active subscribers.
- **Zero-Copy Delivery**: Formats `message` and `smessage` push frames once as `Bytes` and pushes directly to subscribers' bounded backpressure channels.

### 4. Dense DocId Search Index & Balanced `RangeTree` with `FT.AGGREGATE`
- **Dense 32-Bit `DocId`**: Maps document keys to contiguous 32-bit integers, replacing string document identifiers and cutting posting memory by up to 80%.
- **$O(1)$ Term-Directed Deletion**: Deleting documents traverses only the document's indexed terms, eliminating vocabulary scans.
- **Balanced `RangeTree`**: Numeric fields are indexed using balanced B-trees over `OrderedF64`, enabling $O(\log N + K)$ range queries (`@field:[min max]`).
- **Scatter-Gather `FT.AGGREGATE`**: Shards execute parallel search queries and stream matching rows into a pipeline supporting `GROUPBY`, `REDUCE` (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`), `APPLY` arithmetic expressions, and `SORTBY`.

### 5. Per-Shard Parallel TCP Replication Streams (`DFLY FLOW`)
- Legacy replication funnels the entire replication stream through a single TCP connection, bottlenecking multi-core primary servers.
- Rudis implements Dragonfly-compatible multi-flow replication:
  - Negotiated via `REPLCONF capa dragonfly`.
  - Replicas open $N$ parallel TCP connections (`DFLY FLOW <replid> <sync_id> <shard_id>`), one directly to each master shard thread.
  - Shard threads stream mutations independently over their dedicated socket, scaling replication throughput linearly with CPU cores.

### 6. NVMe Tiered Storage (SmallBins & Direct I/O)
- **3-State Lifecycle**: Keys transition from Hot (DRAM) $\to$ Cooled (eviction candidate) $\to$ Cold (NVMe disk).
- **SmallBins 4KB Bin Packing**: Values smaller than 4KB are coalesced into aligned 4KB blocks, eliminating file system block fragmentation.
- **Direct I/O & Hole-Punching**: Reads and writes bypass the kernel page cache via `O_DIRECT` and `io_uring`. Space reclamation executes via `fallocate(FALLOC_FL_PUNCH_HOLE)`.

### 7. Hardware Zero-Copy & Kernel TLS (`kTLS`)
- **AF_XDP (XSK) Kernel Bypass**: Ingress network frames are read directly from driver UMEM rings into worker threads.
- **Registered `io_uring` Buffers**: Fixed buffers are pre-registered with the kernel at startup, eliminating `get_user_pages` and page table walks.
- **Linux Kernel TLS (`kTLS`)**: Symmetric AES-GCM cipher encryption/decryption is offloaded to the Linux kernel (`TCP_ULP`), enabling zero-copy network sends.

### 8. Dual-Protocol Engine: Redis + Memcached
- A single listening port accepts both Redis RESP commands and Memcached text-based commands simultaneously.
- Connections automatically detect protocol framing on initial byte read, sharing database 0 with zero proxy overhead.

---

## Subsystem & Feature Matrix

| Subsystem / Module | Status | Description |
| :--- | :---: | :--- |
| **Thread-per-Core Engine** | Complete | Shared-nothing architecture on Monoio / `io_uring` with lock-free cross-shard mesh. |
| **Core Redis Data Structures** | Complete | Strings, Hashes, Lists, Sets, Sorted Sets (ZSets), Bitmaps, HyperLogLog. |
| **Geospatial Engine** | Complete | 52-bit integer geohashes, Haversine spherical distance, Redis 6.2+ `GEOSEARCH`. |
| **Streams & Consumer Groups** | Complete | Append-only log with radix tree indexing, consumer groups, PEL, and non-blocking / blocking `XREAD`. |
| **Transactions & Multi-Key** | Complete | `MULTI`/`EXEC`/`DISCARD` with Very Lightweight Locking (VLL) distributed multi-shard isolation. |
| **Pub/Sub Messaging** | Complete | High-throughput channels, pattern subscriptions (`PSUBSCRIBE`), and Redis 7 Sharded Pub/Sub (`SPUBLISH`). |
| **Scripting & Functions** | Complete | Redis 7 Function libraries (`FUNCTION LOAD`, `FCALL`) and standard Lua scripting (`EVAL`, `EVALSHA`). |
| **ACL & Security** | Complete | Granular user permissions, category selectors (`+@all`, `-@admin`), passwords, and `AUTH`. |
| **Replication & Persistence** | Complete | Point-in-time RDB snapshots, streaming AOF, `PSYNC`, and per-shard parallel TCP replication (`DFLY FLOW`). |
| **Redis Cluster & Gossip** | Complete | Dedicated cluster bus (`port + 10000`), `-MOVED` / `-ASK` routing, dynamic slot migration, and consensus failover. |
| **Dragonfly Compatibility Suite** | Complete | `DFLYCLUSTER`, `DFLYMIGRATE`, cache pinning (`STICK`/`UNSTICK`), `DELEX`, and Dual-Protocol Memcached Gateway. |
| **RedisJSON Document Store** | Complete | RFC 8259 document store with recursive JSONPath parsing, array slices, and in-place atomic mutations. |
| **RediSearch & Hybrid Fusion** | Complete | Multi-field schema index, Okapi BM25 scoring, balanced `RangeTree`, `FT.AGGREGATE`, and Vector RRF. |
| **RedisBloom Probabilistic Engine** | Complete | Bloom (`BF.*`), Cuckoo (`CF.*`), Count-Min Sketch (`CMS.*`), and Top-K (`TOPK.*`) heavy-hitter trackers. |
| **HNSW Vector Search Engine** | Complete | Cosine, L2, IP metrics, AVX2 SIMD kernels, SQ8 scalar quantization, Product Quantization (PQ), and ADC. |
| **NVMe Tiered Storage** | Complete | 3-state value lifecycle (Hot/Cooled/Cold), SmallBins 4KB bin packing, Direct I/O (`O_DIRECT`), and hole punching. |
| **Zero-Copy Snapshots** | Complete | Linux `ioctl(FICLONE)` reflink snapshotting (<1ms point-in-time checkpointing without stopping traffic). |
| **Multi-Region CRDTs** | Complete | Leaderless active-active replication with 16-byte Hybrid Logical Clocks, LWW, OR-Set, PN-Counter, and tombstone GC. |
| **Hardware Zero-Copy I/O** | Complete | AF_XDP (XSK) kernel bypass, eBPF wire-speed packet filtering, `io_uring` fixed registered buffers, and Linux `SO_ZEROCOPY`. |
| **Hardware Kernel TLS (kTLS)** | Complete | `rustls` user-space TLS 1.2/1.3 handshake offloaded to Linux `TCP_ULP` symmetric AES-GCM kernel cipher pipelines. |
| **Jemalloc Per-Core Telemetry** | Complete | `tikv-jemallocator` integration with live memory metrics via `tikv-jemalloc-ctl` in `INFO memory`. |

---

## Documentation & Contributor Guides

For deep technical walkthroughs, internal architecture specifications, and benchmarks:
* [**Rudis Internals & Architecture Guide**](docs/rudis_internals_guide.md): Comprehensive walkthrough of the thread-per-core engine, memory layout, and lock-free routing mesh.
* [**Component Architecture Documentation**](docs/components.md): Specifications for all 19 subsystems (`RudisTable`, `BlockHub`, Tiering, Vector, Search, XDP, CRDTs, JSON, Pub/Sub, etc.).
* [**Comprehensive Performance Guide & Benchmark Whitepaper**](docs/benchmarks/comprehensive_performance_guide.md): In-depth performance evaluation across payloads, pipeline depths, and multi-core scaling.
* [**Benchmark Directory & Reproduction Scripts**](docs/benchmarks/README.md): Catalog of reproduction scripts comparing Rudis against Redis, Dragonfly, and Valkey.

---

## License

Rudis is open source and released under the [MIT License](LICENSE).
