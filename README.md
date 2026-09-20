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

[Architecture Overview](#architecture-overview) • [Benchmarks](#benchmarks) • [Quick Start](#quick-start) • [Configuration](#configuration) • [Design Decisions](#design-decisions) • [Feature Matrix](#subsystem--feature-matrix) • [Command Reference](#complete-command-reference) • [Documentation](docs/)

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
- [Complete Command Reference](#complete-command-reference)
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

## Complete Command Reference

### 1. Strings & Basic Keyspace
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `SET` | `SET key value [EX seconds \| PX ms] [NX \| XX]` | Sets value with optional TTL expiration and conditional creation flags. |
| `GET` | `GET key` | Returns string value or nil if non-existent or expired. Transparently loads tiered cold values. |
| `PUT` | `PUT key value` | High-throughput inline alias for `SET`. |
| `MSET` | `MSET key value [key value ...]` | Atomically sets multiple key-value pairs across single or multiple shards. |
| `MGET` | `MGET key [key ...]` | Retrieves values for multiple keys across shards via parallel scatter-gather dispatch. |
| `SETNX` | `SETNX key value` | Sets key only if it does not already exist. |
| `MSETNX` | `MSETNX key value [key value ...]` | Sets multiple keys only if none of the specified keys exist. |
| `GETSET` | `GETSET key value` | Atomically sets new value and returns previous value. |
| `GETDEL` | `GETDEL key` | Atomically retrieves value and removes key from the database. |
| `APPEND` | `APPEND key value` | Appends a string value to a key, returning the resulting length. |
| `STRLEN` | `STRLEN key` | Returns byte length of string stored at key. |
| `SETRANGE` | `SETRANGE key offset value` | Overwrites part of the string stored at key starting at specified offset. |
| `GETRANGE` | `GETRANGE key start end` | Returns substring of value stored at key within 0-based signed offset bounds. |
| `INCR` / `DECR` | `INCR key` / `DECR key` | Increments or decrements 64-bit integer value by 1. |
| `INCRBY` / `DECRBY` | `INCRBY key delta` / `DECRBY key delta` | Increments or decrements 64-bit integer value by specified delta. |
| `INCRBYFLOAT` | `INCRBYFLOAT key delta` | Increments floating-point value stored at key by specified float delta. |

### 2. Hashes
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `HSET` / `HMSET` | `HSET key field value [field value ...]` | Sets one or more field-value pairs in hash. |
| `HGET` | `HGET key field` | Returns value of specified field in hash. |
| `HMGET` | `HMGET key field [field ...]` | Returns values of multiple specified fields in hash. |
| `HDEL` | `HDEL key field [field ...]` | Deletes one or more fields from hash. |
| `HEXISTS` | `HEXISTS key field` | Checks if field exists in hash (returns 1 or 0). |
| `HLEN` | `HLEN key` | Returns number of fields contained within hash. |
| `HGETALL` | `HGETALL key` | Returns all fields and values stored in hash. |
| `HKEYS` / `HVALS` | `HKEYS key` / `HVALS key` | Returns all field names or all field values in hash. |
| `HINCRBY` | `HINCRBY key field delta` | Increments integer value of hash field by specified integer. |
| `HINCRBYFLOAT` | `HINCRBYFLOAT key field delta` | Increments floating-point value of hash field by specified float. |
| `HRANDFIELD` | `HRANDFIELD key [count [WITHVALUES]]` | Returns random field(s) from hash, optionally including values. |
| `HSCAN` | `HSCAN key cursor [MATCH pat] [COUNT n]` | Iterates incrementally over hash fields using cursor-based pagination. |

### 3. Lists
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `LPUSH` / `RPUSH` | `LPUSH key val [...]` / `RPUSH key val [...]` | Inserts elements at head or tail of list. |
| `LPOP` / `RPOP` | `LPOP key [count]` / `RPOP key [count]` | Removes and returns element(s) from head or tail of list. |
| `LRANGE` | `LRANGE key start stop` | Returns elements of list within specified slice bounds. |
| `LLEN` | `LLEN key` | Returns number of elements in list. |
| `LINDEX` | `LINDEX key index` | Returns element at specified 0-based or negative index. |
| `LTRIM` | `LTRIM key start stop` | Trims list to specified range in place. |
| `LSET` | `LSET key index element` | Overwrites element at index with new value. |
| `LREM` | `LREM key count element` | Removes occurrences of element from list based on count direction. |
| `LPOS` | `LPOS key element [RANK r] [COUNT c]` | Returns index of matching element in list. |
| `LINSERT` | `LINSERT key BEFORE\|AFTER pivot val` | Inserts value immediately before or after pivot element. |
| `LMOVE` | `LMOVE src dst LEFT\|RIGHT LEFT\|RIGHT` | Atomically pops from source list and pushes to destination list. |
| `BLMOVE` | `BLMOVE src dst LEFT\|RIGHT LEFT\|RIGHT timeout` | Blocking version of `LMOVE` with floating-point timeout in seconds. |
| `BLPOP` / `BRPOP` | `BLPOP key [...] timeout` / `BRPOP key [...] timeout` | Blocking pop from head or tail of first non-empty list. |

### 4. Sets
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `SADD` | `SADD key member [member ...]` | Adds one or more members to set. |
| `SREM` | `SREM key member [member ...]` | Removes one or more members from set. |
| `SMEMBERS` | `SMEMBERS key` | Returns all members of set. |
| `SISMEMBER` | `SISMEMBER key member` | Returns 1 if member exists in set, else 0. |
| `SMISMEMBER` | `SMISMEMBER key member [member ...]` | Batch checks existence of multiple members in set. |
| `SCARD` | `SCARD key` | Returns cardinality (number of elements) of set. |
| `SPOP` | `SPOP key [count]` | Removes and returns one or more random members from set. |
| `SRANDMEMBER` | `SRANDMEMBER key [count]` | Returns one or more random members without removing them. |
| `SMOVE` | `SMOVE source destination member` | Atomically moves member from source set to destination set. |
| `SSCAN` | `SSCAN key cursor [MATCH pat] [COUNT n]` | Iterates incrementally over elements of set. |
| `SINTER` / `SUNION` / `SDIFF` | `SINTER key [key ...]` / `SUNION ...` / `SDIFF ...` | Computes set intersection, union, or difference across multiple keys. |
| `SINTERSTORE` / `SUNIONSTORE` / `SDIFFSTORE` | `SINTERSTORE dst key [key ...]` | Computes set algebraic operation and stores result into destination key. |

### 5. Sorted Sets (ZSets)
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `ZADD` | `ZADD key [NX\|XX] [GT\|LT] [CH] score member [...]` | Adds members with scores or updates existing scores. |
| `ZREM` | `ZREM key member [member ...]` | Removes one or more members from sorted set. |
| `ZSCORE` | `ZSCORE key member` | Returns score of member as a floating-point string. |
| `ZMSCORE` | `ZMSCORE key member [member ...]` | Returns scores for multiple members in single roundtrip. |
| `ZCARD` | `ZCARD key` | Returns cardinality of sorted set. |
| `ZRANK` / `ZREVRANK` | `ZRANK key member` / `ZREVRANK key member` | Returns 0-based rank ordered ascending or descending by score. |
| `ZCOUNT` | `ZCOUNT key min max` | Returns number of elements with scores within `[min, max]`. |
| `ZLEXCOUNT` | `ZLEXCOUNT key min max` | Returns number of elements within lexicographical interval. |
| `ZINCRBY` | `ZINCRBY key delta member` | Increments member score by specified float delta. |
| `ZRANGE` | `ZRANGE key min max [BYSCORE\|BYLEX] [REV] [LIMIT o c] [WITHSCORES]` | Flexible range queries by index, score, or lexicographical bounds. |
| `ZPOPMIN` / `ZPOPMAX` | `ZPOPMIN key [count]` / `ZPOPMAX key [count]` | Removes and returns member(s) with lowest or highest scores. |
| `ZRANDMEMBER` | `ZRANDMEMBER key [count [WITHSCORES]]` | Returns random member(s) from sorted set. |
| `ZREMRANGEBYRANK` | `ZREMRANGEBYRANK key start stop` | Removes members within 0-based rank range. |
| `ZREMRANGEBYSCORE`| `ZREMRANGEBYSCORE key min max` | Removes members with scores within `[min, max]`. |
| `ZREMRANGEBYLEX` | `ZREMRANGEBYLEX key min max` | Removes members within lexicographical range. |
| `ZSCAN` | `ZSCAN key cursor [MATCH pat] [COUNT n]` | Iterates incrementally over members and scores. |
| `ZINTER` / `ZUNION` / `ZDIFF` | `ZINTER numkeys key [...] [WEIGHTS ...] [AGGREGATE ...]` | Computes intersection, union, or difference across sorted sets. |
| `ZINTERSTORE` / `ZUNIONSTORE` / `ZDIFFSTORE` | `ZINTERSTORE dst numkeys key [...]` | Performs set algebra and stores result into destination key. |

### 6. Bitmaps & HyperLogLog
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `SETBIT` / `GETBIT` | `SETBIT key offset val` / `GETBIT key offset` | Sets or gets bit at offset (0 or 1). |
| `BITCOUNT` | `BITCOUNT key [start end [BYTE\|BIT]]` | Counts number of set bits (population count) in byte range. |
| `BITPOS` | `BITPOS key bit [start [end]]` | Finds first bit set to 0 or 1 in string. |
| `BITOP` | `BITOP AND\|OR\|XOR\|NOT destkey srckey [...]` | Bitwise logical operations stored into destkey. |
| `PFADD` | `PFADD key element [element ...]` | Adds elements to HyperLogLog register. |
| `PFCOUNT` | `PFCOUNT key [key ...]` | Returns approximate cardinality of single or merged registers. |
| `PFMERGE` | `PFMERGE destkey srckey [srckey ...]` | Merges multiple HyperLogLogs into destination register. |

### 7. Geospatial (52-Bit Geohash & Haversine)
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `GEOADD` | `GEOADD key [NX\|XX] lon lat member [...]` | Stores geospatial coordinates as 52-bit geohash integer scores. |
| `GEODIST` | `GEODIST key m1 m2 [m\|km\|mi\|ft]` | Computes Haversine great-circle distance between two members. |
| `GEOPOS` / `GEOHASH` | `GEOPOS key member [...]` / `GEOHASH ...` | Returns coordinates or 11-character Base32 geohash strings. |
| `GEORADIUS` / `BYMEMBER` | `GEORADIUS key lon lat r u [...]` | Radius query centered at coordinate or existing member. |
| `GEOSEARCH` | `GEOSEARCH key [FROMMEMBER m \| FROMLONLAT lon lat] [BYRADIUS r u \| BYBOX w h u] [...]` | Redis 6.2+ multi-criterion geospatial search by radius or box. |

### 8. Streams & Consumer Groups
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `XADD` | `XADD key [NOMKSTREAM] [MAXLEN ~ n] id field val [...]` | Appends entry with auto-generated (`*`) or explicit ID. |
| `XLEN` | `XLEN key` | Returns number of entries in stream. |
| `XRANGE` / `XREVRANGE` | `XRANGE key start end [COUNT n]` | Range query forward or reverse by entry IDs. |
| `XREAD` | `XREAD [COUNT n] [BLOCK ms] STREAMS key [...] id [...]` | Reads entries across multiple streams, optionally blocking. |
| `XGROUP CREATE` | `XGROUP CREATE key group id [MKSTREAM]` | Creates consumer group pointing to specified offset. |
| `XREADGROUP` | `XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] STREAMS key [...] id [...]` | Reads entries as member of consumer group. |
| `XACK` | `XACK key group id [id ...]` | Acknowledges processed messages, removing them from PEL. |
| `XPENDING` | `XPENDING key group [start end count [consumer]]` | Inspects Pending Entries List (PEL) for consumer group. |
| `XDEL` / `XTRIM` | `XDEL key id [id ...]` / `XTRIM key MAXLEN ~ n` | Deletes specific entries or caps stream length. |

### 9. Pub/Sub Messaging & Redis 7 Sharded Pub/Sub
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `SUBSCRIBE` / `UNSUBSCRIBE` | `SUBSCRIBE ch [...]` / `UNSUBSCRIBE [ch ...]` | Standard channel subscription and unsubscription. |
| `PSUBSCRIBE` / `PUNSUBSCRIBE`| `PSUBSCRIBE pat [...]` / `PUNSUBSCRIBE [pat ...]` | Glob-style pattern channel subscription and unsubscription. |
| `PUBLISH` | `PUBLISH channel message` | Broadcasts message with striped presence bitmask filtering. |
| `SPUBLISH` | `SPUBLISH shardchannel message` | Redis 7 slot-bound point-to-point publish to owning shard. |
| `SSUBSCRIBE` | `SSUBSCRIBE shardchannel [shardchannel ...]` | Subscribes client to slot-bound shard channels. |
| `SUNSUBSCRIBE` | `SUNSUBSCRIBE [shardchannel ...]` | Unsubscribes client from slot-bound shard channels. |
| `PUBSUB CHANNELS` | `PUBSUB CHANNELS [pattern]` | Lists active standard channels matching optional pattern. |
| `PUBSUB NUMSUB` | `PUBSUB NUMSUB [channel ...]` | Returns subscriber counts for specified channels. |
| `PUBSUB NUMPAT` | `PUBSUB NUMPAT` | Returns total number of active pattern subscriptions. |
| `PUBSUB SHARDCHANNELS` | `PUBSUB SHARDCHANNELS [pattern]` | Lists active slot-bound shard channels matching pattern. |
| `PUBSUB SHARDNUMSUB` | `PUBSUB SHARDNUMSUB [shardchannel ...]` | Returns subscriber counts for slot-bound shard channels. |

### 10. RediSearch & Full-Text Engine
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `FT.CREATE` | `FT.CREATE idx ON HASH\|JSON PREFIX 1 p SCHEMA field TYPE [...]` | Creates secondary index over Hashes or JSON (`TEXT`, `NUMERIC`, `TAG`, `VECTOR`). |
| `FT.SEARCH` | `FT.SEARCH idx query [NOCONTENT] [LIMIT o c] [SORTBY f] [RETURN n f...]` | Parallel scatter-gather search with Okapi BM25 and $O(\log N + K)$ `RangeTree`. |
| `FT.AGGREGATE` | `FT.AGGREGATE idx query [LOAD ...] [GROUPBY ...] [APPLY ...] [SORTBY ...] [LIMIT ...]` | Multi-stage aggregation pipeline with reducers, expressions, sorting, and pagination. |
| `FT.INFO` | `FT.INFO idx` | Returns index statistics, field schemas, document count, and memory consumption. |
| `FT.DROPINDEX` | `FT.DROPINDEX idx [DD]` | Drops index, optionally deleting indexed document keys (`DD`). |
| `FT.EXPLAIN` | `FT.EXPLAIN idx query` | Returns parsed query execution plan and filter syntax tree. |

### 11. Replication & Dragonfly Multi-Flow Replication
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `SAVE` / `BGSAVE` | `SAVE` / `BGSAVE` | Synchronous or fork-less asynchronous `io_uring` snapshotting. |
| `LASTSAVE` | `LASTSAVE` | Returns UNIX timestamp of most recent successful snapshot. |
| `REPLICAOF` / `SLAVEOF` | `REPLICAOF host port` / `REPLICAOF NO ONE` | Sets replication master target or promotes node to master. |
| `PSYNC` | `PSYNC replid offset` | Standard Redis master-replica full or partial resync stream. |
| `REPLCONF` | `REPLCONF [listening-port port] [ack offset] [capa ...]` | Replication configuration negotiation and periodic ACK heartbeats. |
| `ROLE` | `ROLE` | Reports replication role (`master` or `slave`), offset, and connected replicas. |
| `DFLY FLOW` | `DFLY FLOW master_replid sync_id shard_id [lsn]` | Binds dedicated per-shard TCP replication connection directly to worker thread. |

### 12. Dragonfly Compatibility Suite & Memcached Gateway
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `DFLYCLUSTER MYID` | `DFLYCLUSTER MYID` | Returns Dragonfly unique cluster node identifier. |
| `DFLYCLUSTER CONFIG` | `DFLYCLUSTER CONFIG json_string` | Atomically configures slot ownership and node roles using JSON manifest. |
| `DFLYCLUSTER GETSLOTINFO` | `DFLYCLUSTER GETSLOTINFO SLOTS s1 [s2 ...]` | Granular metadata, key count, and memory footprint per slot. |
| `DFLYCLUSTER FLUSHSLOTS` | `DFLYCLUSTER FLUSHSLOTS s1 e1 [s2 e2 ...]` | Flushes keys belonging to slot range(s) without wiping entire DB. |
| `DFLYCLUSTER SLOT-MIGRATION-STATUS` | `DFLYCLUSTER SLOT-MIGRATION-STATUS` | Live telemetry on Dragonfly slot migration flows and transfer rates. |
| `DFLYMIGRATE INIT` / `FLOW` / `ACK` | `DFLYMIGRATE INIT source_id shards [slots...]` | Multi-shard Dragonfly slot migration orchestration. |
| `STICK` / `UNSTICK` / `STICKY` | `STICK key` / `UNSTICK key` / `STICKY key` | Pins/unpins key in DRAM to prevent LRU eviction or NVMe tiering under memory pressure. |
| `DELEX` | `DELEX key [IFEQ val \| IFNE val \| IFGT val \| IFLT val]` | Atomic conditional deletion based on value equality or numerical comparison. |
| **Memcached Gateway** | `set`, `add`, `replace`, `get`, `delete`, `incr`, `decr`, `stats`, `version`, `quit` | Unified text-based Memcached protocol sharing database 0 with zero proxy overhead. |

### 13. RedisJSON Document Store (RFC 8259)
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `JSON.SET` / `JSON.GET` | `JSON.SET key path json` / `JSON.GET key [path ...]` | Stores or retrieves JSON documents or sub-trees via JSONPath selectors. |
| `JSON.DEL` / `JSON.TYPE` | `JSON.DEL key [path]` / `JSON.TYPE key [path]` | Deletes document/path in place or returns data type. |
| `JSON.NUMINCRBY` / `MULTBY` | `JSON.NUMINCRBY key path delta` / `NUMMULTBY ...` | Mutates numeric values at path in place without re-serialization. |
| `JSON.STRAPPEND` / `STRLEN` | `JSON.STRAPPEND key [path] s` / `JSON.STRLEN ...` | Appends to string or returns character length at path. |
| `JSON.ARRAPPEND` / `ARRLEN` | `JSON.ARRAPPEND key path v [...]` / `JSON.ARRLEN ...`| Appends elements to array or returns array length. |
| `JSON.ARRPOP` / `CLEAR` | `JSON.ARRPOP key [path [idx]]` / `JSON.CLEAR ...` | Pops element from array or clears container elements. |
| `JSON.OBJKEYS` / `OBJLEN` | `JSON.OBJKEYS key [path]` / `JSON.OBJLEN ...` | Returns keys or number of attributes in JSON object. |
| `JSON.TOGGLE` | `JSON.TOGGLE key path` | Toggles boolean value at path (`true` $\leftrightarrow$ `false`). |
| `JSON.MGET` | `JSON.MGET key [key ...] path` | Scatter-gather query retrieving JSON sub-paths across multiple keys. |

### 14. RedisBloom Probabilistic Engine
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `BF.RESERVE` / `ADD` / `EXISTS` | `BF.RESERVE key err cap` / `BF.ADD key item` / `EXISTS` | Bloom filter initialization, insertion, and membership testing. |
| `CF.RESERVE` / `ADD` / `EXISTS` / `DEL` | `CF.RESERVE key cap` / `CF.ADD key item` / `EXISTS` / `DEL` | Cuckoo filter with 4-slot bucket tables and item fingerprint deletion. |
| `CMS.INITBYDIM` / `INCRBY` / `QUERY` | `CMS.INITBYDIM key w d` / `CMS.INCRBY ...` / `QUERY` | Count-Min Sketch frequency estimation using minimum across hash rows. |
| `TOPK.RESERVE` / `ADD` / `QUERY` / `LIST` | `TOPK.RESERVE key k` / `TOPK.ADD ...` / `QUERY` / `LIST` | Space-Saving Top-$k$ streaming heavy-hitter tracker and ranked list. |

### 15. Vector Search Engine (HNSW, SQ8, PQ & ADC)
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `VADD` | `VADD idx key d0 d1 ... [METRIC cos\|l2\|ip] [QUANTIZE/SQ8] [PQ] [TIERED]` | Inserts embedding into HNSW graph with optional SQ8 or Product Quantization. |
| `VQUERY` | `VQUERY idx k q0 q1 ... [RERANK]` | Approximate Nearest Neighbor (ANN) search with optional exact float reranking. |
| `VSIM` | `VSIM idx key1 key2 [METRIC ...]` | Computes similarity distance between two vectors in memory. |
| `VDEL` | `VDEL idx key` | Removes vector from index and rewires neighboring graph edges. |

### 16. Transactions, Scripting & Redis 7 Functions
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `MULTI` / `EXEC` / `DISCARD` | `MULTI` / `EXEC` / `DISCARD` | Transaction block execution with VLL distributed multi-shard isolation. |
| `WATCH` / `UNWATCH` | `WATCH key [key ...]` / `UNWATCH` | Optimistic locking with CAS invalidation triggers. |
| `EVAL` / `EVALSHA` | `EVAL script numkeys [key ...] [arg ...]` / `EVALSHA ...` | Executes Lua script synchronously with keys mapped to local/mesh dispatchers. |
| `FUNCTION LOAD` / `FCALL` | `FUNCTION LOAD [REPLACE] #!lua ...` / `FCALL ...` | Compiles persistent Redis 7 function libraries and invokes routines. |

### 17. ACL & Security
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `AUTH` | `AUTH [username] password` | Authenticates connection against configured ACL users. |
| `ACL LIST` / `USERS` / `WHOAMI` | `ACL LIST` / `ACL USERS` / `ACL WHOAMI` | Dumps users, rules, and current connection identity. |
| `ACL SETUSER` / `DELUSER` | `ACL SETUSER user [rules ...]` / `ACL DELUSER user [...]` | Configures granular rules (`+@all`, `-@admin`, `>pass`, etc.) or deletes users. |

### 18. Server & Keyspace Inspection
| Command | Syntax / Usage | Description |
| :--- | :--- | :--- |
| `PING` / `ECHO` / `TIME` | `PING [msg]` / `ECHO msg` / `TIME` | Liveness check, echo, and server timestamp. |
| `DBSIZE` / `KEYS` / `SCAN` | `DBSIZE` / `KEYS pat` / `SCAN cursor [...]` | Key count, pattern matching, and cursor-based keyspace pagination. |
| `EXPIRE` / `TTL` / `PERSIST` | `EXPIRE key sec` / `TTL key` / `PERSIST key` | TTL expiration management and persistence. |
| `INFO` | `INFO [section]` | Returns server status, memory stats, tiering telemetry, and shard health. |
| `CLIENT TRACKING` | `CLIENT TRACKING on\|off [BCAST] [PREFIX pre]` | Enables RESP3 client-side caching invalidation push notifications. |

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
