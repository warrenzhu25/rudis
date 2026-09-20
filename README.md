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
- **Up to 25X more throughput** (crossing **3.2M+ ops/s** on 16 cores and scaling linearly with CPU cores)
- **Sub-millisecond tail latency** ($p99 < 0.5\text{ ms}$) under multi-million concurrent operations
- **Fork-less Linux `io_uring` streaming snapshots** and instant `ioctl(FICLONE)` reflink checkpoints with **zero memory spike** (eliminating Redis copy-on-write memory ballooning)
- **Redis 7 Slot-Bound Sharded Pub/Sub** (`SPUBLISH`, `SSUBSCRIBE`) with 16-stripe atomic presence filtering to eliminate cross-shard broadcast overhead
- **RediSearch Engine with Balanced `RangeTree` & `FT.AGGREGATE`**: $O(1)$ term-directed deletion, $O(\log N + K)$ numeric search, and multi-stage aggregation pipeline
- **Per-Shard Parallel TCP Replication Streams (`DFLY FLOW`)**: Dedicated streaming connections direct to primary worker shard threads
- **Hardware-accelerated Linux kernel bypass**: AF_XDP (XSK) wire-speed filtering, `io_uring` fixed registered buffer pools, Linux `SO_ZEROCOPY`, and hardware kernel TLS (`kTLS`)

---

## Contents

- [Architecture Overview](#architecture-overview)
- [Benchmarks](#benchmarks)
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
                     Client Requests (RESP2 / RESP3 / Memcached)
                                          │
           ┌──────────────────────────────┴──────────────────────────────┐
           │                Linux Kernel Networking Layer                │
           │   • SO_REUSEPORT Connection Balancing (Zero Userspace Hop)  │
           │   • AF_XDP (XSK) Zero-Copy UMEM Rings & eBPF XDP Filtering  │
           │   • Hardware Kernel TLS Acceleration (kTLS / TCP_ULP)       │
           └──────────────┬──────────────────────────────┬───────────────┘
                          │                              │
         ┌────────────────▼─────────────┐      ┌─────────▼────────────────────┐
         │       Core 0 (Shard 0)       │      │     Core N-1 (Shard N-1)     │
         │  ┌────────────────────────┐  │      │  ┌────────────────────────┐  │
         │  │ Monoio Runtime         │  │      │  │ Monoio Runtime         │  │
         │  │ (Independent io_uring) │  │      │  │ (Independent io_uring) │  │
         │  └───────────┬────────────┘  │      │  └───────────┬────────────┘  │
         │              ▼               │      │              ▼               │
         │  ┌────────────────────────┐  │      │  ┌────────────────────────┐  │
         │  │ Connection & Protocol  │  │      │  │ Connection & Protocol  │  │
         │  │ • Zero-Copy RESP Parser│  │      │  │ • Zero-Copy RESP Parser│  │
         │  │ • Memcached Gateway    │  │      │  │ • Memcached Gateway    │  │
         │  └───────────┬────────────┘  │      │  └───────────┬────────────┘  │
         │              ▼               │      │              ▼               │
         │  ┌────────────────────────┐  │      │  ┌────────────────────────┐  │
         │  │ Local Thread Storage   │  │      │  │ Local Thread Storage   │  │
         │  │ • In-Memory RudisTable │  │      │  │ • In-Memory RudisTable │  │
         │  │ • RangeTree & Search   │  │      │  │ • RangeTree & Search   │  │
         │  │ • HNSW (SQ8/PQ Vectors)│  │      │  │ • HNSW (SQ8/PQ Vectors)│  │
         │  │ • RedisJSON / Streams  │  │      │  │ • RedisJSON / Streams  │  │
         │  └───────────┬────────────┘  │      │  └───────────┬────────────┘  │
         │              ▼               │      │              ▼               │
         │  ┌────────────────────────┐  │      │  ┌────────────────────────┐  │
         │  │ NVMe Tiering SmallBins │  │      │  │ NVMe Tiering SmallBins │  │
         │  │ • Direct I/O (O_DIRECT)│  │      │  │ • Direct I/O (O_DIRECT)│  │
         │  └────────────────────────┘  │      │  └────────────────────────┘  │
         └──────────────┬───────────────┘      └──────────────┬───────────────┘
                        │                                     │
           ┌────────────▼─────────────────────────────────────▼──────────┐
           │                  Lock-Free Cross-Shard Mesh                 │
           │   • SPSC Channel Rings with EventFD Cross-Core Wakers       │
           │   • Striped Shard Presence Bitmask (Redis 7 Sharded Pub/Sub)│
           │   • Parallel Pipeline Squashing (Batched Multi-Key Dispatch)│
           └────────────┬─────────────────────────────────────┬──────────┘
                        │                                     │
                        ▼                                     ▼
         ┌──────────────────────────────┐      ┌──────────────────────────────┐
         │ Per-Shard Parallel TCP Flow  │      │ Fork-less io_uring Snapshots │
         │ • Dedicated DFLY FLOW Socket │      │ • Streaming Sequential RDB   │
         │ • Direct Worker Streaming    │      │ • ioctl(FICLONE) Reflink     │
         └──────────────────────────────┘      └──────────────────────────────┘
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

Benchmarked on **AMD EPYC 7B13 (64 vCPUs)** with the server pinned to 16 physical cores (`taskset -c 0-15`) and `memtier_benchmark` driven from client cores `32-63` (32 client threads, 1KB payload, pipeline depth 16, 3 runs median per command):

| Workload | Dragonfly v1.39 Throughput | Rudis Throughput | Speedup vs. DF | Dragonfly p99 | Rudis p99 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **SET (1KB)** | 1,876,363 ops/s | **2,477,258 ops/s** | **+32.0% (1.32x)** | 0.54 ms | **0.41 ms** |
| **GET (1KB)** | 730,007 ops/s | **2,067,300 ops/s** | **+183.2% (2.83x)** | 1.40 ms | **0.49 ms** |
| **INCR** | 2,748,865 ops/s | **3,223,111 ops/s** | **+17.3% (1.17x)** | 0.37 ms | **0.31 ms** |
| **MSET (5 keys)** | 389,854 ops/s | **709,598 ops/s** | **+82.0% (1.82x)** | 0.65 ms | **0.36 ms** |
| **MGET (5 keys)** | 346,020 ops/s | **491,997 ops/s** | **+42.2% (1.42x)** | 0.74 ms | **0.52 ms** |
| **DEL** | 2,879,816 ops/s | **3,234,771 ops/s** | **+12.3% (1.12x)** | 0.35 ms | **0.31 ms** |

> Complete benchmark suites across all 16 data structures, memory telemetry during snapshots, and reproduction scripts are documented in [docs/benchmarks/comprehensive_performance_guide.md](docs/benchmarks/comprehensive_performance_guide.md) and [docs/benchmarks/README.md](docs/benchmarks/README.md).

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
* [**Documentation Hub**](docs/README.md): Central index of all architecture and subsystem specifications.
* [**Architecture & Threading Model**](docs/architecture.md): Shared-nothing thread-per-core engine, Linux `io_uring` Monoio runtime, and request lifecycle.
* [**Per-Shard Parallel Replication**](docs/replication.md): Dragonfly-compatible multi-flow parallel TCP replication (`DFLY FLOW`).
* [**Pub/Sub Messaging Architecture**](docs/pub-sub.md): Striped shard presence bitmask and Redis 7 slot-bound sharded pub/sub (`SPUBLISH`).
* [**Fork-less io_uring Snapshots & Reflinks**](docs/rdbsave.md): Fork-less streaming persistence and sub-millisecond `ioctl(FICLONE)` reflink checkpoints.
* [**Differences with Redis & Dragonfly**](docs/differences.md): Semantic and architectural comparison across memory, limits, and networking.
* [**Component Architecture Specifications**](docs/components.md): Specifications for all 19 subsystems (`RudisTable`, `BlockHub`, Tiering, Vector, Search, XDP, CRDTs, JSON, etc.).
* [**Comprehensive Performance Guide**](docs/benchmarks/comprehensive_performance_guide.md): 16-core benchmark telemetry, latency distributions, and reproduction scripts.

---

## License

Rudis is open source and released under the [MIT License](LICENSE).
