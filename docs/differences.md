# Differences Between Rudis, Dragonfly, and Redis

This document summarizes the architectural, operational, and semantic differences between **Rudis**, **Dragonfly**, and **Redis**.

---

## 1. High-Level Comparison

| Feature / Property | Redis 7.2 | Dragonfly v1.39 | Rudis |
| :--- | :--- | :--- | :--- |
| **Language** | C99 | C++20 | **Rust 2021 (Rust 1.82+)** |
| **Concurrency Model** | Single-threaded event loop (I/O threads in 6.0+) | Shared-nothing with Boost.Fibers and epoll | **Shared-nothing Thread-Per-Core on Linux `io_uring` (Monoio)** |
| **Ingress Load Balancing** | Single listener socket | Single listener socket + thread dispatch | **Kernel `SO_REUSEPORT` 4-tuple balancing** |
| **Kernel Bypass & Hardware** | None (standard libc sockets) | None (standard libc sockets) | **AF_XDP (XSK), eBPF XDP, and Linux `kTLS`** |
| **Memory Allocation** | jemalloc (global heap) | Custom DashTable + Mimalloc | **Thread-local `RudisTable` + jemalloc per-core arenas** |
| **Snapshots (`BGSAVE`)** | Process `fork()` with Linux CoW | Fiber-level snapshot iteration | **Sequential `io_uring` streaming + `ioctl(FICLONE)` reflinks** |
| **Multi-Core Replication** | Single TCP connection (`PSYNC2`) | Per-shard parallel flows (`DFLY FLOW`) | **Per-shard parallel flows (`DFLY FLOW`) + `PSYNC2`** |
| **NVMe Tiered Storage** | None (pure in-memory) | SmallBins tiering | **3-State Lifecycle + SmallBins 4KB + Direct I/O (`O_DIRECT`)** |
| **Dual-Protocol Support** | RESP2 / RESP3 only | Redis + Memcached gateway | **Integrated Redis RESP2/RESP3 + Memcached Text Protocol** |

---

## 2. Structural & Semantic Limits

### String & Value Sizes
* **Redis**: Strings are limited to 512 MB.
* **Dragonfly**: Strings are limited to 256 MB.
* **Rudis**: Supports strings up to 512 MB in DRAM and multi-gigabyte values when NVMe tiering is active.

### Integer & Numeric Precision
* **Redis**: Integers in commands like `INCRBY` are signed 64-bit integers (`[-2^63, 2^63 - 1]`).
* **Dragonfly**: Signed 64-bit integers with 53-bit Lua integer bounds.
* **Rudis**: Fully compliant with 64-bit signed integer math and IEEE 754 double-precision floating point operations for `INCRBYFLOAT` and geospatial coordinates.

### Expiry Precision & Lifetime
* **Redis**: Millisecond-accurate TTLs with probabilistic 100ms active expiration cycles.
* **Dragonfly**: Rounding to nearest second for expiration intervals exceeding $2^{28}\text{ ms}$ (~3 days).
* **Rudis**: Millisecond precision maintained via thread-local time-wheel indexes with zero cross-core locking.

---

## 3. Replication & High Availability

### Parallel Replication vs. Single Stream
* In **Redis**, replicas connect over a single TCP connection. Replication throughput is capped at the maximum single-core egress rate of the master (~10–15 Gbps).
* In **Rudis**, replicas negotiate `REPLCONF capa dragonfly` and establish $N$ parallel TCP connections (`DFLY FLOW <replid> <sync_id> <shard_id>`). Each worker core streams mutations directly from its local thread without locks, scaling replication bandwidth linearly with CPU cores (60+ Gbps).

---

## 4. Multi-Protocol Engine (Redis + Memcached)

* **Redis** requires external proxy layers (like Twemproxy or Envoy) to speak Memcached protocol.
* **Rudis** natively accepts both Redis RESP commands and Memcached text-based commands (`set`, `get`, `incr`, `decr`, `stats`, `quit`) on the same listening port (`6379`) with zero configuration. Framing is automatically detected upon initial connection read, sharing database 0 with zero proxy overhead.
