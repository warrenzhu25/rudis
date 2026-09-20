# Differences Between Rudis, Dragonfly, and Redis

This document summarizes the architectural, operational, and semantic differences between **Rudis**, **Dragonfly**, and **Redis**.

---

## 1. High-Level Comparison

| Feature / Property | Redis 7.2 | Dragonfly v1.39 | Rudis |
| :--- | :--- | :--- | :--- |
| **Language** | C99 | C++20 | **Rust (edition 2024, requires Rust 1.85+)** |
| **Concurrency Model** | Single-threaded event loop (I/O threads in 6.0+) | Shared-nothing with Boost.Fibers and epoll | **Shared-nothing Thread-Per-Core on Linux `io_uring` (Monoio)** |
| **Ingress Load Balancing** | Single listener socket | Single listener socket + thread dispatch | **Kernel `SO_REUSEPORT` 4-tuple balancing** |
| **Kernel Bypass & Hardware** | None (standard libc sockets) | None (standard libc sockets) | **AF_XDP and `SO_ZEROCOPY` code paths exist but are not wired onto the live network path; kernel TLS offload is attempted best-effort and its result is currently discarded — see [architecture.md](architecture.md) and [design/10_kernel_bypass_xdp.md](design/10_kernel_bypass_xdp.md)** |
| **Memory Allocation** | jemalloc (global heap) | Custom DashTable + Mimalloc | **Thread-local `RudisTable` + `tikv-jemallocator` (jemalloc) as the process global allocator** |
| **Snapshots (`BGSAVE`)** | Process `fork()` with Linux CoW | Fiber-level snapshot iteration | **No `fork()`, but blocking sequential per-shard `std::fs` I/O — not `io_uring`-accelerated. `ioctl(FICLONE)` reflinks exist but snapshot the NVMe tiering backing file, not the RDB keyspace image — see [rdbsave.md](rdbsave.md)** |
| **Multi-Core Replication** | Single TCP connection (`PSYNC`) | Per-shard parallel flows (`DFLY FLOW`) | **Rudis-to-Rudis replication uses single-connection `PSYNC`, same as Redis. `DFLY FLOW` exists master-side only, to interoperate with a Dragonfly-protocol client — Rudis replicas never use it. See [replication.md](replication.md)** |
| **NVMe Tiered Storage** | None (pure in-memory) | SmallBins tiering | **3-State Lifecycle + SmallBins 4KB + Direct I/O (`O_DIRECT`)** |
| **Dual-Protocol Support** | RESP2 / RESP3 only | Redis + Memcached gateway | **Redis RESP2 (full) / RESP3 (output-side typed replies via `HELLO 3`) + Memcached Text Protocol on the same port** |

---

## 2. Structural & Semantic Limits

### String & Value Sizes
* **Redis**: Strings are limited to 512 MB, enforced by `proto-max-bulk-len`.
* **Dragonfly**: Strings are limited to 256 MB.
* **Rudis**: **No maximum bulk-string/array length is currently enforced** — the RESP parser
  (`src/resp.rs`) trusts the declared length from the wire, bounded only by available memory
  and `usize` overflow checks. A client sending a multi-gigabyte bulk string will be allowed to
  attempt it before any rejection occurs. This is a real gap relative to Redis's
  `proto-max-bulk-len`, not a claimed advantage — see
  [`docs/design/03_resp_engine.md`](design/03_resp_engine.md) §5.

### Integer & Numeric Precision
* **Redis**: Integers in commands like `INCRBY` are signed 64-bit integers (`[-2^63, 2^63 - 1]`).
* **Dragonfly**: Signed 64-bit integers with 53-bit Lua integer bounds.
* **Rudis**: Signed 64-bit integer arithmetic and IEEE 754 double-precision floats for `INCRBYFLOAT` and geospatial coordinates.

### Expiry Precision & Lifetime
* **Redis**: Millisecond-accurate TTLs with a probabilistic active expiration cycle.
* **Dragonfly**: Rounding to nearest second for expiration intervals exceeding $2^{28}\text{ ms}$ (~3 days).
* **Rudis**: Millisecond precision. `expire_at: Option<Instant>` is stored inline inside each
  `RudisEntry` (no separate expirations table, no time-wheel structure); an expired key is
  reclaimed passively the next time any operation touches it, plus an active sampling cycle
  that periodically scans for and evicts already-expired keys. See
  [`docs/design/05_storage_engine.md`](design/05_storage_engine.md).

---

## 3. Replication & High Availability

### Parallel Replication vs. Single Stream
* In **Redis**, replicas connect over a single TCP connection (`PSYNC`).
* **Rudis's own replica implementation also uses a single-connection `PSYNC` stream** against another Rudis (or Redis/Valkey) master — this is not a difference from Redis.
* Rudis's master side additionally implements the Dragonfly-specific handshake (`REPLCONF capa dragonfly`) and can serve $N$ parallel `DFLY FLOW` connections (`DFLY FLOW <replid> <sync_id> <shard_id>`) to a client that performs that handshake itself — but this is a compatibility surface for Dragonfly-protocol-aware clients, not something Rudis-to-Rudis replication exercises today. No linear-scaling replication bandwidth figure for this path has been independently measured for this revision; treat any such figure as unverified until traced to a committed benchmark document. See [replication.md](replication.md) for the full protocol split.

---

## 4. Multi-Protocol Engine (Redis + Memcached)

* **Redis** requires external proxy layers (like Twemproxy or Envoy) to speak Memcached protocol.
* **Rudis** natively accepts both Redis RESP commands and Memcached text-based commands (`set`, `get`, `incr`, `decr`, `stats`, `quit`) on the same listening port (`6379`) with zero configuration. Framing is automatically detected upon initial connection read, sharing database 0 with zero proxy overhead.

---

## 5. Pub/Sub Messaging & Sharded Delivery

* **Redis**: Standard Pub/Sub broadcasts messages across the entire cluster bus. Redis 7 introduced `SPUBLISH` for slot-bound pub/sub.
* **Dragonfly**: Backed by a 16-shard `ShardedHashMap` with fine-grained mutexes and RCU pointer swaps.
* **Rudis**: Uses a **16-stripe atomic presence bitmask** (`ShardedPresenceTable`) so a global
  `PUBLISH` skips shards with no subscriber in the channel's stripe, avoiding a broadcast to
  every shard on every publish. Because the table is striped by a hash of the channel name
  rather than keyed per-channel, distinct channel names can occasionally collide into the same
  stripe, causing a rare false-positive broadcast to a shard with no real subscriber — a
  documented trade-off, not a claim of zero unnecessary broadcasts. `SPUBLISH` uses
  point-to-point CRC16 slot routing with zero-copy `bytes::Bytes` delivery and bounded
  subscriber queue backpressure. See [pub-sub.md](pub-sub.md).

---

## 6. Document Store & RediSearch Engine

* **Redis**: Requires loading external dynamic C modules (`rejson.so`, `redisearch.so`).
* **Dragonfly**: Implements a native subset of JSON and search commands.
* **Rudis**: Natively integrates RFC 8259 RedisJSON document indexing and RediSearch with:
  - **Balanced `RangeTree`**: $O(\log N + K)$ numeric range indexing replacing naive linear filter scans.
  - **`FT.AGGREGATE` Pipeline**: Full multi-stage execution pipeline supporting `GROUPBY`, `REDUCE` (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`), `APPLY` arithmetic expressions with recursive-descent evaluation, `SORTBY`, and `LIMIT`.
  - **Hybrid Vector Fusion**: Reciprocal Rank Fusion (RRF) combining Okapi BM25 full-text rank scores with HNSW vector cosine similarity.
