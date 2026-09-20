# Rudis

<p align="center">
  <b>A shared-nothing, thread-per-core, Redis- and Memcached-compatible in-memory datastore in Rust</b><br>
  Built on Linux <code>io_uring</code> via the <a href="https://github.com/bytedance/monoio">Monoio</a> runtime
</p>

<p align="center">
  <a href="https://github.com/warrenzhu25/rudis/actions"><img src="https://img.shields.io/badge/build-passing-brightgreen.svg" alt="Build Status"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-1.85+-blue.svg" alt="Rust Version"></a>
  <a href="https://kernel.dk/io_uring.pdf"><img src="https://img.shields.io/badge/Linux-io__uring-orange.svg" alt="Linux io_uring"></a>
</p>

[Architecture Overview](#architecture-overview) • [Benchmarks](#benchmarks) • [Quick Start](#quick-start) • [Configuration](#configuration) • [Design Decisions](#design-decisions) • [Feature Matrix](#subsystem--feature-matrix) • [Documentation](docs/)

---

## What Rudis Is

**Rudis** (crate name `rudis`, currently at version `0.1.0`) is an experimental, from-scratch
Rust implementation of a Redis/Memcached-compatible in-memory datastore, built around a
**shared-nothing, thread-per-core** execution model on Linux `io_uring` (via the `monoio`
async runtime), in the tradition of engines such as ScyllaDB (Seastar) and Dragonfly.

It speaks the **Redis wire protocol (RESP2, plus RESP3 reply encoding negotiated via
`HELLO 3`)** and a **Memcached text-protocol gateway** on the same TCP port, so most existing
Redis and Memcached client libraries can connect without modification. The `Command` enum in
[`src/resp.rs`](src/resp.rs) currently defines close to 300 top-level command variants — strings,
hashes, lists, sets, sorted sets, streams, transactions, pub/sub, scripting, JSON, search,
vector search, probabilistic structures, geospatial commands, cluster/replication/ACL
administration, and a Memcached command subset — plus nested subcommand enums (`ACL`,
`CLUSTER`, `CLIENT`, `MEMORY`, and others) that expand the effective surface further. See
[Subsystem & Feature Matrix](#subsystem--feature-matrix) below for what is implemented, and
what is still partial or experimental.

This is a young, single-maintainer project, not a drop-in production replacement for Redis or
Dragonfly. Several subsystems described in this README and in `docs/` are real and load-bearing;
a few others (kernel-bypass networking, kernel TLS offload, automatic multi-region CRDT sync)
exist as working building blocks that are **not yet wired onto the live request path** — those
are called out explicitly rather than left implicit, both here and in the linked subsystem docs.

---

## Contents

- [Architecture Overview](#architecture-overview)
- [Benchmarks](#benchmarks)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Design Decisions](#design-decisions)
  - [1. Shared-Nothing Thread-Per-Core on Linux io_uring](#1-shared-nothing-thread-per-core-on-linux-io_uring)
  - [2. Fork-less RDB Snapshots](#2-fork-less-rdb-snapshots)
  - [3. Redis 7 Sharded Pub/Sub & Striped Presence Bitmask](#3-redis-7-sharded-pubsub--striped-presence-bitmask)
  - [4. RediSearch: RangeTree Indexing & FT.AGGREGATE](#4-redisearch-rangetree-indexing--ftaggregate)
  - [5. Replication: PSYNC and Dragonfly-Compatible DFLY FLOW](#5-replication-psync-and-dragonfly-compatible-dfly-flow)
  - [6. NVMe Tiered Storage (SmallBins & Direct I/O)](#6-nvme-tiered-storage-smallbins--direct-io)
  - [7. Kernel-Bypass Networking & TLS: Status](#7-kernel-bypass-networking--tls-status)
  - [8. Dual-Protocol Engine: Redis + Memcached](#8-dual-protocol-engine-redis--memcached)
- [Subsystem & Feature Matrix](#subsystem--feature-matrix)
- [Documentation & Contributor Guides](#documentation--contributor-guides)

---

## Architecture Overview

```
                     Client Requests (RESP2 / RESP3 replies / Memcached text)
                                          │
           ┌──────────────────────────────┴──────────────────────────────┐
           │                Linux Kernel Networking Layer                │
           │   • SO_REUSEPORT connection balancing (4-tuple hash)        │
           │   • Standard TCP sockets, driven via io_uring (Monoio)      │
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
         │  │ • Zero-copy RESP parse │  │      │  │ • Zero-copy RESP parse │  │
         │  │ • Memcached gateway    │  │      │  │ • Memcached gateway    │  │
         │  └───────────┬────────────┘  │      │  └───────────┬────────────┘  │
         │              ▼               │      │              ▼               │
         │  ┌────────────────────────┐  │      │  ┌────────────────────────┐  │
         │  │ Thread-Local ShardDb   │  │      │  │ Thread-Local ShardDb   │  │
         │  │ • RudisTable, Search   │  │      │  │ • RudisTable, Search   │  │
         │  │ • HNSW vector index    │  │      │  │ • HNSW vector index    │  │
         │  │ • JSON / Streams       │  │      │  │ • JSON / Streams       │  │
         │  └───────────┬────────────┘  │      │  └───────────┬────────────┘  │
         │              ▼               │      │              ▼               │
         │  ┌────────────────────────┐  │      │  ┌────────────────────────┐  │
         │  │ NVMe Tiering SmallBins │  │      │  │ NVMe Tiering SmallBins │  │
         │  │ • Direct I/O (O_DIRECT)│  │      │  │ • Direct I/O (O_DIRECT)│  │
         │  └────────────────────────┘  │      │  └────────────────────────┘  │
         └──────────────┬───────────────┘      └──────────────┬───────────────┘
                        │                                     │
           ┌────────────▼─────────────────────────────────────▼──────────┐
           │            Lock-Free Cross-Shard Mesh (src/mailbox.rs)      │
           │  • Per-(producer,consumer)-shard SPSC ring, capacity 256    │
           │  • flume channel used only as a sleep/wake signal           │
           │  • Pooled reply descriptors on hot GET/SET/batch paths      │
           └────────────┬─────────────────────────────────────┬──────────┘
                        │                                     │
                        ▼                                     ▼
         ┌──────────────────────────────┐      ┌──────────────────────────────┐
         │ Replication (PSYNC / DFLY)   │      │ RDB Snapshot / AOF           │
         │ • Single-stream PSYNC (rudis)│      │ • Sequential per-shard save  │
         │ • DFLY FLOW for DF clients   │      │ • No fork(); blocking file IO│
         └──────────────────────────────┘      └──────────────────────────────┘
```

Rudis employs a **thread-per-core, shared-nothing** architecture, described in full in
[docs/architecture.md](docs/architecture.md) and, at implementation depth, in
[`docs/design/04_sharding_mesh.md`](docs/design/04_sharding_mesh.md) /
[`docs/internal/04_sharding_mesh.md`](docs/internal/04_sharding_mesh.md):

1. **Thread-per-core pinning**: worker threads are pinned to physical CPU cores via
   `core_affinity` (disable with `--no-pin`). Each thread runs its own `monoio` event loop
   driving an independent `io_uring` instance.
2. **Ingress with `SO_REUSEPORT`**: every worker thread binds its own TCP listener on the same
   port; the Linux kernel distributes new connections across worker threads by 4-tuple hash,
   with no user-space router.
3. **Partitioned in-memory storage**: state is strictly thread-local (`ShardDb`). Local
   operations execute against a thread-local table with no mutex and no atomic CAS in the
   read/write hot path.
4. **Key routing**: in the default **standalone** mode, a key maps to a shard via
   `FxHash(hash_tag(key)) % num_shards` — there is no fixed 16,384-slot table in this mode.
   In **Redis Cluster mode** (`cluster-enabled yes`), routing instead uses the standard Redis
   Cluster scheme, `CRC16(hash_tag(key)) % 16384`, mapped to a shard-owned slot range, for
   wire-compatibility with Redis Cluster clients.
5. **Cross-shard mesh**: a request for a key on another shard is dispatched through a
   lock-free, per-shard-pair SPSC ring (`src/mailbox.rs`) with a mutex-guarded overflow queue
   for bursts beyond the ring's 256-slot capacity; `flume` channels remain in the mesh only as
   a cheap wake-up signal, never as the message payload carrier, on the hot call sites.
6. **Multi-protocol gateway**: a connection's protocol (RESP vs. Memcached text) is detected
   from the first bytes read; both share database 0 on the same listening port.

---

## Benchmarks

All figures below are reproduced from committed, script-generated data in this repository —
[`docs/benchmarks/comprehensive_performance_guide.md`](docs/benchmarks/comprehensive_performance_guide.md)
§6 and [`docs/benchmark_multicore_results.md`](docs/benchmark_multicore_results.md) — measured
on an AMD EPYC 7B13 (64 logical CPUs), server pinned to disjoint cores from the
`memtier_benchmark` client. **No number below is invented or extrapolated**; where the source
document flags a figure as unverified or non-reproducible, it is omitted here.

### Rudis vs. Dragonfly v1.39, common single-key commands (1 KB payload, pipelined)

At **16 physical cores**, Rudis leads Dragonfly on every one of the 16 commands measured in
this comparison. At **32 physical cores**, that lead **reverses**: Dragonfly leads on all 16 —
a reproducible crossover, not a cherry-picked artifact (see
[`docs/benchmarks/multi_command_comparison.md`](docs/benchmarks/multi_command_comparison.md)
§4 for the full 16-command table and discussion).

| Workload | Rudis @16 cores | Dragonfly @16 cores | Δ @16c | Δ @32c |
| :--- | :---: | :---: | :---: | :---: |
| **GET** (1KB) | 2,067,300 ops/s | 730,007 ops/s | **+183.2%** | -18.1% |
| **SET** (1KB) | 2,477,258 ops/s | 1,876,363 ops/s | **+32.0%** | -4.9% |
| **HGET** | 2,999,483 ops/s | 2,331,842 ops/s | **+28.6%** | -34.4% |
| **HSET** | 3,021,190 ops/s | 2,416,707 ops/s | **+25.0%** | -21.6% |
| **INCR** | 3,223,111 ops/s | 2,748,865 ops/s | **+17.3%** | -19.3% |
| **LPUSH** | 2,924,231 ops/s | 2,165,909 ops/s | **+35.0%** | -15.1% |
| **ZADD** | 2,963,699 ops/s | 2,343,497 ops/s | **+26.5%** | -14.6% |

The honest summary: **Rudis's cross-shard architecture wins clearly at moderate core counts,
and Dragonfly's proactor design overtakes it as core count increases further** — see the
linked benchmark documents for the full command list, methodology, coefficient-of-variation
data, and reproduction scripts (`scripts/benchmark_multicore_suite.py`,
`scripts/benchmark_payload_pipeline.py`).

### Core scaling (Rudis alone, `SET`, 1 KB payload)

| Cores | 1 | 2 | 4 | 8 | 16 | 32 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Throughput (ops/s) | 867,251 | 1,181,227 | 1,689,026 | 2,190,518 | **2,795,856** | 2,534,120 |

Scaling is strong through 8-16 cores but is **not strictly monotonic** beyond that in this
measurement (see [`docs/benchmarks/comprehensive_performance_guide.md`](docs/benchmarks/comprehensive_performance_guide.md)
§6 for the corresponding `GET` and mixed-workload columns and further discussion).

---

## Quick Start

### 1. Build from Source

Rudis targets Linux (for `io_uring`) and requires a Rust toolchain new enough for the 2024
edition (`edition = "2024"` in `Cargo.toml`), i.e. **Rust 1.85 or newer**:

```bash
git clone https://github.com/warrenzhu25/rudis.git
cd rudis
cargo build --release
```

### 2. Run Rudis

The binary is `rudis`. Its real command-line flags (from `src/main.rs`, via `clap`) are:

```bash
# Listen on the default port 6379, worker count = min(available_cores, 8)
./target/release/rudis

# Explicit port and thread (shard) count
./target/release/rudis --port 6379 --threads 8

# Load a config file, then apply CLI overrides on top of it
./target/release/rudis -c rudis.conf --port 6380
```

> Only the flags listed in [Configuration](#configuration) below actually exist. There is
> **no** `--bind`, `--dbfilename`, `--requirepass`, `--maxmemory`-as-a-plain-flag beyond what
> is listed, `--cache_mode`, `--tiered_prefix`, or `--memcached_port` flag — those either do
> not exist in the source at all, or are config-file-only directives (see below).

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

Rudis detects Memcached text-protocol framing on the same port and shares database 0 with the
Redis-protocol view of the keyspace:

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

Configuration comes from two layers: an optional **redis.conf-style config file**
(`-c path/to/rudis.conf`), and a small set of **CLI flags** that override it. This section
lists only directives and flags verified against [`src/config.rs`](src/config.rs) and
[`src/main.rs`](src/main.rs) — nothing here is aspirational.

### Config-file directives (`-c rudis.conf`)

| Directive | Meaning | Default |
| :--- | :--- | :--- |
| `bind <ip>` | Bind address | `127.0.0.1` |
| `port <n>` | TCP port | `6379` |
| `threads <n>` (alias `io-threads`) | Worker/shard count | `min(available_cores, 8)` |
| `maxclients <n>` | Maximum concurrent client connections | `10000` |
| `maxmemory <size>` | Memory threshold that drives tiered offload sizing (e.g. `4gb`) | unset |
| `maxmemory-policy <policy>` | Eviction policy name | `noeviction` |
| `appendonly <yes\|no>` | Enable AOF persistence | `no` |
| `dir <path>` | Data directory for RDB (`dump.rdb`, hardcoded filename — not configurable) and AOF files | `.` |
| `requirepass <password>` | Sets the default user's password for `AUTH` | unset |
| `tls-port <n>` | Port for TLS connections | unset |
| `tls-cert-file <path>` / `tls-key-file <path>` | TLS certificate / key PEM files | unset (self-signed cert generated in memory if a `tls-port` is set without them) |
| `cluster-enabled <yes\|no>` | Enable Redis Cluster mode/routing | `no` |
| `tiered-offload-threshold <pct>` | Memory-pressure percentage that starts offloading to NVMe tiering | `60` |
| `tiered-upload-threshold <pct>` | Memory-pressure percentage that triggers more aggressive offload | `80` |

Any other directive (including the classic Redis `save <seconds> <changes>` snapshot-schedule
syntax) parses without error and is stored verbatim, but **`save` is not currently acted on** —
there is no code path that reads it to schedule an automatic `BGSAVE`; only explicit
`SAVE`/`BGSAVE` and replica full-resync produce an RDB file. See
[`docs/rdbsave.md`](docs/rdbsave.md) §2 for the full trigger table.

### CLI flags (override the config file)

`-c/--config <path>`, `-p/--port <n>`, `-t/--threads <n>`, `--aof <bool>`,
`--aof-dir <path>` (sets the *data directory*, reused for RDB too), `--maxmemory <size>`,
`--tiered-offload-threshold <pct>`, `--tiered-upload-threshold <pct>`, `--no-pin` (disable
core-affinity pinning), `--tls-port <n>`, `--tls-cert-file <path>`, `--tls-key-file <path>`,
`--cluster-enabled <yes|no|true|1>`.

There is no CLI equivalent for `bind` or `requirepass` — set those in the config file.

### Example: `rudis.conf` (shipped in the repository root)

```conf
bind 0.0.0.0
port 6379
maxclients 10000

# threads 8   # commented out -> defaults to min(available_cores, 8)

maxmemory 4gb
maxmemory-policy allkeys-lru

tiered-offload-threshold 60
tiered-upload-threshold 80

appendonly yes
dir /var/lib/rudis

# requirepass "changeme_in_production"
# tls-port 6380
# tls-cert-file /etc/rudis/tls/rudis.crt
# tls-key-file /etc/rudis/tls/rudis.key

cluster-enabled no
```

---

## Design Decisions

### 1. Shared-Nothing Thread-Per-Core on Linux `io_uring`

Rudis avoids both the single-core ceiling of a strictly single-threaded store and the
mutex/cache-line-bouncing cost of a traditional shared-memory multi-threaded store:
- **Thread pinning** via `core_affinity` (`--no-pin` to disable).
- **`io_uring` via Monoio**: ingress network I/O and inter-shard notification wakeups run
  through Linux submission/completion queues.
- **`SO_REUSEPORT` ingress**: kernel-balanced connection distribution, no user-space router.
- **Lock-free routing**: cross-shard commands travel over per-shard-pair SPSC rings, not a
  shared mutex-guarded structure — see
  [`docs/design/04_sharding_mesh.md`](docs/design/04_sharding_mesh.md) for the full rationale,
  including the deliberate, narrow exceptions to this model (`BlockHub`, the cluster-topology
  registry, and the mailbox's own burst-overflow queue, each documented explicitly).

### 2. Fork-less RDB Snapshots

- Rudis's `SAVE`/`BGSAVE` never calls `fork()` — there is no reliance on Linux copy-on-write
  page duplication for snapshot isolation, which is a real difference from stock Redis.
- This does **not** mean the save path is `io_uring`-accelerated or truly async: RDB writing
  uses ordinary blocking `std::fs::File`/`std::io::Write`, one shard's serialized chunk at a
  time, and **each shard's own serialization work fully blocks that shard's reactor** for its
  duration — `BGSAVE` returns immediately to the *issuing* connection, but every other client
  pinned to a shard currently being saved will stall while that shard serializes.
- Reflink (`ioctl(FICLONE)`) snapshotting is real, working code — but it belongs to the NVMe
  tiered-storage subsystem (`TIER.SNAPSHOT`), not to `SAVE`/`BGSAVE`. The two are unrelated
  features that happen to both be called "snapshotting."
- Full detail, including the exact save-trigger table and known gaps (no automatic
  `save N M` scheduling), is in [`docs/rdbsave.md`](docs/rdbsave.md).

### 3. Redis 7 Sharded Pub/Sub & Striped Presence Bitmask

- **Slot-bound sharded pub/sub (`SPUBLISH`, `SSUBSCRIBE`)**: channels are routed with the same
  CRC16/16384-slot scheme as cluster-mode keys, so `SPUBLISH` can route point-to-point to the
  owning shard instead of broadcasting.
- **16-stripe presence bitmask (`ShardedPresenceTable`)**: ordinary `PUBLISH` consults an
  atomic 16-stripe bitmask before broadcasting to a shard, skipping shards with no local
  subscribers for that stripe. Because the table is striped by a hash of the channel name
  rather than keyed per-channel, distinct channel names can occasionally collide into the same
  stripe, causing an occasional false-positive broadcast to a shard with no real subscriber —
  a documented, deliberate memory/precision trade-off, not a bug.
- Details: [`docs/pub-sub.md`](docs/pub-sub.md).

### 4. RediSearch: `RangeTree` Indexing & `FT.AGGREGATE`

- Documents are mapped to dense integer document IDs to shrink posting-list memory relative to
  storing string keys directly.
- Numeric fields are indexed with a balanced `RangeTree` for `@field:[min max]` range queries.
- `FT.AGGREGATE` supports a multi-stage pipeline (`GROUPBY`, `REDUCE`, `APPLY`, `SORTBY`).
- Details and known limitations: [`docs/design/09_redisearch.md`](docs/design/09_redisearch.md).

### 5. Replication: `PSYNC` and Dragonfly-Compatible `DFLY FLOW`

Rudis implements **two independent replication protocols on the master side**:
- **Standard single-connection `PSYNC`**, wire-compatible with Redis/Valkey replicas and with
  Rudis's own replica implementation. **This is the protocol Rudis uses when replicating
  against another Rudis node** — replication throughput in that configuration is bounded by a
  single TCP connection, the same as stock Redis.
- **`DFLY FLOW`** (one TCP connection per shard, negotiated via `REPLCONF capa dragonfly`)
  exists on the master side purely for interoperability with a Dragonfly-protocol-aware
  client. **Rudis's own replica implementation never uses this path** — there is currently no
  Rudis-to-Rudis replication mode that gets per-shard parallel streaming.
- Full protocol detail: [`docs/replication.md`](docs/replication.md).

### 6. NVMe Tiered Storage (SmallBins & Direct I/O)

- **Three-state lifecycle**: keys move Hot (DRAM) → Cooled → Cold (NVMe), promoted back to Hot
  on access.
- **`SmallBins` packing**: values under 4 KB are coalesced into aligned 4 KB blocks before a
  single write, avoiding write amplification from many tiny writes.
- **Direct I/O**: reads/writes bypass the page cache via `O_DIRECT`; space reclamation uses
  `fallocate(FALLOC_FL_PUNCH_HOLE)`.
- Details: [`docs/design/07_nvme_tiering.md`](docs/design/07_nvme_tiering.md) and
  [`docs/design/tiered_storage.md`](docs/design/tiered_storage.md) (the latter also documents a
  real correctness gap: `RudisValue::Tiered` values currently serialize to zero bytes in a
  `SAVE`/`BGSAVE` RDB image — see that document for scope and status).

### 7. Kernel-Bypass Networking & TLS: Status

Two subsystems exist as unconditionally-compiled, unit-tested code but are **not on the live
network path today**:
- **`src/xdp.rs`** implements AF_XDP (UMEM rings, an eBPF-style packet classifier) entirely as
  in-process Rust data structures, reachable only via explicit `XDP.*` admin commands. No real
  AF_XDP socket, eBPF program, or NIC binding is created by any code path.
- **`src/zerocopy.rs`** implements correct `SO_ZEROCOPY`/registered-buffer primitives, but
  nothing outside its own unit tests calls them — the real connection write path
  (`src/connection.rs`, on `monoio`) does not use this module.
- **Kernel TLS (kTLS)**: `rustls` performs the real (userspace) TLS handshake and encryption.
  The server attempts to promote a connection to kernel TLS (`TCP_ULP`) afterward on a
  best-effort basis, but the result is currently discarded — `is_ktls_active` is always
  `false` in the current build, so the kTLS zero-copy benefit is aspirational, not realized.

Treat these as real, tested building blocks for a future kernel-bypass data path, not as
currently active acceleration. See
[`docs/design/10_kernel_bypass_xdp.md`](docs/design/10_kernel_bypass_xdp.md) and
[`docs/design/15_security_tls.md`](docs/design/15_security_tls.md) for the full picture,
including *why* this direction is still worth pursuing despite not being wired up yet.

### 8. Dual-Protocol Engine: Redis + Memcached

- A single listening port accepts both RESP (Redis) and Memcached text commands.
- Protocol framing is auto-detected from the first bytes read on a new connection; both
  protocols share database 0.

---

## Subsystem & Feature Matrix

Status reflects source-verified behavior as of this revision, not aspiration. "Design doc"
links go to the corresponding source-verified subsystem specification.

| Subsystem | Status | Notes | Design Doc |
| :--- | :---: | :--- | :--- |
| **Thread-per-core engine** | Implemented | Shared-nothing shards on Monoio/`io_uring`, lock-free cross-shard mesh. | [01](docs/design/01_reactor_runtime.md) / [04](docs/design/04_sharding_mesh.md) |
| **Core Redis data structures** | Implemented | Strings, hashes, lists, sets, sorted sets, bitmaps, HyperLogLog. | [05](docs/design/05_storage_engine.md) |
| **Geospatial commands** | Implemented | Integer geohash encoding, Haversine distance, `GEOSEARCH`. | [17](docs/design/17_geospatial.md) |
| **Streams & consumer groups** | Implemented | Append-only log, consumer groups, PEL, blocking/non-blocking `XREAD`. | — |
| **Transactions (`MULTI`/`EXEC`)** | Implemented | Deterministic cross-shard execution ordering to avoid deadlock. | [rudis_internals_guide §5](docs/rudis_internals_guide.md) |
| **Pub/Sub incl. Sharded Pub/Sub** | Implemented | 16-stripe presence bitmask; CRC16 slot routing for `SPUBLISH`. | [19](docs/design/19_pubsub.md), [pub-sub.md](docs/pub-sub.md) |
| **Scripting & Functions** | Implemented | `EVAL`/`EVALSHA` and `FUNCTION`/`FCALL` via sandboxed `mlua` (Lua 5.4). | [13](docs/design/13_scripting_functions.md) |
| **ACL & Security** | Implemented | Per-user permissions, category selectors, `AUTH`. | [15](docs/design/15_security_tls.md) |
| **RDB persistence** | Implemented, with gaps | No `fork()`; blocking, per-shard-sequential file I/O; `save N M` is parsed but not scheduled; `RudisValue::Tiered` serializes to zero bytes today. | [rdbsave.md](docs/rdbsave.md) |
| **AOF persistence** | Implemented | Async (`monoio`) periodic flush, distinct I/O strategy from RDB save. | [14](docs/design/14_persistence_replication.md) |
| **Replication (`PSYNC`)** | Implemented | Single-connection, used for all Rudis-to-Rudis replication. | [replication.md](docs/replication.md) |
| **Replication (`DFLY FLOW`)** | Implemented, narrow scope | Master-side only, for Dragonfly-protocol clients; Rudis replicas never use it. | [replication.md](docs/replication.md) |
| **Redis Cluster & gossip** | Implemented | Plain-text gossip bus on `port + 10000` (not `io_uring`), quorum failover, real `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD` key migration. | [11](docs/design/11_cluster_topology.md) |
| **Dragonfly compatibility (state only)** | Partial | `DFLYCLUSTER`/`DFLYMIGRATE` remain bookkeeping-only, distinct from the more complete native cluster migration path above. | [11](docs/design/11_cluster_topology.md) |
| **RedisJSON document store** | Implemented | JSONPath parsing, array slicing, in-place mutation. | [16](docs/design/16_json_store.md) |
| **RediSearch (`FT.*`)** | Implemented | Inverted index, BM25 scoring, `RangeTree` numeric range, `FT.AGGREGATE`. | [09](docs/design/09_redisearch.md) |
| **RedisBloom-style probabilistic types** | Implemented | Bloom, Cuckoo, Count-Min Sketch, Top-K. | [18](docs/design/18_probabilistic.md) |
| **HNSW vector search** | Implemented | Cosine/L2/IP metrics, SQ8 scalar quantization, product quantization. | [08](docs/design/08_vector_engine.md) |
| **NVMe tiered storage** | Implemented, with a known gap | Hot/Cooled/Cold lifecycle, `SmallBins`, Direct I/O; see tiered-value RDB gap above. | [07](docs/design/07_nvme_tiering.md), [tiered_storage.md](docs/design/tiered_storage.md) |
| **`TIER.SNAPSHOT` reflink checkpoints** | Implemented | Real `ioctl(FICLONE)` cloning of the NVMe tier's backing file — unrelated to `SAVE`/`BGSAVE`. | [tiered_storage.md](docs/design/tiered_storage.md) |
| **Multi-region CRDTs** | Implemented, manual sync only | LWW-Register, OR-Set, PN-Counter with HLC ordering; export/merge (`CRDT.DUMP`/`CRDT.MERGE`) is explicit and manual — there is no automatic peer discovery or background cross-region streaming. | [12](docs/design/12_crdt_types.md) |
| **AF_XDP kernel bypass** | Experimental, not on live path | Real, unit-tested data structures; reachable only via `XDP.*` admin commands, not real NIC/eBPF I/O. | [10](docs/design/10_kernel_bypass_xdp.md) |
| **`SO_ZEROCOPY` send path** | Experimental, not on live path | Correct primitives in `src/zerocopy.rs`; unused by the real connection write path. | [10](docs/design/10_kernel_bypass_xdp.md) |
| **Kernel TLS (kTLS)** | Attempted, inert | `rustls` handles real TLS; kernel offload is attempted best-effort and its result is currently discarded. | [15](docs/design/15_security_tls.md) |
| **Jemalloc memory telemetry** | Implemented | Global allocator is `tikv-jemallocator`; live stats via `tikv-jemalloc-ctl` in `INFO memory`. | [15](docs/design/15_security_tls.md) |

---

## Documentation & Contributor Guides

For deep technical walkthroughs, internal architecture specifications, and benchmarks:
* [**Documentation Hub**](docs/README.md): index of all architecture and subsystem docs.
* [**Architecture & Threading Model**](docs/architecture.md): thread-per-core engine, `io_uring`/Monoio runtime, request lifecycle.
* [**Replication**](docs/replication.md): `PSYNC` and Dragonfly-compatible `DFLY FLOW`.
* [**Pub/Sub Architecture**](docs/pub-sub.md): striped presence bitmask and Redis 7 sharded pub/sub.
* [**RDB Snapshotting**](docs/rdbsave.md): what actually triggers a save, and how it interacts with the shard model.
* [**Differences from Redis & Dragonfly**](docs/differences.md): architectural and behavioral comparison.
* [**Subsystem Design Specifications**](docs/design/README.md): design rationale ("why") across all 19 subsystems.
* [**Subsystem Implementation References**](docs/internal/README.md): concrete data structures, algorithms, and source references.
* [**Comprehensive Performance Guide**](docs/benchmarks/comprehensive_performance_guide.md) and [**Multicore Benchmark Report**](docs/benchmark_multicore_results.md): reproducible benchmark data and methodology.
* [**Engineering Invariants for Contributors**](agent.md): mandatory architectural rules for anyone changing this codebase.

---

## License

This repository does not currently include a `LICENSE` file, and `Cargo.toml` does not declare
a `license` field. No open-source license terms have been formally granted for this codebase
yet; do not assume MIT (or any other) terms until a `LICENSE` file is added. If you need to use
this code and the licensing status matters to you, contact the maintainer.
