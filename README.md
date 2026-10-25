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

[Architecture Overview](#architecture-overview) • [Benchmarks](#benchmarks) • [Quick Start](#quick-start) • [Configuration](#configuration) • [Production Deployment](#production-deployment) • [Design Decisions](#design-decisions) • [Feature Matrix](#subsystem--feature-matrix) • [Documentation](docs/)

---

## What Rudis Is

**Rudis** (crate name `rudis`, currently at version `0.1.0`) is an experimental, from-scratch
Rust implementation of a Redis/Memcached-compatible in-memory datastore, built around a
**shared-nothing, thread-per-core** execution model on Linux `io_uring` (via the `monoio`
async runtime), in the tradition of engines such as ScyllaDB (Seastar) and Dragonfly.

It speaks the **Redis wire protocol (RESP2, plus RESP3 reply encoding negotiated via
`HELLO 3`)** and a **Memcached text-protocol gateway** on the same TCP port, so most existing
Redis and Memcached client libraries can connect without modification. The `Command` enum in
[`src/resp.rs`](src/resp.rs) currently defines over 330 top-level command variants — strings,
hashes, lists, sets, sorted sets, streams, transactions, pub/sub, scripting, JSON, search,
vector search, AI-native agent memory/checkpoints/quotas, Model Context Protocol (MCP), semantic
caching, probabilistic structures, geospatial commands, cluster/replication/ACL
administration, and a Memcached command subset — plus nested subcommand enums (`ACL`,
`CLUSTER`, `CLIENT`, `MEMORY`, and others) that expand the effective surface further. See
[Subsystem & Feature Matrix](#subsystem--feature-matrix) below for what is implemented, and
what is still partial or experimental.

This is a young, single-maintainer project, not a drop-in production replacement for Redis or
Dragonfly. Several subsystems described in this README and in `docs/` are real and load-bearing;
a few others (kernel-bypass networking, automatic multi-region CRDT sync)
exist as working building blocks that are **not yet wired onto the live request path** — those
are called out explicitly rather than left implicit, both here and in the linked subsystem docs.

---

## Contents

- [Architecture Overview](#architecture-overview)
- [Benchmarks](#benchmarks)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Production Deployment](#production-deployment)
- [Design Decisions](#design-decisions)
  - [1. Shared-Nothing Thread-Per-Core on Linux io_uring](#1-shared-nothing-thread-per-core-on-linux-io_uring)
  - [2. Fork-less RDB Snapshots](#2-fork-less-rdb-snapshots)
  - [3. Redis 7 Sharded Pub/Sub & Striped Presence Bitmask](#3-redis-7-sharded-pubsub--striped-presence-bitmask)
  - [4. RediSearch: RangeTree Indexing, Hybrid Vector Search & FT.AGGREGATE](#4-redisearch-rangetree-indexing-hybrid-vector-search--ftaggregate)
  - [5. Replication: PSYNC and Dragonfly-Compatible DFLY FLOW](#5-replication-psync-and-dragonfly-compatible-dfly-flow)
  - [6. NVMe Tiered Storage (SmallBins & Direct I/O)](#6-nvme-tiered-storage-smallbins--direct-io)
  - [7. Kernel-Bypass Networking & TLS: Status](#7-kernel-bypass-networking--tls-status)
  - [8. Dual-Protocol Engine: Redis + Memcached](#8-dual-protocol-engine-redis--memcached)
  - [9. Agent Memory, LLM Quota, Checkpoints & MCP Server](#9-agent-memory-llm-quota-checkpoints--mcp-server)
- [Subsystem & Feature Matrix](#subsystem--feature-matrix)
- [Redis/Valkey TCL Compatibility Suite](#redisvalkey-tcl-compatibility-suite)
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
[`docs/benchmark_multicore_results.md`](docs/benchmark_multicore_results.md) and
[`benchmark_multicore_results.json`](benchmark_multicore_results.json) (generated by
[`scripts/benchmark_multicore_suite.py`](scripts/benchmark_multicore_suite.py)) — measured on an
AMD EPYC 7B13 (64 logical vCPUs / 32 physical cores across 4 L3 CCXs). Server threads are pinned
to physical cores `0-15` (leaving their SMT siblings `32-47` idle) and `memtier_benchmark` is
pinned to disjoint physical cores `16-31` (`16-31,48-63` on CCX 2–3) so server and client share
neither physical cores nor L3 cache complexes.

### Rudis vs. Dragonfly v1.39 vs. Valkey 8.1 (16 Physical Cores, 3 Interleaved Rounds)

| Workload | Rudis | Dragonfly v1.39 | Valkey 8.1 | Rudis vs. Dragonfly | Rudis p99 | Dragonfly p99 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **SET 100% (Pipeline 16, 1KB)** | **3,371,148 ops/s** | 1,864,914 ops/s | 528,890 ops/s | **+80.8%** | **0.78 ms** | 1.46 ms |
| **GET 100% (Pipeline 16, 1KB)** | **2,251,552 ops/s** | 712,054 ops/s | 684,454 ops/s | **+216.2%** | **1.10 ms** | 3.20 ms |
| **SET 100% (Unpipelined P1, 1KB)** | **574,767 ops/s** | 476,413 ops/s | 402,089 ops/s | **+20.6%** | **0.34 ms** | 0.49 ms |
| **GET 100% (Unpipelined P1, 1KB)** | **557,390 ops/s** | 483,176 ops/s | 406,381 ops/s | **+15.4%** | **0.43 ms** | 0.53 ms |
| **MGET 10-Key Scattered (P8)** | **892,875 ops/s** | 319,051 ops/s | 219,186 ops/s | **+179.9%** | **1.42 ms** | 3.25 ms |
| **DEL 10-Key Scattered (P8)** | **1,128,372 ops/s** | 350,898 ops/s | 387,464 ops/s | **+221.6%** | **1.07 ms** | 3.10 ms |

### Physical Core Scaling (Rudis vs. Dragonfly v1.39, 1 to 16 Physical Cores, Pipeline 16, 1 KB payload)

| Engine / Workload | 1 Core | 2 Cores | 4 Cores | 8 Cores | 16 Cores | 1→16c Speedup |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Rudis SET (P16, 1KB)** | **651,303 ops/s** | **862,089 ops/s** | **1,464,246 ops/s** | **2,332,478 ops/s** | **3,202,127 ops/s** | **4.92x** |
| Dragonfly SET (P16, 1KB) | 349,249 ops/s | 544,296 ops/s | 856,917 ops/s | 1,317,426 ops/s | 1,882,172 ops/s | 5.39x |
| **Rudis GET (P16, 1KB)** | **572,696 ops/s** | **676,476 ops/s** | **1,183,696 ops/s** | **1,898,886 ops/s** | **2,261,864 ops/s** | **3.95x** |
| Dragonfly GET (P16, 1KB) | 64,877 ops/s | 115,796 ops/s | 221,866 ops/s | 418,987 ops/s | 701,858 ops/s | 10.82x |

See [`docs/benchmark_multicore_results.md`](docs/benchmark_multicore_results.md) for the 8-core
head-to-head table, p99 latency breakdown, and topology notes explaining why 32-thread benchmarks
on a single 32-physical-core / 64-vCPU VM suffer from SMT sibling contention between server and
client threads, and [`docs/benchmarks/multi_command_comparison.md`](docs/benchmarks/multi_command_comparison.md)
([`benchmark_common_commands_results.json`](benchmark_common_commands_results.json)) for the
16-command data-structure suite (`SET`, `GET`, `INCR`, `MSET`, `MGET`, `HSET`, `HGET`, `LPUSH`,
`LPOP`, `LRANGE`, `SADD`, `SISMEMBER`, `ZADD`, `ZRANGE`, `DEL`, `EXISTS`) at 8 and 16 physical
cores, where Rudis leads Dragonfly v1.39 on all 16 commands by **+43.9% to +210.8% (1.44x–3.11x)**
at 16 physical cores.

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
> **no** `--dbfilename`, `--requirepass`, `--maxmemory`-as-a-plain-flag beyond what
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
| `bind <addr> [<addr> ...]` | Interfaces to listen on, Redis syntax: IPv4/IPv6 addresses, `*` = all IPv4, `::*` = all IPv6, a leading `-` makes an address optional. Also applies to the TLS port and the cluster bus. Not changeable at runtime. | `* -::*` (same as Redis) |
| `protected-mode <yes\|no>` | While the default user has no password, refuse non-loopback clients with `-DENIED` (Redis semantics). Also `--protected-mode`, `CONFIG SET protected-mode`. | `yes` |
| `port <n>` | TCP port | `6379` |
| `threads <n>` (alias `io-threads`) | Worker/shard count | `min(available_cores, 8)` |
| `maxclients <n>` | Maximum concurrent client connections | `10000` |
| `maxmemory <size>` | Memory threshold that drives tiered offload sizing (e.g. `4gb`) | unset |
| `maxmemory-policy <policy>` | Eviction policy (`noeviction`, `allkeys-lru`, `volatile-lru`, `allkeys-lfu`, `volatile-lfu`, `allkeys-random`, `volatile-random`, `volatile-ttl`) | `noeviction` |
| `appendonly <yes\|no>` | Enable AOF persistence (also runtime-toggleable via `CONFIG SET appendonly`) | `no` |
| `appendfsync <always\|everysec\|no>` | AOF fsync durability mode | `everysec` |
| `auto-aof-rewrite-percentage <pct>` / `auto-aof-rewrite-min-size <size>` | Automatic background AOF compaction thresholds | `100` / `64mb` |
| `dir <path>` / `dbfilename <name>` | Data directory and RDB filename | `.` / `dump.rdb` |
| `save <seconds> <changes>` | Automatic background RDB snapshot schedule | `3600 1 300 100 60 10000` (Redis's) |
| `requirepass <password>` / `aclfile <path>` | Default user password / external ACL users file (`ACL SAVE` / `ACL LOAD`) | unset |
| `tls-port <n>` | Port for TLS connections | unset |
| `tls-cert-file <path>` / `tls-key-file <path>` | TLS certificate / key PEM files | unset (self-signed cert generated in memory if a `tls-port` is set without them) |
| `tls-ca-cert-file <path>` / `tls-auth-clients <yes\|no\|optional>` | mTLS client certificate CA bundle and verification mode | unset / `no` |
| `tls-replication <yes\|no>` / `tls-cluster <yes\|no>` | Encrypt outbound replication links and cluster gossip bus with TLS | `no` / `no` |
| `cluster-enabled <yes\|no>` / `cluster-config-file <path>` | Enable Redis Cluster mode and automatic `nodes.conf` persistence | `no` / `nodes.conf` |
| `enable-debug-command <yes\|no\|local>` | Gate `DEBUG` subcommands | `no` |
| `tiered-offload-threshold <pct>` | Memory-pressure percentage that starts offloading to NVMe tiering | `60` |
| `tiered-upload-threshold <pct>` | Memory-pressure percentage that triggers more aggressive offload | `80` |

`save <seconds> <changes>` schedules automatic background `BGSAVE` runs when at least `<changes>` writes have occurred after `<seconds>` elapsed since the last save, and triggers an RDB save on graceful `SHUTDOWN`/`SIGTERM`. See [`docs/rdbsave.md`](docs/rdbsave.md) §2 for the full trigger table.

### CLI flags (override the config file)

`-c/--config <path>`, `-p/--port <n>`, `-t/--threads <n>`, `--aof <bool>`,
`--aof-dir <path>` (sets the *data directory*, reused for RDB too), `--maxmemory <size>`,
`--tiered-offload-threshold <pct>`, `--tiered-upload-threshold <pct>`, `--no-pin` (disable
core-affinity pinning), `--tls-port <n>`, `--tls-cert-file <path>`, `--tls-key-file <path>`,
`--cluster-enabled <yes|no|true|1>`, `--bind <addrs>`, `--protected-mode <yes|no>`.

There is no CLI equivalent for `requirepass` — set it in the config file.

### Example: `rudis.conf` (shipped in the repository root)

```conf
bind 127.0.0.1 -::1
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

## Production Deployment

Practical guidance for running Rudis somewhere other than a laptop, verified against current
source rather than aspirational. Read the [Subsystem & Feature Matrix](#subsystem--feature-matrix)
and [License](#license) before depending on this in a business-critical path — this is a young,
single-maintainer project with real, documented gaps, called out below rather than smoothed over.

### System Requirements & Kernel Tuning

Rudis requires Linux (for `io_uring`) at runtime. On every startup it runs sanity checks
([`src/syscheck.rs`](src/syscheck.rs)) and prints non-fatal `[!]` warnings to stdout — check
them at boot rather than assuming defaults are fine:

| Check | Recommended | Fix |
| :--- | :--- | :--- |
| `vm.overcommit_memory` | `1` | `sysctl -w vm.overcommit_memory=1` |
| `net.core.somaxconn` (TCP backlog) | `>= 512`, ideally `>= 4096` under load | `sysctl -w net.core.somaxconn=4096` |
| `ulimit -n` (open file descriptors) | `>= 10000` | systemd `LimitNOFILE=`, or `/etc/security/limits.conf` |
| Transparent Huge Pages | `never` | `echo never > /sys/kernel/mm/transparent_hugepage/enabled` |

These checks are advisory only — the process still starts if they fail; the warnings exist so a
degraded deployment is visible in logs instead of silently running with a bad kernel config.

### Running as a systemd Service

```ini
[Unit]
Description=Rudis in-memory datastore
After=network.target

[Service]
Type=notify
NotifyAccess=all
User=rudis
Group=rudis
ExecStart=/usr/local/bin/rudis -c /etc/rudis/rudis.conf
TimeoutStopSec=300s
Restart=on-failure
RestartSec=2
LimitNOFILE=65536
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
```

If a shard thread dies (a panic outside a single client's command, which is caught and counted
per connection), the process logs `FATAL: shard N panicked: ...` and exits with status 1 rather
than serving on without that shard's keys. Run it under a supervisor that restarts it
(`Restart=always`/`on-failure`, a container restart policy); it recovers from the AOF/RDB.

Create the `rudis` user/group and a data directory it owns (matching `dir` in `rudis.conf`)
before enabling the unit. If the host or container has a restricted CPU set (cgroups, a VM with
fewer cores than the physical host), pass `--no-pin` — `core_affinity` pinning assumes it can
address the physical core IDs it sees, which will not line up 1:1 inside a constrained cgroup.

### Docker

A multi-stage [`Dockerfile`](Dockerfile) is included: a `rust:1.85-bookworm` build stage and a
`debian:bookworm-slim` runtime stage that runs as a non-root `rudis` system user, with
`/var/lib/rudis` declared as a volume.

```bash
docker build -t rudis:latest .
docker run -d --name rudis \
  -p 6379:6379 \
  -v rudis-data:/var/lib/rudis \
  rudis:latest
```

The image's `CMD` defaults to `--port 6379 --aof true --aof-dir /var/lib/rudis`. Note that
`--aof` takes an explicit `true`/`false` token (it is `Option<bool>` in [`src/main.rs`](src/main.rs),
not a bare flag) — passing bare `--aof` immediately followed by another `--flag` fails to parse
and the container will not start; any `docker run ... rudis:latest <args>` override must follow
the same rule. To use a config file instead of flags, bind-mount it and pass `-c`:

```bash
docker run -d --name rudis \
  -p 6379:6379 -p 6380:6380 \
  -v rudis-data:/var/lib/rudis \
  -v $(pwd)/rudis.conf:/etc/rudis/rudis.conf:ro \
  rudis:latest -c /etc/rudis/rudis.conf
```

### Logging & Observability

- Rudis logs via `tracing`/`tracing-subscriber`, controlled by the standard `RUST_LOG`
  environment variable (e.g. `RUST_LOG=info,rudis=debug`); it defaults to `info,rudis=info` if
  unset. The startup banner and final shutdown line are printed directly with `println!` and are
  not gated by `RUST_LOG`.
- `INFO` (optionally `INFO <section>` — `server` (default), `clients`, `memory`, `persistence`,
  `replication`, `storage`/`tiered`, `stats`, `commandstats`) returns Redis-protocol text over
  the normal client connection: `redis-cli -p 6379 INFO`.
- Prometheus metrics: start with `--metrics-port 9121` (or `metrics-port 9121` in the config
  file) and scrape `http://127.0.0.1:9121/metrics`. The endpoint listens on loopback by default
  because it has no authentication; use `--metrics-bind 0.0.0.0` / `metrics-bind` to expose it to
  a remote scraper, behind a firewall. Other paths return 404. A metrics port that can't be bound
  stops startup. The same text is available as `INFO metrics` (alias `INFO prometheus`) on the
  normal port ([`src/telemetry.rs`](src/telemetry.rs)): connected clients, memory used/limit,
  expired/evicted keys, tiering hits and misses, and the INFO stats counters (commands processed,
  connections, network bytes, keyspace hits/misses).
- There is no built-in log file rotation or structured JSON logging; redirect and rotate at the
  process-manager level (systemd journal, your container runtime's log driver, or `logrotate`).

### Persistence & Durability Posture

Understand what "durable" means here today before relying on it:

- **AOF** (`appendonly yes`): supports `appendfsync always` (waits for the background AOF writer
  to `fdatasync` the batch before replying to the client), `appendfsync everysec` (default,
  fsyncs roughly every second), and `appendfsync no` (leaves flushing to the OS). Both
  `appendonly` and `appendfsync` can be changed at runtime via `CONFIG SET`. Automatic background
  AOF compaction is driven by `auto-aof-rewrite-percentage` and `auto-aof-rewrite-min-size`.
- **RDB autosave**: `save <seconds> <changes>` points schedule `BGSAVE` like Redis, with
  Redis's defaults (`save 3600 1 300 100 60 10000`) unless the config file or command line sets
  `save`. Save points also make a graceful shutdown save first. `save ""` turns autosave off
  (then nothing is saved on shutdown unless `SHUTDOWN SAVE` is used).
- **NVMe-tiered keys** (`RudisValue::Tiered`) are read back from the tier and written with their
  real value and type by `SAVE`/`BGSAVE` and `BGREWRITEAOF`. After a restart they come back as
  ordinary in-memory keys (tiering state itself is not persisted). If a tiered record cannot be
  read (I/O error or CRC mismatch), the save fails cleanly and keeps any prior `dump.rdb` file
  intact rather than silently dropping records.
- `SAVE`/`BGSAVE` never `fork()`. Each shard serializes its keyspace in steps of at most ~1ms and
  yields between them, and CRC, file writes and fsync run on a separate `rdb-writer` thread, so a
  `BGSAVE` adds latency in ~1ms slices rather than stalling a shard for its whole dataset. The
  snapshot is consistent per key, not a single point in time (see
  [`docs/rdbsave.md`](docs/rdbsave.md)).

### Memory Sizing & Eviction

- `maxmemory-policy` is validated at startup and in `CONFIG SET`, accepting `noeviction`
  (default — new writes get `-OOM` once `maxmemory` is exceeded), `allkeys-lru`, `volatile-lru`,
  `allkeys-lfu`, `volatile-lfu`, `volatile-ttl`, `allkeys-random`, and `volatile-random`.
- Every `RudisValue` carries a 16-bit LRU clock and an 8-bit logarithmic LFU Morris counter with
  time-based decay (`RudisTable::try_evict_one_key` in [`src/table.rs`](src/table.rs)), sampling
  occupied entries (and falling back to non-string stores such as `json_store`,
  `probabilistic_store`, `vector_sets`, and `agent_ctx` when needed). Evicted keys propagate
  synthetic `DEL` commands to the AOF and attached replicas so replicas never retain keys the
  master evicted. Eviction runs only before write commands, never before reads.
- `maxmemory` also drives the thresholds that trigger NVMe tiering offload — see
  [NVMe Tiered Storage](#6-nvme-tiered-storage-smallbins--direct-io) below.

### Limits on Client-Supplied Sizes

Sizes taken from command arguments are checked before they drive an allocation or loop, so
one command cannot hang a shard or abort the process. Where Redis or its modules define a
limit, Rudis uses the same one, with the Redis error text where one exists.

| Argument | Limit | Reply when exceeded |
|----------|-------|---------------------|
| `FT.CREATE` / `FT.ALTER` vector `DIM` | 65536 (Redis vector sets' maximum) | `Bad arguments for vector similarity <ALGO> argument DIM` |
| `FT.CREATE` / `FT.ALTER` HNSW `M`, `VADD ... M` | 4096 (Redis `HNSW_MAX_M`) | `... argument M` / `invalid M` |
| `VADD` vector dimension; `REDUCE` larger than it | 65536 | `invalid vector specification` |
| `VADD ... REDUCE` projection (input dim x reduced dim) | 2^24 floats (64 MiB) | `REDUCE projection too large ...` |
| `VADD` / `VSIM ... EF` | 1000000, as in Redis | `invalid EF` |
| `BF.RESERVE`, `CF.RESERVE`, `CMS.INITBYDIM` | 512 MiB per structure (Redis' value size limit) | `Bloom filter too large ...` / `Cuckoo filter too large ...` / `CMS: width x depth too large ...` |
| `BF.RESERVE` error rate | strictly between 0 and 1 (NaN refused), as in RedisBloom | `(0 < error rate range < 1)` |
| Negative (repeating) `SRANDMEMBER` / `HRANDFIELD` / `ZRANDMEMBER` / `VRANDMEMBER` count | -1048576; Redis streams such replies, Rudis builds them in memory | `value is out of range` |
| `JSON.SET` array index past the end | pads at most 65536 `null`s; negative indexes must be in the array | `index out of bounds` |
| `CRDT.INCRBY` | the counter and each node's component stay within i64 | `increment or decrement would overflow` |

KNN `k`, `VSIM COUNT`, `EF_RUNTIME` and `LIMIT` are not refused: results are capped at what the
index holds, and `FLAT` `INITIAL_CAP` preallocates at most 64 MiB of vectors.

### Graceful Shutdown

`SIGTERM`/`SIGINT` and `SHUTDOWN` run the same sequence ([`src/shutdown.rs`](src/shutdown.rs),
`run_shard_worker` in [`src/server.rs`](src/server.rs)):

1. **Save**: if save points are configured (or `SHUTDOWN SAVE` is given), a snapshot is taken
   first. If it fails the server keeps running and reports the error (`SHUTDOWN NOSAVE` skips it).
2. **Stop accepting**: every shard stops accepting new connections within ~200ms.
3. **Drain clients** (up to 2s): the read side of every client socket is shut. Idle clients see
   EOF at once; a command already running (including a blocked `BLPOP`/`XREAD` until its own
   timeout) finishes and gets its reply. Clients still busy at the deadline are dropped.
4. **Shard barrier** (up to 3s): each shard keeps serving cross-shard requests until every shard
   has drained, so no shard exits while another shard's client is waiting on it.
5. **Final AOF flush + fsync**, then the process exits with status 0.

With AOF enabled, every write acknowledged before exit is in the AOF. `SIGKILL` (a `docker stop`/systemd
stop past its grace period) skips all of the above, so give the process at least ~6s:
`docker stop -t 10`, systemd `TimeoutStopSec=10` or more.

### Security Checklist

- Set `requirepass` in the config file (there is no CLI flag for it — see
  [Configuration](#configuration)) or configure ACL users; without it, `AUTH` is open to any
  client that can reach the port.
- Set `tls-port`/`tls-cert-file`/`tls-key-file` for traffic leaving a trusted network. Without an
  explicit cert/key pair, Rudis generates a self-signed certificate **in memory** at startup — do
  not rely on that beyond local testing, since clients have nothing to verify it against.
- TLS connections run through userspace `rustls` only; there is no kernel TLS offload (see
  [Kernel-Bypass Networking & TLS: Status](#7-kernel-bypass-networking--tls-status)). Mutual TLS
  (mTLS) client certificate verification is supported via `tls-ca-cert-file` and
  `tls-auth-clients` (`yes` | `no` | `optional`), hot-reloadable via `CONFIG SET`. Outbound
  replication (`tls-replication yes`) and cluster gossip (`tls-cluster yes`) also support TLS.
  TLS client connections run the same client loop as plaintext ones, so pipelining (including
  cross-shard squashing), transactions, Pub/Sub, blocking commands, MONITOR, client tracking,
  `CLIENT KILL`/`LIST`/`REPLY` and output-buffer limits behave identically.
- Run as a dedicated non-root user; the included [`Dockerfile`](Dockerfile) already does this via
  a `rudis` system user.
- Bind to a private interface (`bind <ip>`) rather than `0.0.0.0` unless the port is otherwise
  firewalled — command access control is governed by the Redis ACL model, not a network ACL.
- ACL users follow the Redis 7 model: command categories come from the Valkey command table
  (`+@read`, `-@dangerous`, `+config|get`), keys support `~pat` / `%R~pat` / `%W~pat` globs, and
  channels `&pat`. New users start with no channel access (`resetchannels`, the Redis 7 default),
  and commands run from Lua scripts are checked against the calling user. Selectors and
  `acl-pubsub-default` are not supported; unsupported or unknown rules are rejected rather than
  ignored. After updating the command table, regenerate `src/acl_categories.rs` with
  `scripts/gen_acl_categories.py ~/valkey > src/acl_categories.rs`.

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
- The save path is not `io_uring`-based: shards serialize their keyspace segment by segment in
  steps of at most ~1ms, yielding to their other clients in between, and stream ~4MB pieces to
  an `rdb-writer` thread that does the CRC, buffered `std::fs` writes, fsync and rename. A
  `BGSAVE` therefore adds small latency slices on every shard instead of stalling one shard for
  its whole dataset.
- Reflink (`ioctl(FICLONE)`) snapshotting is real, working code — but it belongs to the NVMe
  tiered-storage subsystem (`TIER.SNAPSHOT`), not to `SAVE`/`BGSAVE`. The two are unrelated
  features that happen to both be called "snapshotting."
- Full detail, including the save-trigger table, is in [`docs/rdbsave.md`](docs/rdbsave.md).

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

### 4. RediSearch: `RangeTree` Indexing, Hybrid Vector Search & `FT.AGGREGATE`

- Documents are mapped to dense integer document IDs to shrink posting-list memory relative to
  storing string keys directly.
- Numeric fields are indexed with a balanced `RangeTree` for `@field:[min max]` range queries, and
  `VECTOR` fields support both `FLAT` (exact) and `HNSW` (approximate) indexing over Hash and JSON
  documents (including wildcard multi-vector chunk arrays such as `$.chunks[*].emb`).
- Supports pre-filtered KNN (`@tag:{...}=>[KNN k @vec $blob]`), radius search (`@vec:[VECTOR_RANGE r $blob]`),
  dedicated hybrid fusion (`FT.HYBRID` with `RRF` and `LINEAR` combinations), schema evolution (`FT.ALTER`),
  and query execution profiling (`FT.PROFILE`).
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
  [`docs/design/tiered_storage.md`](docs/design/tiered_storage.md). Tiered keys are read back
  and saved with their real value and type by `SAVE`/`BGSAVE` and `BGREWRITEAOF`.

### 7. Kernel-Bypass Networking & TLS: Status

Two subsystems exist as unconditionally-compiled, unit-tested code but are **not on the live
network path today**:
- **`src/xdp.rs`** implements AF_XDP (UMEM rings, an eBPF-style packet classifier) entirely as
  in-process Rust data structures, reachable only via explicit `XDP.*` admin commands. No real
  AF_XDP socket, eBPF program, or NIC binding is created by any code path.
- **`src/zerocopy.rs`** implements correct `SO_ZEROCOPY`/registered-buffer primitives, but
  nothing outside its own unit tests calls them — the real connection write path
  (`src/connection.rs`, on `monoio`) does not use this module.
- **Kernel TLS (kTLS)**: not implemented. `rustls` performs the handshake and all
  encryption in userspace. Offload would need the session keys installed with
  `setsockopt(SOL_TLS, TLS_TX/TLS_RX)`, TLS 1.3 control records handled out of band on
  receive, and the `tls` kernel module; since Rudis does not use `sendfile`, it would mostly
  move the same AES-GCM work into the kernel. An earlier best-effort `TCP_ULP` attach that
  never installed keys (and so never activated) was removed rather than kept as scaffolding.

Treat these as real, tested building blocks for a future kernel-bypass data path, not as
currently active acceleration. See
[`docs/design/10_kernel_bypass_xdp.md`](docs/design/10_kernel_bypass_xdp.md) and
[`docs/design/15_security_tls.md`](docs/design/15_security_tls.md) for the full picture,
including *why* this direction is still worth pursuing despite not being wired up yet.

### 8. Dual-Protocol Engine: Redis + Memcached

- A single listening port accepts both RESP (Redis) and Memcached text commands.
- Protocol framing is auto-detected from the first bytes read on a new connection; both
  protocols share database 0.

### 9. Agent Memory, LLM Quota, Checkpoints & MCP Server

- **Hierarchical Agent Memory (`AGENT.MEM.*`, `src/agent.rs`)**: token-budgeted FIFO working
  memory window paired with automatic HNSW episodic spill and hybrid recall (`AGENT.MEM.WINDOW`,
  `AGENT.MEM.SEARCH`, `AGENT.MEM.COMPACT`). Gets RDB and AOF/replication coverage.
- **Dual RPM/TPM LLM Quota Governor (`LLM.QUOTA.*`, `src/agent.rs`)**: sliding-window
  Requests-Per-Minute and Tokens-Per-Minute rate limiting with pre-flight token reservation
  (`LLM.QUOTA.RESERVE`) and post-stream actual token settlement (`LLM.QUOTA.SETTLE`). Persisted in
  RDB snapshots (`EXT_TYPE_LLM_QUOTA`) and logged to AOF / replicated.
- **DAG State Checkpointing & Idempotent Tool Leases (`AGENT.CHECKPOINT.*`, `AGENT.TOOL.*`,
  `src/agent.rs`)**: parent-linked execution state lineage (`PUT`/`GET`/`HISTORY`) and
  TTL-guarded exactly-once tool call execution leases (`CLAIM`/`COMPLETE`). Checkpoint timestamps
  are computed server-side at execution time, so they can diverge between a primary and a
  replica/AOF-replay of the same logical checkpoint.
- **Built-in Model Context Protocol Server (`MCP.*`, `src/mcp.rs`)**: native JSON-RPC 2.0 MCP
  server (`MCP.TOOLS`, `MCP.CALL`, `MCP.RPC`) exposing 11 tools that translate into ordinary
  Rudis commands via the normal `execute_command` dispatch path (same ACL/routing as any client
  command).
- **Semantic Cache (`SEMANTIC.*`, implemented in `src/vector.rs`, not a separate file)**:
  cosine-similarity LLM prompt cache (`SEMANTIC.SET`/`GET`/`DEL`/`FLUSH`/`INFO`).

Details: [Component 20 — Agent Memory, LLM Quota & Checkpoints](docs/design/20_agent_memory.md),
[Component 21 — MCP Server](docs/design/21_mcp_server.md), and
[Component 08 — Vector Search Engine](docs/design/08_vector_engine.md) for `SemanticCache`.

---

## Subsystem & Feature Matrix

Status reflects source-verified behavior as of this revision, not aspiration. "Design doc"
links go to the corresponding source-verified subsystem specification.

> [!IMPORTANT]
> The non-Redis command families `JSON.*`, `BF.*`, `CF.*`, `CMS.*`, `TOPK.*`, `FT.*`,
> `SEMANTIC.*`, `CRDT.*`, `LLM.*`, `MCP.*` and `XDP.*` are **off by default** and answer
> `ERR unknown command`. Enable them with `enable-experimental-commands yes` (config file) or
> `--enable-experimental-commands yes`. All mutating commands across `JSON.*`, `FT.*`,
> `SEMANTIC.*`, `BF.*`, `CF.*`, `CMS.*`, `TOPK.*`, `CRDT.*`, `AGENT.*`, and `LLM.QUOTA.*` are
> persisted to RDB and AOF and propagated to replicas.

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
| **RDB persistence** | Implemented, with gaps | No `fork()`; shards serialize in ~1ms yielding steps, I/O on an `rdb-writer` thread; persists KV (including NVMe-tiered keys), Vector Sets, and extended `RDBX` search/vector indexes; the snapshot is not a single point in time. | [rdbsave.md](docs/rdbsave.md) |
| **AOF persistence** | Implemented | Async (`monoio`) periodic flush, distinct I/O strategy from RDB save. | [14](docs/design/14_persistence_replication.md) |
| **Replication (`PSYNC`)** | Implemented | Single-connection, used for all Rudis-to-Rudis replication. | [replication.md](docs/replication.md) |
| **Replication (`DFLY FLOW`)** | Implemented, narrow scope | Master-side only, for Dragonfly-protocol clients; Rudis replicas never use it. | [replication.md](docs/replication.md) |
| **Redis Cluster & gossip** | Implemented | Plain-text gossip bus on `port + 10000` (not `io_uring`), quorum failover, real `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD` key migration. | [11](docs/design/11_cluster_topology.md) |
| **Dragonfly compatibility (state only)** | Partial | `DFLYCLUSTER`/`DFLYMIGRATE` remain bookkeeping-only, distinct from the more complete native cluster migration path above. | [11](docs/design/11_cluster_topology.md) |
| **RedisJSON document store** | Implemented | JSONPath parsing, array slicing, in-place mutation. | [16](docs/design/16_json_store.md) |
| **RediSearch (`FT.*`)** | Implemented | Inverted index, BM25, `RangeTree`, `FLAT` & `HNSW` vector fields, pre-filtered KNN, `VECTOR_RANGE`, `FT.HYBRID` (RRF/Linear), `FT.ALTER`, `FT.PROFILE`, multi-vector JSON chunks (`$.chunks[*].emb`). | [09](docs/design/09_redisearch.md) |
| **RedisBloom-style probabilistic types** | Implemented | Bloom, Cuckoo, Count-Min Sketch, Top-K. | [18](docs/design/18_probabilistic.md) |
| **HNSW & Redis 8 Vector Sets (`V*`)** | Implemented | Redis 8 Vector Sets (`VADD`/`VSIM`/`VEMB`/`VLINKS`/`VSETATTR`), Cosine/L2/IP SIMD kernels, SQ8/BIN/PQ compression, `REDUCE` projection, NVMe `.vtier` disk reranking (`TIERED` + `RERANK`). | [08](docs/design/08_vector_engine.md) |
| **Agent memory, LLM quota & checkpoints** | Implemented | Working + HNSW episodic memory (`AGENT.MEM.*`), DAG checkpoints (`AGENT.CHECKPOINT.*`), tool leases (`AGENT.TOOL.*`), and the RPM/TPM quota governor (`LLM.QUOTA.*`) all get RDB + AOF/replication coverage. | [20](docs/design/20_agent_memory.md) |
| **MCP server & semantic cache** | Implemented | Built-in MCP JSON-RPC 2.0 server (`MCP.*`, 11 tools) translating into ordinary commands via the normal dispatch path; semantic cache (`SEMANTIC.*`) lives in `src/vector.rs`. | [21](docs/design/21_mcp_server.md), [08](docs/design/08_vector_engine.md) |
| **NVMe tiered storage** | Implemented | Hot/Cooled/Cold lifecycle, `SmallBins`, Direct I/O; tiered keys are written with their real value and type by `SAVE`/`BGSAVE` and `BGREWRITEAOF`. | [07](docs/design/07_nvme_tiering.md), [tiered_storage.md](docs/design/tiered_storage.md) |
| **`TIER.SNAPSHOT` reflink checkpoints** | Implemented | Real `ioctl(FICLONE)` cloning of the NVMe tier's backing file — unrelated to `SAVE`/`BGSAVE`. | [tiered_storage.md](docs/design/tiered_storage.md) |
| **Multi-region CRDTs** | Implemented, manual sync only | LWW-Register, OR-Set, PN-Counter with HLC ordering; export/merge (`CRDT.DUMP`/`CRDT.MERGE`) is explicit and manual — there is no automatic peer discovery or background cross-region streaming. | [12](docs/design/12_crdt_types.md) |
| **AF_XDP kernel bypass** | Experimental, not on live path | Real, unit-tested data structures; reachable only via `XDP.*` admin commands, not real NIC/eBPF I/O. | [10](docs/design/10_kernel_bypass_xdp.md) |
| **`SO_ZEROCOPY` send path** | Experimental, not on live path | Correct primitives in `src/zerocopy.rs`; unused by the real connection write path. | [10](docs/design/10_kernel_bypass_xdp.md) |
| **Kernel TLS (kTLS)** | Not implemented | `rustls` handles TLS in userspace; there is no kernel offload. | [15](docs/design/15_security_tls.md) |
| **Jemalloc memory telemetry** | Implemented | Global allocator is `tikv-jemallocator`; live stats via `tikv-jemalloc-ctl` in `INFO memory`. | [15](docs/design/15_security_tls.md) |

---

## Redis/Valkey TCL Compatibility Suite

Rudis is continuously validated against **49 official Redis/Valkey TCL test suites** in `tests/redis-tests` via [`scripts/run_redis_test_suite.sh`](scripts/run_redis_test_suite.sh):

```bash
# Run all 49 core TCL suites
./scripts/run_redis_test_suite.sh 16379 all

# Run a specific suite or comma-separated list of suites
./scripts/run_redis_test_suite.sh 16379 "unit/tracking,unit/acl-v2,unit/acl"
```

| Category | Passing TCL Suites (49) |
| :--- | :--- |
| **Core Data Types** | `unit/type/string`, `unit/type/incr`, `unit/type/increx`, `unit/type/hash`, `unit/type/hash-field-expire`, `unit/type/list`, `unit/type/list-2`, `unit/type/list-3`, `unit/type/list-4`, `unit/type/set`, `unit/type/zset`, `unit/type/stream`, `unit/type/stream-cgroups`, `unit/bitops`, `unit/bitfield`, `unit/hyperloglog`, `unit/geo`, `unit/sort` |
| **Keyspace & Lifecycle** | `unit/expire`, `unit/keyspace`, `unit/scan`, `unit/dump`, `unit/lazyfree`, `unit/maxmemory`, `unit/gcra` |
| **Transactions, Scripting & Pub/Sub** | `unit/multi`, `unit/scripting`, `unit/functions`, `unit/pubsub`, `unit/pubsubshard`, `unit/tracking` |
| **Auth, ACLs & Protocol** | `unit/auth`, `unit/acl`, `unit/acl-v2`, `unit/protocol`, `unit/quit`, `unit/limits`, `unit/obuf-limits`, `unit/replybufsize`, `unit/querybuf` |
| **Introspection & Telemetry** | `unit/info`, `unit/info-command`, `unit/introspection`, `unit/introspection-2`, `unit/slowlog`, `unit/latency-monitor`, `unit/pause`, `unit/other`, `unit/printver` |

---

## Documentation & Contributor Guides

For deep technical walkthroughs, internal architecture specifications, and benchmarks:
* [**Documentation Hub**](docs/README.md): index of all architecture and subsystem docs.
* [**Architecture & Threading Model**](docs/architecture.md): thread-per-core engine, `io_uring`/Monoio runtime, request lifecycle.
* [**Replication**](docs/replication.md): `PSYNC` and Dragonfly-compatible `DFLY FLOW`.
* [**Pub/Sub Architecture**](docs/pub-sub.md): striped presence bitmask and Redis 7 sharded pub/sub.
* [**RDB Snapshotting**](docs/rdbsave.md): what actually triggers a save, and how it interacts with the shard model.
* [**Differences from Redis & Dragonfly**](docs/differences.md): architectural and behavioral comparison.
* [**Subsystem Design Specifications**](docs/design/README.md): design rationale ("why") across all 20 subsystems.
* [**Subsystem Implementation References**](docs/internal/README.md): concrete data structures, algorithms, and source references.
* [**Comprehensive Performance Guide**](docs/benchmarks/comprehensive_performance_guide.md) and [**Multicore Benchmark Report**](docs/benchmark_multicore_results.md): reproducible benchmark data and methodology.
* [**Engineering Invariants for Contributors**](agent.md): mandatory architectural rules for anyone changing this codebase.

---

## License

Released under the [MIT License](LICENSE) (`license = "MIT"` in `Cargo.toml`).
