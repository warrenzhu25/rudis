# Rudis

<p align="center">
  <b>A thread-per-core, Redis-compatible in-memory database in Rust, built on Linux <code>io_uring</code>.</b>
</p>

<p align="center">
  <a href="https://github.com/warrenzhu25/rudis/actions/workflows/ci.yml"><img src="https://github.com/warrenzhu25/rudis/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License"></a>
  <a href="rust-toolchain.toml"><img src="https://img.shields.io/badge/rust-1.99-orange.svg" alt="Rust 1.99"></a>
</p>

Rudis speaks the Redis protocol (RESP2 and RESP3) and Redis Cluster, so existing Redis clients,
`redis-cli` and cluster tooling work unchanged. Each core owns a shard of the keyspace and runs
its own `io_uring` event loop: no global lock and no shared hash table on the request path.

> [!NOTE]
> Rudis is young (0.x) and maintained by one person. The core Redis feature set below is tested
> hard (see [How it's tested](#how-its-tested)), but it is not yet a drop-in production
> replacement for Redis. [What's implemented, partial or experimental](#subsystem--feature-matrix)
> is listed explicitly.

## Highlights

- **Thread-per-core, shared-nothing.** Keys are partitioned across cores; a request for a key on
  another core crosses a lock-free per-core-pair ring. Connections are spread by the kernel
  (`SO_REUSEPORT`).
- **Redis where it matters.** Strings, hashes, lists, sets, sorted sets, streams and consumer
  groups, bitmaps, HyperLogLog, geo; `MULTI`/`EXEC`, Lua scripting and functions, pub/sub
  (including sharded), ACLs, client-side caching, keyspace notifications, `maxmemory` eviction
  and client eviction.
- **Persistence and HA.** RDB snapshots without `fork()`, AOF, `PSYNC` replication, and Redis
  Cluster with gossip, automatic failover and live resharding.
- **Memcached text protocol** on the same port, sharing the keyspace.
- **Optional extras**, off by default: JSON, search and vector search, probabilistic types and
  NVMe tiering.

## Quick start

Linux only (`io_uring`). The Rust toolchain version is pinned in
`rust-toolchain.toml`, and `rustup` installs it on first use.

```bash
git clone https://github.com/warrenzhu25/rudis.git && cd rudis
cargo build --release
./target/release/rudis --port 6379          # one shard per core, up to 8 by default

redis-cli -p 6379 SET hello world           # any Redis client works
redis-cli -p 6379 GET hello
```

A cluster, a replica, persistence and the other options are under [Configuration](#configuration)
and [Production Deployment](#production-deployment).

Where `io_uring` is unavailable (an older kernel, or a seccomp profile that blocks it, such as
Docker's default), Rudis runs on epoll instead and says so at startup. To get `io_uring` in a
container, allow its system calls (`io_uring_setup`, `io_uring_enter`, `io_uring_register`) in the
container's seccomp profile.

## How it's tested

- **Redis's own test suite.** 86 of the Redis/Valkey TCL test suites (data types, scripting,
  ACLs, replication, persistence, cluster, client eviction, ...) gate every merge, run by
  [`scripts/ci_local.sh`](scripts/ci_local.sh) on a dedicated host. GitHub Actions runs the core
  suites on every push and the timing-heavy rest nightly, since a shared 4-vCPU runner makes
  timing assertions flaky. The few tests that don't apply to Rudis are skipped, each
  [with its reason](scripts/tcl_skips/).
- **Chaos soak on every change to `main`.** A 6-node cluster takes steady load for 6 h segments
  while failovers, live resharding, `kill -9` restarts and snapshots happen under it. Each
  segment checks that no acknowledged write was lost and that every replica matches its master
  ([`scripts/soak_cluster.py`](scripts/soak_cluster.py)).
- **A performance gate before every merge.** An A/B run of `memtier_benchmark` against the
  current `main` ([`scripts/check_perf_regression.py`](scripts/check_perf_regression.py)) has to
  pass before a change to the server lands; GitHub also runs a benchmark nightly.
- **Fuzzing** of every untrusted input: the RESP parser, `RESTORE` payloads, RDB and AOF files,
  and the cluster bus.

---

## Contents

- [Architecture Overview](#architecture-overview)
- [Benchmarks](#benchmarks)
- [Configuration](#configuration)
- [Production Deployment](#production-deployment)
- [Design Decisions](#design-decisions)
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
         │ • Single-stream PSYNC (rudis)│      │ • No fork(): CoW shard images│
         │ • DFLY FLOW for DF clients   │      │ • ~1 ms steps, writer thread │
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

Measured with [`scripts/benchmark_multicore_suite.py`](scripts/benchmark_multicore_suite.py)
against the latest releases, **Dragonfly v2.1** (official binary) and **Valkey 9.2.0** (built from
the release tag, `--io-threads 16`). Each server got 16 physical cores (`0-15`, SMT siblings left
idle) of an AMD EPYC 7B13 VM; `memtier_benchmark` ran on the other 16 physical cores with 64
connections, 1 KB values, 3 interleaved rounds of 5 s, medians reported. Full results, 8-core
numbers and 1-to-16-core scaling: [`docs/benchmark_multicore_results.md`](docs/benchmark_multicore_results.md).

| Workload (16 cores) | Rudis | Dragonfly v2.1 | Valkey 9.2.0 | Rudis vs. Dragonfly |
| :--- | ---: | ---: | ---: | ---: |
| SET, pipeline 16 | **2,614,721 ops/s** | 2,380,912 ops/s | 563,486 ops/s | +9.8% |
| GET, pipeline 16 | **2,394,607 ops/s** | 1,626,119 ops/s | 1,315,410 ops/s | +47.3% |
| SET, no pipeline | **543,710 ops/s** | 462,336 ops/s | 323,410 ops/s | +17.6% |
| GET, no pipeline | **597,478 ops/s** | 471,751 ops/s | 507,509 ops/s | +26.7% |
| MGET 10 random keys, pipeline 8 | **1,021,404 ops/s** | 423,544 ops/s | 352,942 ops/s | +141.2% |
| DEL 10 random keys, pipeline 8 | **851,234 ops/s** | 403,231 ops/s | 651,653 ops/s | +111.1% |

How to read these:

- **Unpipelined numbers are mostly latency.** With 64 connections and one request in flight
  each, throughput is 64 divided by the round-trip time, for every engine.
- **Valkey executes every command on one main thread.** Its I/O threads parse and reply but
  don't run commands, which caps write-heavy workloads such as pipelined `SET`.
- **At 8 cores the picture is closer.** Rudis leads Dragonfly by +1% to +121%, and Valkey beats
  Rudis on unpipelined `SET` and on 10-key `DEL` (see the full report).
- **Dragonfly v1.39** answered pipelined `GET`s of values over ~64 bytes about 5x slower than
  v2.1. Earlier versions of this README compared against v1.39 and overstated Rudis's lead.

To reproduce (Linux, 32+ physical cores; set the binary paths for your machine):

```bash
cargo build --release
DRAGONFLY_BIN=/path/to/dragonfly VALKEY_BIN=/path/to/valkey-server \
MEMTIER_BIN=/path/to/memtier_benchmark python3 scripts/benchmark_multicore_suite.py
```

An older 16-command comparison (`INCR`, `HSET`, `LPUSH`, `ZADD`, ...) is in
[`docs/benchmarks/multi_command_comparison.md`](docs/benchmarks/multi_command_comparison.md); it
was measured against Dragonfly v1.39 and predates this rerun.

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
| `appendonly <yes\|no>` | Enable AOF persistence (also at runtime with `CONFIG SET appendonly`, see below) | `no` |
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

The repository ships [`rudis.service`](rudis.service):

```ini
[Unit]
Description=Rudis High-Performance In-Memory Data Store (io_uring)
After=network.target network-online.target
Wants=network-online.target
Documentation=https://github.com/warrenzhu25/rudis

[Service]
Type=notify
NotifyAccess=all
User=rudis
Group=rudis
ExecStart=/usr/local/bin/rudis -c /etc/rudis/rudis.conf
Restart=always
RestartSec=3s
# READY=1 comes once the dataset is loaded, which can take minutes on a
# large RDB / AOF: without this, systemd's default 90 s start timeout
# kills the load and Restart=always loops on it (as Redis's unit does).
TimeoutStartSec=infinity
TimeoutStopSec=300s
KillMode=mixed
KillSignal=SIGTERM

# Hardening & Production Limits
LimitNOFILE=65536
LimitNPROC=65536
LimitMEMLOCK=infinity
TasksMax=infinity

# Security Sandboxing
ProtectSystem=full
ProtectHome=true
NoNewPrivileges=true
PrivateTmp=true

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

A multi-stage [`Dockerfile`](Dockerfile) is included: a `rust:1.99-bookworm` build stage (the
toolchain pinned in `rust-toolchain.toml`) and a
`debian:bookworm-slim` runtime stage that runs as a non-root `rudis` system user, with
`/var/lib/rudis` declared as a volume.

```bash
docker build -t rudis:latest .
docker run -d --name rudis \
  -p 6379:6379 \
  -v rudis-data:/var/lib/rudis \
  rudis:latest
```

The image's `CMD` defaults to `--port 6379 --aof true --aof-dir /var/lib/rudis --protected-mode no`
(protected mode would refuse every client arriving over the container network; set `requirepass`
before publishing the port beyond a trusted network). Note that
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
  connections, network bytes, keyspace hits/misses). The HTTP endpoint also has per-command
  calls/latency histograms, persistence and replication state, keys and used memory per shard
  (`rudis_shard_keys`, `rudis_shard_used_memory_bytes`: a hot or oversized shard limits the
  whole node), the process RSS and cross-shard mailbox overflows.
- There is no built-in log file rotation or structured JSON logging; redirect and rotate at the
  process-manager level (systemd journal, your container runtime's log driver, or `logrotate`).

### Persistence & Durability Posture

Understand what "durable" means here today before relying on it:

- **AOF** (`appendonly yes`): supports `appendfsync always` (waits for the background AOF writer
  to `fdatasync` the batch before replying to the client), `appendfsync everysec` (default,
  fsyncs roughly every second), and `appendfsync no` (leaves flushing to the OS). `appendfsync`
  can be changed at runtime via `CONFIG SET`. `CONFIG SET appendonly yes` turns AOF on for a
  running server as in Redis: writes are logged at once and a background rewrite writes the
  initial AOF (`aof_rewrite_in_progress` in `INFO persistence`; `SHUTDOWN` refuses to exit until
  it is done, unless `FORCE`). `CONFIG REWRITE` then keeps it on across restarts. This is the safe
  way to move a dataset to AOF: restarting with `appendonly yes` and no AOF yet starts empty, as
  in Redis. Automatic background
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
  snapshot is still one point in time across shards, like Redis's forked child: each shard saves
  from a copy-on-write image started at the same moment (see [`docs/rdbsave.md`](docs/rdbsave.md)).

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
  [NVMe Tiered Storage](docs/experimental.md#nvme-tiered-storage-smallbins--direct-io).

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

1. **Wait for replicas** (masters with replicas, as Redis): writes are paused and the master
   waits up to `shutdown-timeout` (default 10s; 0 skips it) for every replica to acknowledge
   everything it was sent, then logs `N of M replicas are in sync` and any `Lagging replica`.
   `INFO server` shows `shutdown_in_milliseconds` meanwhile, and clients waiting in `SHUTDOWN`
   count as `blocked_clients`. `SHUTDOWN ABORT` cancels the shutdown (waiting clients get an
   error, a `SIGTERM`'d server stays up); `SHUTDOWN NOW` skips the wait.
1. **Save**: if save points are configured (or `SHUTDOWN SAVE` is given), a snapshot is taken
   first. If it fails the server keeps running and reports the error (`SHUTDOWN NOSAVE` skips it;
   `SHUTDOWN FORCE` exits anyway).
2. **Stop accepting**: every shard stops accepting new connections within ~200ms.
3. **Drain clients** (up to 2s): the read side of every client socket is shut. Idle clients see
   EOF at once; a command already running (including a blocked `BLPOP`/`XREAD` until its own
   timeout) finishes and gets its reply. Clients still busy at the deadline are dropped.
4. **Shard barrier** (up to 3s): each shard keeps serving cross-shard requests until every shard
   has drained, so no shard exits while another shard's client is waiting on it.
5. **Final AOF flush + fsync**, then the process exits with status 0.

With AOF enabled, every write acknowledged before exit is in the AOF. `SIGKILL` (a `docker stop`/systemd
stop past its grace period) skips all of the above, so give the process at least ~6s, plus
`shutdown-timeout` on a master with replicas: `docker stop -t 20`, systemd `TimeoutStopSec=20`
or more.

### Security Checklist

- Set `requirepass` in the config file (there is no CLI flag for it — see
  [Configuration](#configuration)) or configure ACL users; without it, `AUTH` is open to any
  client that can reach the port.
- Set `tls-port`/`tls-cert-file`/`tls-key-file` for traffic leaving a trusted network. Without an
  explicit cert/key pair, Rudis generates a self-signed certificate **in memory** at startup — do
  not rely on that beyond local testing, since clients have nothing to verify it against.
- TLS connections run through userspace `rustls` only; there is no kernel TLS offload (see
  [Kernel-Bypass Networking & TLS: Status](docs/experimental.md#kernel-bypass-networking--tls-status)). Mutual TLS
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

### Operations Runbook

Alerts to set, crash recovery, backups and restore, runaway scripts, failover, resharding and
upgrades: see [`docs/operations.md`](docs/operations.md).

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
- `SAVE`/`BGSAVE` read tiered (NVMe) values back and write them into the RDB, so an RDB is a
  complete backup of a tiered dataset. (The former `TIER SNAPSHOT`, a reflink copy of the raw
  tier files that nothing could restore, was removed.)
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

### 4. Replication: `PSYNC` and Dragonfly-Compatible `DFLY FLOW`

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

### 5. Dual-Protocol Engine: Redis + Memcached

- A single listening port accepts both RESP (Redis) and Memcached text commands.
- Protocol framing is auto-detected from the first bytes read on a new connection; both
  protocols share database 0.

### 6. Experimental Extensions (Off by Default)

Beyond Redis, Rudis has JSON documents, search and vector search (`FT.*`), probabilistic
types, NVMe tiering, CRDTs and agent-memory commands. They are disabled unless
`enable-experimental-commands yes` is set and get less testing than the core; what each does,
its status and its limits are in [docs/experimental.md](docs/experimental.md). AF_XDP and
`SO_ZEROCOPY` code exists but is not on the live network path, and kernel TLS is not
implemented.

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
> persisted to RDB and AOF and propagated to replicas. `MCP.*` and `XDP.*` are also cargo
> features (default on): a `--no-default-features` build doesn't contain them. The other
> families stay compiled in, since their data is in RDB and AOF files.

| Subsystem | Status | Notes | Design Doc |
| :--- | :---: | :--- | :--- |
| **Thread-per-core engine** | Implemented | Shared-nothing shards on Monoio/`io_uring`, lock-free cross-shard mesh. | [01](docs/design/01_reactor_runtime.md) / [04](docs/design/04_sharding_mesh.md) |
| **Core Redis data structures** | Implemented | Strings, hashes, lists, sets, sorted sets, bitmaps, HyperLogLog. | [05](docs/design/05_storage_engine.md) |
| **Geospatial commands** | Implemented | Integer geohash encoding, Haversine distance, `GEOSEARCH`. | [17](docs/design/17_geospatial.md) |
| **Streams & consumer groups** | Implemented | Append-only log, consumer groups, PEL, blocking/non-blocking `XREAD`. | — |
| **Transactions (`MULTI`/`EXEC`)** | Implemented | Deterministic cross-shard execution ordering to avoid deadlock. | [rudis_internals_guide §5](docs/rudis_internals_guide.md) |
| **Multi-key commands across shards** | Implemented, not all atomic | `MGET`/`MSET`/`EXEC` are isolated from each other and moves/`MSETNX`/`BITOP` take transaction locks, but `DEL`/`UNLINK`/`EXISTS`/`TOUCH` and set/sorted-set/HLL algebra run per shard without isolation. Use hash tags for atomic groups, or `cross-shard-strict yes` to refuse with `CROSSSLOT`. | [differences.md](docs/differences.md#multi-key-atomicity-across-shards) |
| **Pub/Sub incl. Sharded Pub/Sub** | Implemented | 16-stripe presence bitmask; CRC16 slot routing for `SPUBLISH`. | [19](docs/design/19_pubsub.md), [pub-sub.md](docs/pub-sub.md) |
| **Scripting & Functions** | Implemented | `EVAL`/`EVALSHA` and `FUNCTION`/`FCALL` via sandboxed `mlua` (Lua 5.4). | [13](docs/design/13_scripting_functions.md) |
| **ACL & Security** | Implemented | Per-user permissions, category selectors, `AUTH`. | [15](docs/design/15_security_tls.md) |
| **RDB persistence** | Implemented, with gaps | No `fork()`; shards serialize in ~1ms yielding steps, I/O on an `rdb-writer` thread; persists KV (including NVMe-tiered keys), Vector Sets, and extended `RDBX` search/vector indexes; the snapshot is one point in time across shards (copy-on-write images). | [rdbsave.md](docs/rdbsave.md) |
| **AOF persistence** | Implemented | Async (`monoio`) periodic flush, distinct I/O strategy from RDB save. | [14](docs/design/14_persistence_replication.md) |
| **Replication (`PSYNC`)** | Implemented | Single-connection, used for all Rudis-to-Rudis replication. | [replication.md](docs/replication.md) |
| **Replication (`DFLY FLOW`)** | Implemented, narrow scope | Master-side only, for Dragonfly-protocol clients; Rudis replicas never use it. | [replication.md](docs/replication.md) |
| **Redis Cluster & gossip** | Implemented | Plain-text gossip bus on `port + 10000` (not `io_uring`), quorum failover, real `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD` key migration. | [11](docs/design/11_cluster_topology.md) |
| **HNSW & Redis 8 Vector Sets (`V*`)** | Implemented | Redis 8 Vector Sets (`VADD`/`VSIM`/`VEMB`/`VLINKS`/`VSETATTR`), Cosine/L2/IP SIMD kernels, SQ8/BIN/PQ compression, `REDUCE` projection, NVMe `.vtier` disk reranking (`TIERED` + `RERANK`). | [08](docs/design/08_vector_engine.md) |
| **Kernel TLS (kTLS)** | Not implemented | `rustls` handles TLS in userspace; there is no kernel offload. | [15](docs/design/15_security_tls.md) |
| **Experimental extensions** | Off by default | JSON, search and vectors, probabilistic types, NVMe tiering, CRDTs, agent memory, MCP, AF_XDP: see [docs/experimental.md](docs/experimental.md). | [experimental.md](docs/experimental.md) |
| **Jemalloc memory telemetry** | Implemented | Global allocator is `tikv-jemallocator`; live stats via `tikv-jemalloc-ctl` in `INFO memory`. | [15](docs/design/15_security_tls.md) |

---

## Redis/Valkey TCL Compatibility Suite

Rudis is validated against **86 official Redis/Valkey TCL test suites**: the 49 core suites
below, which run against one server via
[`scripts/run_redis_test_suite.sh`](scripts/run_redis_test_suite.sh), plus 37 replication,
persistence and cluster suites that start their own servers:

```bash
# Run the 49 core TCL suites
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

Suites that start their own servers run through
[`scripts/run_redis_integration_suite.sh`](scripts/run_redis_integration_suite.sh):

| Category | Suites (37) |
| :--- | :--- |
| **Replication** (11) | `integration/{replication-2,replication-3,replication-4,replication-psync,block-repl,psync2,psync2-pingoff,psync2-master-restart,psync2-reg,failover,shutdown}` |
| **Persistence** (14) | `unit/{aofrw,client-eviction,wait,violations,memefficiency,info-keysizes}`, `integration/{aof-race,convert-ziplist-hash-on-load,convert-ziplist-zset-on-load,convert-zipmap-hash-on-load,rdb,corrupt-dump,corrupt-dump-fuzzer,logging}` |
| **Cluster** (12) | `unit/cluster/{failure-marking,internal-secret,multi-slot-operations,misc,scripting,sharded-pubsub,slot-ownership,replica-migration,human-announced-nodename,hostnames,announced-endpoints}`, `integration/dismiss-mem` |

All 86 gate every merge through [`scripts/ci_local.sh`](scripts/ci_local.sh). GitHub Actions
runs the core suites on every push and 27 of the other 37 nightly; the rest run locally only. Tests that don't apply to rudis (no `fork()`, no Unix socket, ...)
and known gaps are listed, each with its reason, in [`scripts/tcl_skips/`](scripts/tcl_skips/);
[`scripts/tcl_patches/`](scripts/tcl_patches/) adapts a suite where rudis's behavior differs on
purpose (it refuses corrupt `RESTORE` payloads up front instead of crashing on them later).

### Fuzzing

`fuzz/` holds cargo-fuzz targets for the untrusted inputs: the RESP/inline framer
(`parse_command`), `RESTORE` payloads (`dump_payload`), RDB and AOF files (`load_rdb`,
`load_aof`) and cluster bus lines (`cluster_bus`). The [Fuzz workflow](.github/workflows/fuzz.yml)
runs each of them every night (one build, then each target in turn), keeping the corpus in the Actions cache from night to night
and uploading any crashing input. To run one locally:

```bash
cargo +nightly fuzz run load_rdb -- -max_total_time=600 -rss_limit_mb=4096 -malloc_limit_mb=2048
```

---

## Documentation & Contributor Guides

For deep technical walkthroughs, internal architecture specifications, and benchmarks:
* [**Documentation Hub**](docs/README.md): index of all architecture and subsystem docs.
* [**Architecture & Threading Model**](docs/architecture.md): thread-per-core engine, `io_uring`/Monoio runtime, request lifecycle.
* [**Replication**](docs/replication.md): `PSYNC` and Dragonfly-compatible `DFLY FLOW`.
* [**Pub/Sub Architecture**](docs/pub-sub.md): striped presence bitmask and Redis 7 sharded pub/sub.
* [**RDB Snapshotting**](docs/rdbsave.md): what actually triggers a save, and how it interacts with the shard model.
* [**Differences from Redis & Dragonfly**](docs/differences.md): architectural and behavioral comparison.
* [**Experimental Extensions**](docs/experimental.md): JSON, search and vectors, probabilistic types, tiering, CRDTs, agent memory; off by default.
* [**Subsystem Design Specifications**](docs/design/README.md): design rationale ("why") across all 20 subsystems.
* [**Subsystem Implementation References**](docs/internal/README.md): concrete data structures, algorithms, and source references.
* [**Comprehensive Performance Guide**](docs/benchmarks/comprehensive_performance_guide.md) and [**Multicore Benchmark Report**](docs/benchmark_multicore_results.md): reproducible benchmark data and methodology.
* [**Engineering Invariants for Contributors**](agent.md): mandatory architectural rules for anyone changing this codebase.

---

## License

Released under the [MIT License](LICENSE) (`license = "MIT"` in `Cargo.toml`).
