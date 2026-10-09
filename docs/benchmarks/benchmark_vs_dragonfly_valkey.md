# Comparative Benchmark: Rudis vs. Dragonfly (vs. Valkey, historical)

This document compares Rudis against Dragonfly v1.39.0 across nine single- and multi-key workloads, using the
data currently checked in at [`docs/benchmarks/benchmark_comparison_results.json`](benchmark_comparison_results.json).
It is produced by [`scripts/benchmark_vs_dragonfly_valkey.py`](../../scripts/benchmark_vs_dragonfly_valkey.py).

> **Data provenance notice.** The script supports a three-way comparison against Valkey 8.1.9 in addition to
> Dragonfly, and an earlier revision of this document reported such a comparison. The committed result file
> as of this revision, however, contains **only Dragonfly and Rudis** entries — the harness was last run with
> its `--df-only` flag (added in commit `90898dd`), and no Valkey run has been recorded since commit
> `ea22263`. Rudis and Dragonfly have both changed substantially since that commit (see `git log --oneline --
> docs/benchmarks/benchmark_comparison_results.json`), so the old Valkey figures cannot be meaningfully
> compared against current Rudis/Dragonfly numbers and are **not reproduced here**. Re-run
> `python3 scripts/benchmark_vs_dragonfly_valkey.py` (omitting `--df-only`) to regenerate a current three-way
> comparison; see Section 6.

---

## 1. Executive Summary

Measured at Rudis `98ece39` (October 2026; `idle-poll-us 50`, the default) against Dragonfly v1.39.0, both with
8 threads on cores 0-7. **Rudis matches or beats Dragonfly on all nine workloads.**

* **Pipelined (depth 16)**: Rudis leads by a wide margin: `SET` 1.60M vs 1.06M
  (+51%), `GET` 3.25M vs 1.48M
  (+120%), `Mixed` 2.25M vs 1.18M
  (+91%), with lower p99 latency.
* **Unpipelined single-key (depth 1)**: a tie on `SET` (+0.3%), small Rudis leads on `GET` (+4.4%) and `Mixed`
  (+6.7%). An earlier revision of this document showed Dragonfly ahead by 26-37% here; the cross-shard work
  since then (Section 5.1) closed that gap.
* **Multi-key (10 keys)**: Rudis leads scattered `MGET` (+17.7%, previously -21.7%), scattered `MSET` (+33.7%)
  and co-located `MGET` (+21.2%).

---

## 2. Test Environment & Engine Versions

### Hardware & Operating System
* **Host Machine**: 64 vCPUs (AMD EPYC 7B13 64-Core Processor, 32 physical cores / 64 threads, 1 socket)
* **Memory**: 117 GiB RAM (100+ GiB available)
* **OS / Kernel**: Linux 7.1.6-1rodete1-amd64 (`x86_64`)
* **Host Load**: Idle before each run (1-minute load average 0.3-2.7). Background load is the reason the
  unpipelined cells are measured interleaved (Section 4.1).
* **CPU Pinning & Isolation**:
  * **Server Processes**: Pinned strictly to cores `0-7` (8 dedicated CPU cores) via `taskset -c 0-7`.
  * **Benchmark Client (`memtier_benchmark`)**: Pinned strictly to cores `32-47` (16 dedicated CPU cores) via `taskset -c 32-47`.
  * **Zero CPU Overlap**: Guaranteed 0% CPU core contention between client workload generation and database server execution.

### Software & Engine Binaries
| Engine | Version / Commit | Architecture / Concurrency Model | Flags / Configuration |
| :--- | :--- | :--- | :--- |
| **Rudis** | `98ece39` (`target/release/rudis`) | Shared-Nothing, Thread-per-Core (8 Monoio `io_uring` reactors) | `--threads 8 --port 6379` |
| **Dragonfly** | `v1.39.0` (commit `699862e5da7c`) | Multi-threaded shared memory fiber proactor (8 threads) | `--proactor_threads=8 --cache_mode=false --dbfilename="" --port 6381` |
| **Valkey** *(historical only — see notice above)* | `v8.1.9` GA (commit `a9245aaf3`, jemalloc 5.3.0) | Single main execution thread + 8 I/O read/write threads | `--io-threads 8 --io-threads-do-reads yes --protected-mode no --save "" --appendonly no --port 6380` |
| **Benchmark Tool** | `memtier_benchmark` v2.2.1 | 8 client threads, 8 connections/thread (64 concurrent connections) | `taskset -c 32-47 memtier_benchmark ...` |

---

## 3. How to Benchmark (Step-by-Step Reproduction)

### Step 1: Compile the Engines
```bash
# 1. Compile Rudis in Release mode
cargo build --release

# 2. Compile Valkey 8.1.9 (latest stable GA release with jemalloc)
git clone --branch 8.1.9 https://github.com/valkey-io/valkey.git valkey-stable
cd valkey-stable && make -j16

# 3. Dragonfly binary (v1.39.0)
# Use official Dragonfly v1.39.0 release binary
```

### Step 2: Verify Host Load Before Running
```bash
uptime
# Ensure load average is low (< 2.0 on 64 vCPUs) and CPU idle > 95%
```

### Step 3: Run the Automated 5-Iteration Benchmark Suite
All benchmarks are orchestrated by `scripts/benchmark_vs_dragonfly_valkey.py`. To reproduce the exact tests:
```bash
# Rudis + Dragonfly + Valkey (three-way; requires Valkey built per Step 1)
python3 scripts/benchmark_vs_dragonfly_valkey.py

# Rudis + Dragonfly only (skips Valkey; matches the data currently committed
# in benchmark_comparison_results.json and reproduced in Section 4)
python3 scripts/benchmark_vs_dragonfly_valkey.py --df-only

# Rudis only, or a custom iteration count
python3 scripts/benchmark_vs_dragonfly_valkey.py --rudis-only
python3 scripts/benchmark_vs_dragonfly_valkey.py -i 10
```
This script:
1. Boots each engine sequentially on dedicated cores (`taskset -c 0-7`).
2. Clears the keyspace (`FLUSHALL`) and pre-populates data for read workloads.
3. Runs each workload **5 consecutive times** by default (`-i`/`--iterations` to override).
4. Records mean/std/min/max ops-per-second and latency into `docs/benchmarks/benchmark_comparison_results.json`,
   **overwriting** the previous contents of that file (it is not merged, unlike the common-commands suite).
5. Shuts down each server cleanly before starting the next engine.

---

## 4. Benchmark Results

### 4.1 Unpipelined and Multi-Key Workloads (interleaved, medians)

These cells are dominated by run-to-run noise when the two engines run back to back: in two 5- and 10-iteration
runs of `benchmark_vs_dragonfly_valkey.py` the unpipelined means moved by 10-25% between runs for both engines
(Dragonfly `GET` p1 read 242K, 292K and 319K). They are therefore measured with
[`scripts/benchmark_interleaved_vs_dragonfly.sh`](../../scripts/benchmark_interleaved_vs_dragonfly.sh): the
same setup (8 server threads on cores 0-7, memtier 8 threads x 8 connections on cores 32-47, 1 KB values, 64K
keys), but 10 s timed runs and the engines alternating every round, so host drift hits both equally. Medians of
5 rounds; "spread" is (max - min) / median.

| Workload | Rudis ops/sec | Dragonfly ops/sec | Δ | Spread (Rudis / Dragonfly) |
| :--- | :---: | :---: | :---: | :---: |
| **SET 100% (Pipeline 1, 1KB)** | 274,564 | 273,697 | +0.3% (tie) | 4% / 12% |
| **GET 100% (Pipeline 1, 1KB)** | **285,500** | 273,525 | **+4.4%** | 6% / 19% |
| **Mixed 50:50 (Pipeline 1, 1KB)** | **278,013** | 260,521 | **+6.7%** | 7% / 14% |
| **MSET 10 Keys (Scattered)** | **165,525** | 123,798 | **+33.7%** | 13% / 12% |
| **MGET 10 Keys (Scattered)** | **152,372** | 129,499 | **+17.7%** | 13% / 20% |
| **MGET 10 Keys (Co-located `{tag}`)** | **238,016** | 196,397 | **+21.2%** | 14% / 14% |

### 4.2 Pipelined Workloads (`benchmark_vs_dragonfly_valkey.py --df-only -i 10`, mean ± std)

Source: `docs/benchmarks/benchmark_comparison_results.json`. `Δ` is Rudis relative to Dragonfly. The deltas
here are far larger than the noise.

| Workload | Rudis Ops/sec (Mean ± Std) | Dragonfly Ops/sec (Mean ± Std) | Δ | Rudis p99 | Dragonfly p99 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **SET 100% (Pipeline 16, 1KB)** | **1,604,125 ± 303,440** | 1,062,850 ± 137,674 | **+50.9%** | **1.973 ms** | 3.492 ms |
| **GET 100% (Pipeline 16, 1KB)** | **3,247,474 ± 523,692** | 1,479,369 ± 216,519 | **+119.5%** | **1.037 ms** | 2.486 ms |
| **Mixed 50:50 (Pipeline 16, 1KB)** | **2,252,226 ± 325,593** | 1,180,613 ± 100,805 | **+90.8%** | **1.814 ms** | 2.922 ms |

The same JSON also holds that run's unpipelined and multi-key cells; use Section 4.1 for those.

### 4.3 Reading the Tables

* **Unpipelined (depth 1)** throughput at 64 connections is bounded by per-request network syscalls on both
  engines, so they land close together. Rudis's remaining cost here was the cross-shard hop: with 7 of 8
  keys owned by another shard, a remote `GET` cost ~58 µs of server time against ~1 µs locally, almost all of
  it threads waking from sleep. Idle polling (Section 5.1) cut it to ~39 µs and raised 64-connection `GET`
  throughput by 26% over the same build with polling off.
* **Pipelined (depth 16)**: Rudis squashes each connection's pipeline into one message per target shard and
  coalesces replies into few writes, so it pays the hop once per batch.
* **Multi-key**: scattered `MGET`/`MSET` fan out to up to 8 shards per command; with threads that rarely
  sleep the fan-out is cheap, and co-located keys (`{tag}`) skip it entirely.

---

## 5. Architectural Notes

### 5.1 Where Rudis Currently Leads
1. **Thread-per-Core Shared-Nothing Isolation**: Rudis runs an independent Monoio event loop on every pinned
   CPU core, so each core owns its subset of the keyspace and avoids lock contention entirely during reads.
   Combined with pipelining, this shows up as the large pipelined leads in Section 4.2.
2. **`io_uring` Response Batching**: pipelined writes benefit from Monoio's submit-and-wait ring mechanics,
   aggregating incoming command frames and coalescing outgoing responses into fewer network writes — the
   likely reason pipelined workloads behave differently from unpipelined ones in this comparison.
3. **Co-located multi-key reads**: when all keys in a batch hash to the same shard (the `{tag}` case),
   Rudis's local-ownership fast path removes cross-shard fan-out entirely (+21.2% in Section 4.1).
4. **Cheap cross-shard hops** (`e76bf42`, `98ece39`): Rudis vendors monoio (`vendor/monoio`, changes listed in
   `RUDIS_PATCHES.md`). Wakes from other threads run on every event-loop pass instead of only when a thread
   is about to sleep; shard rings use `COOP_TASKRUN | TASKRUN_FLAG`; and threads busy-poll for 50 µs before
   sleeping (`idle-poll-us`, 0 disables it), so a remote request or reply usually finds its thread awake and
   needs no kernel wake-up. Idle CPU cost: ~0.14 cores on 8 threads vs 0.06 with polling off.

### 5.2 Historical Write-Path Optimization (commit `2c80413`)
The following bottlenecks were identified and fixed by commit `2c80413` (`perf(core): optimize write path
throughput and latency via zero-copy backlog ring buffer and fast paths`), profiled with `perf record -g`
against an earlier build in which write workloads (`SET`, `Mixed`, `MSET`) lagged well behind Dragonfly and
Valkey. At the time, **over 75% of total CPU time** was spent in `libc.so.6 [.] __memmove_avx_unaligned_erms`
and `<std::sys::sync::rwlock::futex::RwLock>::write_contended`. This section is retained as a historical
record of what was fixed and why; the specific ops/sec figures it improved from are superseded by Section 4.

#### Identified Bottlenecks & Fixes:
1. **Replication Backlog 1MB `memmove` under Exclusive Mutex**:
   * *Issue*: `ReplicationBacklog` was implemented with a linear `Vec<u8>`. Whenever total bytes exceeded 1MB (`overflow = len - max_size`), it invoked `self.buffer.drain(..overflow)`. Calling `drain()` on a 1MB vector shifted the entire remaining buffer with `memmove` on **every single write mutation**. At 25,000 writes/sec, this meant shifting **~25 GB/sec of memory in RAM** while holding an exclusive global write lock across all 8 shard threads.
   * *Fix*: Replaced the linear `Vec<u8>` with a zero-copy circular ring buffer. Appends now copy only incoming bytes (`copy_from_slice`) into modular circular ring slices in O(1) time without shifting existing data.
2. **Replication Backlog Active False Alarm in Fast-Path Check**:
   * *Issue*: `has_connected_replicas(port)` was implemented as `hub.has_replicas.load() || hub.backlog_active.load()`. Because `backlog_active` was initialized to `true`, `has_connected_replicas` returned `true` permanently—even when zero replicas were connected. This forced every simple mutation through serialization and the global backlog lock.
   * *Fix*: Replaced with zero-copy ring buffer backlog and streamlined fast-path dispatch.
3. **Redundant Integer Parsing on 1KB Payloads (`parse_i64_bytes`)**:
   * *Issue*: Every string value was checked for 64-bit integer encoding. For 1024-byte strings, `parse_i64_bytes` looped through up to 1024 characters executing `checked_mul(10)` and `checked_add`.
   * *Fix*: Added early return `if s.is_empty() || s.len() > 20 { return None; }`. Because `i64::MAX` has 19 digits (max 20 with sign), any payload longer than 20 bytes exits immediately, saving 1000+ loop iterations per 1KB write.
4. **Duplicate Hashing and Probing in `RudisFlatTable` (`set_extended`)**:
   * *Issue*: `set_extended` probed the SwissTable with `find_or_prepare_insert`. On miss, it called `table.insert(entry)`, which recomputed the hash and probed control bytes a second time.
   * *Fix*: Added `RudisFlatTable::insert_prepared(entry, hash, insert_idx)` to insert directly into the candidate slot with zero re-probing.
5. **Unconditional `monoio::spawn` Auto-Tiering Tasks**:
   * *Issue*: In `execute_commands_squashed` and `ShardMessage::Set`, an unconditional `monoio::spawn(async move { r.check_auto_tier().await; })` task was allocated on every write batch.
   * *Fix*: Replaced with synchronous threshold checking (`check_auto_tier_after_write()`), avoiding heap task allocations when memory is below threshold.
6. **Global `WATCHED_KEYS` Lock Contention**:
   * *Issue*: Acquiring `WATCHED_KEYS.read().unwrap()` on every write created cross-core cache-line bouncing.
   * *Fix*: Added atomic boolean bypass `HAS_WATCHED_KEYS` to skip lock acquisition when no keys are watched.

---

## 6. Summary of Architectural Comparison

This table reflects the current data in Section 4 (Rudis vs. Dragonfly only; see the data provenance notice
at the top of this document regarding Valkey).

| Dimension | Rudis | Dragonfly |
| :--- | :--- | :--- |
| **Execution Model** | Thread-per-core shared-nothing | Multi-threaded fiber proactor |
| **I/O Engine** | Linux `io_uring` (Monoio, vendored with patches) | Linux `epoll` / `io_uring` (Helio) |
| **Inter-Thread IPC** | Lock-free SPSC rings per shard pair, doorbell wake-ups, 50 µs idle polling | Shared memory fibers & mutexes |
| **Unpipelined SET/GET/Mixed (P1, 1KB)** | 275-286K ops/s | 261-274K ops/s |
| **Pipelined SET (P16, 1KB)** | **1.60M ops/s** | 1.06M ops/s |
| **Pipelined GET (P16, 1KB)** | **3.25M ops/s** | 1.48M ops/s |
| **Pipelined Mixed 50:50 (P16, 1KB)** | **2.25M ops/s** | 1.18M ops/s |
| **Co-located MGET (10 keys, `{tag}`)** | **238K ops/s** | 196K ops/s |
| **Scattered MGET / MSET (10 keys)** | **152K / 166K ops/s** | 129K / 124K ops/s |
| **Primary Strength** | Pipelined and multi-key throughput; ties or leads unpipelined | Close on unpipelined single-key |

Unpipelined single-key throughput is close for both engines because per-request network syscalls dominate
at depth 1.
