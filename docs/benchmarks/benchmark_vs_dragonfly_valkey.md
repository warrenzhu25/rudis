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

Reading directly from the current result file, Rudis and Dragonfly split the nine workloads: Dragonfly leads
on single-key, unpipelined operations and on both scattered multi-key workloads, while Rudis leads on
pipelined throughput at depth 16 and on the co-located multi-key read.

* **Pipelined SET (`SET 100% Pipeline 16, 1KB`)**: Rudis **1,485,688 ops/sec** vs. Dragonfly **1,207,022
  ops/sec** — **+23.1%** for Rudis, with lower p99 tail latency (**1.411 ms** vs. **1.979 ms**).
* **Pipelined Mixed 50:50 (`Pipeline 16, 1KB`)**: Rudis **1,423,247 ops/sec** vs. Dragonfly **1,006,959
  ops/sec** — **+41.3%** for Rudis.
* **Pipelined GET (`Pipeline 16, 1KB`)**: Rudis **1,498,605 ops/sec** vs. Dragonfly **1,527,352 ops/sec** —
  within **2%**, effectively a statistical tie; this workload also carries the highest measurement noise in
  the suite (Rudis run-to-run CV ≈ 36%, see Section 3), so the sign of this delta should not be over-read.
* **Unpipelined single-key ops (`Pipeline 1, 1KB`)**: Dragonfly leads on all three — `SET` by 37.3%, `GET` by
  25.8%, and `Mixed` by 34.8%. Unpipelined throughput at this payload size is dominated by per-request
  round-trip and syscall overhead rather than by server-side processing, which favors Dragonfly's proactor
  model in this configuration.
* **Scattered multi-key fan-out (`MSET`/`MGET`, 10 keys)**: Dragonfly leads both — `MSET` by 12.3% and `MGET`
  by 21.7%.
* **Co-located multi-key read (`MGET 10 Keys {tag}`)**: Rudis **178,046 ops/sec** vs. Dragonfly **175,368
  ops/sec** — a statistical tie (+1.5%).

---

## 2. Test Environment & Engine Versions

### Hardware & Operating System
* **Host Machine**: 64 vCPUs (AMD EPYC 7B13 64-Core Processor, 32 physical cores / 64 threads, 1 socket)
* **Memory**: 117 GiB RAM (100+ GiB available)
* **OS / Kernel**: Linux 7.1.6-1rodete1-amd64 (`x86_64`)
* **Host Load**: Verified idle (<1.50 1-minute load average, >98% CPU idle) prior to benchmark execution.
* **CPU Pinning & Isolation**:
  * **Server Processes**: Pinned strictly to cores `0-7` (8 dedicated CPU cores) via `taskset -c 0-7`.
  * **Benchmark Client (`memtier_benchmark`)**: Pinned strictly to cores `32-47` (16 dedicated CPU cores) via `taskset -c 32-47`.
  * **Zero CPU Overlap**: Guaranteed 0% CPU core contention between client workload generation and database server execution.

### Software & Engine Binaries
| Engine | Version / Commit | Architecture / Concurrency Model | Flags / Configuration |
| :--- | :--- | :--- | :--- |
| **Rudis** | `v0.1.0` (`target/release/rudis`) | Shared-Nothing, Thread-per-Core (8 Monoio `io_uring` reactors) | `--threads 8 --port 6379` |
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

### 4.1 Master Throughput & Latency Summary (5 Runs, Mean ± Std)

Source: `docs/benchmarks/benchmark_comparison_results.json`, `Dragonfly v1.39.0` and `Rudis (Thread-per-core)`
entries. `Δ` is Rudis relative to Dragonfly.

| Workload | Rudis Ops/sec (Mean ± Std) | Dragonfly Ops/sec (Mean ± Std) | Δ | Rudis p99 | Dragonfly p99 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **SET 100% (Pipeline 1, 1KB)** | 204,822 ± 15,477 | **326,403 ± 31,606** | -37.3% | 0.691 ms | **0.575 ms** |
| **GET 100% (Pipeline 1, 1KB)** | 238,117 ± 38,393 | **320,829 ± 22,718** | -25.8% | 0.731 ms | **0.719 ms** |
| **Mixed 50:50 (Pipeline 1, 1KB)** | 224,679 ± 1,534 | **344,438 ± 11,351** | -34.8% | 0.667 ms | **0.487 ms** |
| **SET 100% (Pipeline 16, 1KB)** | **1,485,688 ± 171,417** | 1,207,022 ± 120,635 | **+23.1%** | **1.411 ms** | 1.979 ms |
| **GET 100% (Pipeline 16, 1KB)** | 1,498,605 ± 533,044\* | **1,527,352 ± 188,276** | -1.9% | 2.019 ms | **1.947 ms** |
| **Mixed 50:50 (Pipeline 16, 1KB)** | **1,423,247 ± 96,270** | 1,006,959 ± 75,124 | **+41.3%** | 2.559 ms | **2.895 ms** |
| **MSET 10 Keys (Scattered)** | 137,203 ± 26,160 | **156,405 ± 20,507** | -12.3% | 1.251 ms | **0.999 ms** |
| **MGET 10 Keys (Scattered)** | 150,775 ± 18,048 | **192,658 ± 7,021** | -21.7% | 0.859 ms | **0.575 ms** |
| **MGET 10 Keys (Co-located `{tag}`)** | **178,046 ± 11,759** | 175,368 ± 9,210 | +1.5% | **0.819 ms** | 0.939 ms |

\* The Rudis `GET 100% (Pipeline 16, 1KB)` run carries a ±533,044 standard deviation on a 1,498,605 mean — a
coefficient of variation of ~36%, by far the noisiest cell in this table. Treat the -1.9% delta on this row as
inconclusive rather than a real regression; re-running with a higher iteration count (`-i 10` or more) is
recommended before drawing a conclusion from this workload specifically.

### 4.2 Reading the Table

* **Unpipelined workloads (Pipeline 1) favor Dragonfly** by 26-37%, with Dragonfly holding lower p99 latency
  on `SET` and `Mixed`. At pipeline depth 1, each request is a full round trip, so the server's syscall and
  scheduling overhead per request dominates; Dragonfly's fiber proactor model apparently amortizes that
  better than Rudis's shared-nothing shard dispatch in this configuration.
* **Pipelined workloads (Pipeline 16) mostly favor Rudis**: `SET` (+23.1%) and `Mixed` (+41.3%) both show
  clear wins with lower tail latency; `GET` is a statistical tie once the noise in that cell (see note above)
  is accounted for.
* **Scattered multi-key commands favor Dragonfly**: fanning a single client command out across independently
  owned shards and re-assembling the reply currently costs Rudis more than it costs Dragonfly for both
  `MSET` and `MGET`.
* **Co-located multi-key reads (`{tag}`) are a tie**: when all ten keys hash to the same shard, Rudis's
  single-pass local lookup removes the cross-shard fan-out cost entirely, closing the gap seen in the
  scattered case.

---

## 5. Architectural Notes

### 5.1 Where Rudis Currently Leads
1. **Thread-per-Core Shared-Nothing Isolation**: Rudis runs an independent Monoio event loop on every pinned
   CPU core, so each core owns its subset of the keyspace and avoids lock contention entirely during reads.
   Combined with pipelining, this shows up as the +23.1% (`SET`) and +41.3% (`Mixed`) advantages in Section 4.
2. **`io_uring` Response Batching**: pipelined writes benefit from Monoio's submit-and-wait ring mechanics,
   aggregating incoming command frames and coalescing outgoing responses into fewer network writes — the
   likely reason pipelined workloads behave differently from unpipelined ones in this comparison.
3. **Co-located multi-key reads**: when all keys in a batch hash to the same shard (the `{tag}` case),
   Rudis's local-ownership fast path removes cross-shard fan-out entirely, turning what is otherwise a Rudis
   deficit (see scattered `MGET`, Section 4.2) into a tie.

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
| **I/O Engine** | Linux `io_uring` (Monoio) | Linux `epoll` / `io_uring` (Helio) |
| **Inter-Thread IPC** | Channel actor messages (`flume`) | Shared memory fibers & mutexes |
| **Unpipelined SET/GET/Mixed (P1, 1KB)** | 205-239K ops/s | **321-344K ops/s** (lower latency) |
| **Pipelined SET (P16, 1KB)** | **1.49M ops/s** (p99 **1.41ms**) | 1.21M ops/s (p99 1.98ms) |
| **Pipelined GET (P16, 1KB)** | 1.50M ops/s (p99 2.02ms)\* | 1.53M ops/s (p99 **1.95ms**)\* |
| **Pipelined Mixed 50:50 (P16, 1KB)** | **1.42M ops/s** (p99 2.56ms) | 1.01M ops/s (p99 2.90ms) |
| **Co-located MGET (10 keys, `{tag}`)** | 178K ops/s (p99 **0.82ms**) | 175K ops/s (p99 0.94ms) — statistical tie |
| **Scattered MGET / MSET (10 keys)** | 137-151K ops/s | **156-193K ops/s** |
| **Primary Strength** | Pipelined write and mixed throughput | Unpipelined single-key latency and scattered multi-key fan-out |

\* Statistical tie; see the noise note in Section 4.1.

Both engines have room for improvement highlighted directly by this data: Rudis on unpipelined single-key
latency and scattered multi-key fan-out, Dragonfly on pipelined mixed-workload throughput.
