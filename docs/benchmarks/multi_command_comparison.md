# Benchmark: Common-Command Comparison (Rudis vs. Dragonfly)

This document reports Rudis v0.1.0 throughput and latency against Dragonfly v1.39.0 across the sixteen most
frequently used Redis commands, spanning strings, hashes, lists, sets, sorted sets, and generic keyspace
operations, at two server core counts (8 and 16 physical cores) with strict physical-core isolation (every
SMT sibling thread left idle).

The authoritative benchmark harness is [`scripts/benchmark_common_commands_vs_dragonfly.py`](../../scripts/benchmark_common_commands_vs_dragonfly.py),
and its raw output is persisted to [`benchmark_common_commands_results.json`](../../benchmark_common_commands_results.json)
at the repository root. All figures below are read directly from that file.

---

## 1. Methodology

* **Commands covered**: `SET`, `GET`, `INCR`, `MSET` (5 keys), `MGET` (5 keys), `HSET`, `HGET`, `LPUSH`, `LPOP`,
  `LRANGE (0-10)`, `SADD`, `SISMEMBER`, `ZADD`, `ZRANGE (0-10)`, `DEL`, `EXISTS`.
* **Payloads**: 1024 bytes for `SET`/`GET`, 128 bytes for hash/list fields and `MSET`/`MGET` values, 64 bytes
  for set/zset members. Commands with no explicit payload (`INCR`, `DEL`, `EXISTS`, `LPOP`) operate on
  pre-populated keys.
* **Pipeline depth**: 16 for single-key commands, 4 for the cross-shard `MSET`/`MGET` (5-key) workloads.
* **Client**: `memtier_benchmark` (`--print-percentiles 50,90,95,99,99.9`), 16 threads x 4 connections
  (`BENCH_CONNS`, default 4) = up to 64 concurrent connections, issuing randomized-key traffic
  (`--command-key-pattern R`) over a 64,000-key space.
* **CPU pinning**: on this AMD EPYC 7B13 host (32 physical cores / 64 SMT threads, sibling pairs `(N, N+32)`),
  the server is pinned to physical cores `16..16+N-1` (CCX 2–3) and the client to physical cores `0..min(N,16)-1`
  (CCX 0–1) — disjoint physical cores on separate L3 CCX complexes with every SMT sibling (`32..63`) left idle.
  Because the VM has 32 physical cores total, 16 server physical cores + 16 client physical cores is the maximum
  contention-free configuration without SMT sibling oversubscription.
* **Timing**: each run is a steady-state window (`BENCH_TEST_TIME`). One warmup run per workload is discarded
  before three measured iterations (`BENCH_WARMUP=1`, `BENCH_ITERATIONS=3`) to absorb TCP slow start, allocator
  arena warmup, hash-table growth, and first-touch page faults.
* **Headline statistic**: the **median** of the three measured runs (not the mean), so a single outlier run
  does not skew the reported number. The script also reports the coefficient of variation (CV) and true
  `p50`/`p99`/`p99.9` latencies from `memtier_benchmark`.
* **Population**: read/lookup/removal workloads (`GET`, `HGET`, `LPOP`, `LRANGE`, `SISMEMBER`, `ZRANGE`,
  `DEL`, `EXISTS`) are pre-populated across the full 64,000-key space from a single sequential connection
  before each run, guaranteeing a 100% hit rate rather than the largely-miss workload that an
  under-populated keyspace would otherwise produce.

---

## 2. Head-to-Head Results: 8 Physical Cores

Figures are the median of 3 measured runs (`server CPUs: 16-23`, `client CPUs: 0-7`, all SMT siblings idle).
`CV` is the run-to-run coefficient of variation for that engine on that workload. `Δ` is Rudis's throughput
relative to Dragonfly. At 8 physical cores, all sixteen commands favor Rudis (**1.29x to 2.71x**), with every
run-to-run CV at or below 3.1%.

| Workload | Dragonfly (ops/sec) | CV | Rudis (ops/sec) | CV | Δ | Dragonfly p99 (ms) | Rudis p99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **SET (1KB)** | 1,334,226 | 2.4% | **2,169,971** | 0.2% | **+62.6% (1.63x)** | **1.639** | 2.023 |
| **GET (1KB)** | 469,819 | 1.2% | **1,272,565** | 1.6% | **+170.9% (2.71x)** | **4.415** | 5.951 |
| **INCR** | 1,889,107 | 1.5% | **3,469,914** | 2.3% | **+83.7% (1.84x)** | 1.167 | **0.703** |
| **MSET (5-key)** | 321,830 | 2.4% | **769,585** | 2.3% | **+139.1% (2.39x)** | 1.503 | **0.919** |
| **MGET (5-key)** | 228,203 | 0.8% | **294,639** | 0.5% | **+29.1% (1.29x)** | **2.655** | 5.951 |
| **HSET** | 1,700,265 | 0.9% | **2,717,598** | 0.8% | **+59.8% (1.60x)** | 1.375 | **1.031** |
| **HGET** | 1,515,730 | 1.5% | **2,895,275** | 0.9% | **+91.0% (1.91x)** | 1.519 | **0.831** |
| **LPUSH** | 1,576,482 | 2.8% | **2,994,933** | 1.1% | **+90.0% (1.90x)** | 1.503 | **0.823** |
| **LPOP** | 2,009,871 | 1.6% | **3,359,878** | 0.8% | **+67.2% (1.67x)** | 1.119 | **0.719** |
| **LRANGE (0-10)** | 1,369,782 | 2.1% | **2,739,467** | 1.5% | **+100.0% (2.00x)** | 1.543 | **0.919** |
| **SADD** | 1,826,105 | 0.7% | **3,128,334** | 2.8% | **+71.3% (1.71x)** | 1.191 | **0.807** |
| **SISMEMBER** | 1,884,794 | 0.9% | **2,918,715** | 1.1% | **+54.9% (1.55x)** | 1.255 | **1.007** |
| **ZADD** | 1,655,448 | 0.7% | **2,965,327** | 0.4% | **+79.1% (1.79x)** | 1.527 | **0.895** |
| **ZRANGE (0-10)** | 1,349,936 | 0.7% | **2,815,376** | 3.1% | **+108.6% (2.09x)** | 1.719 | **0.887** |
| **DEL** | 1,954,726 | 1.2% | **3,452,557** | 2.0% | **+76.6% (1.77x)** | 1.143 | **0.695** |
| **EXISTS** | 1,987,004 | 0.8% | **3,329,337** | 1.2% | **+67.6% (1.68x)** | 1.207 | **0.775** |

---

## 3. Head-to-Head Results: 16 Physical Cores

Figures are the median of 3 measured runs (`server CPUs: 16-31`, `client CPUs: 0-15`, all SMT siblings idle).
At 16 physical cores, Rudis leads Dragonfly on all sixteen measured commands (**1.44x to 3.11x**) AND achieves
lower `p99` tail latency on every single command.

| Workload | Dragonfly (ops/sec) | CV | Rudis (ops/sec) | CV | Δ | Dragonfly p99 (ms) | Rudis p99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **SET (1KB)** | 1,946,374 | 2.0% | **3,201,642** | 4.0% | **+64.5% (1.64x)** | 1.343 | **0.983** |
| **GET (1KB)** | 722,740 | 2.4% | **2,246,471** | 3.5% | **+210.8% (3.11x)** | 3.023 | **1.351** |
| **INCR** | 2,438,146 | 7.1% | **4,363,648** | 7.4% | **+79.0% (1.79x)** | 1.263 | **0.807** |
| **MSET (5-key)** | 356,584 | 2.8% | **1,028,400** | 4.5% | **+188.4% (2.88x)** | 1.871 | **0.847** |
| **MGET (5-key)** | 355,927 | 0.4% | **512,314** | 3.6% | **+43.9% (1.44x)** | 1.767 | **1.471** |
| **HSET** | 2,294,032 | 5.1% | **3,947,830** | 8.8% | **+72.1% (1.72x)** | 1.287 | **0.911** |
| **HGET** | 2,265,013 | 3.6% | **4,089,839** | 3.6% | **+80.6% (1.81x)** | 1.167 | **0.879** |
| **LPUSH** | 2,288,252 | 3.3% | **4,141,809** | 2.4% | **+81.0% (1.81x)** | 1.263 | **0.887** |
| **LPOP** | 2,781,604 | 3.3% | **4,609,519** | 1.3% | **+65.7% (1.66x)** | 1.007 | **0.759** |
| **LRANGE (0-10)** | 1,936,275 | 4.2% | **3,873,811** | 5.2% | **+100.1% (2.00x)** | 1.511 | **0.863** |
| **SADD** | 2,344,644 | 6.5% | **4,591,894** | 5.0% | **+95.8% (1.96x)** | 1.327 | **0.671** |
| **SISMEMBER** | 2,441,985 | 8.1% | **4,170,741** | 7.8% | **+70.8% (1.71x)** | 1.271 | **0.815** |
| **ZADD** | 2,278,019 | 3.4% | **3,833,418** | 7.8% | **+68.3% (1.68x)** | 1.311 | **0.991** |
| **ZRANGE (0-10)** | 1,816,197 | 1.6% | **4,289,818** | 5.2% | **+136.2% (2.36x)** | 1.623 | **0.655** |
| **DEL** | 2,455,759 | 10.7% | **4,431,700** | 5.7% | **+80.5% (1.80x)** | 1.311 | **0.855** |
| **EXISTS** | 2,568,588 | 3.0% | **4,563,710** | 8.3% | **+77.7% (1.78x)** | 1.223 | **0.759** |

---

## 4. Key Findings

1. **Across-the-board lead at both 8 and 16 physical cores**: Rudis leads Dragonfly v1.39 on all 16 commands
   at 8 physical cores (**+29.1% to +170.9%**) and widens its lead at 16 physical cores (**+43.9% to +210.8%**),
   exceeding **3.8M–4.6M ops/sec** on single-key container and keyspace operations (`INCR`, `HSET`, `HGET`,
   `LPUSH`, `LPOP`, `LRANGE`, `SADD`, `SISMEMBER`, `ZADD`, `ZRANGE`, `DEL`, `EXISTS`).
2. **Pipelined cross-shard `MSET` folding (`+188.4%` vs. Dragonfly at 16 cores)**: Folding `Command::Mset`
   pairs into the per-shard `remote_batches` inside `execute_commands_squashed_inner` eliminates intermediate
   pipeline barriers and collapses `P * K` scatter-gather descriptors into a single `ShardMessage::Batch`
   round-trip per remote shard, lifting 16-core `MSET (5-key)` throughput from `709,598 ops/sec` to
   **`1,028,400 ops/sec`** (**2.88x** Dragonfly's `356,584 ops/sec`) and cutting `p99` latency to **0.847 ms**
   (vs. Dragonfly's `1.871 ms`).
3. **1KB `GET` throughput (`+210.8%` at 16 cores)**: `GET (1KB)` shows the largest single-command advantage
   (**2.25M ops/sec vs. 723K ops/sec**, **3.11x** Dragonfly) with **1.351 ms** `p99` vs. Dragonfly's **3.023 ms**.
4. **Why 32-core numbers were retired on this 32-physical-core VM**: An earlier revision included a `32_cores`
   section recorded on this same 32-physical-core (64-vCPU) host. Running a 32-thread server (`16..47`) alongside
   a 16-thread client (`0..15`) placed 16 server shard threads directly onto SMT siblings (`32..47`) of the
   client physical cores (`0..15`), causing severe load-generator/shard CPU contention. All multi-core
   benchmarks on this host are therefore bounded at 16 server physical cores (`16..31`) + 16 client physical
   cores (`0..15`).

---

## 5. Reproducing This Benchmark

```bash
cargo build --release

# Default 8-core and 16-core physical-core sweep
unshare -rn sh -c 'ip link set lo up && python3 scripts/benchmark_common_commands_vs_dragonfly.py 8,16'

# Narrow to specific commands, adjust duration/iterations
unshare -rn sh -c 'ip link set lo up && BENCH_WORKLOADS=SET,GET,INCR,MSET_5K BENCH_TEST_TIME=5 BENCH_ITERATIONS=3 \
  python3 scripts/benchmark_common_commands_vs_dragonfly.py 8,16'
```

Results are merged (per core-count, per engine) into `benchmark_common_commands_results.json` at the
repository root; re-running a subset of workloads updates only those entries.
