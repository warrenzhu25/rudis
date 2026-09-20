# High-Core Benchmark Report: Rudis vs. Dragonfly (16 & 32 Cores)

**Date:** September 2026
**Hardware Platform:** AMD EPYC 7B13 (64 logical CPUs / 32 physical cores, 128 MiB L3 cache)
**Host Partitioning:** Dedicated Server Cores `0-31` | Dedicated Client Cores `32-63` (zero CPU overlap)
**Client Setup:** `memtier_benchmark`, 16 client threads x 4 connections/thread = 64 concurrent connections

This report is generated verbatim (including all figures) by
[`scripts/benchmark_multicore_suite.py`](../scripts/benchmark_multicore_suite.py), which writes both this
Markdown file and the accompanying raw data at
[`benchmark_multicore_results.json`](../benchmark_multicore_results.json). The two are kept in lockstep by
construction: re-running the script regenerates this document directly, so the numbers below should always
match the JSON file at the repository root. Do not hand-edit the tables in this file — edit the script and
re-run it instead.

## 1. Methodology

* **Workloads**: `SET 100%` and `GET 100%` at pipeline depth 16 with a 1 KB payload (single-key), and 10-key
  scattered `MGET`/`DEL` at pipeline depth 8. Reads and multi-key workloads pre-populate a 64,000-key space
  before each run.
* **Iterations**: 2 runs per workload per engine (`ITERATIONS = 2` in the script); the reported figure is the
  arithmetic mean of those runs, not a median — this suite is a coarser survey than
  [`multi_command_comparison.md`](benchmarks/multi_command_comparison.md), which uses 3-run medians and
  reports coefficient of variation. Treat single-digit percentage deltas here as indicative rather than
  statistically tight.
* **Core scaling profile** (Section 3): the same `SET` (pipeline 16, 1 KB) workload run against Rudis alone
  at 1, 4, 8, 16, and 32 physical cores, 2 runs per core count.
* **Why this benchmark matters**: it is the highest core-count comparison in this repository, and the only
  one that directly measures how Rudis's shared-nothing scaling holds up against Dragonfly's proactor model
  as both engines are given more physical cores.

---

## 2. Head-to-Head Comparison: 16 Cores

| Workload | Dragonfly v1.39 (16T) | Rudis (16T) | Rudis Throughput Advantage | Dragonfly p99 Latency | Rudis p99 Latency |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **SET 100% (Pipeline 16, 1KB)** | 1,607,646 ops/s | **1,732,540 ops/s** | **+7.8%** | 0.62 ms | **0.65 ms** |
| **GET 100% (Pipeline 16, 1KB)** | 2,296,174 ops/s | **1,905,923 ops/s** | **-17.0%** | 0.45 ms | **0.52 ms** |
| **MGET 10-Key Scattered (P8)** | 335,908 ops/s | **162,949 ops/s** | **-51.5%** | 1.49 ms | **2.99 ms** |
| **DEL 10-Key Scattered (P8)** | 203,716 ops/s | **180,163 ops/s** | **-11.6%** | 2.49 ms | **2.87 ms** |

Dragonfly leads on three of four workloads at 16 cores; Rudis's only win is pipelined `SET` (+7.8%).

---

## 3. Head-to-Head Comparison: 32 Cores

| Workload | Dragonfly v1.39 (32T) | Rudis (32T) | Rudis Throughput Advantage | Dragonfly p99 Latency | Rudis p99 Latency |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **SET 100% (Pipeline 16, 1KB)** | 1,742,970 ops/s | **1,336,831 ops/s** | **-23.3%** | 0.59 ms | **0.76 ms** |
| **GET 100% (Pipeline 16, 1KB)** | 2,447,056 ops/s | **2,018,321 ops/s** | **-17.5%** | 0.45 ms | **0.51 ms** |
| **MGET 10-Key Scattered (P8)** | 243,418 ops/s | **128,515 ops/s** | **-47.2%** | 1.98 ms | **4.06 ms** |
| **DEL 10-Key Scattered (P8)** | 150,502 ops/s | **133,398 ops/s** | **-11.4%** | 3.42 ms | **3.82 ms** |

Dragonfly leads on all four workloads at 32 cores, with larger margins than at 16 cores on `SET` (-23.3%
vs. -7.8% favorable at 16c) and `MGET` (-47.2%).

---

## 4. Rudis Core Scaling Profile (1 to 32 Cores)

Single-engine scaling of Rudis alone (no Dragonfly comparison), `SET` pipeline 16 at 1 KB payload.

| Core Count | Throughput (SET P16, 1KB) | Speedup vs 1 Core | Parallel Scaling Efficiency |
| :---: | :---: | :---: | :---: |
| **1 Core** | 540,446 ops/sec | 1.00x | 100.0% |
| **4 Cores** | 1,282,710 ops/sec | 2.37x | 59.3% |
| **8 Cores** | 1,476,382 ops/sec | 2.73x | 34.1% |
| **16 Cores** | 1,468,516 ops/sec | 2.72x | 17.0% |
| **32 Cores** | 1,281,269 ops/sec | 2.37x | 7.4% |

Scaling efficiency (speedup / core count) falls steadily from 100% at 1 core to 7.4% at 32 cores, and absolute
throughput **peaks at 8 cores** (1.48M ops/sec) and *declines* from 16 to 32 cores (1.47M -> 1.28M ops/sec).
This is sub-linear scaling with an actual regression in the last doubling, not linear scaling — see Finding 2
below.

---

## 5. Key Findings

1. **Dragonfly leads at both 16 and 32 cores on this suite**: across the eight combined 16c/32c measurements
   in Sections 2-3, Rudis wins only one cell (`SET` at 16 cores, +7.8%); Dragonfly wins the remaining seven,
   including both scattered multi-key workloads (`MGET`, `DEL`) at both core counts, by margins from -11% to
   -51%. This contradicts an earlier revision of this document, which characterized Rudis's scatter-gather
   dispatch as delivering "massive throughput gains" on these same workloads — the data does not support
   that claim and the wording has been corrected.
2. **Scaling is sub-linear past 8 cores, and regresses from 16 to 32 cores**: Section 4 shows throughput
   peaking at 8 cores and declining through 16 and 32 cores, with parallel efficiency dropping to 7.4% at 32
   cores. This is the opposite of the "linear scalability to 32 cores" previously claimed here; the two-run
   mean methodology (Section 1) means the specific decline magnitude should be treated as indicative, but the
   general shape — diminishing and eventually negative returns from added cores on this workload — is
   consistent with the cross-shard IPC pressure discussed in
   [`multi_command_comparison.md`](benchmarks/multi_command_comparison.md) Finding 1.
3. **Tail latency**: Rudis's p99 is higher than Dragonfly's on every row in Sections 2-3 except pipelined
   `SET` at 16 cores. The previous "tail latency dominance" claim is not supported by this dataset and has
   been removed; see [`comprehensive_performance_guide.md`](benchmarks/comprehensive_performance_guide.md)
   Section 7 for workloads where Rudis does hold a tail-latency edge.

---

## 6. Reproducing This Benchmark

```bash
cargo build --release
python3 scripts/benchmark_multicore_suite.py
```

This regenerates both `benchmark_multicore_results.json` and this file in place.
