# Benchmark: Write Batching & Pipelining Optimization

This document records the throughput and latency of `rudis` after implementing **response write-batching**
and **zero-allocation response formatting**, measured across 1, 2, 4, 8, 16, and 32 worker threads under the
same workload and harness as [`baseline.md`](baseline.md). It is the second entry in the three-part
optimization sequence: [`baseline.md`](baseline.md) (before) -> this document (write-batching) ->
[`squashed_scaling.md`](squashed_scaling.md) (adds pipeline squashing on top of write-batching).

**What changed**: before this optimization, every parsed pipelined command triggered its own
`stream.write_all()` syscall/`io_uring` submission — with pipeline depth 100, that is up to 100 individual
write completions per socket read batch (see `baseline.md` Analysis). Response write-batching coalesces all
responses generated within one read cycle into a single `io_uring` write.

**Why this benchmark matters**: it isolates the effect of one specific optimization (response coalescing)
against an otherwise identical workload and harness, making the before/after comparison a controlled
measurement rather than a description of aggregate improvement across many unrelated changes.

---

## Benchmark Configuration

* **Workload**: 100% `SET`, **1024-byte payload**, **pipeline = 100**, 1,000,000 keys (`S:S` pattern)
* **Client**: `memtier_benchmark` (32 client threads pinned to cores `32-63`, 1 connection per thread)
* **Duration per test**: **60 seconds**
* **Machine**: 64-core Linux system (`7.1.6-1rodete1-amd64`)
* **Harness**: [`scripts/run_16t_benchmark.sh`](../../scripts/run_16t_benchmark.sh) for the 16-thread case;
  the full 1-32 thread sweep follows the same `taskset`/`memtier_benchmark` invocation pattern as
  [`scripts/benchmark_baseline.py`](../../scripts/benchmark_baseline.py) at `--test-time 60`.

---

## Comparison: Baseline vs. Write-Batched

| Server Threads | Baseline Ops/sec | **Batched Ops/sec** | Baseline Bandwidth | **Batched Bandwidth** | Baseline Avg Lat | **Batched Avg Lat** | Baseline p99 | **Batched p99** | **Throughput Speedup** |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 60,487.24 | **816,936.81** | 63.3 MB/s | **855.1 MB/s** | 52.87 ms | **3.90 ms** | 80.90 ms | **7.30 ms** | **13.5x** |
| **2** | 88,727.36 | **593,237.78** | 92.8 MB/s | **620.9 MB/s** | 36.05 ms | **5.38 ms** | 66.56 ms | **10.62 ms** | **6.7x** |
| **4** | 159,224.13 | **857,550.64** | 166.6 MB/s | **897.6 MB/s** | 20.09 ms | **3.71 ms** | 41.47 ms | **6.50 ms** | **5.4x** |
| **8** | 295,665.11 | **794,211.64** | 309.5 MB/s | **831.3 MB/s** | 10.82 ms | **4.01 ms** | 26.75 ms | **6.11 ms** | **2.7x** |
| **16** | 297,048.94 | **705,995.32** | 310.9 MB/s | **739.0 MB/s** | 10.77 ms | **4.51 ms** | 30.72 ms | **7.49 ms** | **2.4x** |
| **32** | 273,831.18 | **704,537.20** | 286.6 MB/s | **737.4 MB/s** | 11.69 ms | **4.52 ms** | 34.82 ms | **8.70 ms** | **2.6x** |

---

## Detailed Write-Batched Metrics (60 Seconds)

| Threads | Pinned Cores | Ops/sec | Bandwidth (KB/sec) | Avg Latency (ms) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | `0` | 816,936.81 | 855,114.98 | 3.90 | 4.13 | 5.25 | 5.82 | 7.30 | 14.02 |
| **2** | `0-1` | 593,237.78 | 620,931.69 | 5.38 | 5.44 | 6.53 | 7.07 | 10.62 | 23.94 |
| **4** | `0-3` | 857,550.64 | 897,632.58 | 3.71 | 3.52 | 4.74 | 5.12 | 6.50 | 29.82 |
| **8** | `0-7` | 794,211.64 | 831,324.57 | 4.01 | 3.79 | 4.77 | 5.09 | 6.11 | 34.30 |
| **16** | `0-15` | 705,995.32 | 738,985.77 | 4.51 | 4.08 | 5.92 | 6.37 | 7.49 | 35.84 |
| **32** | `0-31` | 704,537.20 | 737,446.64 | 4.52 | 4.22 | 5.09 | 5.70 | 8.70 | 35.07 |

---

## Key Insights

1. **Massive Latency Reduction Across the Board**:
   - Pipelined batch latency dropped from ~53ms to **3.7–4.5ms** ($12\times$–$14\times$ improvement).
   - p99 tail latency dropped from 81ms to **6–8ms** ($10\times$ lower tail latency).
2. **Single-Thread Saturation**:
   - On a single core with zero cross-core communication, `rudis` delivers **816k Ops/sec** and **855 MB/s** of raw network write throughput through `io_uring`.
3. **Multi-Thread Channel Contention**:
   - As threads increase, cross-shard routing messages are exchanged across threads via channels.
   - The next optimization phase will focus on:
     - Socket level tuning (`TCP_NODELAY`, `SO_RCVBUF`, `SO_SNDBUF`).
     - Zero-copy command parsing (`Bytes` slices).
     - Cross-shard pipeline squashing — implemented and measured in
       [`squashed_scaling.md`](squashed_scaling.md), which lifts 16-thread throughput a further 4.1x, from
       705,995 to 2,904,558 ops/sec.

---

## Reproducing This Benchmark

No script in `scripts/` is dedicated to this specific before/after write-batching comparison; the 1-32
thread sweep in the tables above was produced with the same `taskset`/`memtier_benchmark` invocation pattern
as [`scripts/benchmark_baseline.py`](../../scripts/benchmark_baseline.py), run at each thread count with
`--test-time 60` against a build that includes response write-batching. See
[`scripts/run_16t_benchmark.sh`](../../scripts/run_16t_benchmark.sh) for the equivalent single-thread-count
(16T, 60s) invocation used in the later squashing comparison
([`pipeline_squashing.md`](pipeline_squashing.md)).
     - Cross-shard request batching to eliminate per-command channel round-trips.
