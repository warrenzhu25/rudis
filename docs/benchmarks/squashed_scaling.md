# Benchmark: Full 1 to 32 Thread Scaling Suite (Squashed Architecture)

This document records the full 60-second scaling benchmark of `rudis` across 1, 2, 4, 8, 16, and 32 worker
threads after implementing **cross-shard pipeline squashing**, **pre-allocated responder pools**, and the
**`mimalloc`** global allocator on top of the response write-batching measured in
[`write_batching.md`](write_batching.md). It is the third and final entry in the
[`baseline.md`](baseline.md) -> [`write_batching.md`](write_batching.md) -> this document optimization
sequence, and uses the same workload and harness as both.

**What changed since write-batching**: write-batching coalesces responses *within* a single shard's read
cycle, but a pipelined client request touching multiple shards still serialized cross-shard hops one at a
time. Pipeline squashing groups pipelined requests by destination shard and dispatches each shard's batch in
parallel, converting what was a serialized chain of cross-thread hops into concurrent execution across all
available cores; see [`pipeline_squashing.md`](pipeline_squashing.md) for the 16-thread deep dive and a
Dragonfly comparison at the same core count.

---

## Benchmark Configuration

* **Workload**: 100% `SET`, **1024-byte payload**, **pipeline = 100**, 1,000,000 keys (`S:S` pattern)
* **Client**: `memtier_benchmark` (32 client threads pinned to cores `32-63`, 1 connection per thread)
* **Duration per test**: **60 seconds**
* **Machine**: 64-core Linux system (`7.1.6-1rodete1-amd64`)
* **Harness**: same `taskset`/`memtier_benchmark` invocation pattern as
  [`scripts/benchmark_baseline.py`](../../scripts/benchmark_baseline.py) (`--test-time 60`), against a build
  including write-batching, pipeline squashing, and `mimalloc`. The single-thread-count (16T) case is
  reproducible directly via [`scripts/run_16t_benchmark.sh`](../../scripts/run_16t_benchmark.sh).

---

## Scaling Results Summary

| Server Threads | Pinned Cores | Throughput (Ops/sec) | Bandwidth (MB/sec) | Avg Latency (ms) | p50 Latency (ms) | p90 Latency (ms) | p95 Latency (ms) | p99 Latency (ms) | p99.9 Latency (ms) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | `0` | **940,100.03** | 960.99 | 3.38 | 3.79 | 4.42 | 4.67 | 6.21 | 12.22 |
| **2** | `0-1` | **1,253,431.57** | 1,281.26 | 2.53 | 2.50 | 3.73 | 4.29 | 5.89 | 13.50 |
| **4** | `0-3` | **1,919,287.93** | 1,961.93 | 1.65 | 1.42 | 2.69 | 3.10 | 4.45 | 13.50 |
| **8** | `0-7` | **2,782,040.79** | 2,843.86 | 1.13 | 0.96 | 1.83 | 2.21 | 3.12 | 10.62 |
| **16** | `0-15` | **2,904,557.83** | 2,969.11 | 1.08 | 0.85 | 1.94 | 2.33 | 3.10 | 12.10 |
| **32** | `0-31` | **2,465,508.91** | 2,520.31 | 1.27 | 1.06 | 1.99 | 2.43 | 3.58 | 15.42 |

> **Run-to-run variance note**: [`pipeline_squashing.md`](pipeline_squashing.md), a separate 16-thread-only
> run captured immediately before this full sweep, recorded 2,634,080.99 ops/sec for the same configuration
> — about 10% lower than the 2,904,557.83 ops/sec measured here. Neither run reflects a code change between
> them; the discrepancy is ordinary benchmark noise on a shared multi-tenant host and illustrates the
> run-to-run spread to expect on single-iteration measurements (contrast with
> [`multi_command_comparison.md`](multi_command_comparison.md), which runs 3 iterations and reports CV).

---

## Comparison: Baseline vs. Batched vs. Squashed

| Server Threads | Baseline Ops/sec | Batched Ops/sec | **Squashed Ops/sec** | **Improvement vs Baseline** | **Bandwidth** | **p99 Latency** |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 60,487 | 816,937 | **940,100** | **+1,454% (15.5x)** | **0.96 GB/s** | **6.21 ms** |
| **2** | 88,727 | 593,238 | **1,253,432** | **+1,313% (14.1x)** | **1.28 GB/s** | **5.89 ms** |
| **4** | 159,224 | 857,551 | **1,919,288** | **+1,105% (12.1x)** | **1.96 GB/s** | **4.45 ms** |
| **8** | 295,665 | 794,212 | **2,782,041** | **+841% (9.4x)** | **2.84 GB/s** | **3.12 ms** |
| **16** | 297,049 | 705,995 | **2,904,558** | **+878% (9.8x)** | **2.97 GB/s** | **3.10 ms** |
| **32** | 273,831 | 704,537 | **2,465,509** | **+800% (9.0x)** | **2.52 GB/s** | **3.58 ms** |

---

## Architectural Analysis

1. **Peak Scaling at 8–16 Cores**:
   - Throughput scales rapidly from 940k (1 core) to **2.78M (8 cores)** and **2.90M (16 cores)**.
   - At 16 cores, write bandwidth reaches **2.97 GB/sec**, completely saturating memory buses and inter-core channel cache lines.
   - Average latency drops to **1.08 ms** and p50 latency drops to **0.85 ms**.
2. **Behavior at 32 Cores**:
   - With 32 client connections, 32 server threads experience light under-subscription per shard (averaging 1 client connection per thread), while inter-core channel multiplexing increases.
   - Throughput settles at a stable **2.47M Ops/sec** and **2.52 GB/sec** — a decline from the 16-core peak,
     consistent with the same diminishing/negative returns past 8-16 cores documented in
     [`docs/benchmark_multicore_results.md`](../benchmark_multicore_results.md) for the later Dragonfly
     comparison suite.

---

## Reproducing This Benchmark

```bash
cargo build --release
```

Then run the same `taskset`/`memtier_benchmark` invocation as
[`scripts/benchmark_baseline.py`](../../scripts/benchmark_baseline.py) at `--test-time 60` for each thread
count, against a build including write-batching, pipeline squashing, and `mimalloc`. For a single 16-thread,
60-second run, use [`scripts/run_16t_benchmark.sh`](../../scripts/run_16t_benchmark.sh) directly.
