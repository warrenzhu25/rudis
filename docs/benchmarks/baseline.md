# Baseline Benchmark: 1 to 32 Threads

This document records the pre-optimization baseline benchmark results for `rudis` across 1, 2, 4, 8, 16, and 32 worker threads.

---

## Benchmark Configuration

* **Server**: `rudis v0.1.0` (Multi-threaded Shared-Nothing via Monoio `io_uring`)
* **Client**: `memtier_benchmark` (32 client threads pinned to cores `32-63`, 1 connection per thread)
* **Workload**:
  * Command: 100% `SET`
  * Payload size: **1024 bytes**
  * Pipeline depth: **100**
  * Key count: 1,000,000 (`S:S` pattern)
  * Duration per test: **60 seconds**
* **Machine**: 64-core Linux system (`7.1.6-1rodete1-amd64`)

---

## Baseline Results Summary

| Server Threads | Pinned Cores | Throughput (Ops/sec) | Bandwidth (MB/sec) | Avg Latency (ms) | p50 Latency (ms) | p90 Latency (ms) | p95 Latency (ms) | p99 Latency (ms) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | `0` | **60,487.24** | 63.26 | 52.87 | 53.50 | 59.65 | 65.54 | 80.90 |
| **2** | `0-1` | **88,727.36** | 92.83 | 36.05 | 37.12 | 43.26 | 44.80 | 66.56 |
| **4** | `0-3` | **159,224.13** | 166.63 | 20.09 | 20.86 | 24.45 | 25.60 | 41.47 |
| **8** | `0-7` | **295,665.11** | 309.47 | 10.82 | 9.86 | 16.13 | 19.20 | 26.75 |
| **16** | `0-15` | **297,048.94** | 310.92 | 10.77 | 10.18 | 13.63 | 15.17 | 30.72 |
| **32** | `0-31` | **273,831.18** | 286.61 | 11.69 | 10.56 | 16.00 | 17.79 | 34.82 |

---

## Analysis

1. **Near-Linear Scaling (1 → 8 threads)**:
   * Throughput scaled from **60k Ops/sec** to **295k Ops/sec** (~$5\times$ scaling).
   * Average latency dropped from **52.87 ms** down to **10.82 ms** as shard contention was distributed across cores.
2. **Bottleneck at 16–32 threads (Unbatched Writes)**:
   * With 32 client connections and pipeline depth of 100, each thread handled fewer concurrent connections while performing unbatched `stream.write_all()` for every individual parsed command.
   * This generates 100 individual `io_uring` write completion cycles per incoming socket read batch, creating a write-serialization bottleneck.
3. **Target Optimization**:
   * Implement **response write-batching** in `src/connection.rs` so all parsed pipelined commands within a read cycle are coalesced into a single `io_uring` write.
