# Benchmark: Cross-Shard Pipeline Squashing vs. Dragonfly (16 Threads)

This document records the benchmark results of `rudis` after implementing **cross-shard pipeline squashing**, **pre-allocated reusable connection responders**, and the **`mimalloc` global allocator**.

---

## Benchmark Configuration

* **Workload**: 100% `SET`, **1024-byte payload**, **pipeline = 100**, 1,000,000 keys (`S:S` pattern)
* **Client**: `memtier_benchmark` (32 client threads pinned to cores `32-63`, 1 connection per thread)
* **Server**: 16 threads pinned to cores `0-15`
* **Duration**: **60 seconds**
* **Machine**: 64-core Linux system (`7.1.6-1rodete1-amd64`)

---

## 16-Thread Comparison: Rudis Evolution vs. Dragonfly

| Engine / Configuration | Threads | Ops/sec | Bandwidth (MB/s) | Avg Latency | p50 | p90 | p95 | p99 | Speedup vs. Baseline |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Rudis (Baseline)** | 16T | 297,048.94 | 310.9 MB/s | 10.77 ms | 9.85 ms | 18.37 ms | 21.31 ms | 30.72 ms | 1.0x |
| **Rudis (Write-Batched)** | 16T | 705,995.32 | 739.0 MB/s | 4.51 ms | 4.08 ms | 5.92 ms | 6.37 ms | 7.49 ms | 2.38x |
| **Dragonfly** | 16T | 1,910,371.13 | 1,999.7 MB/s | 1.64 ms | 1.49 ms | 2.35 ms | 2.70 ms | 3.95 ms | 6.43x |
| **Rudis (Squashed + mimalloc)** | **16T** | **2,634,080.99** | **2,757.2 MB/s** | **1.19 ms** | **1.02 ms** | **1.86 ms** | **2.14 ms** | **2.85 ms** | **8.87x** |

---

## Detailed Metrics: Rudis 16 Threads (60 Seconds)

```
32        Threads
1         Connections per thread
60        Seconds

ALL STATS
============================================================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p90 Latency     p95 Latency     p99 Latency   p99.9 Latency       KB/sec 
------------------------------------------------------------------------------------------------------------------------------------------------------------
Sets      2634080.99          ---          ---         1.19376         1.02300         1.86300         2.14300         2.84700        13.43900   2757241.72 
Gets            0.00         0.00         0.00             ---             ---             ---             ---             ---             ---         0.00 
Waits           0.00          ---          ---             ---             ---             ---             ---             ---             ---          --- 
Totals    2634080.99         0.00         0.00         1.19376         1.02300         1.86300         2.14300         2.84700        13.43900   2757241.72 
```

---

## Key Takeaways

1. **Surpassing Dragonfly on 16 Cores**:
   - `rudis` achieved **2.634 Million Ops/sec** and **2.76 GB/sec** write throughput, outperforming Dragonfly's **1.910 Million Ops/sec** by **+37.9%**.
2. **Sub-3ms p99 Tail Latency Under Heavy Load**:
   - P99 latency dropped to **2.85 ms** (compared to Dragonfly's 3.95 ms and previous Rudis's 7.49 ms).
   - Average latency decreased to **1.19 ms** (vs Dragonfly's 1.64 ms).
3. **Cross-Shard Pipeline Parallelism**:
   - Grouping pipelined requests into single batched hops per destination shard converted serialized cross-thread hops into parallel concurrent execution across all available cores.
   - Combined with zero channel allocations from pre-allocated connection responders and lock-free thread-local heap allocation from `mimalloc`, lock/waker contention was virtually eliminated.
