# Comparative Benchmark: Geospatial Engine (Rudis vs. Dragonfly)

**Date:** 2026-09-18  
**Engines:**  
- **Rudis v0.1.0** (Commit `e5a4222`+, 8 Monoio `io_uring` worker threads, `mimalloc`)
- **Dragonfly v1.39.0** (Commit `699862e`, 8 proactor threads, `--cache_mode=false`)  

**Hardware & Workload Environment:**
- **Host**: AMD EPYC 7B13 64-Core Processor, 117 GiB RAM, Linux 7.1.6 (`x86_64`)
- **Server Pinning**: Cores `0-7` (8 dedicated CPU cores)
- **Client Pinning**: Cores `32-47` (16 dedicated client CPU cores, 0 overlap)
- **Client Concurrency**: 8 client threads, 8 connections per thread (64 concurrent connections)
- **Pipelining**: Pipeline depth 16
- **Dataset Size**: 50,000 active geospatial coordinates indexed into sorted set `geo:points`

---

## 1. Executive Summary & Key Results

Rudis outperforms Dragonfly v1.39.0 across **all 5 geospatial workloads**:
- **Write Throughput (`GEOADD`)**: Rudis delivers **1,624,620 ops/sec** vs Dragonfly's **503,128 ops/sec** (**+222.9% faster / 3.2x**), with a p99 tail latency of **3.31 ms** vs Dragonfly's **8.64 ms**.
- **Distance Calculation (`GEODIST`)**: Rudis delivers **2,289,888 ops/sec** vs Dragonfly's **964,097 ops/sec** (**+137.5% faster / 2.4x**), with p99 tail latency of **1.65 ms** vs Dragonfly's **5.89 ms**.
- **Circular Spatial Search (`GEORADIUS`)**: Rudis achieves **1,002,478 ops/sec** vs Dragonfly's **381,713 ops/sec** (**+162.6% faster / 2.6x**), with p99 tail latency of **3.74 ms** vs Dragonfly's **10.43 ms**.
- **Radius Search (`GEOSEARCH BYRADIUS`)**: Rudis achieves **1,138,987 ops/sec** vs Dragonfly's **365,444 ops/sec** (**+211.7% faster / 3.1x**), with p99 tail latency of **2.11 ms** vs Dragonfly's **10.75 ms**.
- **Bounding Box Search (`GEOSEARCH BYBOX`)**: Rudis achieves **1,119,117 ops/sec** vs Dragonfly's **358,019 ops/sec** (**+212.6% faster / 3.1x**), with p99 tail latency of **3.71 ms** vs Dragonfly's **10.82 ms**.

---

## 2. Complete Benchmark Results Table

| Workload | Metric | Dragonfly v1.39.0 | Rudis v0.1.0 | Delta (%) | Speedup |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`GEOADD` (Write)** | Throughput | 503,128 ops/s | **1,624,620 ops/s** | **+222.9%** | **3.2x** |
| | Avg Latency | 2.030 ms | **0.626 ms** | -69.2% | |
| | p99 Latency | 8.639 ms | **3.311 ms** | **-61.7%** | |
| **`GEODIST` (Distance)** | Throughput | 964,097 ops/s | **2,289,888 ops/s** | **+137.5%** | **2.4x** |
| | Avg Latency | 1.056 ms | **0.442 ms** | -58.1% | |
| | p99 Latency | 5.887 ms | **1.655 ms** | **-71.9%** | |
| **`GEORADIUS` (50km)** | Throughput | 381,713 ops/s | **1,002,478 ops/s** | **+162.6%** | **2.6x** |
| | Avg Latency | 2.676 ms | **1.017 ms** | -62.0% | |
| | p99 Latency | 10.431 ms | **3.743 ms** | **-64.1%** | |
| **`GEOSEARCH` Radius** | Throughput | 365,444 ops/s | **1,138,987 ops/s** | **+211.7%** | **3.1x** |
| | Avg Latency | 2.795 ms | **0.895 ms** | -68.0% | |
| | p99 Latency | 10.751 ms | **2.111 ms** | **-80.4%** | |
| **`GEOSEARCH` Box** | Throughput | 358,019 ops/s | **1,119,117 ops/s** | **+212.6%** | **3.1x** |
| | Avg Latency | 2.854 ms | **0.911 ms** | -68.1% | |
| | p99 Latency | 10.815 ms | **3.711 ms** | **-65.7%** | |

---

## 3. Architectural Analysis

1. **Geohash Bounding Interval Pruning**:
   - Rudis maps 2D bounding boxes and circles to 1D Morton space-filling intervals (`geohash_search_ranges`), seeking directly into the underlying BTree in $O(\log N)$.
   - Instead of scanning all 50,000 spatial points across Europe, only the candidate geohash intervals covering the target region are queried.
2. **Lock-Free Thread-per-Core Execution**:
   - Rudis's thread-local storage architecture avoids mutex contention during sorted set reads and writes, achieving over 1.62 million GEOADDs/sec and over 2.28 million GEODIST evaluations/sec.
3. **Reproducibility**:
   - Run `python3 scripts/benchmark_geo_vs_dragonfly.py` to reproduce these measurements.
