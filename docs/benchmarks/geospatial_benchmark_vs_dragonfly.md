# Comparative Benchmark: Geospatial Engine (Rudis vs. Dragonfly)

**Date:** 2026-09-18
**Engines:**
- **Rudis v0.1.0** (Commit `e5a4222`+, 8 Monoio `io_uring` worker threads, `mimalloc`)
- **Dragonfly v1.39.0** (Commit `699862e`, 8 proactor threads, `--cache_mode=false`)

**Hardware & Workload Environment:**
- **Host**: AMD EPYC 7B13 64-Core Processor, 117 GiB RAM, Linux 7.1.6 (`x86_64`)
- **Server Pinning**: CPU cores `0-7` (8 dedicated cores, `taskset`)
- **Client Pinning**: CPU cores `32-47` (16 dedicated cores, no overlap with the server)
- **Client Concurrency**: 8 `memtier_benchmark` threads x 8 connections per thread (64 concurrent connections)
- **Pipelining**: Pipeline depth 16
- **Test Duration**: 10 seconds per workload
- **Dataset**: 50,000 geospatial coordinates spread across a synthetic 10.0-18.0°E x 35.0-45.0°N grid (Mediterranean/Southern Europe bounding box), pre-loaded into a single geospatial sorted set (`geo:points`) before the read workloads run

Script: `scripts/benchmark_geo_vs_dragonfly.py`.

---

## 1. Executive Summary & Key Results

Rudis outperforms Dragonfly v1.39.0 across all five measured geospatial workloads:
- **Write Throughput (`GEOADD`)**: Rudis delivers **1,624,620 ops/sec** vs Dragonfly's **503,128 ops/sec** (+222.9%, 3.2x), with p99 tail latency of **3.31 ms** vs Dragonfly's **8.64 ms**.
- **Distance Calculation (`GEODIST`)**: Rudis delivers **2,289,888 ops/sec** vs Dragonfly's **964,097 ops/sec** (+137.5%, 2.4x), with p99 tail latency of **1.65 ms** vs Dragonfly's **5.89 ms**.
- **Circular Spatial Search (`GEORADIUS`)**: Rudis achieves **1,002,478 ops/sec** vs Dragonfly's **381,713 ops/sec** (+162.6%, 2.6x), with p99 tail latency of **3.74 ms** vs Dragonfly's **10.43 ms**.
- **Radius Search (`GEOSEARCH ... BYRADIUS`)**: Rudis achieves **1,138,987 ops/sec** vs Dragonfly's **365,444 ops/sec** (+211.7%, 3.1x), with p99 tail latency of **2.11 ms** vs Dragonfly's **10.75 ms**.
- **Bounding Box Search (`GEOSEARCH ... BYBOX`)**: Rudis achieves **1,119,117 ops/sec** vs Dragonfly's **358,019 ops/sec** (+212.6%, 3.1x), with p99 tail latency of **3.71 ms** vs Dragonfly's **10.82 ms**.

**Verification note**: the percentage deltas and speedup multiples above were recomputed from the raw ops/sec and latency figures in this document and match the stated values exactly. No raw JSON result artifact for this specific run is present in the repository (`scripts/benchmark_geo_vs_dragonfly.py` writes to `benchmark_logs/geo_rudis_vs_dragonfly.json`, which is not checked in), so the absolute ops/sec and latency figures themselves could not be cross-checked against stored machine output — only their internal arithmetic consistency was verified.

---

## 2. Benchmark Methodology

For each engine, the script (`scripts/benchmark_geo_vs_dragonfly.py`):
1. Starts the server pinned to cores 0-7 (`taskset -c 0-7`).
2. Waits for the listening port to accept connections, then bulk-loads 50,000 members into the `geo:points` sorted set via pipelined `GEOADD` calls (batches of 2,000), spanning the synthetic Mediterranean grid described above.
3. Runs five sequential `memtier_benchmark` workloads, each for 10 seconds at pipeline depth 16, 8 threads x 8 connections:
   - **`GEOADD`** — writes new members into a *separate* sorted set (`geo:stream`), with a fixed coordinate pair and a keyspace-randomized member name (`pt:__key__`, 1-50,000). This isolates pure insertion throughput (geohash encoding + skip-list/zset insertion) from the pre-populated read dataset.
   - **`GEODIST`** — computes the distance between a randomized member of `geo:points` and a fixed reference member (`loc:1`), in kilometers.
   - **`GEORADIUS`** — a legacy-API circular query: `GEORADIUS geo:points 13.361389 38.115556 50 km WITHDIST COUNT 10` (50 km radius around Palermo, Italy, with distances and a result cap of 10).
   - **`GEOSEARCH ... BYRADIUS`** — the Redis 6.2+ equivalent: `GEOSEARCH geo:points FROMLONLAT 13.361389 38.115556 BYRADIUS 50 km WITHDIST COUNT 10`.
   - **`GEOSEARCH ... BYBOX`** — a 100 km x 100 km bounding-box search: `GEOSEARCH geo:points FROMLONLAT 13.361389 38.115556 BYBOX 100 100 km WITHDIST COUNT 10`.
4. Records `Totals`/`Sets`/`Gets` throughput and latency (average, p50, p99) as reported by `memtier_benchmark`.

All five commands (`GEOADD`, `GEODIST`, `GEORADIUS`, `GEOSEARCH`, and the `FROMLONLAT`/`BYRADIUS`/`BYBOX` subcommands) were confirmed present in Rudis's command dispatch table (`src/resp.rs`); the geohash interval-pruning logic referenced in Section 3 below (`geohash_search_ranges`) was confirmed present in `src/geo.rs`.

---

## 3. Complete Benchmark Results Table

| Workload | Metric | Dragonfly v1.39.0 | Rudis v0.1.0 | Delta (%) | Speedup |
| :--- | :--- | ---: | ---: | ---: | :---: |
| **`GEOADD` (Write)** | Throughput | 503,128 ops/s | **1,624,620 ops/s** | **+222.9%** | **3.2x** |
| | Avg Latency | 2.030 ms | **0.626 ms** | -69.2% | |
| | p99 Latency | 8.639 ms | **3.311 ms** | -61.7% | |
| **`GEODIST` (Distance)** | Throughput | 964,097 ops/s | **2,289,888 ops/s** | **+137.5%** | **2.4x** |
| | Avg Latency | 1.056 ms | **0.442 ms** | -58.1% | |
| | p99 Latency | 5.887 ms | **1.655 ms** | -71.9% | |
| **`GEORADIUS` (50 km)** | Throughput | 381,713 ops/s | **1,002,478 ops/s** | **+162.6%** | **2.6x** |
| | Avg Latency | 2.676 ms | **1.017 ms** | -62.0% | |
| | p99 Latency | 10.431 ms | **3.743 ms** | -64.1% | |
| **`GEOSEARCH` Radius** | Throughput | 365,444 ops/s | **1,138,987 ops/s** | **+211.7%** | **3.1x** |
| | Avg Latency | 2.795 ms | **0.895 ms** | -68.0% | |
| | p99 Latency | 10.751 ms | **2.111 ms** | -80.4% | |
| **`GEOSEARCH` Box** | Throughput | 358,019 ops/s | **1,119,117 ops/s** | **+212.6%** | **3.1x** |
| | Avg Latency | 2.854 ms | **0.911 ms** | -68.1% | |
| | p99 Latency | 10.815 ms | **3.711 ms** | -65.7% | |

---

## 4. Architectural Analysis

1. **Geohash Bounding-Interval Pruning**: Rudis maps 2D bounding boxes and circles onto 1D geohash (Morton-order) intervals via `geohash_search_ranges` (`src/geo.rs`), then seeks directly into the underlying sorted-set index in $O(\log N)$ rather than performing a linear scan of all 50,000 indexed points.
2. **Thread-per-Core Execution**: Rudis's thread-per-core, shard-local storage architecture avoids cross-thread lock contention on the `geo:points` sorted set during concurrent reads and writes, which is consistent with the throughput advantage observed on both the write-heavy (`GEOADD`) and read-heavy (`GEODIST`, `GEORADIUS`, `GEOSEARCH`) workloads above.
3. **Reproducibility**: Run `python3 scripts/benchmark_geo_vs_dragonfly.py` (with `RUDIS_BIN`, `DRAGONFLY_BIN`, and `MEMTIER_BIN` paths adjusted for your environment) to reproduce these measurements. The script prints a comparison table to stdout and writes detailed per-workload results to `benchmark_logs/geo_rudis_vs_dragonfly.json`.
