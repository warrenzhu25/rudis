# Benchmark: Multi-Command Performance Comparison (Rudis vs. Dragonfly)

This document records the official benchmark suite comparing **Rudis v0.1.0** and **Dragonfly v1.39.0** across 8 core Redis workloads on a 16-thread configuration.

---

## Benchmark Configuration

* **Server Threads**: 16 worker threads pinned to CPU cores `0-15`
  * **Rudis**: `./target/release/rudis --threads 16 --port 6379`
  * **Dragonfly**: `/usr/local/google/home/warrenzhu/dragonfly --proactor_threads=16 --port 6379`
* **Client**: `memtier_benchmark` (32 client threads pinned to cores `32-63`, 1 connection per thread)
* **Workload Specifications**:
  * Payload size: **1024 bytes** (for `SET`, `GET`, `SET/GET`, `LPUSH`, `HSET`)
  * Pipeline depth: **100**
  * Key space: 1,000,000 keys (`S:S` pattern)
  * Test duration: **10 seconds per command**
  * Prior to read benchmarks (`GET`, `HGET`), keys were pre-populated to ensure 100% cache hit rate.
* **Environment**: 64-core Linux system (`7.1.6-1rodete1-amd64`)

---

## Head-to-Head Summary: Rudis vs. Dragonfly (16 Threads)

| Workload | Payload | Rudis (Ops/sec) | Dragonfly (Ops/sec) | Rudis Speedup | Winner |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **SET** | 1KB | **3,415,601** | 1,981,566 | **+72.4% (1.72x)** | **Rudis** |
| **GET** | 1KB | **3,301,207** | 520,571 | **+534.2% (6.34x)** | **Rudis** |
| **SET/GET 1:1** | 1KB | **3,075,108** | 818,964 | **+275.5% (3.76x)** | **Rudis** |
| **LPUSH** | 1KB | **2,774,462** | 1,658,130 | **+67.3% (1.67x)** | **Rudis** |
| **HSET** | 1KB | **2,636,209** | 2,181,202 | **+20.9% (1.21x)** | **Rudis** |
| **HGET** | 1KB | **2,892,417** | 525,713 | **+450.2% (5.50x)** | **Rudis** |
| **INCR** | Small | **4,175,571** | 3,998,671 | **+4.4% (1.04x)** | **Rudis** |
| **ZADD** | Small | 3,468,035 | **3,849,669** | -9.9% (0.90x) | Dragonfly |

---

## Detailed Latency & Bandwidth Metrics

### Rudis (16 Threads)

| Workload | Ops/sec | Bandwidth (MB/s) | Avg Latency | p50 | p90 | p95 | p99 | p99.9 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **SET (1KB)** | **3,415,601** | 3,491.4 MB/s | 0.92 ms | 0.81 ms | 1.34 ms | 1.57 ms | 2.17 ms | 13.50 ms |
| **GET (1KB)** | **3,301,207** | 1,658.3 MB/s | 0.95 ms | 0.85 ms | 1.48 ms | 1.76 ms | 2.45 ms | 13.06 ms |
| **SET/GET 1:1** | **3,075,108** | 3,135.8 MB/s | 1.04 ms | 0.90 ms | 1.63 ms | 1.94 ms | 2.70 ms | 12.86 ms |
| **LPUSH (1KB)** | **2,774,462** | 2,840.5 MB/s | 1.14 ms | 1.02 ms | 1.67 ms | 1.94 ms | 2.58 ms | 13.44 ms |
| **HSET (1KB)** | **2,636,209** | 2,722.4 MB/s | 1.20 ms | 0.99 ms | 1.77 ms | 2.09 ms | 2.93 ms | 14.78 ms |
| **HGET (1KB)** | **2,892,417** | 1,354.6 MB/s | 1.09 ms | 0.93 ms | 1.64 ms | 1.99 ms | 3.07 ms | 12.80 ms |
| **INCR** | **4,175,571** | 157.8 MB/s | 0.73 ms | 0.66 ms | 1.01 ms | 1.15 ms | 1.58 ms | 7.97 ms |
| **ZADD** | **3,468,035** | 194.6 MB/s | 0.89 ms | 0.81 ms | 1.23 ms | 1.40 ms | 1.96 ms | 9.92 ms |

### Dragonfly (16 Threads)

| Workload | Ops/sec | Bandwidth (MB/s) | Avg Latency | p50 | p90 | p95 | p99 | p99.9 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **SET (1KB)** | 1,981,566 | 2,025.5 MB/s | 1.58 ms | 1.46 ms | 2.15 ms | 2.39 ms | 3.15 ms | 13.82 ms |
| **GET (1KB)** | 520,571 | 529.4 MB/s | 6.14 ms | 6.14 ms | 7.04 ms | 7.36 ms | 9.60 ms | 26.24 ms |
| **SET/GET 1:1** | 818,964 | 834.6 MB/s | 3.90 ms | 3.79 ms | 4.54 ms | 4.80 ms | 5.98 ms | 19.33 ms |
| **LPUSH (1KB)** | 1,658,130 | 1,697.5 MB/s | 1.89 ms | 1.74 ms | 2.62 ms | 2.94 ms | 3.90 ms | 16.51 ms |
| **HSET (1KB)** | 2,181,202 | 2,252.5 MB/s | 1.43 ms | 1.32 ms | 1.90 ms | 2.11 ms | 2.80 ms | 12.86 ms |
| **HGET (1KB)** | 525,713 | 540.6 MB/s | 6.08 ms | 6.05 ms | 7.04 ms | 7.39 ms | 10.05 ms | 26.24 ms |
| **INCR** | 3,998,671 | 151.0 MB/s | 0.76 ms | 0.69 ms | 1.07 ms | 1.25 ms | 1.70 ms | 8.32 ms |
| **ZADD** | 3,849,669 | 215.9 MB/s | 0.79 ms | 0.73 ms | 1.08 ms | 1.25 ms | 1.73 ms | 8.00 ms |

---

## Architectural Findings

1. **Massive Read Advantage (5.5x – 6.34x Faster)**:
   * Rudis delivers **3.30M Ops/s on GET** and **2.89M Ops/s on HGET** compared to Dragonfly's ~520k Ops/s.
   * Rudis averages **0.95–1.09 ms latency** vs. Dragonfly's **6.08–6.14 ms latency** on reads.
2. **Superior Write Bandwidth (+72% Throughput)**:
   * On 1KB `SET` and `LPUSH`, Rudis reaches **3.49 GB/s** (3.42M Ops/s) and **2.84 GB/s** (2.77M Ops/s).
   * Dragonfly reaches 2.03 GB/s and 1.70 GB/s respectively.
3. **Leading on Integer Arithmetic (`INCR`)**:
   * With stack-allocated integer formatting (`format_i64`), direct byte parsing (`parse_i64_bytes`), and zero-allocation integer response serialization (`write_resp_integer`), Rudis achieves **4.18M Ops/s**, outperforming Dragonfly's **4.00M Ops/s** with lower average latency (0.73 ms vs 0.76 ms).
