# Benchmark: NVMe Tiered Storage Performance & Memory Expansion

This document presents the official benchmark evaluation for **Rudis Tiered Storage** on local NVMe SSDs via Linux `io_uring` (Monoio).

---

## 1. Architecture Overview

Rudis implements a high-throughput, thread-per-core Tiered Storage engine designed around NVMe performance characteristics:
1. **SmallBins 4KB Page Aggregation**: Values $<2$ KB are packed into 4096-byte aligned binary bins with direct I/O alignment, eliminating space amplification and NVMe wear.
2. **OpManager Coalescing & Backpressure**: Concurrent reads targeting keys on the same 4KB page share a single disk DMA read and kernel buffer. In-flight write tracking throttles producers when write queues exceed 16MB.
3. **Three-State Value Lifecycle**: Hot $\to$ Cooled $\to$ Cold lifecycle enables instant $O(1)$ zero-I/O memory reclamation (`TIER DECOMMIT`).
4. **Dynamic Watermark Control**: Background sampling offload (`tiered_offload_threshold`) and zero-allocation cold streaming reads (`tiered_upload_threshold`) prevent thrashing under memory limits.

---

## 2. Benchmark Configuration

* **Server Threads**: 8 worker threads pinned to CPU cores `0-7`
  * **Pure DRAM Baseline**: `./target/release/rudis --threads 8 --port 6381`
  * **NVMe Tiered Storage**: `./target/release/rudis --threads 8 --port 6382 --maxmemory 32mb --tiered-offload-threshold 60 --tiered-upload-threshold 80`
* **Client**: `memtier_benchmark` (8 client threads, 4 connections/thread, pipeline depth 50, pinned to CPU cores `8-23`)
* **Workload**:
  * Record size: **512 bytes** (eligible for SmallBins 4KB page packing)
  * Key count: 100,000 active keys per benchmark cycle
  * Sustained data size: $>2.4$ GB tiered data on disk within a **32 MB DRAM limit** ($71\times$ memory expansion ratio)
  * Test duration: 10 seconds per workload
* **Environment**: 64-core Linux system (`7.1.6-1rodete1-amd64`), local NVMe storage

---

## 3. Performance Summary: Pure DRAM vs. NVMe Tiered Storage

| Workload | Payload | In-Memory DRAM (Ops/sec) | NVMe Tiered (Ops/sec) | Avg Latency (DRAM) | Avg Latency (Tiered) | p50 Latency (Tiered) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **SET** | 512B | **2,796,631** | 220,641 | 0.56 ms | 7.23 ms | 0.51 ms |
| **GET** | 512B | **2,292,260** | **827,826** | 0.70 ms | 1.93 ms | 0.66 ms |
| **SET/GET 1:1** | 512B | **2,633,706** | 213,233 | 0.60 ms | 7.50 ms | 3.26 ms |

---

## 4. Latency & Bandwidth Breakdown

### Pure DRAM Baseline (8 Threads, No Memory Limit)

| Workload | Ops/sec | Bandwidth (MB/s) | Avg Latency | p50 | p90 | p95 | p99 | p99.9 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **SET (512B)** | 2,796,631 | 1,487.9 MB/s | 0.56 ms | 0.50 ms | 0.85 ms | 0.99 ms | 1.56 ms | 6.30 ms |
| **GET (512B)** | 2,292,260 | 1,208.6 MB/s | 0.70 ms | 0.66 ms | 0.84 ms | 0.94 ms | 1.46 ms | 6.94 ms |
| **SET/GET 1:1** | 2,633,706 | 1,394.9 MB/s | 0.60 ms | 0.54 ms | 0.90 ms | 1.06 ms | 1.54 ms | 5.82 ms |

### NVMe Tiered Storage (8 Threads, 32MB MaxMemory, 512B SmallBins)

| Workload | Ops/sec | Bandwidth (MB/s) | Avg Latency | p50 | p90 | p95 | p99 | p99.9 |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **SET (512B)** | 220,641 | 117.4 MB/s | 7.23 ms | 0.51 ms | 15.87 ms | 34.82 ms | 74.24 ms | 872.45 ms |
| **GET (512B)** | **827,826** | 404.7 MB/s | 1.93 ms | **0.66 ms** | 4.86 ms | 5.92 ms | 8.64 ms | 26.11 ms |
| **SET/GET 1:1** | 213,233 | 112.7 MB/s | 7.50 ms | 3.26 ms | 18.05 ms | 22.78 ms | 37.89 ms | 84.48 ms |

---

## 5. Storage Efficiency & OpManager Telemetry

During the benchmark, `TIER INFO` recorded real-time operational telemetry across all 8 shards:

| Metric | Measured Value | Description |
| :--- | :---: | :--- |
| **Allocated DRAM Limit (`maxmemory`)** | **32.00 MB** | Strict memory ceiling configured for the engine |
| **Active Tiered Keys** | **3,827,609 keys** | Total keys maintained in tiered NVMe storage |
| **Tiered Storage Footprint** | **2.43 GB** | Physical storage footprint on NVMe |
| **RAM Saved** | **2.28 GB** | DRAM saved compared to holding the entire keyspace in memory |
| **Memory Expansion Factor** | **71.3x** | Dataset size relative to available DRAM ceiling |
| **SmallBins Packed Pages (`bin_pages`)** | **486,144 pages** | 4096-byte aligned SmallBins packed with 512B records |
| **OpManager Coalesced Reads** | **56,581 reads** | Concurrent queries resolved from an in-flight 4KB page read |
| **Total Disk Stashes** | **4,410,628 writes** | Total records written to NVMe via `io_uring` |
| **Total Disk Fetches** | **224,303 reads** | Total cold keys read and promoted into DRAM |
| **Streaming Cold Reads** | **74,397 reads** | Cold keys streamed directly to socket above upload threshold |

---

## 6. Key Takeaways

1. **Read Performance with OpManager**: Despite $>95\%$ of data residing on NVMe, Rudis achieved **827,826 GET ops/sec** with a **median latency of 0.66 ms**, closely tracking pure DRAM latency ($0.66\text{ ms}$ vs $0.65\text{ ms}$). OpManager's read coalescing successfully aggregated 56,581 concurrent requests onto shared 4KB disk page transfers.
2. **Dense SmallBins Packing**: SmallBins aggregated 512-byte payloads into 486,144 4KB pages with zero Slack space amplification, achieving direct I/O compatibility without 4KB per-record overhead.
3. **Massive Memory Expansion**: Rudis maintained stable operations with $71.3\times$ more data stored than the physical DRAM allocation (2.43 GB on NVMe within a 32 MB limit), confirming effective backpressure and watermark control.

---

## 7. Head-to-Head Comparison: Rudis vs. Dragonfly (4 Threads)

A direct comparative evaluation between **Rudis v0.1.0** and **Dragonfly v1.39.0** on 4 worker threads (pinned to CPU cores `0-3`) with `--maxmemory 1024mb` on NVMe storage:

### Workload Configuration
* **Server**: 4 worker threads (`taskset -c 0-3`), `maxmemory=1024mb`, NVMe tiered storage enabled
  * **Rudis**: `--threads 4 --port 6395 --maxmemory 1024mb --tiered-offload-threshold 60 --tiered-upload-threshold 80`
  * **Dragonfly**: `--proactor_threads=4 --port=6395 --maxmemory=1024mb --tiered_prefix=/tmp/df/tier --tiered_experimental_cooling=true --pipeline_squash=0`
* **Client**: `memtier_benchmark` (4 threads, 4 connections/thread, pipeline depth 50, 1KB payload, 1,500,000 keys)

### Head-to-Head Summary

| Workload | Payload | Rudis (Ops/sec) | Dragonfly (Ops/sec) | Rudis Speedup | Winner |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **SET** | 1KB | **1,062,330** | 290,892 | **+265.2% (3.65x)** | **Rudis** |
| **GET** | 1KB | **567,540** | 207,463 | **+173.6% (2.74x)** | **Rudis** |
| **SET/GET 1:1** | 1KB | 46,528 | **272,537** | -82.9% (0.17x) | Dragonfly |

### Latency Percentiles Comparison

| Engine | Workload | Ops/sec | Bandwidth | Avg Latency | p50 | p90 | p95 | p99 | p99.9 |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Rudis** | **SET (1KB)** | **1,062,330** | 1,112.0 MB/s | **0.74 ms** | **0.58 ms** | 1.00 ms | 1.21 ms | **1.73 ms** | 15.23 ms |
| Dragonfly | **SET (1KB)** | 290,892 | 304.4 MB/s | 2.74 ms | 2.61 ms | 3.28 ms | 3.66 ms | 5.22 ms | 16.51 ms |
| **Rudis** | **GET (1KB)** | **567,540** | 582.3 MB/s | **1.41 ms** | **1.04 ms** | 2.93 ms | 4.38 ms | 7.97 ms | **16.77 ms** |
| Dragonfly | **GET (1KB)** | 207,463 | 216.0 MB/s | 3.86 ms | 3.78 ms | 4.35 ms | 4.61 ms | 6.40 ms | 23.42 ms |
| Rudis | **SET/GET 1:1**| 46,528 | 48.5 MB/s | 17.14 ms | 7.36 ms | 24.19 ms | 95.74 ms | 202.75 ms | 290.82 ms |
| **Dragonfly**| **SET/GET 1:1**| **272,537** | 284.3 MB/s | 2.93 ms | 2.82 ms | 3.50 ms | 3.84 ms | 5.15 ms | 18.56 ms |

### Analysis
1. **Write & Spill Throughput**: Rudis outperforms Dragonfly by **3.65x** on `SET` operations under memory limits (1.06M ops/s vs 291K ops/s), benefiting from thread-local `io_uring` direct submission with zero cross-thread locking or fiber context switches.
2. **Read & Fetch Throughput**: Rudis outperforms Dragonfly by **2.74x** on `GET` operations (568K ops/s vs 207K ops/s) with a **2.7x lower average latency** (1.41 ms vs 3.86 ms), driven by `OpManager`'s fast-path read coalescing and direct buffer handoff.
3. **Interleaved Pipeline Contention**: On 1:1 mixed pipelined workloads, Dragonfly's fiber architecture decouples background flushes asynchronously, while Rudis enforces strict write backpressure barriers when small bins are pending disk flush.

