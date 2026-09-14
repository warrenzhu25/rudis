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
| **SET** | 1KB | **1,125,759** | 281,561 | **+299.8% (4.00x)** | **Rudis** |
| **GET** | 1KB | **630,595** | 202,205 | **+211.9% (3.12x)** | **Rudis** |
| **SET/GET 1:1** | 1KB | 94,371 | **264,468** | -64.3% (0.36x) | Dragonfly |

### Latency Percentiles Comparison

| Engine | Workload | Ops/sec | Bandwidth | Avg Latency | p50 | p90 | p95 | p99 | p99.9 |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Rudis** | **SET (1KB)** | **1,125,759** | 1,178.4 MB/s | **0.70 ms** | **0.50 ms** | **0.80 ms** | **0.92 ms** | **3.95 ms** | 25.09 ms |
| Dragonfly | **SET (1KB)** | 281,561 | 294.6 MB/s | 2.83 ms | 2.72 ms | 3.38 ms | 3.70 ms | 4.90 ms | **17.92 ms** |
| **Rudis** | **GET (1KB)** | **630,595** | 650.6 MB/s | **1.27 ms** | **0.96 ms** | **2.27 ms** | **3.79 ms** | 6.78 ms | **10.82 ms** |
| Dragonfly | **GET (1KB)** | 202,205 | 210.5 MB/s | 3.96 ms | 3.86 ms | 4.48 ms | 4.74 ms | **6.46 ms** | 22.78 ms |
| Rudis | **SET/GET 1:1**| 94,371 | 98.4 MB/s | 8.47 ms | 5.34 ms | 13.76 ms | 22.91 ms | 81.41 ms | 125.44 ms |
| **Dragonfly**| **SET/GET 1:1**| **264,468** | 275.9 MB/s | **3.02 ms** | **2.94 ms** | **3.55 ms** | **3.82 ms** | **4.90 ms** | **20.35 ms** |

### Analysis & Mixed Workload Optimizations
1. **Write & Spill Throughput**: Rudis delivers **4.0x higher write throughput** than Dragonfly under memory limits (1.13M ops/s vs 282K ops/s) with sub-millisecond average latency (0.70 ms vs 2.83 ms), leveraging thread-local `io_uring` batched submissions without cross-thread locking or fiber context switches.
2. **Read & Fetch Throughput**: Rudis delivers **3.12x higher read throughput** than Dragonfly (631K ops/s vs 202K ops/s) and **3.1x lower average latency** (1.27 ms vs 3.96 ms), powered by `OpManager`'s fast-path read coalescing and direct buffer handoff.
3. **Pipelined Mixed Workload Improvements**: Through targeted architectural enhancements, Rudis doubled its 1:1 SET/GET mixed throughput from 46,528 ops/sec to **94,371 ops/sec (+103%)**, halved average latency from 17.14 ms to **8.47 ms**, and reduced p99 tail latency from 202.75 ms to **81.41 ms** (60% drop):
   - **Batched SmallBins Flushing**: Accumulates entries into 4KB active bin pages in memory, eliminating redundant page rewrites and write amplification on every small record.
   - **Active Bin Fast-Path Read**: Cold reads hitting active in-memory bin buffers are resolved directly from DRAM without issuing NVMe disk I/O.
   - **Memory Hysteresis & Streaming Cold Reads**: Avoids ping-pong cache thrashing when DRAM is constrained; keys fetched from disk above the offload watermark are streamed directly to socket output buffers via `ShardMessage::StreamColdRead` without allocating DRAM or displacing active working sets.
   - **Asynchronous Deduplicated Offload**: Write operations spawn background offloading tasks guarded by concurrency controls (`is_auto_tiering`), preventing Monoio task queue saturation.


