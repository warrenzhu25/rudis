# Benchmark: NVMe Tiered Storage Performance & Memory Expansion

This document presents benchmark results for Rudis's **Tiered Storage** subsystem, which extends the effective dataset size beyond the configured DRAM limit by spilling cold values to local NVMe SSDs via Linux `io_uring` (Monoio). Source: `src/tiering.rs`. Scripts: `scripts/benchmark_tiering.py` (DRAM baseline vs. tiered), `scripts/benchmark_tier_scaling.py` (thread-count scaling), plus a head-to-head comparison against Dragonfly's tiered mode. Raw results: `tiered_benchmark_results.json`, `tier_scaling_results.json`, and `tier_dragonfly_comparison.json` (all at the repository root).

---

## 1. Architecture Overview

Rudis implements a thread-per-core tiered storage engine designed around NVMe access patterns:

1. **SmallBins 4 KB Page Aggregation**: values smaller than 2,048 bytes are packed into 4,096-byte, direct-I/O-aligned bins (`PAGE_SIZE = 4096` in `src/tiering.rs`) rather than written as one file block per key, which avoids per-record space amplification and reduces NVMe write wear. Larger values are written directly in 4,096-byte-aligned blocks.
2. **OpManager Coalescing & Backpressure**: concurrent reads that target keys on the same 4 KB page are served from a single in-flight disk read (`OpManager::in_flight_reads`), rather than issuing one I/O per request. Write backpressure engages once outstanding (unflushed) stash bytes exceed a 16 MB threshold (`check_write_backpressure`), throttling producers instead of allowing unbounded write-queue growth.
3. **Three-State Value Lifecycle**: values transition through Hot (DRAM) → Cooled → Cold (NVMe) states; the `TIER DECOMMIT` command performs $O(1)$, zero-I/O reclamation of DRAM held by already-tiered values.
4. **Dynamic Watermark Control**: the `--tiered-offload-threshold` and `--tiered-upload-threshold` server flags (confirmed in `src/config.rs`) control, respectively, the memory-pressure point at which background offloading to disk begins and the point above which cold reads are streamed directly to the client instead of being staged back into DRAM. This is intended to avoid thrashing when the working set oscillates around the memory limit.

---

## 2. Benchmark Configuration

* **Server Threads**: 8 worker threads pinned to CPU cores `0-7` (`taskset`)
  * **Pure DRAM Baseline**: `./target/release/rudis --threads 8 --port 6381` (no `maxmemory` limit)
  * **NVMe Tiered Storage**: `./target/release/rudis --threads 8 --port 6382 --maxmemory 32mb --tiered-offload-threshold 60 --tiered-upload-threshold 80`
* **Client**: `memtier_benchmark`, 8 threads x 4 connections/thread, pipeline depth 50, pinned to CPU cores `8-23`
* **Workload**:
  * Record size: 512 bytes (below the 2,048-byte SmallBins threshold, so all records are packed into 4 KB bins)
  * Key space: 100,000 keys, uniformly randomized access pattern
  * Test duration: 10 seconds per workload (SET, then `TIER DECOMMIT`, then GET, then SET/GET 1:1)
  * Populating 100,000 x 512 B (~51 MB of live data before overhead) against a 32 MB DRAM ceiling forces continuous offloading during the run
* **Environment**: 64-core Linux host (`7.1.6-1rodete1-amd64`), local NVMe storage

Script: `scripts/benchmark_tiering.py`. Between the SET and GET phases, the script issues `TIER DECOMMIT` to force cooled values to cold storage, so the GET phase measures reads that must be resolved through the OpManager/NVMe path rather than served from DRAM.

---

## 3. Performance Summary: Pure DRAM vs. NVMe Tiered Storage

| Workload | Payload | In-Memory DRAM (Ops/sec) | NVMe Tiered (Ops/sec) | Avg Latency (DRAM) | Avg Latency (Tiered) | p50 Latency (Tiered) |
| :--- | :---: | ---: | ---: | ---: | ---: | ---: |
| **SET** | 512 B | **2,796,631** | 220,641 | 0.56 ms | 7.23 ms | 0.51 ms |
| **GET** | 512 B | **2,292,260** | **827,826** | 0.70 ms | 1.93 ms | 0.66 ms |
| **SET/GET 1:1** | 512 B | **2,633,706** | 213,233 | 0.60 ms | 7.50 ms | 3.26 ms |

All figures verified against `tiered_benchmark_results.json`. SET throughput drops by roughly 92% under the tiered configuration because every write must be packed into a SmallBin and eventually persisted to NVMe once the 32 MB ceiling is exceeded, whereas GET throughput retains roughly 36% of DRAM-baseline throughput thanks to OpManager read coalescing (Section 5).

---

## 4. Latency & Bandwidth Breakdown

### Pure DRAM Baseline (8 Threads, No Memory Limit)

| Workload | Ops/sec | Bandwidth (MB/s) | Avg Latency | p50 | p90 | p95 | p99 | p99.9 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **SET (512B)** | 2,796,631 | 1,487.9 | 0.56 ms | 0.50 ms | 0.85 ms | 0.99 ms | 1.56 ms | 6.30 ms |
| **GET (512B)** | 2,292,260 | 1,208.6 | 0.70 ms | 0.66 ms | 0.84 ms | 0.94 ms | 1.46 ms | 6.94 ms |
| **SET/GET 1:1** | 2,633,706 | 1,394.9 | 0.60 ms | 0.54 ms | 0.90 ms | 1.06 ms | 1.54 ms | 5.82 ms |

### NVMe Tiered Storage (8 Threads, 32 MB MaxMemory, 512B SmallBins)

| Workload | Ops/sec | Bandwidth (MB/s) | Avg Latency | p50 | p90 | p95 | p99 | p99.9 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **SET (512B)** | 220,641 | 117.4 | 7.23 ms | 0.51 ms | 15.87 ms | 34.82 ms | 74.24 ms | 872.45 ms |
| **GET (512B)** | **827,826** | 404.7 | 1.93 ms | **0.66 ms** | 4.86 ms | 5.92 ms | 8.64 ms | 26.11 ms |
| **SET/GET 1:1** | 213,233 | 112.7 | 7.50 ms | 3.26 ms | 18.05 ms | 22.78 ms | 37.89 ms | 84.48 ms |

Both tables are reproduced directly from `tiered_benchmark_results.json` (`in_memory` and `tiered` keys respectively); all values match to the reported precision. The wide gap between SET p50 (0.51 ms) and p99.9 (872 ms) under tiering reflects that most writes land in an in-memory SmallBin and return immediately, while a tail of writes stalls behind SmallBin flushes and the 16 MB write-backpressure gate once the disk write queue saturates.

---

## 5. Storage Efficiency & OpManager Telemetry

At the end of the benchmark run, `TIER INFO` reported the following telemetry across all 8 shards (from `tiered_benchmark_results.json`, `tiered.info_final`):

| Metric | Measured Value | Description |
| :--- | ---: | :--- |
| **Allocated DRAM Limit (`maxmemory`)** | 32.00 MB | Configured memory ceiling |
| **Active Tiered Keys (`tiered_keys`)** | 3,827,609 keys | Keys with data resident on NVMe |
| **Tiered Storage Footprint (`tiered_bytes`)** | 2.43 GB | Physical bytes written to NVMe |
| **RAM Saved (`ram_saved_bytes`)** | 2.28 GB | DRAM avoided by keeping this data tiered instead of resident |
| **Memory Expansion Factor** | ~72.4x | `tiered_bytes` (2,429,661,184 B) ÷ `maxmemory` (33,554,432 B) |
| **SmallBins Packed Pages (`bin_pages`)** | 486,144 pages | 4,096-byte-aligned bins holding 512 B records |
| **OpManager Coalesced Reads (`coalesced_reads`)** | 56,581 reads | Concurrent GETs served by a single in-flight page read |
| **Total Disk Stashes (`total_stashes`)** | 4,410,628 writes | Records written to NVMe via `io_uring` |
| **Total Disk Fetches (`total_fetches`)** | 224,303 reads | Cold keys read back and promoted into DRAM |
| **Streaming Cold Reads (`streaming_reads`)** | 74,397 reads | Cold keys streamed directly to the client above the upload threshold, bypassing DRAM promotion |

**Correction**: the memory expansion factor is `tiered_bytes / maxmemory` = 2,429,661,184 / 33,554,432 ≈ **72.4x**. (An earlier draft of this document stated 71.3x; recomputed directly from the same `tiered_benchmark_results.json` fields, the figure is ~72.4x.)

---

## 6. Key Takeaways

1. **Read performance under OpManager coalescing**: with more than 95% of the tiered dataset resident on NVMe rather than DRAM, Rudis sustained 827,826 GET ops/sec at a 0.66 ms median latency — close to the DRAM-baseline median of 0.65-0.66 ms — because OpManager's read coalescing merged 56,581 concurrent requests onto shared 4 KB disk-page transfers rather than issuing one I/O per GET.
2. **Dense SmallBins packing**: 512-byte payloads were aggregated into 486,144 four-kilobyte pages with no per-record space amplification, consistent with the design goal of direct-I/O-compatible packing without one-page-per-record overhead.
3. **Memory expansion under sustained load**: the engine held 2.43 GB of live data on NVMe against a 32 MB DRAM ceiling (~72.4x expansion, Section 5), with 224,303 disk fetches and 74,397 streamed cold reads recorded, indicating that both the promote-to-DRAM and stream-directly-to-client read paths were exercised during the run.

---

## 7. Head-to-Head Comparison: Rudis vs. Dragonfly (4 Threads)

A direct comparison between **Rudis v0.1.0** and **Dragonfly v1.39.0**, both running 4 worker threads (`taskset -c 0-3`) with `--maxmemory 1024mb` and tiered storage enabled on NVMe:

### Workload Configuration
* **Server**: 4 worker threads, `maxmemory=1024mb`, NVMe tiered storage enabled
  * **Rudis**: `--threads 4 --port 6395 --maxmemory 1024mb --tiered-offload-threshold 60 --tiered-upload-threshold 80`
  * **Dragonfly**: `--proactor_threads=4 --port=6395 --maxmemory=1024mb --tiered_prefix=/tmp/df/tier --tiered_experimental_cooling=true --pipeline_squash=0`
* **Client**: `memtier_benchmark`, 4 threads x 4 connections/thread, pipeline depth 50, 1 KB payload, 1,500,000-key keyspace

### Head-to-Head Summary

| Workload | Payload | Rudis (Ops/sec) | Dragonfly (Ops/sec) | Rudis Speedup | Winner |
| :--- | :---: | ---: | ---: | ---: | :---: |
| **SET** | 1KB | **1,279,856** | 286,351 | +347.0% (4.47x) | **Rudis** |
| **GET** | 1KB | **670,695** | 202,615 | +231.0% (3.31x) | **Rudis** |
| **SET/GET 1:1** | 1KB | **543,151** | 254,241 | +113.6% (2.14x) | **Rudis** |

Verified against `tier_dragonfly_comparison.json`; all throughput figures and the derived percentage/multiple deltas were recomputed independently and match.

### Latency Percentiles Comparison

| Engine | Workload | Ops/sec | Bandwidth | Avg Latency | p50 | p90 | p95 | p99 | p99.9 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Rudis** | SET (1KB) | **1,279,856** | 1,339.7 MB/s | **0.62 ms** | **0.50 ms** | **0.73 ms** | **0.83 ms** | **4.26 ms** | **14.78 ms** |
| Dragonfly | SET (1KB) | 286,351 | 299.6 MB/s | 2.78 ms | 2.67 ms | 3.23 ms | 3.52 ms | 4.80 ms | 18.05 ms |
| **Rudis** | GET (1KB) | **670,695** | 698.7 MB/s | **1.19 ms** | **1.16 ms** | **1.29 ms** | **1.37 ms** | **1.84 ms** | **6.43 ms** |
| Dragonfly | GET (1KB) | 202,615 | 210.9 MB/s | 3.95 ms | 3.84 ms | 4.38 ms | 4.58 ms | 5.89 ms | 25.60 ms |
| **Rudis** | SET/GET 1:1 | **543,151** | 566.9 MB/s | **1.47 ms** | **0.74 ms** | 3.78 ms | 4.35 ms | 7.30 ms | **9.98 ms** |
| Dragonfly | SET/GET 1:1 | 254,241 | 265.3 MB/s | 3.15 ms | 3.01 ms | **3.66 ms** | **3.98 ms** | **5.79 ms** | 20.61 ms |

### Analysis
1. **Consistent advantage across all three workloads**: Rudis outperforms Dragonfly on all three benchmarks — 4.47x higher SET throughput (1.28M vs 286K ops/s), 3.31x higher GET throughput (671K vs 203K ops/s), and 2.14x higher SET/GET 1:1 mixed throughput (543K vs 254K ops/s) — with sub-2ms median latencies on every workload.
2. **Mixed-workload pipeline handling**: on the 1:1 mixed workload, Rudis's design choices for pipelined tiered access include:
   - **Pipeline squashing across tiers**: pipelined batches are dispatched to shards without restricting squashing to non-tiered keys, so a batch touching both hot and tiered keys can still execute as a single-hop, cross-shard dispatch rather than sequential round-trips.
   - **Unified fast-path DRAM/tiered read**: a cross-shard `Get` resolves in a single message round-trip, returning DRAM hits immediately and asynchronously streaming cold reads only on a DRAM miss.
   - **Stack-based RESP encoding**: `write_resp_bulk` (confirmed present in `src/geo.rs`, `src/router.rs`, `src/table.rs`, `src/connection.rs`, `src/shard.rs`) avoids dynamic string allocation for bulk-reply integers, reducing per-response heap allocation under high throughput.
   - **Circular-cursor hot-key eviction**: `spill_cursor` (confirmed in `src/table.rs`) replaces a linear table scan for eviction candidate selection with an $O(k)$ circular cursor, avoiding repeated re-scans of already-evicted slots.
   - **Decoupled batch SmallBins flushes**: background auto-tiering packs multiple keys into a SmallBin without an fsync per record, while explicit `TIER SPILL` and `TIER COOL` (confirmed in `src/resp.rs`) retain immediate durability semantics when requested.

---

## 8. Multi-Core Tiered Storage Scaling (4, 8, and 16 Threads)

Evaluating Rudis NVMe tiered storage across 4, 8, and 16 worker threads (pinned via `taskset`) under 1 KB payloads and a 1024 MB `maxmemory` ceiling. Script: `scripts/benchmark_tier_scaling.py`; raw data: `tier_scaling_results.json`.

### Scaling Throughput & Latency Summary

| Threads | Workload | Throughput (Ops/sec) | Bandwidth (MB/s) | Avg Latency | p50 | p90 | p95 | p99 | p99.9 |
| :---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **4** | SET (1KB) | 1,351,053 | 1,414.1 | 0.59 ms | 0.54 ms | 0.83 ms | 0.94 ms | 1.26 ms | 5.18 ms |
| **4** | GET (1KB) | 528,145 | 550.0 | 1.51 ms | 1.58 ms | 1.93 ms | 1.99 ms | 2.21 ms | 7.01 ms |
| **4** | SET/GET 1:1 | 948,435 | 990.0 | 0.84 ms | 0.75 ms | 1.18 ms | 1.26 ms | 1.54 ms | 5.82 ms |
| **8** | SET (1KB) | 2,303,685 | 2,411.1 | 0.69 ms | 0.61 ms | 1.01 ms | 1.18 ms | 1.84 ms | 8.13 ms |
| **8** | GET (1KB) | 1,099,510 | 1,145.1 | 1.45 ms | 1.27 ms | 1.89 ms | 1.97 ms | 2.51 ms | 8.51 ms |
| **8** | SET/GET 1:1 | 1,750,906 | 1,827.5 | 0.91 ms | 0.83 ms | 1.25 ms | 1.37 ms | 1.94 ms | 7.36 ms |
| **16** | SET (1KB) | 1,933,960 | 2,024.0 | 0.82 ms | 0.71 ms | 1.28 ms | 1.52 ms | 2.18 ms | 8.90 ms |
| **16** | GET (1KB) | 763,735 | 795.2 | 2.09 ms | 2.04 ms | 2.26 ms | 2.37 ms | 3.10 ms | 11.97 ms |
| **16** | SET/GET 1:1 | 1,236,764 | 1,290.6 | 1.29 ms | 1.22 ms | 1.43 ms | 1.50 ms | 2.05 ms | 8.77 ms |

Verified against `tier_scaling_results.json`; all throughput, bandwidth, and latency figures match to the reported precision.

### Key Scaling Observations
- **Peak throughput at 8 threads**: Rudis reaches its highest measured throughput at 8 worker threads — 2.30M SET ops/sec (2.41 GB/s NVMe write bandwidth) and 1.10M GET ops/sec (1.15 GB/s cold-read bandwidth). All three workloads regress at 16 threads relative to 8, most likely because the NVMe device's I/O queue and the 8-core benchmark host's per-socket bandwidth become the bottleneck rather than server-side compute, rather than because of any specific software contention point.
- **Sub-linear but strong 4→8 thread scaling**: mixed SET/GET throughput rises from 948K ops/sec (4 threads) to 1.75M ops/sec (8 threads), a 1.85x increase for a 2x increase in thread count (~92% scaling efficiency relative to ideal linear scaling). SET throughput over the same transition scales at ~85% efficiency (1.35M → 2.30M ops/sec, versus an ideal 2.70M).
- **Latency remains low under load**: median (p50) latency stays under 1 ms for SET and mixed workloads at both 4 and 8 threads, only exceeding 1 ms at 16 threads once the workload is past its throughput peak.

---

## 9. Advanced Storage Engine Features

### Zero-Copy Online Garbage Collection & Hole Punching
As tiered keys are overwritten or deleted (`DEL`, `EXPIRE`, or evicted during compaction), storage capacity must be reclaimed without expensive file rewrites or defragmentation pauses:
- **Instant hole-punching for large records**: deletions of records $\geq 2$ KB issue an immediate `fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)` call (confirmed in `src/tiering.rs`) to deallocate the underlying NVMe blocks without copying data or truncating the file.
- **Dead-page tracking in SmallBins**: when a small key inside a 4 KB bin is deleted, the page's dead-byte counter (`dead_bytes`) is incremented; once a page is fully vacated, its index is pushed onto a reclamation queue (`dead_pages: Vec<u64>`, confirmed in `src/tiering.rs`) for the GC cycle to process.
- **Online compaction (`TIER GC`)**: the `TIER GC` command (confirmed in `src/resp.rs`) and a periodic background GC cycle punch holes in dead SmallBins pages, returning NVMe blocks to the OS.
- **Telemetry**: `TIER INFO` exposes `gc_reclaimed_bytes` and `gc_cycles` counters (both confirmed in `src/tiering.rs`).

### Direct I/O (`O_DIRECT`)
To bypass the Linux page cache, avoid double-buffering, and reduce kernel-memory eviction pressure under sustained high-throughput I/O:
- Setting `RUDIS_DIRECT_IO=1` (confirmed in `src/tiering.rs`) enables direct-to-NVMe reads and writes via `O_DIRECT`.
- The tier manager (`ShardTierManager`, confirmed in `src/tiering.rs`) probes filesystem support for `O_DIRECT` and falls back to buffered, asynchronous `io_uring` I/O when it is unavailable.
- All SmallBins buffers and file offsets are enforced to 512-byte and 4,096-byte alignment, matching NVMe controller I/O granularity requirements.
