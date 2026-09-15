# Rudis Performance & Benchmark Suite

This directory contains the official performance evaluations, scalability analyses, and architectural benchmark whitepapers for **Rudis v0.1.0**.

---

## Benchmark Index

| Report | Description | Key Metric / Focus |
| :--- | :--- | :--- |
| **[Comprehensive Performance Guide](file:///usr/local/google/home/warrenzhu/github/rudis/docs/benchmarks/comprehensive_performance_guide.md)** | **Master Whitepaper** covering payload size sweeps, pipeline depth sensitivity, specialized engines, tail latencies, and multi-core scaling. | **>4.18M Ops/s**, **8.40 GB/s (67.2 Gbps)**, sub-millisecond p50 SLAs across all data structures. |
| **[Multi-Core Scaling & CPU Profiling](file:///usr/local/google/home/warrenzhu/github/rudis/docs/benchmarks/scaling_and_profiling_report.md)** | Scaling evaluation from 1 to 32 worker threads on 64-core AMD EPYC, with Linux `perf` instruction hotspot analysis. | Shared-nothing zero mutex contention; linear scaling up to 4.25M Ops/s. |
| **[Multi-Command Comparison vs. Dragonfly](file:///usr/local/google/home/warrenzhu/github/rudis/docs/benchmarks/multi_command_comparison.md)** | Head-to-head comparison against Dragonfly v1.39 across 8 standard Redis commands on 16 threads. | Rudis outperforms Dragonfly by **+534% (6.34x) on GET**, **+275% on SET/GET**, and **+72% on SET**. |
| **[Vector Search & SQ8 Quantization](file:///usr/local/google/home/warrenzhu/github/rudis/docs/benchmarks/vector_search.md)** | In-memory HNSW approximate nearest neighbor benchmark evaluating Recall@10, QPS, and memory reduction. | **75% DRAM reduction** with SQ8 (128 bytes/vec) while retaining **53.0% Recall@10** and >3,700 QPS. |
| **[Tiered Storage Benchmark](file:///usr/local/google/home/warrenzhu/github/rudis/docs/benchmarks/tiered_storage.md)** | NVMe/SSD cold-data offloading performance, caching ratios, and recovery times. | Sub-millisecond hot tier access with seamless NVMe spillover. |
| **[Write Batching Analysis](file:///usr/local/google/home/warrenzhu/github/rudis/docs/benchmarks/write_batching.md)** | Evaluates socket buffering, syscall reduction, and `writev` batching on network rings. | Optimal buffer packing and throughput saturation. |
| **[Pipeline Squashing Analysis](file:///usr/local/google/home/warrenzhu/github/rudis/docs/benchmarks/pipeline_squashing.md)** | Algorithmic batching of redundant writes within a single client pipeline. | Up to 10x throughput enhancement on duplicate key overwrites. |

---

## Reproducing Benchmarks

All benchmark automation scripts reside in [`scripts/`](file:///usr/local/google/home/warrenzhu/github/rudis/scripts/):

```bash
# 1. Payload & Pipeline Depth Sensitivity Sweeps
python3 scripts/benchmark_payload_pipeline.py

# 2. Specialized Engines Benchmark (JSON, Streams, Bloom, Cuckoo, CMS, Top-K, Geo)
python3 scripts/benchmark_specialized_engines.py

# 3. Multi-Core Scaling (1-32 Cores) & Perf Profiling
python3 scripts/profile_and_benchmark.py

# 4. Standalone Vector Search HNSW & SQ8 Benchmarks
cargo run --release --bin vector_bench
```
