# Rudis Performance & Benchmark Suite

This directory contains the performance evaluations, scalability analyses, and architectural benchmark
reports for **Rudis v0.1.0**, an in-memory data structure store written in Rust around a shared-nothing,
thread-per-core, Linux `io_uring` (Monoio) architecture. Every report below states its own methodology,
hardware, and — where one exists — the raw result file it was checked against; several reports explicitly
correct numbers from earlier revisions once fresher data became available; read each report's own data
provenance note before quoting a figure from it.

---

## Benchmark Index

| Report | Focus | Headline, As Currently Verified |
| :--- | :--- | :--- |
| **[`baseline.md`](baseline.md)** | Pre-optimization 1-32 thread scaling baseline (unbatched response writes). First entry in the baseline -> write-batching -> squashing optimization sequence. | Peaks at **297,049 ops/sec** (16 threads); write-serialization bottleneck at pipeline depth 100 motivates the next two reports. |
| **[`write_batching.md`](write_batching.md)** | Effect of coalescing all responses from one read cycle into a single `io_uring` write. Second entry in the sequence. | Single-thread throughput rises **13.5x** (60,487 -> 816,937 ops/sec); p99 latency falls from 80.90 ms to 7.30 ms. |
| **[`squashed_scaling.md`](squashed_scaling.md)** | Full 1-32 thread sweep after adding cross-shard pipeline squashing, pre-allocated responders, and `mimalloc`. Third and final entry in the sequence. | Peaks at **2,904,558 ops/sec** (16 threads), a further **4.1x** over write-batching alone. |
| **[`pipeline_squashing.md`](pipeline_squashing.md)** | 16-thread deep dive on the squashed architecture, with a same-configuration Dragonfly comparison. | Rudis **2,634,081 ops/sec** vs. Dragonfly **1,910,371 ops/sec** (**+37.9%**) at 16 threads. |
| **[`benchmark_vs_dragonfly_valkey.md`](benchmark_vs_dragonfly_valkey.md)** | Rudis vs. Dragonfly v1.39.0 across 9 single- and multi-key workloads (8 physical cores). Historical 3-way Valkey data is called out as stale and not reproduced. | Split result: Rudis leads pipelined `SET` (+23.1%) and `Mixed` (+41.3%); Dragonfly leads unpipelined single-key ops (+26-37%) and scattered multi-key `MSET`/`MGET` (+12-22%). |
| **[`multi_command_comparison.md`](multi_command_comparison.md)** | Rudis vs. Dragonfly v1.39.0 across 16 common Redis commands at 16 and 32 physical cores. | Reproducible core-count crossover: Rudis leads all 16 commands at **16 cores** (+12% to +183%); Dragonfly leads all 16 at **32 cores** (-5% to -40% for Rudis). |
| **[`geospatial_benchmark_vs_dragonfly.md`](geospatial_benchmark_vs_dragonfly.md)** | Rudis vs. Dragonfly v1.39.0 on `GEOADD`/`GEODIST`/`GEORADIUS`/`GEOSEARCH` (8 physical cores). | Rudis leads all 5 geospatial workloads, from **+137.5%** (`GEODIST`) to **+222.9%** (`GEOADD`). No committed raw JSON for this run; only the reported deltas' internal arithmetic was verified. |
| **[`tiered_storage.md`](tiered_storage.md)** | NVMe/SSD cold-data offloading: DRAM-vs-tiered comparison, thread-count scaling (4/8/16), and a Dragonfly tiered-mode comparison. | Sustains **827,826 GET ops/sec** at 0.66 ms p50 with ~72.4x memory expansion (32 MB DRAM ceiling, 2.43 GB resident on NVMe); leads Dragonfly's tiered mode by 2.1x-4.5x. |
| **[`vector_search.md`](vector_search.md)** | In-memory HNSW approximate nearest-neighbor search with 8-bit scalar quantization (SQ8). | SQ8 gives a **75% DRAM reduction** (512 B -> 128 B/vector) for a ~1.8-point Recall@10 cost (53.0% vs. 54.8%). Single-run figures; no committed result file. |
| **[`comprehensive_performance_guide.md`](comprehensive_performance_guide.md)** | Master whitepaper: payload-size sweep, pipeline-depth sweep, specialized engines (JSON, Streams, probabilistic, geo, vector, transactions), tail-latency SLAs, and a competitor summary. | Up to **8.40 GB/s (67.2 Gbps)** on 64 KB payload writes; competitor section (Section 6) is corrected in this revision to match `benchmark_common_commands_results.json` and `benchmark_multicore_results.json`. |
| **[`production_benchmark_validation.md`](production_benchmark_validation.md)** | Regression check of steady-state SET/GET throughput after landing panic isolation, signal handling, `maxclients` limits, telemetry, and multi-key ACL enforcement. | **1,551,947 ops/sec** `SET` / **886,960 ops/sec** `GET` (8 physical cores, pipeline 16, 1 KB). `SET` shows no regression against the comparable configuration in `benchmark_vs_dragonfly_valkey.md`; `GET` reads 40.8% below that baseline's mean but within its documented high-variance band, and is flagged as inconclusive rather than confirmed. |
| **[`scaling_and_profiling_report.md`](scaling_and_profiling_report.md)** | 1-32 core scaling curve for `SET`/`GET`/50:50 Mixed, plus a Linux `perf` CPU hotspot profile at 16 cores. | `GET` peaks at **4,251,151 ops/sec** (8 cores); no `pthread_mutex`/`futex`/CAS symbols appear in the top-30 profiled hotspots, consistent with the shared-nothing design. |

### Reading order

New readers evaluating Rudis's raw throughput should read the optimization sequence first
(`baseline.md` -> `write_batching.md` -> `squashed_scaling.md` / `pipeline_squashing.md`), then the
competitive comparisons (`benchmark_vs_dragonfly_valkey.md`, `multi_command_comparison.md`,
`geospatial_benchmark_vs_dragonfly.md`), then the subsystem-specific reports (`tiered_storage.md`,
`vector_search.md`), and finally the two whitepaper-style summaries
(`comprehensive_performance_guide.md`, `scaling_and_profiling_report.md`) and the production regression
check (`production_benchmark_validation.md`).

---

## Data Provenance

Reports differ in how their figures can be independently re-verified. Each report states its own
provenance in an introductory note; the summary below is for quick reference only — defer to the
individual report when the two disagree.

| Backed by a committed result file | Not backed by a committed result file (single-run or illustrative) |
| :--- | :--- |
| `multi_command_comparison.md` (`benchmark_common_commands_results.json`) | `comprehensive_performance_guide.md` Sections 3-5 (payload/pipeline/engine sweeps write to an uncommitted `benchmark_logs/` directory) |
| `benchmark_vs_dragonfly_valkey.md` (`benchmark_comparison_results.json`) | `geospatial_benchmark_vs_dragonfly.md` (writes to an uncommitted `benchmark_logs/geo_rudis_vs_dragonfly.json`) |
| `tiered_storage.md` (`tiered_benchmark_results.json`, `tier_scaling_results.json`, `tier_dragonfly_comparison.json`) | `vector_search.md` (`vector_bench` prints to stdout only; no result file) |
| | `baseline.md`, `write_batching.md`, `squashed_scaling.md`, `pipeline_squashing.md` (console output transcribed directly into the report; no JSON artifact) |
| | `production_benchmark_validation.md` (raw `memtier_benchmark` console output is reproduced verbatim in the report itself, but no separate result file exists) |
| | `scaling_and_profiling_report.md` (`scripts/profile_and_benchmark.py` writes this Markdown file directly; there is no intermediate JSON to diff against) |

---

## Reproducing Benchmarks

All automation lives in [`scripts/`](../../scripts/). Rebuild Rudis in release mode first:

```bash
cargo build --release
```

Then run the script(s) for the report you want to regenerate:

```bash
# baseline.md — pre-optimization 1-32 thread sweep
python3 scripts/benchmark_baseline.py

# write_batching.md / squashed_scaling.md — same harness pattern as benchmark_baseline.py,
# run against a build with write-batching / squashing enabled (see each report's
# "Reproducing This Benchmark" section for the exact invocation)

# pipeline_squashing.md — single 16-thread, 60-second run
scripts/run_16t_benchmark.sh

# benchmark_vs_dragonfly_valkey.md — 8-core Rudis vs. Dragonfly (vs. optional Valkey) comparison
python3 scripts/benchmark_vs_dragonfly_valkey.py --df-only    # matches the committed JSON
python3 scripts/benchmark_vs_dragonfly_valkey.py              # full 3-way, requires a Valkey build

# multi_command_comparison.md — 16 common commands at 16 and/or 32 physical cores
python3 scripts/benchmark_common_commands_vs_dragonfly.py 16,32

# geospatial_benchmark_vs_dragonfly.md
python3 scripts/benchmark_geo_vs_dragonfly.py

# tiered_storage.md — DRAM-vs-tiered, thread scaling, and the Dragonfly tiered comparison
python3 scripts/benchmark_tiering.py
python3 scripts/benchmark_tier_scaling.py
python3 scripts/compare_tier_dragonfly.py

# vector_search.md — standalone HNSW + SQ8 benchmark binary
cargo run --release --bin vector_bench

# comprehensive_performance_guide.md — payload/pipeline sweeps and specialized-engine suite
python3 scripts/benchmark_payload_pipeline.py
python3 scripts/benchmark_specialized_engines.py

# scaling_and_profiling_report.md — regenerates that report file directly, including the
# Linux `perf` CPU hotspot section (requires perf and sufficient privileges to profile
# the server process)
python3 scripts/profile_and_benchmark.py
```

`production_benchmark_validation.md` has no dedicated script; it records a manual
`memtier_benchmark` invocation against a release build, documented inline in that report so the exact
command can be copied and re-run.

Most scripts assume `memtier_benchmark` (v2.2.1+) is installed and that the invoking user can `taskset`
pin processes to specific CPU cores; several also assume a locally built Dragonfly (and, for the historical
Valkey path, a Valkey 8.1.9 build) binary. Absolute paths to these binaries are hardcoded at the top of each
script and must be adjusted for your environment before running.
