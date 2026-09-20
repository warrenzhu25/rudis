# Rudis High-Concurrency Multi-Core Scaling & CPU Profiling Report

## 1. Executive Summary & Benchmark Setup

This report measures how Rudis's shared-nothing, thread-per-core architecture scales as server threads
increase from 1 to 32 on a single 64-core host, across three workload shapes (`SET`, `GET`, and a 50:50
mixed workload), and then profiles a 16-thread run with Linux `perf` to identify where CPU time is spent.
It answers two questions this directory's other reports do not: (1) where does Rudis's own throughput
peak and start to regress as thread count grows, independent of any competitor comparison, and (2) what,
concretely, is consuming CPU cycles at that peak.

- **Hardware Platform**: AMD EPYC 7B13 64-Core Processor (NUMA Node 0, 117 GiB RAM)
- **Server Configuration**: dedicated pinned cores `0..(N-1)` for `N` server threads, each driving an
  independent `monoio` `io_uring` reactor instance (`taskset -c 0-(N-1) ./target/release/rudis --threads N`)
- **Client Generator**: `memtier_benchmark`, pinned to dedicated cores `32-63`, disjoint from the server's
  cores at every thread count tested
- **Workload Parameters (Section 2 scaling sweep)**: 1 KB payload, pipeline depth **100**,
  1,000,000-key keyspace (`--key-pattern S:S`), 10-second test window per (workload, thread-count) cell,
  1 client process with 32 `memtier_benchmark` threads
- **Data provenance**: this entire report — including every number in Sections 2 and 3 — is generated
  directly by [`scripts/profile_and_benchmark.py`](../../scripts/profile_and_benchmark.py), which writes
  this Markdown file in place rather than emitting an intermediate JSON artifact. There is therefore no
  separate result file to diff these tables against; re-running the script (Section 4) is the only way to
  independently reproduce or refresh the figures below. The script and this report were added together in
  commit `0f37614`.

---

## 2. Multi-Core Scaling Results

For each workload, the server is restarted and pinned to `0..(N-1)` at each thread count `N`, and a fresh
10-second `memtier_benchmark` run is issued from the disjoint client cores. Each row is a single run (not a
mean of several iterations — contrast with the median-of-3 methodology in
[`multi_command_comparison.md`](multi_command_comparison.md)), so treat small differences between adjacent
thread counts as somewhat noisier than the median-based reports elsewhere in this directory.

### Workload: 100% SET (1KB)

| Cores | Throughput (Ops/sec) | Bandwidth (MB/s) | Avg Latency (ms) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | **867,251.14** | 886.29 | 3.67 | 4.03 | 5.02 | 5.28 | 6.43 | 14.85 |
| **2** | **1,181,226.65** | 1,207.27 | 2.69 | 2.53 | 4.00 | 4.45 | 5.70 | 13.63 |
| **4** | **1,689,026.09** | 1,726.42 | 1.88 | 1.79 | 2.73 | 3.13 | 4.25 | 13.82 |
| **8** | **2,190,518.36** | 2,239.09 | 1.44 | 1.31 | 2.00 | 2.29 | 3.41 | 16.89 |
| **16** | **2,795,855.75** | 2,857.88 | 1.13 | 1.00 | 1.63 | 1.90 | 2.86 | 14.34 |
| **32** | **2,534,119.69** | 2,590.36 | 1.24 | 1.07 | 1.83 | 2.17 | 3.52 | 16.06 |

### Workload: 100% GET (1KB)

| Cores | Throughput (Ops/sec) | Bandwidth (MB/s) | Avg Latency (ms) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | **1,707,230.40** | 376.87 | 1.85 | 1.48 | 3.26 | 3.42 | 4.25 | 7.84 |
| **2** | **2,410,931.77** | 403.04 | 1.30 | 1.04 | 2.40 | 3.06 | 4.32 | 7.93 |
| **4** | **3,546,695.54** | 633.96 | 0.87 | 0.71 | 1.54 | 1.96 | 2.81 | 6.37 |
| **8** | **4,251,150.90** | 706.52 | 0.72 | 0.59 | 1.15 | 1.42 | 2.24 | 6.66 |
| **16** | **3,260,984.24** | 589.97 | 0.95 | 0.86 | 1.39 | 1.68 | 2.67 | 9.15 |
| **32** | **3,278,615.38** | 619.36 | 0.94 | 0.83 | 1.30 | 1.57 | 2.64 | 10.24 |

### Workload: 50/50 SET/GET (1KB)

| Cores | Throughput (Ops/sec) | Bandwidth (MB/s) | Avg Latency (ms) | p50 (ms) | p90 (ms) | p95 (ms) | p99 (ms) | p99.9 (ms) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | **448,088.38** | 457.76 | 3.57 | 3.49 | 4.00 | 4.35 | 5.05 | 10.43 |
| **2** | **578,853.15** | 591.44 | 2.76 | 2.69 | 3.82 | 4.38 | 5.86 | 13.31 |
| **4** | **757,423.40** | 774.00 | 2.11 | 1.89 | 3.42 | 3.94 | 4.99 | 15.74 |
| **8** | **1,057,310.06** | 1,080.59 | 1.51 | 1.34 | 2.32 | 2.61 | 3.42 | 13.95 |
| **16** | **1,401,317.02** | 1,432.28 | 1.14 | 1.05 | 1.61 | 1.85 | 2.93 | 10.49 |
| **32** | **1,341,147.65** | 1,370.77 | 1.19 | 1.02 | 1.85 | 2.19 | 3.38 | 11.90 |

### 2.1 Speedup and Parallel Efficiency

Speedup is throughput at `N` cores relative to the 1-core measurement for the same workload; efficiency is
speedup divided by core count. Both are derived arithmetically from the tables above (no additional
measurement).

| Cores | SET Speedup | SET Efficiency | GET Speedup | GET Efficiency | Mixed Speedup | Mixed Efficiency |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **1** | 1.00x | 100.0% | 1.00x | 100.0% | 1.00x | 100.0% |
| **2** | 1.36x | 68.1% | 1.41x | 70.6% | 1.29x | 64.6% |
| **4** | 1.95x | 48.7% | 2.08x | 51.9% | 1.69x | 42.3% |
| **8** | 2.53x | 31.6% | **2.49x** | 31.1% | 2.36x | 29.5% |
| **16** | **3.22x** | 20.1% | 1.91x | 11.9% | **3.13x** | 19.5% |
| **32** | 2.92x | 9.1% | 1.92x | 6.0% | 2.99x | 9.4% |

All three workloads show the same qualitative shape: efficiency falls monotonically from 100% at 1 core,
and absolute throughput peaks before 32 cores are reached — `GET` peaks earliest, at **8 cores**
(4,251,151 ops/sec, 2.49x speedup), while `SET` and the `Mixed` workload both peak at **16 cores** (3.22x
and 3.13x speedup respectively) before regressing at 32 cores. This is consistent with the same diminishing-
and-negative-returns pattern documented at higher core counts in
[`squashed_scaling.md`](squashed_scaling.md) and in
[`../benchmark_multicore_results.md`](../benchmark_multicore_results.md)'s Dragonfly comparison, and is
attributed there to growing cross-shard channel/IPC pressure as shard count increases relative to a fixed
client concurrency; this report does not independently isolate a root cause beyond the CPU profile in
Section 3.

---

## 3. CPU Hotspot & Performance Profiling (16 Cores, 50/50 Workload)

### 3.1 Profiling Methodology

The profiling sub-run uses a **different client configuration than the Section 2 scaling sweep** — this is
a property of the underlying script (`profile_with_perf()` in
[`scripts/profile_and_benchmark.py`](../../scripts/profile_and_benchmark.py)), not an inconsistency to be
corrected, but it means the CPU hotspot data below should not be read as a per-request cost breakdown for
the exact 50/50 row in Section 2's table above:

- Server: 16 threads pinned to cores `0-15`, same as the Section 2 16-core row.
- Client: 2 `memtier_benchmark` processes x 16 threads (32 total), pinned to cores `32-63`, issuing a 1:1
  `SET`:`GET` ratio at **pipeline depth 50** (not 100) with a 1 KB payload over a 1,000,000-key range, for a
  **15-second** sustained load window (not the 10-second window used in Section 2).
- `perf record -F 99 -p <rudis_pid> -- sleep 5` samples the server process at 99 Hz for 5 seconds, starting
  2 seconds into the 15-second client load window (i.e. sampling steady-state traffic, not startup).
- The top 30 non-comment lines of `perf report --sort comm,dso,symbol` are reproduced verbatim below.

Top functions sampled by Linux `perf` under sustained load:

```text
     1.61%  rudis-shard-9   libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     1.42%  rudis-shard-2   libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     1.36%  rudis-shard-14  libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     1.31%  rudis-shard-6   libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     1.23%  rudis-shard-8   libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     1.14%  rudis-shard-4   libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     1.01%  rudis-shard-3   libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     1.01%  rudis-shard-5   libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     1.01%  rudis-shard-7   libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     0.90%  rudis-shard-15  libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     0.87%  rudis-shard-0   libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     0.87%  rudis-shard-12  libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     0.84%  rudis-shard-1   libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     0.82%  rudis-shard-9   rudis          [.] rudis::connection::handle_connection::{closure#0}                                                                                                                                                                                                                                                                                                                                                                                                     -      -            
     0.79%  rudis-shard-10  libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     0.79%  rudis-shard-2   rudis          [.] rudis::connection::handle_connection::{closure#0}                                                                                                                                                                                                                                                                                                                                                                                                     -      -            
     0.76%  rudis-shard-3   rudis          [.] <rudis::table::RudisTable>::get                                                                                                                                                                                                                                                                                                                                                                                                                       -      -            
     0.71%  rudis-shard-10  rudis          [.] <rudis::table::RudisTable>::get                                                                                                                                                                                                                                                                                                                                                                                                                       -      -            
     0.71%  rudis-shard-13  libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
     0.71%  rudis-shard-13  rudis          [.] <rudis::table::RudisTable>::get                                                                                                                                                                                                                                                                                                                                                                                                                       -      -            
     0.65%  rudis-shard-12  rudis          [.] rudis::connection::handle_connection::{closure#0}                                                                                                                                                                                                                                                                                                                                                                                                     -      -            
     0.65%  rudis-shard-6   rudis          [.] rudis::connection::handle_connection::{closure#0}                                                                                                                                                                                                                                                                                                                                                                                                     -      -            
     0.63%  rudis-shard-5   rudis          [.] rudis::connection::handle_connection::{closure#0}                                                                                                                                                                                                                                                                                                                                                                                                     -      -            
     0.60%  rudis-shard-4   rudis          [.] rudis::connection::handle_connection::{closure#0}                                                                                                                                                                                                                                                                                                                                                                                                     -      -            
     0.52%  rudis-shard-11  rudis          [.] <rudis::table::RudisTable>::get                                                                                                                                                                                                                                                                                                                                                                                                                       -      -            
     0.49%  rudis-shard-1   rudis          [.] <rudis::table::RudisTable>::get                                                                                                                                                                                                                                                                                                                                                                                                                       -      -            
     0.49%  rudis-shard-8   rudis          [.] rudis::connection::handle_connection::{closure#0}                                                                                                                                                                                                                                                                                                                                                                                                     -      -            
     0.46%  rudis-shard-12  rudis          [.] <rudis::table::RudisTable>::get                                                                                                                                                                                                                                                                                                                                                                                                                       -      -            
     0.44%  rudis-shard-0   rudis          [.] <rudis::table::RudisTable>::get                                                                                                                                                                                                                                                                                                                                                                                                                       -      -            
     0.44%  rudis-shard-11  libc.so.6      [.] __memmove_avx_unaligned_erms                                                                                                                                                                                                                                                                                                                                                                                                                          -      -            
```

### 3.2 Reading the Profile

The sampled cost is spread evenly across all 16 `rudis-shard-N` threads (each in the 0.4-1.6% range per
symbol), which is itself a data point: no single shard dominates CPU time, consistent with a uniform,
hash-distributed keyspace and no hot-shard imbalance during this run.

Two symbol groups account for essentially all of the sampled time in the excerpt above:

1. **`libc.so.6 [.] __memmove_avx_unaligned_erms`** — the AVX-accelerated `memmove`/`memcpy` implementation,
   appearing once per shard thread and consistently the single largest line item per thread (0.44%-1.61%).
   This is expected for a 1 KB-payload workload: every `SET` copies its 1 KB value at least once into
   table-owned storage, and every `GET` copies it back out into the response buffer.
2. **`rudis::connection::handle_connection::{closure#0}`** and **`<rudis::table::RudisTable>::get`** — the
   per-connection request-handling loop and the hash-table lookup path, appearing on roughly half the
   sampled shards in this particular 5-second window (sampling noise, not an indication that the other
   shards skip this code).

### Architectural Observations

1. **Zero Mutex Contention**: no `pthread_mutex`, `futex`, or atomic CAS-related symbol appears anywhere in
   the reproduced top-30 excerpt above. This is consistent with — though a 99 Hz, 5-second sample is too
   coarse to prove exhaustively for — the shared-nothing multi-reactor model's design goal of no
   inter-thread locking on the hot path.
2. **`io_uring` Proactor Saturation**: the absence of syscall-entry symbols (e.g. `io_uring_enter`) in the
   captured top-30 excerpt suggests the sampled time is dominated by userspace work (memory copies, table
   lookups, connection-handling logic) rather than kernel ring submission overhead at this thread count;
   the original capture's default framing describes kernel completion processing as a major contributor,
   which this specific excerpt's symbol list does not, on its own, show line items for — treat that framing
   as a hypothesis motivated by the architecture rather than a claim directly visible in the reproduced
   sample above.
3. **Memory-Copy-Bound at 1 KB Payloads**: `__memmove_avx_unaligned_erms` being the top line item on every
   shard is consistent with the payload-size sensitivity findings in
   [`comprehensive_performance_guide.md`](comprehensive_performance_guide.md) Section 3, where throughput
   transitions from packet-rate-bound to memory-bandwidth-bound as payload size grows past roughly 4 KB;
   at 1 KB this run sits in between, with per-copy cost already visible but not yet dominant enough to cap
   throughput outright.

---

## 4. Reproducing This Benchmark

```bash
cargo build --release
python3 scripts/profile_and_benchmark.py
```

This regenerates this file (`docs/benchmarks/scaling_and_profiling_report.md`) in place, re-running the
full 1-32 core sweep for all three workloads (Section 2) and the 16-core `perf` profile (Section 3).
Requirements:

- `memtier_benchmark` (v2.2.1+) reachable at the path hardcoded near the top of the script.
- Linux `perf` installed, and sufficient privileges to profile the `rudis` server process (either run as
  root, or set `/proc/sys/kernel/perf_event_paranoid` low enough for an unprivileged user to profile another
  process it owns).
- At least 64 free logical CPUs on the host, since the client is pinned to cores `32-63` independent of how
  many server threads are under test, and the server itself is pinned to `0..(N-1)` for `N` up to 32.

The script's `THREAD_COUNTS` (`[1, 2, 4, 8, 16, 32]`) and profiling parameters (server thread count, client
concurrency, pipeline depth, `perf` sample rate and duration) are constants near the top of
`scripts/profile_and_benchmark.py`; adjust them there to explore other configurations.
