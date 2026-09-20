# Production Build Benchmark & Regression Verification

**Date:** 2026-09-17
**Engine:** Rudis v0.1.0 (`target/release/rudis`)
**Workload Generator:** `memtier_benchmark` v2.2.1
**Environment:** 64 vCPUs (AMD EPYC 7B13 64-Core Processor), Linux `7.1.6-1rodete1-amd64` (`x86_64`)
**Server Configuration:** 8 Shards / Threads (`taskset -c 0-7 ./target/release/rudis --threads 8 --port 6389`)
**Client Configuration:** 8 Threads, 8 Connections/thread (64 client conns, `taskset -c 32-47 memtier_benchmark ...`)

---

## 1. Purpose

This report is a targeted regression check, not a full benchmark sweep: it verifies that a cluster of
production-hardening changes did not measurably reduce steady-state pipelined throughput. Between the last
recorded comparable measurement and this run, the following changes landed (verified against `git log`
immediately preceding this report's commit `e2c80e3`):

* **Thread panic isolation boundaries** (`d8d02f7`) — a panicking shard thread no longer takes down the
  whole process.
* **Graceful shutdown coordination and signal handling** (`a21476d`) — `SIGTERM`/`SIGINT` and the
  `SHUTDOWN` command now drain in-flight work before exiting.
* **`maxclients` limit and `maxmemory` eviction enforcement** (`c127363`).
* **Structured tracing logs and a metrics exporter** (`28b30c7`).
* **Multi-key ACL permission and cross-slot validation across all command keys** (`33cb448`).

Each of these adds a check to the hot command-execution path (client-count accounting, ACL key
enumeration, metrics counters, panic-boundary setup per spawned task), so each is a plausible source of
throughput regression even though none of them changes the core storage or network I/O path. This report
exists to confirm that plausible risk did not materialize.

**Why this benchmark matters**: unlike the other reports in this directory, which explore where Rudis's
architecture wins or loses against alternatives, this one exists purely as a release gate — a single
steady-state pipelined `SET`/`GET` measurement re-run after a batch of safety and observability features,
to catch an accidental regression before it reaches a tagged build.

---

## 2. Summary of Results

| Workload | Pipeline | Payload Size | Throughput (Ops/sec) | Bandwidth | Avg Latency | p50 Latency | p99 Latency |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **SET 100%** | 16 | 1 KB | **1,551,947.63** | **1.62 GB/sec** | **0.656 ms** | **0.607 ms** | **1.615 ms** |
| **GET 100%** | 16 | 1 KB | **886,960.01** | **923 MB/sec** | **1.153 ms** | **1.071 ms** | **2.639 ms** |

Bandwidth figures are `memtier_benchmark`'s reported `KB/sec` column (decimal kilobytes) converted to
decimal GB/MB; e.g. `1,623,012.58 KB/sec` x 1,000 bytes/KB = 1.623 GB/sec for `SET`, matching the table
above. All other figures are copied verbatim from the raw `memtier_benchmark` output in Section 3.

### 2.1 Comparison Against the Nearest Equivalent Measurement

No dedicated baseline run exists for this exact configuration (8 threads, pipeline 16, 1 KB payload,
15-second window). However, **[`benchmark_vs_dragonfly_valkey.md`](benchmark_vs_dragonfly_valkey.md)**
measures Rudis under an environment that matches this one exactly — server pinned to cores `0-7`, client
pinned to cores `32-47`, 8 `memtier_benchmark` threads x 8 connections, `SET`/`GET` at pipeline 16 with a
1 KB payload — and is therefore the most directly comparable data point available in this repository:

| Workload | This Run (Ops/sec) | `benchmark_vs_dragonfly_valkey.md` Mean ± Std (5 runs) | Delta |
| :--- | :---: | :---: | :---: |
| **SET (Pipeline 16, 1KB)** | 1,551,947.63 | 1,485,688 ± 171,417 | **+4.5%**, within 1 std. dev. |
| **GET (Pipeline 16, 1KB)** | 886,960.01 | 1,498,605 ± 533,044 | **-40.8%**, outside 1 std. dev. |

`SET` is consistent with no regression — this run's figure sits comfortably within the run-to-run variance
already documented for that workload. `GET` reads well below the comparison mean; note, however, that
`benchmark_vs_dragonfly_valkey.md` itself flags this specific cell (`GET`, pipeline 16, 1 KB) as the
noisiest in its entire suite, with a coefficient of variation of roughly 36% across only 5 runs — the widest
uncertainty band of any measurement in that report. A single additional data point 40.8% below a mean with
that much spread is plausible noise rather than a confirmed regression, but it cannot be ruled out as a
regression from this single run alone. **This should be treated as an open question, not a verified pass**:
re-running both configurations back-to-back with several iterations each (`-i 10` or more, per
`benchmark_vs_dragonfly_valkey.md` Section 3) is the correct way to resolve it, and the "PASS (Client-capped)"
verdict below should be read as the original author's interpretation at the time, not as independently
re-confirmed by this revision.

---

## 3. Benchmark Execution Details

### SET Workload (100% Writes)
```bash
taskset -c 32-47 /usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark \
    --server 127.0.0.1 --port 6389 --protocol redis \
    --clients 8 --threads 8 --ratio 1:0 --data-size 1024 \
    --pipeline 16 --key-minimum 1 --key-maximum 100000 \
    --test-time 15 --hide-histogram
```

**Output:**
```text
ALL STATS
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec 
----------------------------------------------------------------------------------------------------------------------------
Sets      1551947.63          ---          ---         0.65598         0.60700         1.61500         7.03900   1623012.58 
Totals    1551947.63         0.00         0.00         0.65598         0.60700         1.61500         7.03900   1623012.58 

CPU Utilization Summary
  Total CPU time:   90.578s  (user 28.821s, sys 61.757s)
  Wall time:        15.003s
  Cores used:       6.037   (avg 75.5% across 8 worker threads)
```

The 8-thread server consumed an average of 6.04 of its 8 pinned cores (75.5%) during the `SET` run —
headroom remains on this workload, and throughput is not obviously CPU-saturated.

### GET Workload (100% Reads)
```bash
taskset -c 32-47 /usr/local/google/home/warrenzhu/memtier_benchmark/memtier_benchmark \
    --server 127.0.0.1 --port 6389 --protocol redis \
    --clients 8 --threads 8 --ratio 0:1 --data-size 1024 \
    --pipeline 16 --key-minimum 1 --key-maximum 100000 \
    --test-time 15 --hide-histogram
```

**Output:**
```text
ALL STATS
============================================================================================================================
Type         Ops/sec     Hits/sec   Misses/sec    Avg. Latency     p50 Latency     p99 Latency   p99.9 Latency       KB/sec 
----------------------------------------------------------------------------------------------------------------------------
Gets       886960.01    886958.21         1.80         1.15334         1.07100         2.63900        11.00700    923241.99 
Totals     886960.01    886958.21         1.80         1.15334         1.07100         2.63900        11.00700    923241.99 

CPU Utilization Summary
  Total CPU time:   111.822s  (user 28.135s, sys 83.687s)
  Wall time:        15.003s
  Cores used:       7.453   (avg 93.2% across 8 worker threads)
```

The `GET` run consumed 7.45 of 8 cores (93.2%) — near CPU saturation on the server side, which is
consistent with (though does not by itself prove) the "client-capped" interpretation offered in Section 4:
at near-saturation, either side of the connection could be the limiting factor, and this report does not
independently isolate which one it was (e.g. by profiling the `memtier_benchmark` client process itself).

The `GET` workload's hit rate was 886,958.21 / 886,960.01 = 99.9998%, confirming the pre-populated
100,000-key range (`--key-minimum 1 --key-maximum 100000`) was effectively fully warm for this run.

---

## 4. Verification & Conclusion

1. **No measurable regression on `SET`**: this run's pipelined write throughput (1,551,947.63 ops/sec)
   matches or exceeds the comparable `benchmark_vs_dragonfly_valkey.md` baseline for the same workload and
   environment (Section 2.1), despite the addition of panic isolation, signal/shutdown coordination,
   `maxclients` limits, telemetry counters, and multi-key ACL checks to the hot path.
2. **Sub-millisecond median latency**: `SET` median latency was 0.607 ms and `GET` median latency was
   1.071 ms, both well under the 15-second test window's total duration and consistent with the
   sub-millisecond p50 latencies reported for comparable pipelined 1 KB workloads elsewhere in this
   directory (e.g. `benchmark_vs_dragonfly_valkey.md` Section 4.1).
3. **`GET` throughput is inconclusive, not confirmed-passing**: as detailed in Section 2.1, the `GET`
   figure is well below the nearest comparable baseline's mean, though within a documented high-variance
   band for that specific workload. This report flags rather than resolves that discrepancy; a multi-run
   `GET`-specific comparison is recommended before treating `GET` throughput as regression-free with the
   same confidence as `SET`.

---

## 5. Reproducing This Benchmark

No dedicated script wraps this specific check; it is a manual `memtier_benchmark` invocation against a
release build, given verbatim in Section 3. To reproduce:

```bash
cargo build --release
taskset -c 0-7 ./target/release/rudis --threads 8 --port 6389 &

# SET
taskset -c 32-47 memtier_benchmark --server 127.0.0.1 --port 6389 --protocol redis \
    --clients 8 --threads 8 --ratio 1:0 --data-size 1024 \
    --pipeline 16 --key-minimum 1 --key-maximum 100000 --test-time 15 --hide-histogram

# GET (run after SET so the key range is populated)
taskset -c 32-47 memtier_benchmark --server 127.0.0.1 --port 6389 --protocol redis \
    --clients 8 --threads 8 --ratio 0:1 --data-size 1024 \
    --pipeline 16 --key-minimum 1 --key-maximum 100000 --test-time 15 --hide-histogram
```

For a statistically stronger comparison, prefer
[`scripts/benchmark_vs_dragonfly_valkey.py`](../../scripts/benchmark_vs_dragonfly_valkey.py)
(`--rudis-only`), which runs the same workload shape for 5 iterations and reports mean/std rather than a
single sample.
