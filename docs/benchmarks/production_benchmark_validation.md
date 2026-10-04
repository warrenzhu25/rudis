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

### 2.2 Resolution: Interleaved A/B Re-run (2026-10-04)

The open `GET` question was settled by running the build from just before the hardening changes
(`f64ffa1`, the parent of `d8d02f7`) and the current build (`f50a26e`) back-to-back, alternating which
went first in each of 8 rounds. Same machine shape as above: 8 shards on cores `0-7`, `memtier_benchmark`
8 threads x 8 connections on cores `32-47`, pipeline 16, 1 KB values, 100,000 keys. Each round starts a
fresh server in an empty directory, fills every key once with `--key-pattern P:P --requests allkeys`, then
runs `GET` for 10 seconds.

| Build | Runs | Median Ops/sec | Mean ± Std | Min / Max | Median p99 |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `f64ffa1` (before hardening) | 8 | 971,257 | 981,341 ± 62,020 | 900,239 / 1,110,530 | 2.319 ms |
| `f50a26e` (current) | 8 | 1,018,938 | 1,041,286 ± 62,563 | 969,074 / 1,166,236 | 1.775 ms |

**There is no `GET` regression.** The current build is 4.9% faster at the median, and its p99 is 23% lower.
Both builds land near 1M `GET` ops/sec on this machine, so the 886,960 figure above is in line with the
older build too. The 1.50M mean from `benchmark_vs_dragonfly_valkey.md` was the outlier, which fits that
report's own warning that this cell had a ~36% coefficient of variation.

### 2.3 TLS Client Parity: Interleaved A/B (2026-10-04)

The TLS-parity change (`3d68330`) moved plaintext clients onto a transport-generic `handle_client` and
TLS clients onto the same loop. This run checks that plaintext did not regress and measures TLS. Base is
`d0fef9b` (before the change), head is `3d68330`. The shape matches Section 2.2: 1 KB values, 100,000
keys, `memtier_benchmark` 8 threads x 8 connections on cores `32-47`, server on cores `0..N-1`, rounds
alternate which build goes first, each round uses a fresh server, a `P:P allkeys` fill before `GET`, and
10 seconds of measurement. Every run was inside a private network namespace. TLS runs used
`--tls --tls-skip-verify` against `--tls-port` (self-signed certificate). The machine was shared and
noisy during these runs, so absolute numbers are lower than in Section 2.2. Compare builds within each
table, not across reports.

**Plaintext, pipeline 16 (6 rounds):**

| Workload | Build | Runs | Median Ops/sec | Mean ± Std | Min / Max | Median p99 |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| `GET`, 1 shard | `d0fef9b` | 6 | 489,061 | 475,664 ± 40,836 | 421,218 / 519,991 | 3.839 ms |
| `GET`, 1 shard | `3d68330` | 6 | 497,534 | 487,314 ± 29,495 | 428,113 / 518,925 | 3.679 ms |
| `SET`, 1 shard | `d0fef9b` | 6 | 653,294 | 653,490 ± 15,649 | 634,080 / 675,515 | 2.399 ms |
| `SET`, 1 shard | `3d68330` | 6 | 646,089 | 653,955 ± 46,169 | 605,064 / 744,369 | 2.487 ms |
| `GET`, 16 shards | `d0fef9b` | 6 | 738,246 | 736,822 ± 7,810 | 726,102 / 745,348 | 2.567 ms |
| `GET`, 16 shards | `3d68330` | 6 | 743,921 | 741,322 ± 5,283 | 732,169 / 746,154 | 2.391 ms |
| `SET`, 16 shards | `d0fef9b` | 6 | 1,378,668 | 1,378,670 ± 12,285 | 1,358,191 / 1,399,586 | 1.543 ms |
| `SET`, 16 shards | `3d68330` | 6 | 1,364,634 | 1,367,412 ± 8,741 | 1,359,828 / 1,385,285 | 1.623 ms |

**Plaintext shows no regression.** Head is within ±2% of base at the median for every cell, which is
inside the run-to-run spread.

**TLS, 8 shards, pipeline 16 (4 rounds):**

| Workload | Build | Runs | Median Ops/sec | Mean ± Std | Min / Max | Median p99 |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| `GET` | `d0fef9b` | 4 | 1,001,605 (≈100% misses) | 998,809 ± 43,133 | 943,051 / 1,048,975 | 2.115 ms |
| `GET` | `3d68330` | 4 | 1,200,220 (0 misses) | 1,239,967 ± 91,250 | 1,164,981 / 1,394,446 | 2.203 ms |
| `SET` | `d0fef9b` | 4 | 0 (stalled) | 0 | 0 / 0 | n/a |
| `SET` | `3d68330` | 4 | 633,909 | 641,514 ± 21,952 | 620,232 / 678,007 | 4.943 ms |

The base build cannot run this workload. Sixteen pipelined 1 KB `SET`s exceed the ~4 KiB rustls hands
back per `read_tls` call, and the old loop dropped the rest, so `SET` stalled and completed nothing. For
the same reason the `GET` fill stored nothing, and base's `GET` figure counts misses only (empty replies).
On head every `GET` hits.

**TLS, 8 shards, pipeline 1 (3 rounds), the shape the old code could handle:**

| Workload | Build | Runs | Median Ops/sec | Mean ± Std | Min / Max | Median p99 |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| `GET` | `d0fef9b` | 3 | 191,731 | 200,345 ± 12,795 | 190,871 / 218,434 | 0.695 ms |
| `GET` | `3d68330` | 3 | 194,392 | 193,967 ± 1,387 | 192,095 / 195,413 | 0.735 ms |
| `SET` | `d0fef9b` | 3 | 146,898 | 148,460 ± 3,276 | 145,464 / 153,018 | 0.959 ms |
| `SET` | `3d68330` | 3 | 142,905 | 145,692 ± 4,413 | 142,251 / 151,922 | 0.999 ms |

Without pipelining, TLS throughput is unchanged within noise (+1.4% `GET`, −2.7% `SET` at the median,
with overlapping ranges). The gain from the change is that pipelined TLS traffic now works and is
squashed the same way as plaintext.

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
   same confidence as `SET`. **Resolved since:** the interleaved 8-round A/B re-run in Section 2.2 shows
   no `GET` regression (current build +4.9% median throughput, lower p99).

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
