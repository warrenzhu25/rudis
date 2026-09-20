# Benchmark: Common-Command Comparison (Rudis vs. Dragonfly)

This document reports Rudis v0.1.0 throughput and latency against Dragonfly v1.39.0 across the sixteen most
frequently used Redis commands, spanning strings, hashes, lists, sets, sorted sets, and generic keyspace
operations, at two server core counts (16 and 32 physical cores). It supersedes an earlier revision of this
document whose numbers were collected with a CPU-pinning layout that placed the client and server on SMT
sibling threads of the same physical cores (see *Methodology Note* below); that layout is known to have
produced results with run-to-run swings of up to 2.05x and is no longer used.

The authoritative benchmark harness is [`scripts/benchmark_common_commands_vs_dragonfly.py`](../../scripts/benchmark_common_commands_vs_dragonfly.py),
and its raw output is persisted to [`benchmark_common_commands_results.json`](../../benchmark_common_commands_results.json)
at the repository root. All figures below are read directly from that file.

---

## 1. Methodology

* **Commands covered**: `SET`, `GET`, `INCR`, `MSET` (5 keys), `MGET` (5 keys), `HSET`, `HGET`, `LPUSH`, `LPOP`,
  `LRANGE (0-10)`, `SADD`, `SISMEMBER`, `ZADD`, `ZRANGE (0-10)`, `DEL`, `EXISTS`.
* **Payloads**: 1024 bytes for `SET`/`GET`, 128 bytes for hash/list fields and `MSET`/`MGET` values, 64 bytes
  for set/zset members. Commands with no explicit payload (`INCR`, `DEL`, `EXISTS`, `LPOP`) operate on
  pre-populated keys.
* **Pipeline depth**: 16 for single-key commands, 4 for the cross-shard `MSET`/`MGET` (5-key) workloads.
* **Client**: `memtier_benchmark`, 16 threads x 4 connections (`BENCH_CONNS`, default 4) = up to 64 concurrent
  connections, issuing randomized-key traffic (`--command-key-pattern R`) over a 64,000-key space.
* **CPU pinning**: on this AMD EPYC 7B13 host (32 physical cores / 64 SMT threads), the server is pinned to
  physical cores 16-31 and the client to physical cores 0-15 — disjoint physical cores with every SMT sibling
  left idle. The client is deliberately placed on cores 0-15 rather than 0 alone because core 0 absorbs
  orders of magnitude more softirq/NET_RX load than the rest of the range; giving that load to the load
  generator (rather than to shard 0) avoids handicapping one shard relative to its peers.
* **Timing**: each run is a 5-second steady-state window (`BENCH_TEST_TIME`). One warmup run per workload is
  discarded before three measured iterations (`BENCH_WARMUP=1`, `BENCH_ITERATIONS=3`) to absorb TCP slow
  start, allocator arena warmup, hash-table growth, and first-touch page faults.
* **Headline statistic**: the **median** of the three measured runs (not the mean), so a single outlier run
  does not skew the reported number. The script also reports the coefficient of variation (CV); workloads
  with CV above 5% are flagged as noise-dominated rather than being read as a clean win or loss.
* **Population**: read/lookup/removal workloads (`GET`, `HGET`, `LPOP`, `LRANGE`, `SISMEMBER`, `ZRANGE`,
  `DEL`, `EXISTS`) are pre-populated across the full 64,000-key space from a single sequential connection
  before each run, guaranteeing a 100% hit rate rather than the largely-miss workload that an
  under-populated keyspace would otherwise produce.
* **Why this benchmark matters**: these are the commands an application actually issues in steady-state
  production traffic — single-key CRUD, small multi-key batches, and container membership/lookup ops across
  every core Redis data type — rather than a synthetic microbenchmark of one code path.

---

## 2. Head-to-Head Results: 16 Physical Cores

Figures are the median of 3 measured runs; `CV` is the run-to-run coefficient of variation for that engine on
that workload. `Δ` is Rudis's throughput relative to Dragonfly. At this core count, all sixteen measured
commands favor Rudis.

| Workload | Dragonfly (ops/sec) | CV | Rudis (ops/sec) | CV | Δ | Dragonfly p99 (ms) | Rudis p99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **SET** | 1,876,363 | 4.1% | **2,477,258** | 1.1% | **+32.0%** | 0.540 | **0.409** |
| **GET** | 730,007 | 1.1% | **2,067,300** | 0.8% | **+183.2%** | 1.402 | **0.494** |
| **INCR** | 2,748,865 | 0.6% | **3,223,111** | 1.1% | **+17.3%** | 0.368 | **0.313** |
| **MSET (5-key)** | 389,854 | 1.8% | **709,598** | 0.9% | **+82.0%** | 0.654 | **0.359** |
| **MGET (5-key)** | 346,020 | 1.6% | **491,997** | 1.4% | **+42.2%** | 0.739 | **0.519** |
| **HSET** | 2,416,707 | 1.2% | **3,021,190** | 2.8% | **+25.0%** | 0.419 | **0.334** |
| **HGET** | 2,331,842 | 0.9% | **2,999,483** | 0.3% | **+28.6%** | 0.432 | **0.335** |
| **LPUSH** | 2,165,909 | 3.7% | **2,924,231** | 1.1% | **+35.0%** | 0.467 | **0.345** |
| **LPOP** | 2,785,459 | 1.8% | **3,157,086** | 6.6%\* | **+13.3%** | 0.361 | **0.318** |
| **LRANGE (0-10)** | 1,994,760 | 1.7% | **2,913,396** | 1.0% | **+46.1%** | 0.505 | **0.344** |
| **SADD** | 2,479,047 | 1.1% | **3,114,815** | 0.4% | **+25.6%** | 0.408 | **0.324** |
| **SISMEMBER** | 2,589,361 | 2.5% | **2,989,364** | 1.9% | **+15.4%** | 0.389 | **0.336** |
| **ZADD** | 2,343,497 | 2.3% | **2,963,699** | 1.7% | **+26.5%** | 0.432 | **0.341** |
| **ZRANGE (0-10)** | 1,919,469 | 1.0% | **2,959,873** | 0.8% | **+54.2%** | 0.525 | **0.338** |
| **DEL** | 2,879,816 | 1.2% | **3,234,771** | 1.6% | **+12.3%** | 0.351 | **0.312** |
| **EXISTS** | 2,868,076 | 0.5% | **3,219,819** | 2.6% | **+12.3%** | 0.351 | **0.312** |

\* LPOP's 6.6% Rudis CV exceeds the 5% noise threshold used by the harness; treat the +13.3% delta on that row
as directionally correct but less statistically tight than the others.

---

## 3. Head-to-Head Results: 32 Physical Cores

At 32 physical cores, Dragonfly leads on every measured command. Note the methodological caveat below the
table: the 32-core sweep in the current result file predates the median/CV instrumentation described in
Section 1 and reports only a single mean per engine (no per-run CV), so treat these deltas as directionally
indicative rather than statistically as tight as the 16-core table above.

| Workload | Dragonfly (ops/sec, mean) | Rudis (ops/sec, mean) | Δ | Dragonfly p99 (ms) | Rudis p99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **SET** | 1,706,748 | 1,622,986 | -4.9% | 0.633 | 0.612 |
| **GET** | 2,428,145 | 1,987,573 | -18.1% | 0.411 | 0.510 |
| **INCR** | 2,280,494 | 1,841,396 | -19.3% | 0.449 | 0.595 |
| **MSET (5-key)** | 373,626 | 331,167 | -11.4% | 0.669 | 0.786 |
| **MGET (5-key)** | 517,575 | 312,270 | -39.7% | 0.493 | 0.823 |
| **HSET** | 2,471,780 | 1,938,713 | -21.6% | 0.411 | 0.524 |
| **HGET** | 2,261,975 | 1,483,352 | -34.4% | 0.449 | 0.738 |
| **LPUSH** | 2,244,723 | 1,904,965 | -15.1% | 0.455 | 0.528 |
| **LPOP** | 2,107,545 | 1,378,592 | -34.6% | 0.487 | 0.719 |
| **LRANGE (0-10)** | 1,797,278 | 1,343,774 | -25.2% | 0.548 | 0.664 |
| **SADD** | 2,139,052 | 1,871,513 | -12.5% | 0.473 | 0.546 |
| **SISMEMBER** | 2,768,932 | 1,762,166 | -36.4% | 0.366 | 0.609 |
| **ZADD** | 1,910,593 | 1,631,928 | -14.6% | 0.577 | 0.588 |
| **ZRANGE (0-10)** | 2,354,015 | 1,796,282 | -23.7% | 0.420 | 0.542 |
| **DEL** | 2,348,931 | 1,689,657 | -28.1% | 0.445 | 0.639 |
| **EXISTS** | 2,471,321 | 1,916,462 | -22.5% | 0.416 | 0.517 |

---

## 4. Findings

1. **Core-count crossover**: Rudis leads Dragonfly on every one of the sixteen measured commands at 16
   physical cores (+12% to +183%), but Dragonfly leads on every command at 32 physical cores (-5% to -40%
   for Rudis). This is a genuine, reproducible crossover, not a cherry-picked comparison — both tables are
   read from the same `benchmark_common_commands_results.json` file produced by the same harness. The most
   likely contributors are increased cross-shard channel/IPC pressure as Rudis's shared-nothing shard count
   grows relative to the fixed 16-thread client, and Dragonfly's proactor design scaling more favorably at
   higher thread counts on this host; this document reports the effect without asserting a specific root
   cause, since it has not been isolated with `perf`.
2. **Read throughput at 16 cores**: `GET` shows the largest single-workload advantage for Rudis (+183.2%,
   2.83x Dragonfly's throughput) alongside the lowest p99 latency of the whole suite (0.494 ms vs. 1.402 ms).
3. **Cross-shard multi-key commands** (`MSET`/`MGET`, 5 keys): Rudis leads at 16 cores (+82.0% / +42.2%) but
   shows the single largest regression at 32 cores (`MGET` -39.7%), consistent with cross-shard fan-out being
   the most sensitive workload to the core-count effect described in finding 1.
4. **Superseded numbers**: an earlier version of this document reported Rudis winning 7 of 8 sampled commands
   at 16 threads with deltas up to +534% (e.g., a since-corrected `GET` figure of ~3.3M ops/sec). Those
   figures came from `multi_command_results.json`, produced by a since-abandoned CPU layout that placed the
   memtier client on the SMT siblings of the server's own physical cores (see Section 1). That file has not
   been updated since the layout was fixed and should not be treated as current; this document now reflects
   only `benchmark_common_commands_results.json`.

---

## 5. Reproducing This Benchmark

```bash
cargo build --release

# 16-core sweep only (default)
python3 scripts/benchmark_common_commands_vs_dragonfly.py

# 16-core and 32-core sweep
python3 scripts/benchmark_common_commands_vs_dragonfly.py 16,32

# Narrow to specific commands, adjust duration/iterations
BENCH_WORKLOADS=SET,GET,INCR BENCH_TEST_TIME=10 BENCH_ITERATIONS=5 \
  python3 scripts/benchmark_common_commands_vs_dragonfly.py 16
```

Results are merged (per core-count, per engine) into `benchmark_common_commands_results.json` at the
repository root; re-running a subset of workloads updates only those entries.
