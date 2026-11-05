# 24-hour soak test

**Result: passed.** After 24 hours and 3.7 billion commands the replica's dataset was identical to
the master's (`DEBUG DIGEST` equal), with one full sync and no partial resyncs, output-buffer
disconnections or anomalies. Memory stayed flat at the `maxmemory` cap from the second hour on
(no leak) while eviction removed 32 million keys.

Run with `scripts/soak.py` on rudis `6054795`, 2026-11-04 04:23 to 2026-11-05 04:23 UTC:

```bash
python3 scripts/soak.py --binary target/release/rudis --hours 24 --interval 60 --dir /tmp/rudis-soak
```

## Setup

| | |
|---|---|
| Host | 64 cores, Linux 7.1; the soak pinned to cores 16-31 (other work ran on the rest) |
| Master | port 21000, 4 shards, AOF on, `maxmemory 2gb`, `allkeys-lru`, `repl-backpressure-threshold 64mb`, dual-channel replication, replica output-buffer limit `512mb 256mb 60` |
| Replica | port 21001, 4 shards, AOF on |
| Load | two rate-limited memtier processes: a 1:3 SET:GET mix over 4M keys (more than fits in 2 GB, so eviction runs throughout), and a writer of keys with 5-600 s TTLs; `BGSAVE` every 10 minutes and a replica `BGREWRITEAOF` every 30, plus automatic AOF rewrites on growth |
| Sampling | INFO and a 200-GET latency probe on both servers every 60 s (1439 samples) |

## Results

| | first hour | last hour |
|---|---:|---:|
| master used_memory (MB) | 994 | 1915 |
| master used_memory_rss (MB) | 1037 | 2050 |
| master ops/s | 43,555 | 42,942 |
| master probe p50 / p99 (ms) | 0.08 / 0.36 | 0.09 / 0.55 |
| replica used_memory_rss (MB) | 1023 | 1987 |
| replica ops/s (replicated writes + its share of reads) | 14,551 | 14,484 |
| replica probe p50 / p99 (ms) | 0.08 / 0.35 | 0.08 / 0.27 |

The first hour includes the dataset filling up to the 2 GB cap; from hour 1 to hour 24 the
master's RSS stayed between 2128 and 2151 MB and throughput between 42.6k and 44.3k ops/s.

Counters at the end:

| | master | replica |
|---|---:|---:|
| commands processed | 3,735,802,501 | 1,290,066,453 |
| evicted keys | 32,289,320 | 0 |
| expired keys | 66,793,484 | 0 (replicated as DELs) |
| `sync_full` / `sync_partial_ok` | 1 / 0 | — |
| output-buffer limit disconnections | 0 | 0 |
| backpressure-delayed writes | 0 | — |
| error replies | 6 | 0 |
| BGSAVEs / AOF rewrites completed | 274 / 607 | — / 643 |

At the end the harness dropped the TTL keys (so expiry could not race the comparison), waited
for the replica (`WAIT 1` returned 1) and compared: both digests were
`324a09c1e4834bb931ebdc581decd9307560b371`. (DBSIZE differed by 20, keys that expired around the
comparison and were still counted on the replica pending the master's DEL; the digest skips
logically expired keys.)

## Findings

* **Two ~160 ms latency spikes** (165.6 ms and 161.3 ms single-probe maxima, out of 1439
  samples), each in the minute of an automatic AOF rewrite on the master (12:19 and 02:50 UTC);
  the other 605 rewrites and all 274 BGSAVEs stayed under 20 ms. The host had transparent huge
  pages set to `always`, which rudis warns about at startup. Worth a closer look; p99 stayed
  under 5.1 ms in every hour.
* **6 error replies** on the master over 3.7 billion commands, spread over the day; the load
  generators do not report them individually.
* Runs found two bugs before this one started, both fixed: a cross-shard multi-key `UNLINK` was
  only partly applied on replicas (`efa8fd7`), and the harness's digest comparison raced key
  expiry.
