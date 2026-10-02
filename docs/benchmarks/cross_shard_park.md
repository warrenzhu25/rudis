# Benchmark: Parking Instead of Spin-Waiting on Cross-Shard Replies

A connection waiting on a reply from another shard used to busy-poll before
parking: 32 polls for FastGet/FastSet, 48 in `execute_remote`, 64–256 for
MGET/MSET scatter, 256 in the pipeline `flush_remote_batches` loop, and a
128-poll wait on the descriptor pool's `Arc::strong_count`. While a
connection busy-polled, its event loop could not serve its other connections
or drain the mailbox. The spin count is now the `cross-shard-spin` setting
(config file and `CONFIG SET`). The default is 0, which parks immediately. The
descriptor pool reuses an entry only when the owner shard has already dropped
it, and never waits for that.

Baseline: `c8c3f93`. New: the commit that adds `cross-shard-spin`.
Machine: 64 cores. `MONOIO_FORCE_LEGACY_DRIVER=1`.

## Cross-shard mix (4 server threads, unpinned)

memtier `-t 4 -c 25`, 100k keys, 10 s per row, two alternating rounds per
binary. Latencies in ms.

| Workload | base ops/s | new ops/s | Δ | base p50 / p99 | new p50 / p99 |
| :--- | ---: | ---: | ---: | :---: | :---: |
| GET/SET 1:1, pipeline 1 | 161,791 / 163,031 | 171,306 / 164,886 | +4% | 0.59 / 1.26–1.31 | 0.56–0.58 / 1.22–1.26 |
| GET/SET 1:1, pipeline 16 | 1,521,845 / 1,537,050 | 1,756,790 / 1,713,416 | **+13%** | 0.975 / 2.19–2.24 | 0.81–0.82 / 2.14–2.18 |
| MGET 10 keys, pipeline 1 | 112,348 / 112,879 | 128,311 / 127,346 | **+13%** | 0.84 / 1.78–1.91 | 0.73 / 1.75–1.78 |
| MSET 10 keys, pipeline 1 | 124,246 / 121,403 | 123,570 / 125,029 | ±0 | 0.76–0.78 / 1.79–1.82 | 0.75 / 1.75–1.95 |

With `CONFIG SET cross-shard-spin 64`, the new build drops back toward the
baseline: 158,964 at pipeline 1, 1,672,971 at pipeline 16, and 126,096 for
MGET. So 0 is the default.

## Standard protocol (agent.md §3)

16 server threads pinned to cores 0-15, memtier with 32 threads pinned to
cores 32-63, 100% SET, 1 KB values, pipeline 100, 60 s:

| Build | Ops/sec | p50 | p90 | p99 | p99.9 |
| :--- | ---: | ---: | ---: | ---: | ---: |
| base | 2,723,731 | 1.023 | 1.559 | 2.863 | 15.871 |
| new | 2,737,362 | 1.015 | 1.551 | 2.943 | 16.895 |

No regression (+0.5%, within run-to-run noise).
