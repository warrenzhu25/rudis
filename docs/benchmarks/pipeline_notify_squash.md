# Benchmark: Pipeline Batching with Keyspace Notifications Enabled

Any non-empty `notify-keyspace-events` setting turned off pipeline batching
(`can_squash`), so every pipelined command ran one at a time with a full
cross-shard round trip.

Now a pipeline is still batched when each command is either a read (reads emit
no events) or a write on a checked list. For every write on the list, the
events from the batched paths match unbatched execution, event for event and
in the same order per key. The write fast paths in the local batch loop now
also require notifications to be off, as the remote batch handler already did.
With notifications on, those writes take `execute_local_command`, which emits
the events. Other writes, such as MSET, keep the whole pipeline unbatched, as
before.

Checked with a script that compares the events published for 38 pipelined
write cases. The new build ran with batching forced on for every write. The
baseline doesn't batch when notifications are on, so its events come from
unbatched execution. Every case matched, apart from XADD's time-based IDs in
the replies.

Baseline: `a457281`. Machine: 64 cores, `MONOIO_FORCE_LEGACY_DRIVER=1`.
4 server threads (unpinned), `notify-keyspace-events KEA`. memtier
`-t 4 -c 25`, GET/SET 1:1, 100k keys, 10 s per row. Latencies in ms.

| Workload | base ops/s | new ops/s | Δ | base p50 / p99 | new p50 / p99 |
| :--- | ---: | ---: | ---: | :---: | :---: |
| Pipeline 16, no subscriber | 557,539 / 566,966 | 1,565,045 / 1,584,473 | **+180%** | 2.5–2.6 / 7.6–8.2 | 0.91 / 2.5–2.6 |
| Pipeline 1, no subscriber | 164,430 / 161,524 | 159,710 / 161,216 | ±0 | 0.59 / 1.23–1.26 | 0.59–0.60 / 1.32–1.34 |
| Pipeline 16, `PSUBSCRIBE __keyevent@0__:set` | 171,681 | 212,002 | +23% | 6.9 / 55.6 | 7.8 / 21.5 |
| Pipeline 1, with subscriber | 136,024 | 135,332 | ±0 | 0.55 / 3.1 | 0.54 / 3.2 |

With a subscriber, delivering about 100k+ messages/s to a single client is
the bottleneck either way.

Notifications off (no change expected): GET/SET pipeline 1 170,288 → 165,823
and pipeline 16 1,800,805 → 1,758,772 ops/s, within run-to-run noise.

## Standard protocol (agent.md §3)

16 server threads pinned to cores 0-15, memtier with 32 threads pinned to
cores 32-63, 100% SET, 1 KB values, pipeline 100, 60 s, notifications off:

| Run | base ops/s | new ops/s |
| :--- | ---: | ---: |
| 1 (new first) | 2,804,129 | 2,745,277 |
| 2 (base first) | 2,759,202 | 2,782,288 |

No regression; the two orders disagree in sign, so it's within noise.
