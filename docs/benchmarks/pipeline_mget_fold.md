# Benchmark: Folding Pipelined MGET into the Per-Shard Batches

In a pipeline, an MGET used to first flush and wait for every per-shard batch
queued so far. Then it sent its own scatter message to each shard it touched,
with a pooled descriptor and notify channel. A 16-deep MGET pipeline therefore
sent up to 16 × (shards − 1) messages, and every GET before an MGET cost an
extra round trip.

Now each MGET key becomes one GET item in the per-shard batches the pipeline
already builds, with its own reply slot after the pipeline's slots. Local keys
are read in place. The array is assembled once the batches reply, and a key
holding a non-string value reads as nil, as before. Order within a shard keeps
earlier writes from the same pipeline visible, so no flush is needed. A
pipeline now sends at most one message per remote shard.

Baseline: `a457281`. Machine: 64 cores, `MONOIO_FORCE_LEGACY_DRIVER=1`.
4 server threads (unpinned). memtier `-t 4 -c 25`, 100k preloaded keys,
10 random keys per MGET, 10 s per row, two alternating rounds. Latencies in ms.

| Workload | base ops/s | new ops/s | Δ | base p50 / p99 | new p50 / p99 |
| :--- | ---: | ---: | ---: | :---: | :---: |
| MGET 10 keys, pipeline 16 | 390,467 / 420,565 | 522,541 / 507,078 | **+25%** | 3.5–3.7 / 9.7–12.2 | 2.7–2.8 / 8.3–8.5 |
| MGET 10 keys + GET, pipeline 16 | 472,373 / 478,102 | 824,823 / 866,872 | **+77%** | 3.0–3.1 / 8.6–8.7 | 1.6–1.7 / 4.7–5.2 |
| MGET 10 keys, pipeline 1 | 114,907 / 121,494 | 119,302 / 121,372 | ±0 | 0.76–0.78 / 1.9–2.2 | 0.76–0.77 / 1.9–2.0 |
| GET/SET 1:1, pipeline 16 | 1,661,236 / 1,662,969 | 1,671,499 / 1,641,167 | ±0 | 0.84 / 2.3–2.4 | 0.85 / 2.3–2.4 |

## Standard protocol (agent.md §3)

16 server threads pinned to cores 0-15, memtier with 32 threads pinned to
cores 32-63, 100% SET, 1 KB values, pipeline 100, 60 s:

| Build | Ops/sec | p50 | p90 | p99 | p99.9 |
| :--- | ---: | ---: | ---: | ---: | ---: |
| base | 2,493,555 | 1.095 | 1.727 | 3.503 | 19.455 |
| new | 2,694,565 | 1.031 | 1.567 | 3.007 | 16.511 |

No regression; the SET-only path is unchanged, and the difference is
run-to-run noise.
