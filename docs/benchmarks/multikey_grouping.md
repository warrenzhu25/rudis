# Benchmark: Per-Shard Grouping for UNLINK, EXISTS and TOUCH

UNLINK, EXISTS (several keys) and TOUCH used to make one sequential
cross-shard round trip per key. They now group keys by owner shard. Local keys
run in place. Each remote shard gets one command holding all its keys, and
those commands go out at once through `Router::execute_remote_many`. So K keys
on S remote shards cost one concurrent round trip instead of K sequential ones.

Baseline: `33bfe82`. Machine: 64 cores, `MONOIO_FORCE_LEGACY_DRIVER=1`.
4 server threads (unpinned). memtier `-t 4 -c 25`, 100k preloaded keys, 10
random keys per command, pipeline 1, 10 s per row, two alternating rounds.
Latencies in ms.

| Command (10 keys) | base ops/s | new ops/s | Δ | base p50 / p99 | new p50 / p99 |
| :--- | ---: | ---: | ---: | :---: | :---: |
| EXISTS | 61,810 / 62,408 | 121,811 / 123,299 | **+97%** | 1.46–1.50 / 3.89–3.98 | 0.735 / 1.90–1.91 |
| TOUCH | 56,672 / 55,278 | 121,650 / 119,151 | **+115%** | 1.58–1.62 / 5.06–5.12 | 0.735–0.76 / 1.90 |
| UNLINK | 57,545 / 56,494 | 120,953 / 121,883 | **+113%** | 1.57–1.60 / 4.58–4.61 | 0.743 / 1.87–2.02 |

Paths this change doesn't touch, three alternating rounds:

| Workload | base ops/s | new ops/s |
| :--- | ---: | ---: |
| MGET 10 keys | 129,263 / 115,587 / 130,383 | 130,006 / 133,604 / 129,035 |
| MSET 10 keys | 127,101 / 120,426 / 127,220 | 128,164 / 130,244 / 128,528 |
| GET/SET pipeline 1 / 16 | 165,991 / 1,759,103 | 166,123 / 1,745,232 |

No regression; the spread between runs is ±5%.
