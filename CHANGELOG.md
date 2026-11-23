# Changelog

All notable changes to Rudis are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Before 1.0, a minor version may change behavior
or the persistence format; such changes are called out under **Breaking**.

## [0.1.0] - 2026-11-23

The first tagged release.

### Server

- Thread-per-core, shared-nothing engine on `io_uring` (Monoio): one shard per core, a
  lock-free per-core-pair mesh for cross-shard requests, `SO_REUSEPORT` ingress.
- Redis protocol (RESP2, and RESP3 via `HELLO 3`) and the Memcached text protocol on the same
  port.
- Data types: strings, hashes (with field TTLs), lists, sets, sorted sets, streams and consumer
  groups, bitmaps and bitfields, HyperLogLog, geo.
- `MULTI`/`EXEC`/`WATCH`, Lua scripting (`EVAL`) and functions (`FCALL`), pub/sub including
  sharded pub/sub, keyspace notifications, client-side caching (`CLIENT TRACKING`), blocking
  commands.
- ACLs, `AUTH`, protected mode, TLS (including client certificates and the cluster bus).
- Memory: `maxmemory` with the Redis eviction policies, `maxmemory-clients` client eviction,
  per-client memory in `CLIENT LIST`, output and query buffer limits.

### Persistence and high availability

- RDB snapshots without `fork()`, loading Redis-written RDB files; AOF with rewrite.
- `PSYNC` replication with partial resync, diskless sync, `WAIT`/`WAITAOF`.
- Redis Cluster: gossip bus, automatic failover, manual `CLUSTER FAILOVER`, live slot
  migration and resharding.

### Experimental (off by default)

- JSON, search and vector search (`FT.*`, Vector Sets), probabilistic types (Bloom, Cuckoo,
  Count-Min, Top-K), NVMe tiering; enabled with `enable-experimental-commands yes`.

### Testing

- 81 of the Redis/Valkey TCL test suites run in CI, with every skipped test listed with its
  reason in `scripts/tcl_skips/`.
- A continuous 6-hour chaos soak of a 6-node cluster (failovers, resharding, `kill -9`,
  snapshots) checks for lost writes and replica divergence on every change to `main`.
- A per-commit performance gate and nightly fuzzing of every untrusted input.
