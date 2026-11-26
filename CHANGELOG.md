# Changelog

All notable changes to Rudis are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/). Before 1.0, a minor version may change behavior
or the persistence format; such changes are called out under **Breaking**.

## [Unreleased]

### Added

- `aof-multipart yes` (startup option, off by default): the per-shard AOF files are kept in
  Redis 7's multi-part layout, `appendonlydir/` with an `appendonly.aof.manifest` naming one
  incr file per shard (`appenddirname` and `appendfilename` are honoured). Redis and Valkey
  can load the directory directly. Existing flat `appendonly-<i>.aof` files move into the
  layout at startup; a thread-count change reshards as before; turning the option off imports
  the layout back into flat files.
- `INFO keysizes`: Redis 8's per-type key size histograms (`db0_distrib_strings_sizes`,
  `..._lists_items`, and so on), computed when asked for.
- `CLUSTER SLOT-STATS` (Valkey 8): per-slot `key-count`, and with `cluster-slot-stats-enabled`
  `cpu-usec`, `network-bytes-in`/`-out` and `memory-bytes`.
- `repl-diskless-load swapdb` loads a full sync asynchronously (Redis's `async_loading`)
  when the master's replication id is the replica's own: the old dataset keeps being
  served until the new one has loaded, and a failed load keeps it.
- `repl-diskless-sync-delay` is honoured: a master waits that long before starting a
  replica's full sync (default 0; Redis's is 5).
- While a master sends a replica its full-sync snapshot, `INFO replication` shows the replica
  as `state=wait_bgsave` and `INFO persistence` shows `rdb_bgsave_in_progress:1`, as Redis
  does during a diskless sync.

- Atomic slot migration (Redis 8.4's `CLUSTER MIGRATION IMPORT|CANCEL|STATUS` and
  `TRIMSLOTS`) between rudis nodes: the destination imports a set of slots as one unit (their
  keys, then the writes to them, then a short write pause on the source while it hands the
  slots over). Keys of slots being imported are hidden and never expire or get evicted until
  the destination serves them; failed imports are retried; failovers, flushes, client pauses
  and slot changes cancel tasks as in Redis; the slots' keys left behind are trimmed in the
  background or actively. CLUSTER INFO and INFO report tasks, trims and buffers.
- `INFO keyspace` reports `subexpiry` (keys with hash fields that expire).
- In cluster mode KEYS and SCAN return keys slot by slot, as Redis does.

### Changed

- `CLUSTER MEET` with a node the cluster forgot (`CLUSTER FORGET`) lifts the ban on every
  node, so it rejoins the whole cluster at once rather than after the minute-long ban.

### Fixed

- An RDB with a vector set element whose stored key (`vec:<set>:<element>`) had a length of
  252 to 255 modulo 256 could not be loaded: restarts refused the file and replicas failed
  every full sync. Such files load now.

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
