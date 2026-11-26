# Differences Between Rudis, Dragonfly, and Redis

This document summarizes the architectural, operational, and semantic differences between **Rudis**, **Dragonfly**, and **Redis**.

---

## 1. High-Level Comparison

| Feature / Property | Redis 7.2 | Dragonfly | Rudis |
| :--- | :--- | :--- | :--- |
| **Language** | C99 | C++20 | **Rust (edition 2024, toolchain pinned to 1.99 in `rust-toolchain.toml`)** |
| **Concurrency Model** | Single-threaded event loop (I/O threads in 6.0+) | Shared-nothing with fibers on `io_uring` (epoll fallback, `--force_epoll`) | **Shared-nothing Thread-Per-Core on Linux `io_uring` (Monoio)** |
| **Ingress Load Balancing** | Single listener socket | Single listener socket + thread dispatch | **Kernel `SO_REUSEPORT` 4-tuple balancing** |
| **Kernel Bypass & Hardware** | None (standard libc sockets) | None (standard libc sockets) | **AF_XDP and `SO_ZEROCOPY` code paths exist but are not wired onto the live network path; kernel TLS offload is not implemented (TLS is userspace `rustls` only) — see [architecture.md](architecture.md) and [design/10_kernel_bypass_xdp.md](design/10_kernel_bypass_xdp.md)** |
| **Memory Allocation** | jemalloc (global heap) | Custom DashTable + Mimalloc | **Thread-local `RudisTable` + `tikv-jemallocator` (jemalloc) as the process global allocator** |
| **Snapshots (`BGSAVE`)** | Process `fork()` with Linux CoW | Fiber-level snapshot iteration | **No `fork()`: every shard saves from a copy-on-write image started at the same moment, in steps of ~1 ms between its other clients; CRC, writes and fsync run on a writer thread (plain `std::fs`, not `io_uring`). Tiered values are read back into the RDB — see [rdbsave.md](rdbsave.md)** |
| **Multi-Core Replication** | Single TCP connection (`PSYNC`) | Per-shard parallel flows (`DFLY FLOW`) | **Rudis-to-Rudis replication uses single-connection `PSYNC`, same as Redis. `DFLY FLOW` exists master-side only, to interoperate with a Dragonfly-protocol client — Rudis replicas never use it. See [replication.md](replication.md)** |
| **NVMe Tiered Storage** | None (pure in-memory) | SmallBins tiering | **3-State Lifecycle + SmallBins 4KB + Direct I/O (`O_DIRECT`)** |
| **Dual-Protocol Support** | RESP2 / RESP3 only | Redis + Memcached gateway | **Redis RESP2 (full) / RESP3 (output-side typed replies via `HELLO 3`) + Memcached Text Protocol on the same port** |

---

## 2. Structural & Semantic Limits

### String & Value Sizes
* **Redis**: Strings are limited to 512 MB, enforced by `proto-max-bulk-len`, and client input buffers are bounded by `client-query-buffer-limit` (1 GB default).
* **Dragonfly**: Strings are limited to 256 MB.
* **Rudis**: Enforces both `proto-max-bulk-len` (default 512 MB, configurable at runtime via `CONFIG SET proto-max-bulk-len`, minimum 1 MB) and `client-query-buffer-limit` (default 1 GB, configurable via `CONFIG SET client-query-buffer-limit`, minimum 1 MB, as in Redis) in the RESP parser (`src/resp.rs`) and connection read loop (`src/connection.rs`).

### Integer & Numeric Precision
* **Redis**: Integers in commands like `INCRBY` are signed 64-bit integers (`[-2^63, 2^63 - 1]`).
* **Dragonfly**: Signed 64-bit integers with 53-bit Lua integer bounds.
* **Rudis**: Signed 64-bit integer arithmetic and IEEE 754 double-precision floats for `INCRBYFLOAT` and geospatial coordinates.

### Expiry Precision & Lifetime
* **Redis**: Millisecond-accurate TTLs with a probabilistic active expiration cycle.
* **Dragonfly**: Rounding to nearest second for expiration intervals exceeding $2^{28}\text{ ms}$ (~3 days).
* **Rudis**: Millisecond precision. Expiry is bit-packed as a 54-bit Unix millisecond timestamp directly inside the 16-byte `CompactKey` header of each `RudisEntry` (no separate expirations table, no extra 16-byte `Option<Instant>` field); an expired key is reclaimed passively the next time any operation touches it, plus an active sampling cycle that periodically scans for and evicts already-expired keys. See [`docs/design/05_storage_engine.md`](design/05_storage_engine.md).

---

### Multi-Key Atomicity Across Shards
* **Redis**: Single-threaded, so every command and `MULTI`/`EXEC` is atomic.
* **Dragonfly**: Multi-key commands and transactions are scheduled across shards atomically.
* **Rudis**: multi-key commands whose keys live on different shards all run, but they are **not
  all atomic**. This is a deliberate trade-off (per-shard execution keeps the thread-per-core fast
  path); Redis Cluster refuses such commands outright with `CROSSSLOT`. What each kind guarantees:

  | Commands (keys on several shards) | Guarantee |
  |---|---|
  | `MGET`, `MSET`, `MULTI`/`EXEC` | Isolated from each other: an `MGET` never sees an `MSET` or `EXEC` half applied (a seqlock check with a fallback to per-shard VLL locks; [`design/04_sharding_mesh.md`](design/04_sharding_mesh.md) §4.4), and `EXEC`s are ordered against each other. Not isolated from plain single-key commands, and two concurrent `MSET`s (or an `MSET` and an `EXEC`) of the same keys can apply in a different order on each shard. |
  | `RENAME`, `RENAMENX`, `COPY`, `LMOVE`, `RPOPLPUSH`, `SMOVE`, `MSETNX`, `BITOP` | Hold every involved shard's transaction lock, like `EXEC`: serialized with transactions and with each other, never seen half done by them, and replicated as one logical command (moves) or per shard. Like `EXEC`, not isolated from plain single-key commands. |
  | `DEL`, `UNLINK`, `EXISTS`, `TOUCH` | **Not atomic**: each shard handles its own keys independently and the counts are summed. A concurrent reader can see some keys deleted and others not yet; a concurrent writer can recreate a key between the shards' steps. |
  | `SINTER`/`SUNION`/`SDIFF` (and `*STORE`), `SINTERCARD`, `ZUNION`/`ZINTER`/`ZDIFF` (and `*STORE`), `ZINTERCARD`, `ZRANGESTORE`, `PFCOUNT`, `PFMERGE`, `LCS`, `SORT … STORE`, `GEOSEARCHSTORE` | **Not atomic**: the source keys are read from their shards one at a time (not one snapshot), then the destination, if any, is written. A write racing the command can be seen in some sources and not others. |
  | `LMPOP`, `ZMPOP`, `XREAD` over several keys | Each key is read or popped on its own shard; the command acts on the first key that has data, as Redis does, with no snapshot across keys. |
  | Blocking `BLMOVE`/`BRPOPLPUSH` | Woken when the source gets an element, then moved like `LMOVE`; if another client takes the element first, it blocks again for the rest of its timeout (Redis hands the element to the blocked client directly). |
  | `EVAL`/`FCALL` | Refused with `CROSSSLOT`: a script runs on one shard. |

  To make related keys atomic in every case, put them on one shard with a hash tag
  (`{user:1}:a`, `{user:1}:b`): commands on one shard are fully atomic, as in Redis. To have rudis
  enforce that, set `cross-shard-strict yes` (config file, `--cross-shard-strict yes`, or
  `CONFIG SET`): a multi-key command or a transaction whose keys live on different shards then fails
  with Redis Cluster's `CROSSSLOT` error instead of running with the weaker guarantees above.
  In cluster mode (`cluster-enabled yes`) such commands always get `CROSSSLOT`, as in Redis Cluster.

### Long-Running Scripts
* **Redis**: While a script runs past `busy-reply-threshold`, Redis keeps processing events from
  inside the script's hook: every other client gets `-BUSY`, and `SCRIPT KILL`, `FUNCTION KILL`
  and `SHUTDOWN NOSAVE` are served.
* **Rudis**: The same, from every connection, on any shard and with `--threads 1`. Past the
  threshold the script is suspended every 10 ms so its shard can answer (`-BUSY`, the kill
  commands, `SHUTDOWN NOSAVE`, `FUNCTION STATS`); nothing else touches the shard's data until the
  script ends, so it still looks atomic. One limit: Lua cannot suspend inside a C call, so a
  script stuck inside one (for example a `table.sort` comparator that never returns) holds its
  shard until it returns from it; other shards still answer `-BUSY` and accept the kill.

### Hash Templates (HIMPORT)
* **Redis** (8.6): `HIMPORT PREPARE` / `SET` / `DISCARD` / `DISCARDALL` build hashes from a
  per-connection field list, stored in a shared-template encoding (`template-listpack` /
  `template-array`, counted by INFO `hash_templates` / `hash_template_keys`) that keeps one copy
  of the field names for all keys using them.
* **Rudis**: The commands behave the same (per-connection fieldsets, cleared by `RESET` or
  disconnect; each script run its own session; `SET` replaces an existing hash, refuses other
  types; fields listed in template order), but they build ordinary hashes: no memory is saved,
  `OBJECT ENCODING` reports `listpack` / `hashtable`, and the template INFO fields and
  `hash-min-template-entries` have no effect. `HIMPORT SET` reaches the AOF and replicas as
  `DEL` + `HSET`. Redis' `unit/type/hash-templates` suite is not run, since most of it checks the
  encoding.

### Event-Loop Metrics in INFO

Redis reports metrics of its single-threaded event loop (`eventloop_cycles`,
`eventloop_duration_sum`, `eventloop_duration_cmd_sum`,
`instantaneous_eventloop_cycles_per_sec`, `instantaneous_eventloop_duration_usec`,
`eventloop_cycles_with_clients_processing`, `total_client_processing_events`, and in
`INFO debug` the `eventloop_duration_*`/`eventloop_*_max` fields). Rudis runs one event
loop per thread and does not report these. Per-client pipeline counters are in
`CLIENT LIST` (`tot-cmds`, `read-events`, `avg-pipeline-len-sum`/`-cnt`). The Redis
tests for them are skipped (`scripts/tcl_skips/unit/info.txt`).

## 3. Replication & High Availability

### Parallel Replication vs. Single Stream
* In **Redis**, replicas connect over a single TCP connection (`PSYNC`).
* **Rudis's own replica implementation also uses a single-connection `PSYNC` stream** against another Rudis (or Redis/Valkey) master — this is not a difference from Redis.
* Rudis's master side additionally implements the Dragonfly-specific handshake (`REPLCONF capa dragonfly`) and can serve $N$ parallel `DFLY FLOW` connections (`DFLY FLOW <replid> <sync_id> <shard_id>`) to a client that performs that handshake itself — but this is a compatibility surface for Dragonfly-protocol-aware clients, not something Rudis-to-Rudis replication exercises today. No linear-scaling replication bandwidth figure for this path has been independently measured for this revision; treat any such figure as unverified until traced to a committed benchmark document. See [replication.md](replication.md) for the full protocol split.

### Full-Sync Loading on the Replica
* A rudis replica always receives the master's RDB into a temp file and loads it once the transfer is complete: a transfer that breaks off leaves the old dataset untouched. With `repl-diskless-load disabled` (the default) or `on-empty-db` it reports `loading:1` from the load on, like Redis's disk-based load; with `swapdb` (when not loading asynchronously), `flush-before-load` or `flushdb` it reports it from the start of the transfer, as a Redis replica loading from the socket does, and data commands get `-LOADING` throughout (`flush-before-load` and `flushdb` keep the old dataset until the load rather than dropping it first). The old dataset is dropped a piece at a time (or in the background with `replica-lazy-flush`) and the new one loads in steps, so `INFO`, `PING` and the other commands Redis allows while loading keep being answered. Every shard drops and loads its part in parallel, so a large dataset is replaced much faster than on Redis's one thread. As with a Redis replica (busy on its one thread while it loads), a command arriving during a load first waits for it, up to 100 ms, so a small dataset's load (a millisecond or so) never answers `-LOADING`.
* `repl-diskless-load swapdb` enables `async_loading` as in Redis: when the master's replication id is the replica's own (the replica already holds an earlier point of the same history), the old dataset keeps being served during the transfer and the load, `INFO` shows `async_loading:1`, and only the commands Redis flags `no-async-loading` (REPLICAOF, SAVE, BGSAVE, cluster topology changes, ...) get `-LOADING`. Each shard loads its part into a fresh dataset between serving its clients; once every shard has loaded, each one swaps the new dataset in, so for a moment some shards serve the new dataset and others the old one (Redis swaps at one instant). The old dataset is freed at the swap on the shard's thread, not in the background. A load that fails keeps the old dataset. Otherwise `swapdb` loads as described above.
* `repl-diskless-sync-delay` makes a master wait that many seconds before it starts a replica's full sync, but each replica's snapshot is streamed on its own, so there is no shared BGSAVE for late replicas to join; the default is 0 (Redis: 5).

### Replication Backlog Without Replicas
* As in Redis, a master keeps its backlog for `repl-backlog-ttl` seconds after its last replica leaves, then releases it (new replication id; later replicas full-sync). Every rudis shard appends each write to that one backlog (~2.5x SET throughput on 8 threads), so it is released as soon as it is unused for that long, and after a restart from an RDB a master keeps it only for 60 seconds (at most `repl-backlog-ttl`) unless a replica comes back: Redis keeps it for `repl-backlog-ttl` there too. Replicas returning within the window still continue with a partial resync. `INFO replication`'s `repl_backlog_active` shows the state.

### Expired Keys on a Writable Replica
* As in Redis, a read-only replica hides a key from its master once it expires (reads find it
  gone) but keeps it until the master's `DEL`. On a writable replica (`replica-read-only no`)
  rudis deletes such a key when any command finds it, reads included, where Redis deletes it
  only for a write and keeps hiding it from reads. Rudis's write paths take a key they can't
  find to be absent and insert it afresh, so hiding it from a write would store a second entry
  for the key. Like Redis, the first write after `REPLICAOF NO ONE` or `CONFIG SET
  replica-read-only no` starts afresh from an expired key (`INCR` gives 1, with no TTL).

### Loading the Dataset at Startup
* As in Redis, the server serves clients while it loads its RDB / AOF at startup: `INFO` shows
  `loading:1`, and commands Redis allows while loading (`INFO`, `CONFIG GET`, `AUTH`, `CLIENT`,
  pub/sub, ...) run; data commands get `-LOADING` until every shard has loaded its part (each
  shard's loader thread runs on that shard's core). Rudis also refuses, during the startup load
  only, the commands that would act on the part loaded so far: `REPLICAOF`, `CONFIG SET
  appendonly` and `DEBUG` other than `LOG`/`SLEEP`/`HELP`. `SHUTDOWN` or a `SIGTERM` during the
  load exits at once without saving, as in Redis. The "Ready to accept connections" log line and
  systemd's `READY=1` come once the load is done.

### Snapshots Without fork()
* `SAVE`/`BGSAVE` don't fork, but write one point in time like Redis's forked child: every shard
  starts a copy-on-write image with cross-shard writes held back for that moment, and a key
  written while the save runs is copied as it was first. A transaction or MSET spanning shards is
  in the file whole or not at all. Keys evicted while the save runs are in the file too (as Redis's
  child still has them), and a `SWAPDB` during a save leaves the file as of its start.

### Turning AOF On at Runtime
* `CONFIG SET appendonly yes` works as in Redis (writes logged at once, initial AOF written by a background rewrite, `SHUTDOWN` refused until it is done). Until then the writes go to `appendonly-<shard>.aof.incr` and no `appendonly-<shard>.aof` exists, so a crash in between never leaves an AOF without the dataset. Redis 7 tracks this in its multi-part AOF manifest; with `aof-multipart yes` (opt-in, below) rudis takes the shard's file out of its manifest instead.

### AOF Files
* **Redis 7**: one multi-part AOF in `appenddirname`: a base file and incr files listed in a manifest.
* **Rudis**: one AOF per shard, `appendonly-<shard>.aof` in `dir`, plus `appendonly.shards` recording the shard count. A Redis AOF directory or single file found at startup is imported. With `aof-multipart yes` (startup option, off by default) the shard files live in Redis 7's layout instead: `appenddirname` with a manifest naming one incr file per shard (tagged `shard <i>`, which Redis and Valkey ignore), so Redis or Valkey can load the directory as is. There is no separate base file yet: each shard's rewrite replaces its own incr file.

### Turning AOF Off During a Rewrite
* `CONFIG SET appendonly no` during an AOF rewrite stops logging at once, as in Redis, but the rewrite runs to its end (its result is not used); Redis kills the rewriting child. A `BGREWRITEAOF` meanwhile gets "Background append only file rewriting already in progress" until it ends.

### Cluster Slots Not Covered
* As in Redis, `cluster-require-full-coverage yes` (the default) takes the node down while some slot is unassigned or held by a failed master no replica has taken over yet: every key gets `-CLUSTERDOWN The cluster is down`. With `no`, the node keeps serving the slots it owns and only the uncovered slots get `-CLUSTERDOWN Hash slot not served`. `CLUSTER INFO` reports the slot counts as Redis does (`cluster_slots_assigned`, `_ok`, `_pfail`, `_fail`). A master cut off from the majority of masters refuses its keys, as in Redis.

---

### Atomic Slot Migration (`CLUSTER MIGRATION`)
* Redis 8.4's atomic slot migration works between rudis nodes: `CLUSTER MIGRATION IMPORT|CANCEL|STATUS`, `TRIMSLOTS`, the `cluster-slot-migration-*` settings, the `CLUSTER INFO` task and trim fields, and the `mem_cluster_slot_migration_*` INFO fields. The nodes talk over rudis's own protocol (`CLUSTER SYNCSLOTS`), so a migration can't run between a rudis node and a Redis node.
* Redis uses two connections (a forked child sends the snapshot of the slots on one while the writes to them stream on the other); rudis uses one. The source sends the keys, then the writes made to the slots meanwhile, then the live writes. The fail points (`DEBUG ASM-FAILPOINT`) and error messages name Redis's channels and states as they map onto that link.
* As in Redis, the destination hides keys of slots it is importing from KEYS, SCAN, RANDOMKEY, DBSIZE and `CLUSTER GETKEYSINSLOT`/`COUNTKEYSINSLOT` (INFO keyspace counts them), and never expires or evicts them until it serves the slots; its replicas do the same. A failed import is tried again (from the slots' new master after a failover) until it completes or is canceled; failovers, FLUSHALL, CLIENT PAUSE, slot reassignments and FORGET cancel tasks as in Redis.
* Trimming (the source's keys after a migration, the destination's after a failed one) is `bg` by default (dropped at once, counted in `lazyfreed_objects`) or `active` (a key at a time) while clients use tracking; replicas and the AOF get one `TRIMSLOTS` instead of a DEL per key. A master that started up, or a replica that became master, trims its keys of slots other nodes serve.
* Extended structures (JSON documents, probabilistic structures, vector sets) migrate with the slots. `CLUSTER MIGRATION` has its own `cluster|migration` entry in `COMMAND` and the ACL table.
* **The source dumps the slots' keys in one step per shard.** Redis's forked child writes the snapshot while the parent keeps serving. A rudis shard instead serializes all of its keys in the migrated slots at once on its own thread, so the snapshot and the start of the write stream are one consistent point, and its clients wait meanwhile. The source also holds the whole serialized snapshot in memory until it is sent. Migrating a few hundred slots of an ordinary dataset takes well under a second (the cluster soak migrates 50-400 slots at a time). Dumping takes about 5 s per GB of keys on a shard (measured: 500,000 1 KiB keys on one shard, 2.5 s, during which that shard answered nothing), and needs that much memory again until it is sent. Migrate large datasets in smaller ranges, or across more shards. A stepped, copy-on-write snapshot like the one full syncs use is planned.
* A master whose slots a migration took all of stays an empty master unless `cluster-allow-replica-migration` is on (as in Redis).

## 4. Multi-Protocol Engine (Redis + Memcached)

* **Redis** requires external proxy layers (like Twemproxy or Envoy) to speak Memcached protocol.
* **Rudis** natively accepts both Redis RESP commands and Memcached text-based commands (`set`, `get`, `incr`, `decr`, `stats`, `quit`) on the same listening port (`6379`) with zero configuration. Framing is automatically detected upon initial connection read, sharing database 0 with zero proxy overhead.

---

## 5. Pub/Sub Messaging & Sharded Delivery

* **Redis**: Standard Pub/Sub broadcasts messages across the entire cluster bus. Redis 7 introduced `SPUBLISH` for slot-bound pub/sub.
* **Dragonfly**: Backed by a 16-shard `ShardedHashMap` with fine-grained mutexes and RCU pointer swaps.
* **Rudis**: Uses a **16-stripe atomic presence bitmask** (`ShardedPresenceTable`) so a global
  `PUBLISH` skips shards with no subscriber in the channel's stripe, avoiding a broadcast to
  every shard on every publish. Because the table is striped by a hash of the channel name
  rather than keyed per-channel, distinct channel names can occasionally collide into the same
  stripe, causing a rare false-positive broadcast to a shard with no real subscriber — a
  documented trade-off, not a claim of zero unnecessary broadcasts. `SPUBLISH` uses
  point-to-point CRC16 slot routing with zero-copy `bytes::Bytes` delivery and bounded
  subscriber queue backpressure. See [pub-sub.md](pub-sub.md).

### Subkey (Hash Field) Notifications

Rudis publishes the Redis 8.8 subkey notifications (`notify-keyspace-events` flags `S`, `T`,
`I`, `V` with `h`) with the same channels and payloads. Two details differ in how they are
produced:

* Hash fields that expire are removed per key, all expired fields at once (not one by one as
  each is read). The `hexpired` event, and the `HDEL` replicated for them, lists those fields
  shortest name first, then bytewise (`f2` before `f10`), rather than in access order.
* A read-only replica does not expire fields itself; its master's `HDEL` for them is their
  expiry. When every field such an `HDEL` names is already past its deadline on the replica, it
  reports `hexpired` (keyspace and subkey events) instead of `hdel`, as the master did.

---

## 6. Document Store & RediSearch Engine

* **Redis**: Requires loading external dynamic C modules (`rejson.so`, `redisearch.so`).
* **Dragonfly**: Implements a native subset of JSON and search commands.
* **Rudis**: Natively integrates RFC 8259 RedisJSON document indexing and RediSearch with:
  - **Balanced `RangeTree`**: $O(\log N + K)$ numeric range indexing replacing naive linear filter scans.
  - **`FT.AGGREGATE` Pipeline**: Full multi-stage execution pipeline supporting `GROUPBY`, `REDUCE` (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`), `APPLY` arithmetic expressions with recursive-descent evaluation, `SORTBY`, and `LIMIT`.
  - **Hybrid Vector Fusion**: Reciprocal Rank Fusion (RRF) combining Okapi BM25 full-text rank scores with HNSW vector cosine similarity.
