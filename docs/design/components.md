# Rudis Subsystem Architecture & High-Level Design Guide

This document provides the high-level architectural specifications for all 19 core subsystems of **Rudis**.
It summarizes **what** each subsystem does, **why** it was designed that way, its **key concurrency invariants**,
and the most important findings from this session's source-verification pass against the current codebase.

Each entry links to its full per-subsystem design document under `docs/design/`, which carries the complete
invariant list, architecture diagrams, and performance discussion. This document is a rollup, not a
replacement — read the linked document for implementation-adjacent depth, and see
[`docs/internal/components.md`](../internal/components.md) for concrete Rust struct definitions,
step-by-step algorithms, and source line references.

---

## Subsystem Index

- [01. Reactor Runtime & Server Lifecycle](#component-01-reactor-runtime--server-lifecycle) (`src/main.rs, src/server.rs`)
- [02. Connection Lifecycle & Command Execution](#component-02-connection-lifecycle--command-execution) (`src/connection.rs`)
- [03. RESP Protocol Engine & Command Parser](#component-03-resp-protocol-engine--command-parser) (`src/resp.rs`)
- [04. Sharding Architecture & Cross-Core Mesh](#component-04-sharding-architecture--cross-core-mesh) (`src/router.rs, src/shard.rs, src/mailbox.rs`)
- [05. Storage Engine & Compact Encodings](#component-05-storage-engine--compact-encodings) (`src/table.rs`)
- [06. Blocking Operations & The Reactive Event Hub](#component-06-blocking-operations--the-reactive-event-hub) (`src/block.rs`)
- [07. NVMe SSD Tiered Storage Engine](#component-07-nvme-ssd-tiered-storage-engine) (`src/tiering.rs, src/tiering/`)
- [08. Vector Search Engine: HNSW, SQ8 & Product Quantization](#component-08-vector-search-engine-hnsw-sq8--product-quantization) (`src/vector.rs`)
- [09. RediSearch Full-Text Engine & Reciprocal Rank Fusion](#component-09-redisearch-full-text-engine--reciprocal-rank-fusion) (`src/search.rs`)
- [10. Kernel Bypass & Zero-Copy Networking](#component-10-kernel-bypass--zero-copy-networking) (`src/xdp.rs, src/zerocopy.rs`)
- [11. Redis Cluster Topology & Gossip Protocol](#component-11-redis-cluster-topology--gossip-protocol) (`src/cluster.rs`)
- [12. CRDT Data Types & Manual Multi-Region Sync](#component-12-crdt-data-types--manual-multi-region-sync) (`src/crdt.rs`)
- [13. Lua Scripting & Redis 7 Functions Engine](#component-13-lua-scripting--redis-7-functions-engine) (`src/scripting.rs`)
- [14. Persistence & Replication Engines](#component-14-persistence--replication-engines) (`src/replication.rs, src/aof.rs`)
- [15. Security, Memory Allocator & TLS](#component-15-security-memory-allocator--tls) (`src/acl.rs, src/allocator.rs, src/tls.rs`)
- [16. JSON Document Store & JSONPath Engine](#component-16-json-document-store--jsonpath-engine) (`src/json.rs`)
- [17. Geospatial Commands](#component-17-geospatial-commands) (`src/geo.rs`)
- [18. Probabilistic Data Structures](#component-18-probabilistic-data-structures) (`src/probabilistic.rs`)
- [19. Pub/Sub Messaging Hub](#component-19-pubsub-messaging-hub) (`src/pubsub.rs`)

---

## Component 01: Reactor Runtime & Server Lifecycle

**Source files:** `src/main.rs, src/server.rs`

**Purpose & problem statement.** Redis's single event loop saturates one CPU core while leaving the rest of a
multi-core host idle; naive multi-threaded alternatives (Memcached-style global or fine-grained mutexes) trade
that bottleneck for spinlock contention and cross-socket cache-line bouncing. Rudis instead pins one
`monoio`/io_uring reactor per physical core, each owning an exclusive `SO_REUSEPORT` listener and a
thread-local `ShardDb`.

**Key design choice.** `ShardDb` lives in `Rc<RefCell<ShardDb>>`, a type that is not `Send` — the compiler
itself refuses to let a shard's data cross a thread boundary, enforcing the shared-nothing invariant instead
of relying on convention or review. Shard count defaults to `min(available_cores, 8)`. A small, explicitly
enumerated set of structures is the deliberate exception to "zero locks": the blocking-command hub
(`Arc<Mutex<BlockHub>>`), the replication hub, and connection-count balancing counters — none of these sit in
the local-key hot path.

**Key invariants.**
- Thread-per-core pinning via `core_affinity`, unless `--no-pin`; `SO_REUSEPORT` (non-cluster) or per-shard
  dedicated ports (cluster mode) balance ingress without userspace dispatch.
- Every accepted connection is panic-isolated via a hand-rolled `catch_unwind_async` future combinator, so one
  connection's panic cannot take down the other tasks sharing that shard's single-threaded runtime.
- Shutdown is real but partial: a signal sets an atomic flag, both accept loops break within ~200ms and the AOF
  writer flushes and fsyncs — but in-flight connections and background periodic tasks are not drained.
- Bounded, fixed-size periodic work only: a 100ms active-expiration cycle, a 20ms auto-tiering pressure check,
  and a 2s tiering GC pass — none scans the whole shard, so none shows up as a latency spike.
- The cross-shard receiver loop amortizes wakeup cost by draining up to 64 already-queued messages per wakeup
  instead of suspending once per message.

**Performance characteristics.** Local key access has no cross-core cost at all — no lock, no channel, no
extra atomic beyond the storage engine's own. `SO_REUSEPORT`'s kernel-side load balancing is measurably uneven
for a small number of long-lived connections (the codebase's own `conn_balance.rs` module comment gives a
64-connections-over-16-shards example where the busiest shard gets far more than its share); this imbalance is
mitigated by the `conn_balance` module, not eliminated.

**Verified findings.** No correctness gaps specific to this subsystem were identified in this session's
verification pass. Shutdown's lack of connection/task draining and no automatic `SAVE` on exit are documented,
known limitations rather than bugs (see Component 14 for the related, more serious `save <seconds> <changes>`
scheduling gap).

**Further reading:** [`docs/internal/01_reactor_runtime.md`](../internal/01_reactor_runtime.md)

---

## Component 02: Connection Lifecycle & Command Execution

**Source files:** `src/connection.rs`

**Purpose & problem statement.** In a thread-per-core design, naive key-by-key dispatch of a pipelined
client's commands would incur one inter-thread round trip per remote-routed command, and a fresh
channel-based reply path allocates on every hop. Rudis instead pins a connection to its accepting core for its
whole lifetime and "squashes" an eligible pipeline into one batched message per remote shard touched.

**Key design choice.** The reply path for squashed batches is not a `flume`-channel responder pool but
`BatchResponder`, a pooled, lock-free single-slot mailbox in `src/mailbox.rs` (`AtomicBool` + `UnsafeCell`):
the remote shard writes the payload then flips the flag (`Release`), and the connection polls the flag directly
(`Acquire`) without an allocation. A paired `flume::bounded(1)` channel exists purely as an async wake-up for
the fallback path — it never carries the payload itself.

**Key invariants.**
- Pure single-threaded client state: `handle_connection`'s locals are plain stack variables — no `Arc`/`Mutex`.
- A blocking command (e.g. `BLPOP`) flushes any already-buffered pipelined replies before suspending, so
  earlier responses are never held hostage by a later blocking wait.
- Cross-shard `MULTI`/`EXEC` acquires a per-shard advisory lock across every touched shard, in ascending
  shard-ID order, to avoid deadlocking against a differently-ordered concurrent transaction.
- Pipeline squashing is gated on more than routability: the client must be authenticated and, under active
  ACLs or cluster mode, have real permission for *every* key of *every* command in the batch — one ineligible
  command defeats squashing for the whole batch, falling back to the sequential path.
- `MGET`/`MSET` genuinely parallelize across shards via pooled scatter-gather descriptors (`src/mailbox.rs`),
  bounded by the slowest touched shard rather than the sum of every key's round-trip.

**Performance characteristics.** Pooled, cross-connection-reused mailboxes give zero-allocation steady-state
cross-shard fan-out; small replies are stored inline (`CompactResp::Small`, up to 30 bytes) with no heap
allocation. The spin-then-block cross-shard harvest trades CPU for latency: non-blocking sweeps happen without
yielding to the cooperative scheduler, so other connections on the *same core* make no progress during the
spin phase — an explicit, accepted fairness cost for the common case where remote shards reply in microseconds.

**Verified findings.** The plain `SET` fast path inside the squashed-pipeline path
(`execute_commands_squashed`) does **not** call `touch_watched_key`/`notify_key_invalidation`, unlike its
sibling inline fast paths (`INCRBY`, `DEL`, `HSET`, `SADD`, `ZADD`, `LPUSH`, `LPOP`, all of which do). A plain
`SET` executed inside a squashed batch, under the AOF-less/replica-less conditions that gate the fast path,
will not taint an active `WATCH` on that key and will not emit a RESP3 client-side-cache invalidation — a real,
verified correctness gap, not a design choice.

**Further reading:** [`docs/internal/02_connection_lifecycle.md`](../internal/02_connection_lifecycle.md)

---

## Component 03: RESP Protocol Engine & Command Parser

**Source files:** `src/resp.rs`

**Purpose & problem statement.** On a thread-per-core design, the same core that parses a command also
executes and serializes its reply, so every allocation or copy spent parsing subtracts directly from that
core's throughput budget. `resp.rs` owns exactly two responsibilities — frame parsing and command
construction — with no reply-serialization code (that lives in `connection.rs`).

**Key design choice.** RESP-array arguments are extracted via `BytesMut::split_to(len).freeze()` — a
reference-count bump, never a byte copy — and a dedicated fast-dispatch path in `parse_resp_array` handles
arrays of ≤16 elements for the highest-frequency commands, building the `Command` enum directly from cached
offsets and skipping the 283-arm `build_command` match entirely. The inline-text and Memcached-compatibility
parsers deliberately copy instead, since neither is on the pipelined hot path.

**Key invariants.**
- All-or-nothing frame consumption: a partially buffered command leaves the input buffer completely untouched
  (`Ok(None)`); a complete command is fully consumed in one call.
- Three independent input grammars — RESP arrays, inline space-separated text, and Memcached's ASCII storage
  grammar — dispatched from one entry point (`parse_command`) purely by the first byte, or by trial-parse for
  the Memcached case.
- No RESP3 *input* parsing exists in this file: `HELLO` is recognized, but RESP3 is purely an output-side
  concern implemented in `connection.rs`.
- Errors are untyped `String`s, distinguished only by convention (e.g. a literal `WRONGTYPE`/`CROSSSLOT`/`MOVED`
  prefix that callers inspect).

**Performance characteristics.** The fast path does one refcount bump per whole command instead of one per
argument, and bypasses the large `build_command` match for the highest-frequency verbs. No throughput/latency
benchmark figures are published for this component — the design doc explicitly declines to assert numbers that
have not been measured against the current build.

**Verified findings.** No maximum bulk-string/array length is enforced (overflow is guarded, but a large,
valid length has no configurable cap — no `proto-max-bulk-len` equivalent). This is a documented gap, not a
newly discovered correctness bug.

**Further reading:** [`docs/internal/03_resp_engine.md`](../internal/03_resp_engine.md)

---

## Component 04: Sharding Architecture & Cross-Core Mesh

**Source files:** `src/router.rs, src/shard.rs, src/mailbox.rs`

**Purpose & problem statement.** A shared-nothing design still needs a way to move a key request from the
core that accepted the connection to the core that owns the key, without reintroducing the locking the
architecture exists to avoid. Every key maps deterministically to exactly one shard, and a remote request is
never satisfied by locking another shard's table — it is serialized into a message and handed across a
lock-free channel (the "mesh") to the owning shard.

**Key design choice — corrects a materially stale prior description.** Cross-shard communication is **not**
"exclusively `flume` channels" carrying every payload. The hottest paths — single-key `GET`/`SET`, pipelined
`Batch` replies, and `MGET`/`MSET` scatter-gather — use purpose-built, pooled, lock-free shared-memory
completion cells defined in `src/mailbox.rs` (`FastGetDescriptor`, `FastSetDescriptor`, `BatchResponder`,
`ScatterMgetDescriptor`, `ScatterMsetDescriptor`), each an `Arc`'d, single-writer/single-reader structure
using an `AtomicBool` + `UnsafeCell` release/acquire handoff. `flume` channels remain throughout the mesh, but
on these hot paths they carry only a zero-sized wake-up signal, never the payload. Lower-traffic per-key
operations (`DEL`, `EXISTS`, `EXPIRE`, `PERSIST`, `TTL`, …) genuinely still allocate a fresh
`flume::bounded(1)` request/reply channel per remote call.

**Routing model.** Standalone mode: 64-bit FxHash of the key's hash-tag, mod `num_shards` — no fixed slot
space. Cluster mode: CRC16/XMODEM of the hash-tag mod 16,384 → slot → contiguous shard range
(`slot_to_shard(slot, num_shards) = (slot * num_shards) / 16384`, multiply-then-divide, not modulo).

**Key invariants.**
- `Router` is `Clone`, not a singleton — cloning bumps `Rc`/`Arc` refcounts only.
- A shard never blocks another shard's event loop; a slow remote shard only delays the caller awaiting it.
- Live cluster slot migration is tracked via a per-slot `Stable`/`Migrating`/`Importing`/`Moved` state machine
  that gates `-MOVED`/`-ASK` redirection on the single-command path.

**Performance characteristics.** Local execution has no cross-core cost at all. Remote execution cost is
dominated by scheduling latency (an SPSC push/pop is a handful of atomic operations), not the mesh mechanism
itself. Batching amortizes fixed per-message overhead by turning N per-key round-trips into one message per
remote shard actually touched.

**Verified findings.** Two genuinely different key-routing entry points coexist in `router.rs`, and they can
disagree during a live slot migration: the **static** free functions (`target_shard`, `target_shard_and_hash`)
compute `slot_to_shard` directly and never consult `slot_owners`, while the **dynamic** instance method
`Router::target_shard` indexes `self.slot_owners`, which reflects any live `set_slot_owner` override. Call-site
split: static routing is used by `get`, `set`, `expire`, `persist`, `ttl`, `incr_by`, `exists`, and the
pipelined dispatch phase `begin_mget_resp`; dynamic routing is used by `expiretime`, `del_keys`, the synchronous
`mget`/`mset`, and `json_mget`. The practical consequence: the same `MGET` command can route differently
depending on which of the two `Router` entry points handles it — `Router::mget` (dynamic) versus
`begin_mget_resp`/`finish_mget_resp` (static) — a real, currently unresolved routing inconsistency, not a
theoretical one. A related, distinct gap: `MGET`/`MSET`'s dispatch never consults per-key slot-migration state
at all (only the single-command path does).

**Further reading:** [`docs/internal/04_sharding_mesh.md`](../internal/04_sharding_mesh.md)

---

## Component 05: Storage Engine & Compact Encodings

**Source files:** `src/table.rs`

**Purpose & problem statement.** A classic separate value-table/expiry-table design (as in Redis's `dict.c`)
needs two hash lookups per access and pays pointer-chasing costs walking chained buckets. `RudisTable` is a
SwissTable/hashbrown-style open-addressed hash table that inlines a key's value **and** its expiration
timestamp into one 88-byte `RudisEntry` (`key: Bytes` 32B + `val: RudisValue` 40B + `expire_at: Option<Instant>`
16B), so a single probe answers both "does this key exist" and "is it still valid."

**Key design choice.** Fingerprint-and-probe over chaining: an 8-bit control byte per slot plus a
1-byte hash fingerprint let a lookup reject most non-matching slots via a single SIMD compare before ever
touching the full key. Probing uses the same triangular group-stepping scheme hashbrown uses, avoiding the
primary clustering plain linear probing causes.

**Key invariants.**
- Thread isolation: no mutexes, `RwLock`s, or atomics guard the table itself (only two process-wide `AtomicU64`
  counters exist, for `INFO` stats, not synchronization).
- `Hash`, `Set`, and `ZSet` values get adaptive small-form/full-form promotion past a size threshold; `List`
  and `Stream` do not (their backing types are already the right shape at any size).
- Passive inline eviction: any operation encountering an expired key frees it immediately, unless a debug-only
  `ALLOW_ACCESS_EXPIRED` flag is set.
- Cold-storage awareness without cold-storage I/O: `table.rs` performs no disk I/O itself, but
  `RudisValue::Tiered`/`RudisValue::Cooled` exist so the table can represent "this value lives on disk" or
  "this value lives on disk *and* is cached in RAM" for Component 07 to act on.

**Performance characteristics.** Probing is one unaligned 128-bit SIMD load and compare per 16-slot group on
x86_64 (no ARM/NEON path — a portable scalar fallback covers non-x86_64), at up to 7/8 load factor. Resize is
still monolithic: `RudisFlatTable::resize` rehashes the *entire* table in one synchronous pass — the
segmented/incremental resize from the original design's roadmap was never built (see the companion deep-dive
below). `used_memory` is a heuristic estimate (a fixed per-variant overhead plus a flat 64 bytes/entry), not
exact allocator accounting.

**Verified findings.** The companion design document [`rudis_table.md`](rudis_table.md) — the original
bucket-layout deep-dive — has been corrected against the current implementation: its 64-byte-cache-line-aligned
bucket layout, ARM/NEON SIMD path, and segmented/incrementally-splitting DASH-style directory were **never
built**; `src/table.rs` uses unaligned SIMD loads, a monolithic (non-segmented) `RudisFlatTable`, and FxHash
only (not a mix of "foldhash or fxhash" as an earlier draft stated). Separately, a value in the `Tiered` state
(spilled to NVMe by Component 07) is written as zero payload bytes during RDB/AOF serialization — see
Component 14's Verified Findings for the full description of that data-loss bug.

**Further reading:** [`docs/internal/05_storage_engine.md`](../internal/05_storage_engine.md) ·
[`rudis_table.md`](rudis_table.md) (original bucket-layout design, corrected against the implementation)

---

## Component 06: Blocking Operations & The Reactive Event Hub

**Source files:** `src/block.rs`

**Purpose & problem statement.** A client blocked on `BLPOP` on shard A must be woken by a write executed on
shard B, but the shared-nothing model gives shard B no way to reach into shard A's thread-local state.
`BlockHub` is a thread-safe reactive registry for blocking operations (`BLPOP`, `BRPOP`, `BZPOPMIN`,
`XREAD BLOCK`) that signals a waiting connection without polling for the value itself.

**Key design choice.** `BlockHub` is the one deliberate, narrow exception to "zero locks": one
`std::sync::Mutex<BlockHub>` per listening port, reached via `get_block_hub_for_port(port)` and shared by
every shard serving that port. The pop happens *inside* the same critical section that selects the waiter —
"pick a waiter" and "remove the value" are atomic by construction, avoiding a race between a separate
notify-then-pop design.

**Key invariants.**
- Reactor threads never sleep on a plain timer: a blocked command yields via `.await`ing a polling helper that
  also actively watches for client disconnect (a channel receive alone cannot detect a dead TCP socket without
  a clean FIN).
- FIFO fairness per key: waiter queues are `VecDeque`s, and notification always pops from the front.
- Guaranteed cleanup via RAII: a `BlockedClientGuard`'s `Drop` impl unconditionally unregisters the waiter,
  whether it resolved by pop, timeout, `CLIENT UNBLOCK`, or the connection task itself being dropped.
- `pause()`/`resume()` are wired to `MULTI`/`EXEC` deferral, not to Redis's `CLIENT PAUSE` — which is a
  complete no-op stub in Rudis.

**Performance characteristics.** A blocked client costs a wakeup-and-poll cycle at most every 20ms purely for
disconnect detection, but is woken immediately (no polling delay) on a real notification. The global per-port
mutex is held only for a short, synchronous, non-`.await`-ing critical section, and contention scales with
blocking-waiter registration/notification activity — ordinary (non-blocking) commands never touch this lock.

**Verified findings.** No correctness gaps specific to this subsystem were identified in this session's
verification pass.

**Further reading:** [`docs/internal/06_blocking_hub.md`](../internal/06_blocking_hub.md)

---

## Component 07: NVMe SSD Tiered Storage Engine

**Source files:** `src/tiering.rs, src/tiering/`

**Purpose & problem statement.** DRAM capacity is finite and expensive; naive spill-to-disk designs either
fragment small values across many small files or lose NVMe's latency advantage by going through the buffered
page cache. Keys follow a Hot (DRAM) → Cooled (DRAM, eviction candidate) → Tiered (NVMe) lifecycle; sub-2KB
records are packed into 4KB-aligned SmallBins pages to eliminate write amplification.

**Key design choice.** Each shard owns a thread-local `ShardTierManager` (`Rc`-based, not `Arc`/`Mutex`) —
cross-shard tiering requests go through ordinary `ShardMessage::Tier*` mesh messages (Component 04), never a
shared manager. `O_DIRECT` is opt-in via the `RUDIS_DIRECT_IO` environment variable and silently falls back to
buffered I/O if the underlying filesystem doesn't support it — there is no page-cache-bypass guarantee unless
both the env var is set *and* the filesystem supports it.

**Key invariants.**
- Read coalescing is the concurrency-sensitive part: `OpManager::read_page_coalesced` ensures only one physical
  read happens per in-flight 4KB page; other callers wait on the first reader's result.
- Write backpressure is a hardcoded 16MB byte-counter ceiling (`pending_stash_bytes`), not a queue depth;
  `stash_record` returns `WouldBlock` once exceeded.
- Once a key has ever been tiered, `Router::load_local` restores it to `RudisValue::Cooled`, not back to a
  bare hot value — there is no direct `Cooled → Hot` transition; `Cooled` is a permanent write-through layer.
- `upload_threshold_pct` is configured and reported via `TIER INFO`, but is not consumed by any gating
  decision — only `offload_threshold_pct` actually gates `Router::is_memory_constrained`.

**Performance characteristics.** GC reclaims whole 4KB pages, not individual records — a page with even one
surviving record cannot be punched, so deletion-heavy small-value workloads can accumulate dead-but-unfreed
bytes. Snapshotting cost depends on filesystem reflink support: instant on btrfs/XFS-with-reflink, an in-kernel
`copy_file_range` loop otherwise, falling back to a full userspace `std::fs::copy` only if neither
kernel-assisted path is available.

**Verified findings.** The companion design document [`tiered_storage.md`](tiered_storage.md) — the original
Dragonfly-inspired deep-dive — has been corrected against the implementation: its proposed 16-byte packed-
pointer `RudisExternalPtr` union, intrusive doubly-linked `CoolRecord` LRU cooling queue, and mimalloc-style
segmented `ExternalAllocator` with a free-range tree were **not built**. What shipped is simpler: the plain
17-byte `TieredPointer` struct, the `RudisValue::Cooled{ptr, val}` enum variant (no LRU ordering — eviction is
either targeted or a full-table sweep), and a monotonically increasing `current_offset` write cursor.
**Critically, freed disk space returned via `fallocate(FALLOC_FL_PUNCH_HOLE)` on delete is never reused by
future writes** — `current_offset` keeps growing regardless of how many holes exist behind it, so the backing
file's logical size grows unboundedly under a sustained tier-churn workload even though physical disk usage
stays bounded. Separately: `RudisValue::Tiered` entries are written as zero payload bytes during RDB/AOF
serialization (see Component 14).

**Further reading:** [`docs/internal/07_nvme_tiering.md`](../internal/07_nvme_tiering.md) ·
[`tiered_storage.md`](tiered_storage.md) (original allocator/cooling-queue design, corrected against the
implementation)

---

## Component 08: Vector Search Engine: HNSW, SQ8 & Product Quantization

**Source files:** `src/vector.rs`

**Purpose & problem statement.** Storing and searching millions of high-dimensional embeddings at full `f32`
precision is memory-intensive, and brute-force nearest-neighbor search does not scale to large corpora. A
thread-local HNSW graph index per shard gives approximate nearest-neighbor search with logarithmic-ish graph
traversal, with optional SQ8 scalar quantization or Product Quantization to shrink the per-vector footprint.

**Key design choice.** Distance kernels (`dot_product`, `l2_distance_sq`, `cosine_distance`) probe CPU
features at call time — AVX-512 first, then AVX2/FMA, then a portable 8-lane-unrolled scalar fallback — with
no ARM/NEON path. All three metrics return a "smaller is closer" distance (inner product is negated) so the
graph-traversal min-heap logic is metric-agnostic.

**Key invariants.**
- Thread-local only: an `HnswIndex` lives inside one shard's `ShardDb` with no `ShardMessage` variant to reach
  a vector index on another shard — this is unimplemented cross-shard support, not a locking decision.
- Product Quantization codebooks are **not trained on data** — each `(dim, m)` combination produces
  byte-for-byte identical, deterministically generated centroids (unit basis vectors plus a fixed hash-derived
  fill), with no k-means or training pass over real vectors.
- HNSW layer assignment is deterministic: the graph's PRNG is seeded with a fixed constant every time, not OS
  randomness — two indexes built from the same insertion order produce identical topology.

**Performance characteristics.** SQ8 gives a 4x memory reduction (4 bytes/dim → 1 byte/dim), and PQ gives up
to 32x — both figures are derived directly from the type definitions, not measured. No throughput/latency
numbers are published: prior unsourced figures (">3,500 vectors/sec", "<400µs p99", "75% RAM reduction") have
been removed rather than repeated unverified.

**Verified findings.** No data-loss or correctness bugs were identified. The most significant documented gap
is architectural, not a bug: vector indexes have no cross-shard reach at all, and (like Component 16's JSON
store) are excluded from RDB persistence.

**Further reading:** [`docs/internal/08_vector_engine.md`](../internal/08_vector_engine.md)

---

## Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion

**Source files:** `src/search.rs`

**Purpose & problem statement.** A full-text/secondary-index engine needs to see writes from every shard (a
document's fields can change from any core), which is in tension with a shared-nothing, per-shard storage
model. Rudis supports `TEXT` (Okapi BM25), `TAG`, `NUMERIC` (balanced range tree), and `VECTOR` (brute-force
cosine) fields via `FT.CREATE`/`FT.SEARCH`/`FT.AGGREGATE`, with lexical and vector results fusable via
Reciprocal Rank Fusion.

**Key design choice — corrects a materially stale prior description.** The indexing model is genuinely
**per-shard partitioned, not a single global lock**. Each index is stored twice: the real query-execution path
is a per-shard partition (`ShardDb.search_indices: HashMap<String, InvertedIndex>`) — `Router::ft_search`
queries the local partition first, then scatter-gathers to every other shard, each of which answers using only
its own local partition. A process-wide mirror registry (`SEARCH_INDICES`, behind a real `RwLock`) exists
purely for `FT.INFO`/metadata queries and as a defensive fallback — real lock contention exists only on that
mirror, not on the per-shard query path. Document identity is a dense `u32` `DocId` with free-list reclamation
on delete (`key_to_id`/`free_ids`), **not the document's string key** — deletion is O(terms in that document),
not a vocabulary scan.

**Key invariants.**
- Automatic ingestion is restricted to `HSET`/`HMSET`/`JSON.SET` (root path) only — no deferred/background
  indexing job, and other write paths (`SET`, `LPUSH`, …) never trigger re-indexing.
- BM25 uses one whole-document length, not per-field lengths — `FieldType::Text.weight` is parsed by
  `FT.CREATE` but never read anywhere in the scoring function, so declared field weights have no effect.
- Vector KNN is exact brute-force (`O(indexed documents)` per query), not an HNSW lookup — there is no
  `vector_index: Option<HnswIndex>` field anywhere in `InvertedIndex`.

**Performance characteristics.** Numeric range queries are genuinely sub-linear (a `BTreeMap`-backed range
tree, O(log N + K)). Dual-write to the per-shard partition and the process-wide mirror roughly doubles
indexing CPU/memory cost. `And`/`Or`/`Not` evaluation materializes a full intermediate result set per AST node
rather than streaming or short-circuiting. Posting lists are uncompressed `Vec<Posting>` with no
delta-encoding.

**Verified findings.** This session corrected a stale prior characterization of this subsystem as a single
globally locked index registry with string-keyed postings; the actual, verified architecture is per-shard
partitioned with dense integer document IDs, as described above.

**Further reading:** [`docs/internal/09_redisearch.md`](../internal/09_redisearch.md)

---

## Component 10: Kernel Bypass & Zero-Copy Networking

**Source files:** `src/xdp.rs, src/zerocopy.rs`

**Purpose & problem statement.** At very high packet rates, the Linux kernel's networking stack is a
documented source of CPU overhead that true kernel-bypass networking (AF_XDP) is designed to eliminate. This
subsystem does not perform real kernel bypass today and is not on the server's live network path — stated
plainly in its own design document, not a criticism reconstructed after the fact.

**Key design choice.** `xdp.rs` implements the AF_XDP data structures — UMEM, fill/completion/Rx/Tx rings, a
socket abstraction — as real, tested in-process Rust collections (`Vec`, atomics, `RwLock`), instantiated once
per shard at boot and driven by a genuine background polling loop in `src/server.rs`. `zerocopy.rs`
(`RegisteredBufferPool`, `ZeroCopyEngine`) implements real `MSG_ZEROCOPY`/`SO_ZEROCOPY` send primitives with
correct fallback behavior. Neither module makes a real AF_XDP syscall, loads eBPF, or binds a physical
interface; the only way real payloads enter either path is through explicit `XDP.*` admin commands.

**Key invariants.**
- `XdpMode` (`Driver`/`Skb`/`Simulated`) is cosmetic: it only changes what `XDP.INFO` reports as a string, and
  `Driver` mode is never actually selected by any code path.
- The CIDR rule table and per-source-IP token-bucket rate limiter are real, correct, O(small-N) userspace
  logic — a testable filter-policy engine, just not connected to live traffic.
- `zerocopy.rs`'s `send_zc` has zero callers outside its own unit tests anywhere in `src/`; the real connection
  write path (`connection.rs`) does not use this module.

**Performance characteristics.** No line-rate, packets-per-second, or CPU-overhead figures are claimed for
this subsystem — any such numbers would necessarily be invented, since nothing here sees real network traffic
at scale.

**Verified findings.** No new correctness bugs. This session's verification corrected an earlier internal-doc
revision that incorrectly claimed `XdpEngine` had no socket/ring/UMEM types at all — those types exist and are
real, just disconnected from the live path.

**Further reading:** [`docs/internal/10_kernel_bypass_xdp.md`](../internal/10_kernel_bypass_xdp.md)

---

## Component 11: Redis Cluster Topology & Gossip Protocol

**Source files:** `src/cluster.rs`

**Purpose & problem statement.** Horizontal scaling requires partitioning the keyspace across independent
nodes, redirecting misrouted clients, detecting and failing over dead nodes, and moving live data between
nodes — all while remaining compatible with unmodified Redis Cluster clients. Rudis implements the standard
16,384-slot/CRC16 model with `-MOVED`/`-ASK` redirection, gossip-based quorum failure detection, and
majority-vote replica promotion.

**Key design choice.** The cluster bus intentionally runs on plain, synchronous `std::net::TcpStream`/
`TcpListener` on dedicated OS threads — not `io_uring`/`monoio` — since it is a low-rate control-plane
concern, separate from the thread-per-core data path. `ClusterHub` is process-wide shared state behind
`RwLock`s/atomics (one per port, via a global registry), the same category of deliberate shared-nothing
exception as `BlockHub` (Component 06) and the search-index mirror (Component 09).

**Key invariants.**
- Full-state gossip, not incremental: `cluster_bus_tick` resends the entire known node table to every peer on
  every 500ms tick — O(peers²) bandwidth per tick, an explicit, acknowledged scalability boundary, not designed
  or tested for hundreds of nodes.
- Failure detection requires quorum corroboration: a peer is locally marked `"fail?"` after 5s of missed
  PONGs, piggybacked in gossip to other nodes; escalation to a broadcast `"fail"` requires corroborating votes
  from a strict majority of known masters — protecting against one node's own network partition causing a
  false failover.
- Replica promotion requires a genuine majority vote gated by monotonically increasing epochs (one vote per
  epoch per master) — `CLUSTER FAILOVER FORCE` is the only way to bypass voting.

**Performance characteristics.** Slot migration is fully synchronous and sequential, not pipelined — keys are
migrated in batches of 100 over a single connection, one migration at a time, despite `CLUSTER REBALANCE`
accepting (but silently ignoring) a `PIPELINE` argument. The cluster bus opens a brand-new blocking `TcpStream`
per peer per tick or handshake — "the opposite of the rest of Rudis's zero-copy, `monoio`-based data path,"
acceptable only at control-plane rates.

**Verified findings.** `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD` perform a **genuine DUMP-and-replay of every
key** in a migrating slot to the target node — a real data-path operation, not bookkeeping. This is distinct
from (and more fully built out than) the Dragonfly-compatible `DFLYMIGRATE`/`DFLYCLUSTER` command family, which
remains state-bookkeeping only ("no keys are read, serialized, or transferred" by that path) — a real
correction against any prior characterization of Rudis's cluster migration as bookkeeping-only. Separately:
slot-ownership/migration state is tracked in three independent places — `router.slot_states` (Component 04,
per-shard), `ClusterHub.my_slots`/`.nodes` (gossip-populated), and `ClusterHub.slot_states` (a second,
distinct migrating/importing tracker) — all three are genuinely consulted by different parts of the redirect
path but do not share storage, a documented "latent source of future drift" related to, but distinct from,
Component 04's two-routing-entry-point inconsistency.

**Further reading:** [`docs/internal/11_cluster_topology.md`](../internal/11_cluster_topology.md)

---

## Component 12: CRDT Data Types & Manual Multi-Region Sync

**Source files:** `src/crdt.rs`

**Purpose & problem statement.** Active-active multi-region replication cannot rely on synchronous consensus
without cross-region round-trip latency on every write, and naive last-write-wins merges can silently lose
concurrent updates. Rudis exposes `LwwRegister`, `OrSet`, and `PnCounter` CRDTs ordered by a Hybrid Logical
Clock (HLC), each with a mathematically commutative/associative/idempotent merge function.

**Key design choice.** Synchronization is entirely manual and operator-driven: there is no background peer
discovery or automatic transport. A caller runs `CRDT.DUMP`, moves the bytes externally by whatever means it
chooses, and calls `CRDT.MERGE` on the receiving side. `CrdtStore` is a store parallel to the main keyspace —
CRDT values are not `RudisValue` variants and share none of the main keyspace's TTL/eviction machinery.

**Key invariants.**
- Every merge function is genuinely commutative/associative/idempotent, exercised by dedicated convergence unit
  tests (`test_lww_register_convergence`, `test_pn_counter_convergence`, `test_orset_add_wins`).
- `OrSet` is add-wins: `remove` only tombstones the specific add-tags it has observed so far, so a concurrent,
  not-yet-seen add survives a merge.
- The HLC advances via a lock-free compare-exchange retry loop, not a mutex.
- Tombstones are not reclaimed automatically — only an explicit `CRDT.GC [ttl_ms]` call prunes them; nothing
  in `server.rs` schedules this.

**Performance characteristics.** `export_sync_payload`/`CRDT.DUMP` is O(total CRDT state size) and
single-threaded — there is no incremental/delta export; every call re-serializes the entire store into one flat
buffer.

**Verified findings.** CRDT writes are **not** appended to the AOF and **not** streamed to connected PSYNC
replicas — `command_to_resp` (Component 14) has no match arm for any `Command::Crdt*` variant. A node's CRDT
state today survives only in memory plus whatever external process runs `CRDT.DUMP`/`CRDT.MERGE`; a restart or
failover loses it. This is a real, verified gap, not a documented design trade-off.

**Further reading:** [`docs/internal/12_crdt_types.md`](../internal/12_crdt_types.md)

---

## Component 13: Lua Scripting & Redis 7 Functions Engine

**Source files:** `src/scripting.rs`

**Purpose & problem statement.** Read-modify-write and multi-step business logic implemented purely via
client round-trips is slow and non-atomic. An embedded Lua 5.4 interpreter (`mlua`) executes `EVAL`/`EVALSHA`
scripts and Redis 7 `FUNCTION`/`FCALL` libraries entirely on the connection's local shard, with `redis.call`
bridging back into the same command-execution path (`execute_local_command`) as ordinary traffic — atomicity
falls out of the shard's single-threaded event loop for free, with no explicit lock.

**Key design choice, stated plainly by the design doc itself: Rudis's Lua environment is not sandboxed.**
Each invocation constructs a fresh `mlua::Lua::new()` with the full default standard library present (`os`,
`io`, `package`, `debug` all available), no instruction/time limit, and no `SCRIPT KILL`. `EVAL`/`EVALSHA`/
`FCALL` should be treated as requiring the same trust level as direct shell access.

**Key invariants.**
- Fresh interpreter per call, no persistent VM or bytecode cache — only the script's **source text** is
  cached, keyed by SHA1, in a process-wide `RwLock<HashMap<String, String>>`.
- Scripts and functions never route based on `KEYS` — `EVAL`/`EVALSHA`/`FCALL` always execute on the
  connection's local shard; a script touching a key belonging to a different shard silently operates on the
  wrong shard's slot for that key rather than erroring.
- `SCRIPT_CACHE`/`FUNCTION_LIBS` are process-wide, not per-shard — a script loaded on one shard's connection is
  immediately visible to calls arriving on any other shard.
- Every write a script performs via `redis.call` is independently AOF-logged and replicated as its own
  constituent command (effects replication), matching modern Redis's default behavior.

**Performance characteristics.** `FUNCTION LOAD` registers function names via a one-time discovery pass, but
`FCALL` **re-executes the entire library's top-level source on every call** in a fresh interpreter — there is
no cached, ready-to-invoke closure between calls.

**Verified findings.** No data-loss bugs were identified. The absence of sandboxing and per-call full-library
re-execution are both real, verified, and load-bearing operational characteristics documented above rather than
newly discovered defects.

**Further reading:** [`docs/internal/13_scripting_functions.md`](../internal/13_scripting_functions.md)

---

## Component 14: Persistence & Replication Engines

**Source files:** `src/replication.rs, src/aof.rs`

**Purpose & problem statement.** A datastore needs both durability across restarts and a warm replica copy,
without entangling the two or paying Redis's `fork()`-based copy-on-write risk on a large, busy dataset. Every
mutating command is serialized to canonical RESP once (`command_to_resp`), then independently handed to the
local AOF writer and to replica fan-out — a node can run AOF-only, replication-only, both, or neither.

**Key design choice.** Full-dataset transfer (RDB save, initial replica sync) uses no `fork()`: the
coordinating shard serializes its own data first, then messages each other shard in turn over the existing
cross-shard mailbox, holding only one shard's chunk in memory at a time. Two wire-compatible master-side
replication paths exist: standard `PSYNC` (full resync via RDB-shaped blob, or partial resync via a genuine
fixed-capacity ring-buffer backlog) and a Dragonfly-compatible `DFLY FLOW` per-shard streaming handshake
(master-side only — Rudis's own replica only ever speaks `PSYNC`).

**Key invariants.**
- `BGREWRITEAOF` is real and synchronous-per-shard: an atomic temp-file rename plus a directory fsync, with
  remote shards rewritten sequentially (not concurrently) to bound peak memory during compaction.
- Partial resync is real on both sides: the master answers `+CONTINUE <replid>` with just the backlog diff
  when the requested offset is still in the retained window, else falls back to `+FULLRESYNC`.
- Any command without an explicit `command_to_resp` match arm is invisible to *both* AOF and replication —
  this is the mechanism behind Component 12's CRDT-write gap.
- `ReplicationHub` is shared, cross-thread state (`Arc`/`RwLock`/atomics) looked up per port — a second,
  independent exception (alongside `BlockHub`) to the zero-lock architecture, needed because a write on any
  shard must reach every connected replica.

**Performance characteristics.** AOF write cost is O(1) amortized per command; file size and restart replay
time both grow unboundedly with lifetime write volume until an explicit `BGREWRITEAOF` compacts — nothing
compacts automatically. The replication backlog size and AOF fsync cadence are compile-time constants, not
runtime-configurable (no `appendfsync always/everysec/no` policy choice exists).

**Verified findings.** **`RudisValue::Tiered` values (keys currently offloaded to NVMe by Component 07) are
serialized as zero payload bytes during both `SAVE`/`BGSAVE` (RDB) and `BGREWRITEAOF`.** The serialization match
arm for `RudisValue::Tiered` writes nothing at all — not even a type tag — while the key's length and bytes are
still written ahead of it; this desynchronizes the reader's cursor for everything that follows in that shard's
chunk. This is a genuine, currently-unhandled data-loss/corruption edge case affecting any deployment that
saves or compacts while tiered storage is active, not a hypothetical one. Separately: **the classic
`save <seconds> <changes>` autosave directive is parsed and stored verbatim in Rudis's config parser, but no
code path in `router.rs` or `server.rs` ever reads it to schedule an automatic `BGSAVE`** — the directive is
accepted without a parse error but has no runtime effect; there is currently no automatic, save-point-triggered
background snapshotting in Rudis.

**Further reading:** [`docs/internal/14_persistence_replication.md`](../internal/14_persistence_replication.md)

---

## Component 15: Security, Memory Allocator & TLS

**Source files:** `src/acl.rs, src/allocator.rs, src/tls.rs`

**Purpose & problem statement.** Production deployments need per-user credentials and command/key-level
authorization, allocator visibility under a workload of many small short-lived allocations, and encrypted
client connections. A per-port `AclManager` enforces real authentication and authorization; `jemalloc` (with a
thread-local `SmallCollectionArena` recycling pool layered on top) is the global allocator; `rustls` terminates
TLS on an optional dedicated port.

**Key design choice.** `AclManager` lives behind `PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<...>>>>>`
— one manager per listening port, shared by every shard on that port, the same category of deliberate
shared-nothing exception as `BlockHub` (Component 06). Every command after authentication takes a real
`can_execute_command`/`can_access_key` check; a denied command gets a real `-NOPERM` reply, and the
squash-eligibility path falls back to the sequential path (where the denial actually fires) rather than
silently allowing a denied command through.

**Key invariants.**
- Passwords are hashed (SHA1 with a fixed, hardcoded salt) but plaintext storage was **not removed** —
  `ACL SETUSER user >password` still pushes the plaintext into `passwords` *and* the hash into
  `password_hashes`, and `check_auth` accepts either. SHA1 is a fast general-purpose hash, not a slow
  work-factor KDF (Argon2/bcrypt/scrypt) — offline brute force is cheap, and the single shared salt makes a
  precomputed rainbow table viable across every user and deployment.
- `CONFIG SET requirepass <pw>` mutates the default user's password fields directly but never calls
  `AclManager::set_user` — it never sets `HAS_CUSTOM_ACL` and never clears the default user's `nopass` flag,
  so auth enforcement is **not** actually turned on by `requirepass` alone (via `CONFIG SET` or the config-file
  directive); only an explicit `ACL SETUSER default ... >password` reliably enforces auth.
- ACL command *categories* (`+@read`/`-@write`) and pub/sub channel patterns (`&channel:*`) are parsed as
  accepted tokens but have no effect — only individual command names and simple key-prefix patterns are
  actually enforced.

**Performance characteristics.** Auth check cost is one `RwLock` read plus a linear scan over 0–1 password
entries plus one SHA1 computation — negligible, paid once per connection. Real per-command ACL overhead now
exists: every command after auth takes a read-lock and a hash-set lookup, small but no longer zero.

**Verified findings.** TLS is genuinely wired up end-to-end for the safe `rustls` path — the handshake is a
correct async adaptation of the rustls handshake loop, not dead code. **But the kTLS offload fast path has a
critical, live bug: it silently transmits application data in plaintext.** `enable_ktls` performs only the
first of two required `setsockopt` calls (`TCP_ULP`, attaching the kernel TLS module) — this succeeds on
essentially any modern Linux host regardless of whether key material was ever installed, so `is_ktls_active`
becomes `true` on real deployments. The second, actually-required call that installs the negotiated
cipher/key/IV into the kernel socket does not exist. Once `is_ktls_active` is set, `read_plaintext`/
`write_plaintext` read/write the raw socket directly with **no rustls encryption at all**, while both client
and server believe a real TLS session is active because the handshake itself genuinely succeeded. This is
worse than TLS simply being absent: enabling `--tls-port` today produces a working handshake followed by
silent plaintext for anyone who trusts it.

**Further reading:** [`docs/internal/15_security_tls.md`](../internal/15_security_tls.md)

---

## Component 16: JSON Document Store & JSONPath Engine

**Source files:** `src/json.rs`

**Purpose & problem statement.** Bolting a JSON document type onto Redis normally requires a dynamically
loaded C module (RedisJSON). `JsonStore` is a native, per-shard `HashMap<Bytes, serde_json::Value>` alongside
the main keyspace, with a hand-rolled JSONPath engine supporting field/index/wildcard/slice selectors and
in-place, auto-vivifying mutations.

**Key design choice.** The JSONPath implementation is real but deliberately partial: `$`, `.field`, `[idx]`
(including negative indices), `[*]` wildcards, `[start:end]` slices, and quoted field names are supported, but
there is **no recursive descent (`$..field`) and no filter-expression syntax (`?(@.price < 10)`)** — a path
using either silently matches nothing rather than erroring.

**Key invariants.**
- Whole-document storage: one complete `serde_json::Value` tree per key; `JSON.GET` always re-serializes the
  matched subtree fresh via `serde_json::to_string`, with no cached serialized form or memoization.
- Auto-vivification happens on `SET`, not on read; `NX`/`XX` are checked once up front against whether the
  target path already resolves to something.
- `query_json_path`/`query_json_path_mut` are hand-duplicated for immutable/mutable access — a real, verified
  duplication with no stated rationale; a bugfix to one traversal rule must be applied to both copies by hand.

**Performance characteristics.** `JSON.GET` cost scales with matched-subtree size, not query specificity (no
memoization). Path traversal is O(document breadth) per segment for `Wildcard`/`Slice` segments (no path
index), though plain `Field` lookups are O(1).

**Verified findings.** `JSON.NUMMULTBY` silently fails on multi-match wildcard paths: because it is composed
from two `JSON.NUMINCRBY` calls and parses the intermediate result as a single `f64`, a path matching more than
one node returns a bracketed multi-value string that fails to parse, so the command errors instead of
multiplying every match — even though the equivalent `JSON.NUMINCRBY` on the same path works correctly.
`JSON.SET` can silently destroy a differently-typed intermediate value while auto-vivifying a deep path: an
existing-but-wrong-type intermediate element is overwritten with a fresh empty container rather than raising an
error. `JSON.MGET` is dispatched sequentially, one shard round-trip per key, unlike the bucketed fan-out
`MGET`/`MSET` received. `JsonStore` is excluded from RDB persistence entirely — JSON documents do not survive a
restart via `SAVE`/`BGSAVE` (the same gap Component 08's vector indexes have).

**Further reading:** [`docs/internal/16_json_store.md`](../internal/16_json_store.md)

---

## Component 17: Geospatial Commands

**Source files:** `src/geo.rs`

**Purpose & problem statement.** Proximity queries need an efficient way to find nearby points without a
dedicated spatial index structure. Coordinates are encoded as 52-bit interleaved geohashes and stored directly
as ordinary ZSet scores, so `GEOADD`/`GEODIST`/`GEOPOS` reuse the sorted-set storage engine (Component 05)
outright — a "geo set" *is* a `RudisZSet`.

**Key design choice.** Radius/box search decomposes the query region into geohash score intervals
(`geohash_search_ranges`) before touching the ZSet, issuing one range query per interval rather than decoding
every member — pruning narrows the candidate set but the final exact Haversine/box distance check
(`GeoSearchShape::is_inside`) guarantees no false positives make it into the result.

**Key invariants.**
- 52-bit interleaved encoding (26 bits longitude + 26 bits latitude) matches real Redis's internal encoding —
  the 11-character textual geohash is computed on demand only for `GEOHASH` command output.
- Latitude is clamped to `±85.05112878°` (not `±90°`), matching real Redis's own documented limitation exactly.
- Distance uses Haversine (great-circle spherical), not an ellipsoidal (Vincenty) model, with the same Earth
  radius constant real Redis's own implementation uses — an intentional match to upstream behavior, not a
  simplification unique to Rudis.
- Geo commands route per-key across shards exactly like ordinary `ZSET` commands — no geo-specific routing.

**Performance characteristics.** `GEOADD`/`GEODIST`/`GEOPOS` are O(1)-ish, bounded by the underlying
`ZADD`/`ZSCORE` cost plus fixed bit-interleaving/Haversine math. Against the `Full` (skip-list) ZSet
representation, radius/box search is roughly O(log N + k) per interval; against the `Small` (linear-vector)
representation, each interval query is a linear scan regardless.

**Verified findings.** No correctness gaps were identified. All of this subsystem's approximations (spherical
rather than ellipsoidal distance, a flat-Earth local approximation for box width/height, a self-derived
interval decomposition rather than real Redis's literal "9 neighboring cells" technique) are documented as
deliberate choices that intentionally match or closely approximate real Redis's own behavior.

**Further reading:** [`docs/internal/17_geospatial.md`](../internal/17_geospatial.md)

---

## Component 18: Probabilistic Data Structures

**Source files:** `src/probabilistic.rs`

**Purpose & problem statement.** Tracking set membership, frequency, or heavy hitters over billions of events
in exact structures (hash sets, counters) exhausts memory at scale. Bloom filters, Cuckoo filters, Count-Min
Sketch, and a Top-K/Space-Saving tracker give bounded-error answers in a small, fixed memory footprint.

**Key design choice.** A single hash-function family underlies all four structures: `fnv1a_hash` (seeded
64-bit FNV-1a) and Kirsch-Mitzenmacher `double_hash` derive every additional hash from two real computed
hashes rather than one call per hash function — no cryptographic hash appears anywhere in this file, which is
appropriate for these structures' intended use.

**Key invariants.**
- Bloom filter sizing follows standard textbook formulas computed once at creation, with hash count clamped to
  `[1, 30]`.
- The Cuckoo filter is a complete implementation including real eviction ("cuckoo kicks", capped at 500) and
  real per-fingerprint deletion — not a simplified always-fails-when-full variant.
- Top-K is a genuine Space-Saving algorithm, not an exact top-K: at capacity, it evicts the minimum-count
  tracked item and gives the new item that evicted item's count plus the increment.
- No structure ever shrinks or auto-resizes; capacity/width/depth are fixed at creation, and over-inserting
  silently degrades accuracy rather than growing the structure.

**Performance characteristics.** Bloom `add`/`contains` cost is O(num_hashes, ≤30); Cuckoo `add`/`contains` is
O(1) (two fixed 4-slot bucket scans); Count-Min Sketch operations are O(depth), independent of the number of
distinct items tracked.

**Verified findings.** No correctness gaps were identified. All four structures survive a restart: they are
serialized into the RDB chunk stream by `ShardDb::save_extended_rdb_chunk` and reconstructed verbatim on load
— unlike Components 08, 12, and 16, this subsystem has no RDB persistence gap.

**Further reading:** [`docs/internal/18_probabilistic.md`](../internal/18_probabilistic.md)

---

## Component 19: Pub/Sub Messaging Hub

**Source files:** `src/pubsub.rs`

**Purpose & problem statement.** A globally addressable publish/subscribe namespace is in tension with
shared-nothing shards: broadcasting every `PUBLISH` to every shard wastes cross-core messages at scale, while
centralizing subscription state in one lock reintroduces the contention the architecture exists to avoid.

**Key design choice.** Subscription state stays fully local per shard (`PubSubHub`, a plain `Rc<RefCell<_>>`
— no cross-thread synchronization at all). A shared `ShardedPresenceTable` — 16 striped atomic bitmasks keyed
by channel-name hash, plus one unstriped pattern-presence bitmask — lets a publisher decide which *other*
shards are worth contacting before dispatching. The bitmask is a hint, never a source of truth: stripe
collisions can cause an occasional spurious remote lookup (a harmless false positive), but it can never cause a
missed delivery, since the receiving shard's own hub still performs the exact channel/pattern match.

**Key invariants.**
- `Router::publish` delivers to local subscribers first, then consults `presence_table.interested_shards`
  and dispatches `ShardMessage::Publish` only to shards whose bit is set — not unconditionally to every shard —
  issuing every send before awaiting any reply ("dispatch all, then await all").
- Sharded Pub/Sub (`SPUBLISH`) is a separate, deterministic routing class: the channel name is hashed via the
  same CRC16 slot function as key routing, resolving to exactly one target shard with no presence-table
  consultation at all.
- A full per-subscriber delivery queue causes a silent, non-blocking drop (`try_send`) rather than backpressure
  on the publisher — a dropped message is not counted in `PUBLISH`'s reported receiver count.
- Glob matching (`glob_match`) is a hand-written backtracking `*`/`?` matcher with **no `[...]`
  character-class support** — narrower than real Redis's fuller glob syntax.

**Performance characteristics.** Direct-channel publish is O(subscribers to that channel). Pattern publish is
O(total registered patterns in that shard's hub) per publish, not O(matching patterns) — a deployment with many
active `PSUBSCRIBE` patterns pays a glob-match per pattern on every `PUBLISH` regardless of how many actually
match. One reply frame is built once and cloned per subscriber, not re-serialized per recipient.

**Verified findings.** No correctness gaps were identified. The presence-bitmask's over-approximating (never
under-approximating) behavior, the glob matcher's lack of character classes, and sharded Pub/Sub's
single-shard-only delivery scope are all documented as deliberate, stated design trade-offs.

**Further reading:** [`docs/internal/19_pubsub.md`](../internal/19_pubsub.md)
