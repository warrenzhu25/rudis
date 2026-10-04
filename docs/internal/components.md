# Rudis Subsystem Implementation Deep-Dive & Code Reference

This document provides a concise, implementation-focused summary for all 21 core subsystems of **Rudis**:
the concrete Rust data structures, the key algorithm or workflow, and the most important findings from this
session's source-verification pass against the current codebase (the source nearly doubled in size during
this pass — several large files, notably `src/connection.rs`, `src/resp.rs`, `src/vector.rs`, `src/search.rs`,
and `src/table.rs`, roughly doubled, and two brand-new subsystems — Agent Memory and the MCP Server — were
added as Components 20 and 21).

Each entry links to its full per-subsystem document under `docs/internal/`, which carries complete
struct layouts, step-by-step algorithm walkthroughs, and source line references. This document is a rollup for
orientation, not a replacement — read the linked document for exhaustive code-level depth, and see
[`docs/design/components.md`](../design/components.md) for architectural rationale and invariants.

---

## Subsystem Index

- [01. Reactor Runtime & Server Lifecycle](#component-01-reactor-runtime--server-lifecycle) (`src/main.rs, src/server.rs`)
- [02. Connection Lifecycle & Command Execution](#component-02-connection-lifecycle--command-execution) (`src/connection.rs`)
- [03. RESP Protocol Engine & Command Parser](#component-03-resp-protocol-engine--command-parser) (`src/resp.rs`)
- [04. Sharding Architecture & Cross-Core Mesh](#component-04-sharding-architecture--cross-core-mesh) (`src/router.rs, src/shard.rs, src/mailbox.rs`)
- [05. Storage Engine & Compact Encodings](#component-05-storage-engine--compact-encodings) (`src/table.rs`)
- [06. Blocking Operations & The Reactive Event Hub](#component-06-blocking-operations--the-reactive-event-hub) (`src/block.rs`)
- [07. NVMe SSD Tiered Storage Engine](#component-07-nvme-ssd-tiered-storage-engine) (`src/tiering.rs, src/tiering/`)
- [08. Vector Search Engine: HNSW, Redis 8 Vector Sets & NVMe Tiering](#component-08-vector-search-engine-hnsw-redis-8-vector-sets--nvme-tiering) (`src/vector.rs`)
- [09. RediSearch Full-Text Engine & Hybrid Vector Fusion](#component-09-redisearch-full-text-engine--hybrid-vector-fusion) (`src/search.rs`)
- [10. Kernel Bypass & Zero-Copy Networking](#component-10-kernel-bypass--zero-copy-networking) (`src/xdp.rs, src/zerocopy.rs`)
- [11. Redis Cluster Topology & Gossip Protocol](#component-11-redis-cluster-topology--gossip-protocol) (`src/cluster.rs`)
- [12. CRDT Data Types & Manual Multi-Region Sync](#component-12-crdt-data-types--manual-multi-region-sync) (`src/crdt.rs`)
- [13. Lua Scripting & Redis 7 Functions Engine](#component-13-lua-scripting--redis-7-functions-engine) (`src/scripting.rs`)
- [14. Persistence & Replication Engines](#component-14-persistence--replication-engines) (`src/replication.rs, src/aof.rs`)
- [15. Security, Memory Allocator & TLS](#component-15-security-memory-allocator--tls) (`src/acl.rs, src/allocator.rs, src/tls.rs`)
- [16. JSON Document Store & JSONPath Engine](#component-16-json-document-store--jsonpath-engine) (`src/json.rs`)
- [17. Geospatial Commands](#component-17-geospatial-commands) (`src/geo.rs`)
- [18. Probabilistic Data Structures](#component-18-probabilistic-data-structures) (`src/probabilistic.rs, src/hll.rs`)
- [19. Pub/Sub Messaging Hub](#component-19-pubsub-messaging-hub) (`src/pubsub.rs`)
- [20. Agent Memory, LLM Quota & Checkpoints](#component-20-agent-memory-llm-quota--checkpoints) (`src/agent.rs`)
- [21. MCP Server](#component-21-mcp-server) (`src/mcp.rs`)

---

## Component 01: Reactor Runtime & Server Lifecycle

**Source files:** `src/main.rs` (278 lines: process entry, OS tuning, config/ACL priming, thread spawn) ·
`src/server.rs` (2,079 lines: `run_shard_worker`, the per-shard event loop)

**Key data structures.** `Args`/`RudisConfig` (CLI + config-file merge, now including
`tiered_upload_threshold`); `AofConfig{enabled, dir, fsync_every_sec}` and `Option<TlsWorkerConfig>`, each
cloned once per shard; `ShardMessage` — grew from "60+" to **66 variants** (new: `RemoveClientPubSub`,
sharded-pubsub variants, `Scan`/`Keys`/`RandomKey`/`ExpireTime`/`Delex`,
`InitSearchIndex`/`DropSearchIndex`/`SearchQuery`); `CatchUnwind`/`catch_unwind_async` (a hand-rolled `Future`
combinator wrapping every `poll()` in `std::panic::catch_unwind`); `mailbox::create_shard_mesh` builds an N×N
matrix of `SpscQueue<ShardMessage>` rings (capacity 256) plus a per-shard doorbell `flume` channel and
`sleeping` `AtomicBool`.

**Key algorithm / workflow.** `main.rs` disables THP, parses config, primes the default ACL user's password
from `requirepass` *before any shard starts* (new — see Component 15), computes `num_shards =
threads.unwrap_or(num_cores.min(8))` via a cgroup/taskset-aware `get_process_affinity_cores` (calls
`sched_getaffinity` directly, not just `core_affinity::get_core_ids()`), builds the shard mesh, and spawns one
OS thread per shard running `run_shard_worker`. Each shard thread pins its core, builds a
`monoio::RuntimeBuilder<FusionDriver>`, opens a `SO_REUSEPORT` listener (and a second one on `--tls-port`),
restores from RDB or replays AOF, opens its tiering manager, calls `connection::set_current_router`
(populating a thread-local `CURRENT_ROUTER` used by `notify_keyspace_event`), spawns three periodic tasks
(100ms expire / 20ms auto-tier / 2s tiering GC) plus an AF_XDP ingress loop, spawns the cross-shard receiver
loop, then enters the TLS and plain accept loops. The cross-shard receiver drains up to 64 queued messages per
wakeup via `try_recv()` before yielding back to `recv_async().await`.

**Notable implementation detail.** `ShardMessage::Batch`'s handler (`server.rs`) contains a `let needs_async =
false;` that is never reassigned — a ~340-line duplicate async fast-path dispatcher is permanently dead code;
only the `else` branch (synchronous dispatch, with tiered-GET misses deferred to a follow-up
`monoio::spawn`) ever runs. Connection rebalancing (`conn_balance.rs`) reserves a least-loaded shard via a
single `compare_exchange`, bounded to 4 retries, tracking up to 256 shards — measured data in the module's own
doc comment shows 64 connections over 16 shards without this fix would run at ~51.9% of ideal capacity due to
`SO_REUSEPORT` hashing skew.

**Verified findings.** New: **the TLS accept loop never calls `conn_balance::register_conn`/`claim_owner`/
`unregister_conn`** — TLS connections are never rebalanced off an overloaded shard, and because they don't
increment `CONN_COUNTS`, their presence silently skews the plain-TCP accept loop's least-loaded-shard
calculation (a TLS-heavy shard looks artificially idle). Re-verified, still true: graceful shutdown does not
drain in-flight work or run an automatic `SAVE`/`BGSAVE` — only the AOF writer gets a final flush+fsync; the
cross-shard receiver, periodic tasks, AF_XDP loop, and TLS accept loop are simply dropped with the runtime.
`agent.rs`/`mcp.rs` (Components 20/21) need **zero** special startup wiring — their state lives as plain
`HashMap` fields on `ShardDb`, populated lazily, and their commands are parsed/dispatched exactly like any
other Redis command with no separate listener or task.

**Full documentation:** [`docs/internal/01_reactor_runtime.md`](01_reactor_runtime.md)

---

## Component 02: Connection Lifecycle & Command Execution

**Source files:** `src/connection.rs` (20,971 lines — roughly doubled since the previous revision) · reply
mailbox primitives in `src/mailbox.rs`

**Key data structures.** `ClientInfo` (new fields: `lib_name`/`lib_ver` from `CLIENT SETINFO`,
`reply_mode`); `WATCHED_KEYS`/`CLIENT_WATCH_TAINTED`/`TRACKING_CLIENTS` (per-port static maps, gated by
fast-path `AtomicBool`s); `CMD_STATS`/`ERROR_STATS`/`FAILED_CMD_STATS` (global maps, the latter two new, fed
by a new `ErrorStatTracker` RAII guard); `ConnScratch` (pooled per-reactor-thread scratch buffers, up to 32
instances; its `results_pool` field is now dead weight — `execute_commands_squashed` only ever does `let _ =
results_pool;`); **`BatchResponder`** (`src/mailbox.rs`) — materially redesigned since the prior revision:
`state: CachePadded<AtomicU8>` (`BATCH_IDLE/RUNNING/SLEEPING/COMPLETED`) plus `responses_ptr:
AtomicPtr<CompactResp>` that writes reply payloads **directly into the caller's pre-allocated `responses:
Vec<CompactResp>`** via raw pointer, not a second handed-back `Vec`.

**Key algorithm / workflow.** `execute_command` runs an RAII-guarded, eight-gate sequence (slowlog + new
`ErrorStatTracker` → NOAUTH → ACL → ASKING → CROSSSLOT → cluster redirection → READONLY → OOM), checking
**every** key of a command via `cmd_keys`/`for_each_cmd_key`, not just the primary key.
`execute_commands_squashed`'s inline write fast paths (`SET`, `INCRBY`, `DEL`, `HSET`, `SADD`, `ZADD`,
`LPUSH`, `LPOP`, `RPOP`) are gated on "no AOF and no connected replica" (plus per-command extra gates: no
keyspace-notify flags for `SET`, no active search index for `DEL`/`HSET`, no blocked waiters for
`ZADD`/`LPUSH`). Cross-shard batch harvest spins up to 256 iterations on a `pending_mask: u64` before falling
back to `responder.wait_take().await`.

**Notable implementation detail — a previously-flagged gap is now fixed; a new one of the same class has
appeared.** The prior revision's finding that the squashed `SET` fast path skipped
`touch_watched_key`/`notify_key_invalidation` is **confirmed fixed** — `SET` now calls both, matching its
sibling fast paths, on both the local and remote-shard (`server.rs`) sides. However, the harvest loop's
`pending_mask: u64` (bit = `1u64 << target_shard`) is only correct for `num_shards <= 64`; for more than 64
shards, high-numbered shard bits alias back onto bits 0-63 via unchecked shift wraparound, which can make the
harvest loop treat a still-outstanding batch as already-collected — the same bug class the prior revision
flagged (and which was fixed) in `MGET`/`MSET` scatter-gather has reappeared here in a different mechanism.

**Verified findings.** `CLIENT REPLY OFF`/`SKIP` is tracked (`ClientInfo.reply_mode`) but never consulted
anywhere else — replies are written unconditionally regardless of mode. `CLIENT KILL`/`PAUSE`/`UNPAUSE`/
`NO-TOUCH`/`CACHING` remain accepted-but-inert `+OK` stubs. TLS connections now run the same generic
`handle_client<T: ClientTransport>` loop as plaintext connections (the separate `handle_tls_connection` loop
was deleted), so they take the pipeline-squashing fast path too. A newly-discovered dead-code duplicate: two separate `else if` arms in
the squashed fast-path chain match `Command::IncrBy` under the identical guard condition; the second is
unreachable. Live cluster slot migration (`migrate_keys_to_node`/`execute_rebalance_plans`, now documented as
living in `connection.rs` rather than `router.rs`) is a real DUMP-and-replay-as-write-commands protocol,
batched 100 keys at a time — see Components 07/11 for its tiered-value data-loss interaction.

**Full documentation:** [`docs/internal/02_connection_lifecycle.md`](02_connection_lifecycle.md)

---

## Component 03: RESP Protocol Engine & Command Parser

**Source files:** `src/resp.rs` (14,357 lines — roughly doubled since the previous revision: was 9,155 lines
/ 108 `Command` variants / ~6,470-line `build_command`)

**Key data structures.** `pub enum Command` — grew from **108 to 369 variants** (~1,435 lines), spanning 30
section-comment groups including entirely new families: hash-field TTLs (`HEXPIRE`/`HTTL`/`HGETEX`/
`HSETEX`), stream claim/delete (`XCLAIM`/`XAUTOCLAIM`/`XDELEX`/`XACKDEL`), Redis 8 Vector Sets (14 `V*`
variants), CRDT multi-region (10), Redis 7 Functions, RedisJSON (16), Geospatial (8), Probabilistic (22),
full-text search (10), AF_XDP control (8), Dragonfly extensions (7), and the entire AI-native surface —
semantic cache, agent memory, LLM quota, agent checkpoint/tool, and MCP (21 variants total). `build_command`
is now a ~10,719-line, 346-arm match (up from ~6,470 lines) covering 373 distinct command-name string
literals.

**Key algorithm / workflow.** `parse_command` dispatches on the first byte (`*` → `parse_resp_array`, else
Memcached-storage-then-inline). `parse_resp_array` is two-pass: pass one proves frame completeness and — new
— **enforces `proto-max-bulk-len`** per argument (rejecting an over-length bulk string at parse time, default
512 MiB); pass two either fast-paths ≤16-arg frames directly from cached offsets (now **20** fast-pathed
command names, up from 17: `UNLINK`/`READONLY`/`READWRITE` added) or falls through to `build_command`.

**Notable implementation detail — three real wire-protocol gotchas, all newly documented.** (1)
`Command::Unlink` is **never constructed by the parser** — a client-sent `UNLINK` is normalized to
`Command::Del` identically to `DEL`; the only constructor of `Command::Unlink` is `table.rs`'s lazy-expiry
path, purely to name the command differently in AOF/replication output. (2) `VADD` has two unrelated argument
grammars (Redis 8 Vector Sets vs. Rudis-native legacy) selected by content-sniffing whether `args[2]`
uppercases to `REDUCE`/`FP32`/`VALUES` — not by any explicit flag, so a legacy element literally named
`reduce`/`fp32`/`values` would misparse. (3) `FT.HYBRID` has **no `Command` variant of its own** — it
string-concatenates its text and vector sub-queries into one combined query string and constructs a plain
`Command::FtSearch`, with no validation that the resulting string is well-formed.

**Verified findings.** The previously-flagged gap — no `proto-max-bulk-len`-equivalent cap — **is now fixed**
(enforced in `parse_resp_array` pass 1, exposed via `CONFIG GET/SET proto-max-bulk-len`, also reused to bound
`SETBIT`/`BITFIELD` offsets). Still open: no cap on the RESP array **element count** itself (`*N\r\n`'s `N`)
— a client streaming ~2 billion tiny empty-bulk arguments could still force a `Vec<Bytes>` of that size in the
general (`num_args > 16`) path before `build_command` rejects the command name. The error-reply prefix
whitelist grew from 7 to 9 recognized codes (`NOGROUP`, `INVALIDOBJ` added). No cross-shard
mailbox/routing/WATCH logic exists in this file — confirmed still entirely in `connection.rs`/`router.rs`/
`shard.rs`.

**Full documentation:** [`docs/internal/03_resp_engine.md`](03_resp_engine.md)

---

## Component 04: Sharding Architecture & Cross-Core Mesh

**Source files:** `src/router.rs` (4,483 lines: `Router`, key routing, per-command methods, descriptor/channel
pooling) · `src/shard.rs` (4,137 lines: `ShardDb`, `ShardMessage`, `CompactResp`, `SlotState`) ·
`src/mailbox.rs` (931 lines: lock-free SPSC cross-shard transport, shard mesh, shared-memory reply
descriptors)

**Key data structures.** `Router` (21 fields, including `slot_states: Rc<RefCell<HashMap<u16, SlotState>>>`
(sparse, absent ⇒ `Stable`) and `slot_owners: Rc<RefCell<Vec<usize>>>` (dense, 16,384 entries)). `ShardMessage`
— 66 variants (Component 01). `CompactResp` grew to 5 variants (`Small`, `Big`, `Bulk`, `Array1Bulk`,
`RawBytes`). `ShardDb` — 238 `pub` methods, five new fields (`semantic_caches`, `agent_memories`,
`llm_quotas`, `agent_checkpoints`, `agent_tools`) backing the AI-native agent runtime. `mailbox.rs`:
`CachePadded<T>`, `SpscQueue<T>` (lock-free ring + mutex-guarded overflow `VecDeque`, capacity 256),
`ShardSender`/`ShardReceiver` with a `target_sleeping`/`sleeping` "sleeping flag," `FastGetDescriptor`/
`FastSetDescriptor` (plain `AtomicBool`), `ScatterMgetDescriptor`/`ScatterMsetDescriptor` (3-state
`DESC_RUNNING`/`DESC_COMPLETED`/`DESC_SLEEPING` handshake), `BatchResponder` (4-state handshake plus
`responses_ptr: AtomicPtr<CompactResp>` direct-write, Component 02).

**Key algorithm / workflow.** Standalone mode routes `fxhash::hash64(hash_tag) % num_shards`; cluster mode
computes CRC16/XMODEM slot then `slot_to_shard(slot, n) = (slot*n)/16384` (contiguous ranges, not modulo).
`MGET`/`MSET` use shared-memory scatter-gather (`begin_mget_resp`/`finish_mget_resp`, `begin_mset`/
`finish_mset`): remote shards write results directly into disjoint slice slots and call `finish_shard()` (only
the last-to-finish shard pays a `flume` notify, and only if the waiter parked). `execute_remote` was rewritten
to use `BatchResponder.prepare(&mut slot as *mut _)` instead of a serialized results-vec. The sleeping-flag
fast path on `ShardSender::send` skips `flume::try_send` entirely unless `target_sleeping == true`.

**Notable implementation detail.** `Router`'s own methods were consolidated onto dynamic `self.target_shard`/
`self.target_shard_and_hash` routing almost everywhere — the prior revision's "static vs. dynamic Router
routing" finding is now fixed **except for** `Router::del` (`router.rs:1619`), the sole remaining
static-routing call site; since `del_keys` delegates single-key deletes to `self.del`, a single-key `DEL` can
target a different shard than a multi-key `DEL` on the same key during live migration.

**Verified findings.** The disagreement moved, not disappeared: `src/connection.rs`'s `target_shard_of_cmd`/
`target_shard_and_hash_of_cmd` (used by the squashed-pipeline hot path) are built entirely on the **static**
free functions, never `Router::slot_owners`; the squash-eligibility gate (`can_squash`) checks `SlotState`/
cluster-bus ownership but never `slot_owners`, so a slot reassigned via `Router::set_slot_owner` (which clears
`SlotState`, leaving it `Stable`) can cause a squashed pipeline to execute against a stale shard with **no
`-MOVED` redirect**. `CompactResp::RawBytes` and the legacy `ShardMessage::Get`/`Set`/`Mget`/`Mset` variants
are fully implemented but never constructed in production (only in `#[cfg(test)]`). `execute_remote`'s
`responses_ptr` points at a bare stack local with no `ConnScratch`-style strong-count/`is_idle()` guard — sound
today but undocumented as an invariant. `check_slot_redirection` remains defined but never called. The prior
"MGET/MSET invisible to redirection" finding is now fixed (`cmd_primary_key` gained `Mget`/`Mset` arms).
`Router::del_keys` still allocates one fresh `flume::bounded(1)` per remote shard rather than using the pooled
scatter-gather path. `FastGetDescriptor`/`FastSetDescriptor` never got the Parker-handshake treatment — they
still unconditionally `try_send` on every `finish()`.

**Full documentation:** [`docs/internal/04_sharding_mesh.md`](04_sharding_mesh.md)

---

## Component 05: Storage Engine & Compact Encodings

**Source file:** `src/table.rs` (16,925 lines, 349 `pub fn`) — the entire thread-local storage engine:
`RudisFlatTable` (extendible-hashing directory + `Vec<RawSegment>`), the per-shard façade `RudisTable`, every
`RudisValue` compact/full encoding, key- and hash-field-level TTL bookkeeping, NVMe-tiering hooks,
cluster-slot counting, small-collection arena.

**Key data structures.** `RawSegment`: fixed-capacity SwissTable-style segment, `ctrl: Vec<u8>` +
`slots: Vec<Option<RudisEntry>>`, `capacity` (power of two up to `SEG_CAP = 1024`), `local_depth: u8`, plus a
4-slot (`STASH_CAP`) DashTable-style overflow stash. `RudisFlatTable`: `segments: Vec<RawSegment>`,
`directory: Vec<u32>` (len `2^global_depth`), `global_depth: u8`, `slot_counts: Box<[u32; 16384]>`.
`RudisEntry{key: Bytes, val: RudisValue, expire_at: Option<Instant>}`, verified by unit test at 88 bytes
(`RudisValue` is 40 bytes). `RudisValue` is an 11-variant enum (`String`, `Int`, `SmallHash`, `Hash`, `List`,
`Set`, `ZSet`, `HyperLogLog`, `Stream`, `Tiered`, `Cooled`); the four collection variants are boxed to keep the
type at 40 bytes.

**Key algorithm / workflow.** Lookup: `mix_hash(fxhash::hash64(key))` → directory index → segment → a
triangular SIMD probe (`GROUP_SIZE=16`-byte groups via `_mm_cmpeq_epi8`/`_mm_movemask_epi8`, scalar fallback
on non-x86_64) walks the segment, falling into the 4-slot stash after 2 groups (32 slots) are exhausted.
Insert: `find_or_prepare_insert` checks `growth_left == 0`; a genuinely new key triggers
`split_or_grow_segment`, a four-way decision tree: (1) tombstone-dominated (<50% live) → in-place compact; (2)
sub-`SEG_CAP` and full → double in place; (3) at `SEG_CAP` and full → a real extendible-hashing split into two
1024-slot segments (directory doubled only if the segment's `local_depth` equals `global_depth`); (4) loop
repeats if the target segment is still full. Every split/grow touches at most ~1,024-2,048 entries regardless
of total table size.

**Notable implementation detail — `table.rs`'s hash table was rebuilt twice, and the first rebuild is now dead
code.** Commit `e244128` ("progressive incremental table rehashing for latency spike elimination") first added
a real cooperative rehash protocol to the old flat single-array table — an `old_table` field, `rehash_step(n)`
migrating n buckets per call, `migrate_key_if_in_old` checking the old array on miss — the classic "rehash a
little on every operation" design. Commit `4713691` ("implement Dragonfly-style extendible hashing directory")
then **replaced the entire flat-array + `old_table` design** with the segmented directory structure above,
because a segment split now costs O(1,024) worst-case regardless of table size, eliminating the need for
incremental spreading. Commit `b3d3362` kept `is_rehashing` (always `false`), `rehash_step` (always
`false`/no-op), `finish_rehash`, `migrate_key_if_in_old`, and `RudisTable::prepare_key_lookup` as **no-op
stubs rather than deleting them** — verified by grep to be called nowhere outside `table.rs`: pure vestigial
API surface from the superseded design, currently unreachable dead code. There is no "incremental rehashing
feature" today in the sense of a multi-call cooperative protocol — the thing that made it unnecessary is the
extendible-hashing segment cap.

**Verified findings.** A source comment claims a full segment is "~48KB, L1/L2 cache resident" — the actual
size is **≈89.4 KiB** (`(1024+4)*88` bytes), off by ~1.9x, a stale comment never updated after `RudisEntry`
shrank to 88B. `RudisValue::HyperLogLog(Box<[u8;16384]>)` is now **legacy-read-only**: live `PFADD` always
creates sparse `RudisValue::String` via `hll_create_sparse_empty`; the only live construction site for the
dense variant is `RESTORE` deserializing a legacy DUMP payload (type tag 5) — see Component 18 for the
resulting byte-leak bug. `ZRANK`/`ZREVRANK` remain O(n) in both `Small` and `Full` ZSet forms (no
order-statistics structure), while `ZRANGEBYSCORE` is genuinely O(log n + k) via `BTreeSet::range`. Eviction
(`try_evict_one_key`) has no real LRU: `allkeys-lru`/`volatile-lru` just take the first sampled occupied slot —
indistinguishable from `allkeys-random`. `RudisTable::sample_cursor` is now shared by three independent
samplers (key-TTL expiry, hash-field-TTL expiry, eviction); `spill_cursor` (tiering) remains independent.
`slot_counts` cluster-slot counting only stays correct while `HAS_ACTIVE_CLUSTER` is true at mutation time.

**Full documentation:** [`docs/internal/05_storage_engine.md`](05_storage_engine.md)

---

## Component 06: Blocking Operations & The Reactive Event Hub

**Source files:** `src/block.rs` (765 lines: `BlockHub` waiter registry, pop/notify algorithms — no async
code, no command parsing) · plus call sites in `src/connection.rs`, `src/server.rs`, `src/router.rs`,
`src/shard.rs`, `src/replication.rs` (`wait_replicas`, a separate mechanism)

**Key data structures.** `BlockHub{list_waiters, zset_waiters: HashMap<Bytes, VecDeque<_>> (FIFO per key),
stream_waiters: HashMap<Bytes, Vec<StreamWaiter>> (not FIFO), blocked_clients, blocked_zset_clients,
paused_count, pending_notifies}`. `PORT_BLOCK_HUBS: LazyLock<Mutex<HashMap<u16, Arc<Mutex<BlockHub>>>>>` —
exactly one `BlockHub` per listening port, not per shard, shared by all shard threads. `WaiterOp::Movem` (new,
backs `BLMOVEM`); `ZSetWaiter.is_zmpop`/`.count` generalize the old single-element waiter to also cover
`BZMPOP`.

**Key algorithm / workflow.** Blocking commands always check-then-register: try local/remote non-blocking pop
first, only register a waiter if truly empty, avoiding a lost-push race. `notify_list`/`notify_zset` run
entirely under the hub's `Mutex` lock using the caller's local `RudisTable`, popping on the waiter's behalf
after the triggering write already landed. Cross-shard wakeup works because there is only **one**
process-wide hub per port: registration and notification both lock the same `Arc<Mutex<BlockHub>>` regardless
of which shard thread calls. `MULTI`/`EXEC` pauses the hub (writes call `add_pending_notify` instead of
notifying immediately), then `resume()` dispatches each pending key either locally or via
`ShardMessage::NotifyList` to the owning shard.

**Notable implementation detail.** `WAIT`/`WAITAOF` are **not** part of `BlockHub` at all — `wait_replicas`
(`replication.rs:687-712`) is a pure poll loop, re-checking replica ACK offsets every 5ms via
`monoio::time::sleep`, with no waiter registration or `flume` channel. `XREADGROUP ... CLAIM` is the one
blocking command that is not purely event-driven: it re-registers its stream waiter every iteration, layered
on top of the push-based `notify_stream` wakeup.

**Verified findings.** `notify_stream` (`block.rs:601`) calls `std::thread::sleep(1ms)` **synchronously**
between each of N woken waiters to stagger a thundering herd — since Rudis is a thread-per-core async reactor,
this genuinely **blocks the entire owning shard's OS thread for up to `(N-1)`ms on a hot stream**, violating
the non-blocking-reactor invariant used everywhere else; flagged in the per-subsystem doc as "the single most
actionable finding in this file." `has_blocked_waiters`'s `port` parameter is unused —
`TOTAL_BLOCKED_WAITERS` is one process-wide `AtomicUsize` despite `PORT_BLOCK_HUBS` being keyed by port, and
`sync_atomic_waiters_count` does an unconditional absolute `store` — harmless with one port per process but a
real missed-wakeup hazard with multiple `BlockHub`s in one process. `BLMOVE`/`BLMOVEM` satisfy only one waiter
per push event (unlike `BLPOP`/`BLMPOP`/`BZPOPMIN`/`BZMPOP`, which drain while data is available). `CLIENT
UNBLOCK` does not actually work for `XREAD`/`XREADGROUP BLOCK` — the stream channel's zero-payload `Sender<()>`
can't distinguish real data from a forced unblock, so the client just spuriously re-polls. `CLIENT KILL` is a
complete no-op (`+OK` only). The `ZADD` fast-path block-hub-skip bug was already fixed in commit `3607df7`.

**Full documentation:** [`docs/internal/06_blocking_hub.md`](06_blocking_hub.md)

---

## Component 07: NVMe SSD Tiered Storage Engine

**Source files:** `src/tiering.rs` (1,082 lines) — integration in `src/router.rs`, `src/table.rs`,
`src/shard.rs`/`src/server.rs`, `src/connection.rs`, `src/aof.rs`

**Key data structures.** `TieredPointer{file_id: u32 (== shard_id), offset: u64, length: u32, value_type: u8}`
(17 logical bytes, 24 bytes in RAM after alignment padding); `ShardTierManager{file, current_offset:
Cell<u64>, preallocated_len: Cell<u64>, stats: Arc<TieringStats>, op_manager: Rc<OpManager>, small_bins:
RefCell<SmallBinsManager>, is_direct_io, free_pages: RefCell<Vec<u64>>, free_extents:
RefCell<Vec<(u64,u64)>>}` — the last two fields are new; `TieringStats` has 21 `AtomicU64` fields including
`offload_threshold_pct` (default 60, live) and `upload_threshold_pct` (default 80, still reported but never
consumed).

**Key algorithm/workflow.** `stash_record` packs records < `SMALL_VALUE_LIMIT` (2048 bytes) into a shared 4KB
`ActiveBin`, else writes a standalone page-aligned extent. Checksum is now `xxh3_64` (SIMD), replacing the old
bit-by-bit CRC64. `allocate_page`/`allocate_extent` now check `free_pages`/`free_extents` **first** before
growing `current_offset`. `check_auto_tier` spills Hot keys in 64-key slices down to a 5%-headroom target
(changed from a single 256-key shot to exactly `shard_max_mem`).

**Notable implementation detail — the free-space-reuse bug is FIXED.** Commit `8d899a1` added
`free_pages`/`free_extents` free lists, populated by `on_key_deleted` and a new `on_key_overwritten` hook
(threaded through `set_extended_with_hash` and both squashed-pipeline SET fast paths), and consumed by
`allocate_page`/`allocate_extent` — the previously-documented unbounded logical-file-growth bug is resolved
and covered by `test_free_extent_and_page_reuse`. Residual: `free_extents` is first-fit, not best-fit, so
fragmentation is bounded but not eliminated. Free lists are **not persisted** across restart or `TIER
SNAPSHOT` — they always start empty.

**Verified findings.** `upload_threshold_pct` remains fully configured/reported but never read anywhere —
dead config, unchanged from prior pass. **Critical, confirmed in two independently hand-duplicated copies:**
cluster slot migration (`MIGRATE` command handler, `connection.rs:10908-10909`, and `migrate_keys_to_node`
used by `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD`, `connection.rs:3711`) both have an empty match arm for
`RudisValue::Tiered`/`Cooled`, serializing nothing — but `Cooled` is actually safe (`get_entry` unwraps it to
its RAM copy), so only pure `Tiered` (fully disk-resident) keys are affected. `Router::dump_key` calls
`ensure_loaded`, which silently returns `false` without loading when `is_memory_constrained()` is true. Both
migration paths then **unconditionally delete** the source key regardless of whether anything was written to
the destination — **silent data loss for fully-tiered keys migrated while the source shard is under memory
pressure** (also flagged independently in Component 11's own doc). New: RDB snapshot (`ShardDb::
save_rdb_chunk`) silently omits a `Tiered` entry entirely (no key or value bytes) if `read_ptr_sync` fails,
potentially leaving an orphaned expire-opcode in the RDB stream. New: `O_DIRECT` mode page-aligns offsets/
lengths but never uses aligned buffer allocation (no `posix_memalign`) — correctness depends entirely on
`monoio`'s internal buffer handling.

**Full documentation:** [`docs/internal/07_nvme_tiering.md`](07_nvme_tiering.md)

---

## Component 08: Vector Search Engine: HNSW, Redis 8 Vector Sets & NVMe Tiering

**Source files:** `src/vector.rs` (3,305 lines, grown from ~1,378)

**Key data structures.** `HnswIndex{name, dim, metric, m=16, m0=32, ef_construction=64, ef_search=32, ml,
entry_point, max_layer, nodes: Vec<Option<HnswNode>>, free_ids, key_to_id, pq_quantizer:
Option<ProductQuantizer>, pq_trained, quant: VQuant, attributes: HashMap<Bytes,String>, projection:
Option<Vec<f32>>, is_redis_vset: bool, tier_path: Option<PathBuf>, rng_state}`; `HnswNode{vector: Vec<f32>
(empty if tiered), quantized: Option<QuantizedVector>, binary: Option<Vec<u64>>, pq: Option<PQVector>,
neighbors: Vec<Vec<usize>>}`; `FlatIndex` (exact brute-force, structure-of-arrays); `VectorFieldIndex::
{Flat,Hnsw}` (used identically by Redis 8 Vector Sets, RediSearch VECTOR fields, `SemanticCache`, and
agent-memory recall — four independent owners of the same two types); `SemanticCache{namespace, index:
HnswIndex (always Cosine), entries, hits, misses, tokens_saved}` — genuinely defined in `vector.rs`, **no
separate `semcache.rs` file exists**. There is also **no `RudisValue::VectorSet` enum variant and no
`VectorSetValue` wrapper struct** — Redis 8 Vector Sets are just `HnswIndex` instances in
`ShardDb.vector_indexes`, distinguished by the `is_redis_vset: bool` flag.

**Key algorithm/workflow — corrects an earlier "brute-force KNN" characterization.** Vector KNN search is now
**genuine HNSW**: real graph construction (`add_quantized_ext`, Malkov & Yashunin Algorithm-4 diversity-aware
neighbor selection with `keepPrunedConnections=true`), real bounded best-first `search_layer`/
`search_layer_filtered` beam search, and real delete-time graph repair with former-neighbor reconnection plus
node-slot reuse via `free_ids`. SIMD distance kernels (`dot_product`/`l2_distance_sq`/`cosine_distance`)
three-tier dispatch AVX-512 → AVX2+FMA → portable scalar (no NEON path). Three independent compression tiers
exist: SQ8 (75% reduction, asymmetric ADC via precomputed sums), 1-bit binary/Hamming (96.9% reduction, zero
training), and Product Quantization (now a real k-means++ seeded + Lloyd's-iteration trainer, replacing the
old deterministic-basis-only codebook).

**Notable implementation detail.** `HnswIndex::new` seeds its private xorshift64 PRNG with the fixed constant
`0x853c49e6748fea9b` every time — deterministic across restarts, not OS entropy. NVMe `.vtier` disk spilling
for the HNSW graph itself is a separate, ad-hoc append-only file sharing nothing with `ShardTierManager`
(Component 07) — spilled bytes are never reclaimed on delete/reinsert.

**Verified findings.** `enable_pq(m)` does not retroactively PQ-encode existing vectors — only elements
inserted after with `quantize_pq=true` get real codes. **`vsim_ext`'s rerank heuristic is inverted**: `rerank
= (quant == NoQuant)`, meaning quantized (`Q8`/`Bin`) Redis-8 Vector Sets — the ones that actually need an
exact-distance rerank pass — never get it, while already-exact `NoQuant` sets rerank pointlessly. `VSIM`/
`VLINKS` similarity-score formula (`(1 - dist/2).clamp(0,1)`) assumes a Cosine `[0,2]` distance range
regardless of the index's actual metric — meaningless/clamped output for `L2`/`IP` indexes. PQ silently drops
trailing dimensions when `dim % m != 0`. PQ's `compute_distance_with_vec` rebuilds the full `m×256` ADC table
from scratch on every single candidate (no shared per-query table). `VSIM ... FILTER` string literals don't
decode escape sequences (`\n` becomes literal `n`). `HnswIndex` still lives entirely inside one shard with no
cross-shard `ShardMessage` support.

**Full documentation:** [`docs/internal/08_vector_engine.md`](08_vector_engine.md)

---

## Component 09: RediSearch Full-Text Engine & Hybrid Vector Fusion

**Source files:** `src/search.rs` (4,117 lines, grown from ~2,488)

**Key data structures.** `pub type DocId = u32`; `InvertedIndex{schema, inverted: HashMap<String,
Vec<Posting>>, numeric_trees: HashMap<String,RangeTree>, key_to_id, id_to_meta, vector_indices:
HashMap<String, VectorFieldIndex>, next_doc_id, free_ids, total_docs, total_terms, indexing_failures}`;
`Posting{doc_id,term_freq}` (8 bytes); `DocMeta{key, doc_len (whole-document), fields, numeric_fields,
tag_fields, vector_fields: HashMap<String,Vec<f32>>, multi_vector_fields: HashMap<String,Vec<Vec<f32>>>,
terms}`; `RangeTree{entries: BTreeMap<OrderedF64,Vec<DocId>>}` (genuine O(log N + K) range queries).

**Key algorithm/workflow — reverses a prior finding.** `InvertedIndex.vector_indices: HashMap<String,
VectorFieldIndex>` now holds **real** `FlatIndex`/`HnswIndex` instances built by `build_vector_index`, and
`knn_search` genuinely traverses the HNSW graph via `HnswIndex::search_filtered` — this corrects the earlier
characterization of vector fields as a brute-force linear scan over `DocMeta.vector_fields`. A
RediSearch-accurate "ADHOC_BF" heuristic (`KNN_ADHOC_BF_RATIO = 0.1`) falls back to brute force only when the
pre-filter candidate set is small or the index is `FLAT`. Each index still exists in **two independent
copies**: a per-shard partition (`ShardDb.search_indices`, what real `FT.SEARCH`/`FT.AGGREGATE`
scatter-gather against) and a process-wide mirror (`static SEARCH_INDICES`) read only by `FT.INFO`/`FT._LIST`
and as a defensive fallback — confirming the prior finding unchanged. Hybrid RRF/Linear fusion runs at two
layers: within a shard's `execute_search`, and again, more coarsely, in `Router::ft_search`, which issues
**two full independent cross-shard scatters** (BM25-only AST, then KNN-only AST) and fuses the merged results
client-side — roughly double the cross-shard messages of a plain query.

**Notable implementation detail.** `bm25_score` is textbook Okapi BM25 (k1=1.2, b=0.75, with a 0.0001 IDF
floor) using one whole-document `doc_len`; `FieldType::Text.weight` is parsed by `FT.CREATE` but confirmed (by
grep) never read in scoring. `QueryAst::Exact` remains dead code — `parse_query` never constructs it.
Multi-vector chunk fields (`\x00`-separated chunk keys) work only for JSON-indexed documents;
`add_hash_document` never produces more than one chunk per field.

**Verified findings.** **`FT.ADD` documents are invisible to `FT.SEARCH`/`FT.AGGREGATE`** once an index
exists on every shard: `FT.ADD` writes only the global mirror, but queries read per-shard partitions — a
real, user-visible count/result mismatch (and `FT.ADD` can never index vectors, always passing `vectors:
None`). `VECTOR` schema-level `EPSILON`/`BLOCK_SIZE`, `DIALECT`, and `TIMEOUT` are all parsed, validated, and
stored but never read/enforced anywhere — dead configuration, confirmed by grep. `CASESENSITIVE` `TAG` fields
are unqueryable: indexing respects case, but `parse_query`'s tag branch unconditionally lowercases query
literals, so a mixed-case tag value can never match. `WITHSCORES` and the KNN-distance `YIELD_DISTANCE_AS`
field use inconsistent numeric formatting (`{:.6}` vs. shortest-round-trip `Display`). `FT.DROPINDEX ... DD`
parses but discards the flag — underlying keys are never deleted. `Prefix` and `TagFilter` remain full
O(vocabulary)/O(N) scans; only `Term` and numeric `RangeTree` queries are sub-linear.

**Full documentation:** [`docs/internal/09_redisearch.md`](09_redisearch.md)

---

## Component 10: Kernel Bypass & Zero-Copy Networking

**Source files:** `src/xdp.rs` (706 lines — in-process simulation of AF_XDP UMEM/rings/XSK sockets, a CIDR
rule engine, a per-source-IP token-bucket rate limiter, manual Ethernet/IPv4/TCP parsing) · `src/zerocopy.rs`
(352 lines — real, standalone `MSG_ZEROCOPY`/`io_uring` primitives, unwired)

**Key data structures.** `XdpAction{Pass,Drop,Redirect,Tx}`; `XdpMode{Driver,Skb,Simulated}` (`Driver` is
never constructed anywhere); `XdpRule{id, action, cidr, network, netmask}` scanned linearly, first-match-wins;
`TokenBucket{tokens, capacity, refill_rate, last_update}` — default `capacity=50_000.0`,
`refill_rate=100_000.0` tokens/sec, 1 token/packet, created lazily per source IP and **never evicted**;
`XskRing<T>{producer, consumer, mask, entries: Vec<RwLock<T>>}` (per-slot `RwLock`, not true lock-free SPSC);
`XskUmem{frame_size:2048 fixed, num_frames, frames}` — 128 frames (Simulated) vs **4096 frames (Skb, the
practical default on real Linux hosts)** = 8 MiB UMEM per shard; `UmemFrame` struct is declared but **never
constructed or referenced anywhere in the crate** (dead code); `XskSocket{rx_ring, fill_ring, tx_ring,
comp_ring, umem, rx_packets, tx_packets}`.

**Flagged finding — `XdpEngine` is a process-wide singleton enabling cross-shard packet injection.**
`GLOBAL_XDP_ENGINE: LazyLock<Arc<XdpEngine>>` (xdp.rs:586) is constructed once at startup; `get_xdp_engine()`
clones that one `Arc` — "every shard, and every client connection, shares exactly one `XdpEngine`, including
its `sockets` registry." The doc states explicitly: "a connection accepted on shard B can inject a packet
into the ring polled by shard A's background task, simply by calling `XDP.INJECT <queue_id=A> <payload>` —
nothing ties the issuing connection's shard to the `queue_id` argument." `sockets: RwLock<HashMap<(u16,u32),
Arc<XskSocket>>>` is keyed by `(port, queue_id)`, not per-shard-exclusive, since the engine itself is shared.

**Key algorithm / workflow.** Every shard's `run_shard_worker` spawns a background task polling its own
`XskSocket` (`queue_id = shard_id`): `rx_burst` → `process_packet` (CIDR rule scan → rate limit →
unconditional `Redirect` fallback, ignoring the destination port despite a comment claiming otherwise) →
`extract_transport_payload` → `resp::parse_command` → `target_shard_of_cmd` (explicit whitelist of single-key
commands, catch-all defaults to **local** execution) → real execution via `execute_local_command`/
`Router::execute_remote` against actual shard state, with AOF logging → response written to `tx_burst`. The
only producer of `rx_ring` entries in the whole codebase is `XskSocket::inject_rx`, called exclusively by
`XDP.INJECT`.

**Notable implementation detail.** `src/main.rs` has zero references to either module; no Cargo feature gates
either file — both compile unconditionally on every build/platform. Neither module performs real kernel
bypass; a `/sys/class/net`-existence check selects `Skb` mode in practice, which only changes `XDP.INFO`
output and memory footprint, not real AF_XDP wiring. `zerocopy.rs`'s `send_zc`/`RegisteredBufferPool`/
`enable_so_zerocopy` are real, correct syscall wrappers but have **zero callers outside their own
`#[cfg(test)]` module**, and even if wired up, `send_zc` never polls `MSG_ERRQUEUE` for completion.

**Verified findings.** `target_shard_of_cmd`'s silent local-execution fallback is a correctness trap for
`XDP.INJECT`-driven multi-key or non-whitelisted commands. `rate_limiters` grows unbounded (one `TokenBucket`
per IP ever seen). `XDP.RULEADD`/`RULEDEL`/`RULELIST` are not separate commands — they're subcommands of one
`XDP.RULE ADD|DEL|LIST`.

**Full documentation:** [`docs/internal/10_kernel_bypass_xdp.md`](10_kernel_bypass_xdp.md)

---

## Component 11: Redis Cluster Topology & Gossip Protocol

**Source files:** `src/cluster.rs` (2,306 lines — gossip bus, `ClusterHub`, election, migration
**planning**) · cross-referenced `src/router.rs`, `src/shard.rs`, `src/mailbox.rs`; migration **execution**
lives in `src/connection.rs` (`migrate_keys_to_node:3449`, `execute_rebalance_plans:3735`).

**Key data structures.** `ClusterNodeInfo{id, ip, port, cport, flags, master_id, ping_sent, pong_recv,
config_epoch, link_state, slots:Vec<(u16,u16)>}`; `ActiveMigration` (DFLYMIGRATE bookkeeping only);
`SlotMigrationPlan{slot, source_node_id, source_addr, target_node_id, target_addr}`;
`RebalanceOptions{weights, simulate, threshold=1.25, pipeline:usize (parsed then discarded), target_host_port}`;
`ClusterHub{port, cport=port+10000, nodes, my_slots:Vec<(u16,u16)> default [(0,16383)], pfail_reports,
active_migration, slot_states: HashMap<u16,(String,String)>, cluster_enabled, num_shards}`.
`generate_node_id` builds a 40-hex string from two `fxhash::hash64` calls over port+time-ns — same length as
real Redis node IDs but not collision-resistant.

**Slot-migration data-loss finding (this doc's framing, cross-referenced with Component 07).** "**New —
silent data loss for tiered (NVMe-spilled) keys during migration under memory pressure.**"
`migrate_keys_to_node`'s per-type match contains `RudisValue::Tiered(_) | RudisValue::Cooled { .. } => {}` —
**an empty match arm** (`connection.rs:3711`) — that serializes nothing to the wire for a key still in
tiered/cooled representation. `router.dump_key` calls `ensure_loaded(key)` first, but `ensure_loaded` returns
`false` immediately, without loading anything, if `is_memory_constrained()` (over per-shard `maxmemory`) is
true. Because the source-side deletion loop unconditionally removes every key `dump_key` returned regardless
of whether it produced output, the doc concludes: "A slot migration performed while a node is over its
configured `maxmemory` can silently drop tiered keys." Applies uniformly to `CLUSTER SETSLOT ... MIGRATING`,
`CLUSTER REBALANCE`, and `CLUSTER RESHARD` (same `migrate_keys_to_node` path).

**Key algorithm / workflow.** Gossip tick every 500ms (`cluster_bus_tick`) opens a fresh TCP connection per
peer, re-sends full node-table state each time (no incremental gossip). PFAIL→FAIL escalation requires
silence > 5000ms then quorum `(total_masters/2)+1`. Election (`start_election`) is the one genuine
quorum-vote mechanism; `CLUSTER FAILOVER FORCE` bypasses the vote entirely. Rebalance planning uses
largest-remainder apportionment (`compute_rebalance_plan`); migration execution is real DUMP-and-replay over
live write commands, batched at exactly 100 keys per round trip, sequential/unpipelined.

**Notable implementation details / new findings this revision.** `Router::check_slot_redirection`
duplicates the inline `SlotState` match in `connection.rs::execute_command` but is **never called anywhere**
— dead code (re-confirmed from Component 04). `CLUSTER ADDSLOTS`/`ADDSLOTSRANGE` update `ClusterHub`
bookkeeping (`my_slots`) but **never touch `Router::slot_owners`** — real command routing is unaffected by a
manually-declared, non-default slot assignment until each slot is walked through an actual `CLUSTER SETSLOT
... NODE` migration. The migration path's final `CLUSTER SETSLOT <slot> NODE myself` step calls
`router.set_slot_owner`, which is identified as "exactly the trigger condition" for Component 04's
squashed-pipeline stale-routing bug — cluster.rs's migration algorithm is "correct in isolation" but this
handshake step arms that separately-documented routing bug.

**Verified findings.** `CLUSTER REBALANCE ... PIPELINE n` is parsed into `RebalanceOptions.pipeline` then
discarded — the handler hardcodes `16`, and batch size is separately hardcoded to 100 — "two separate no-ops
stacked on the same option." Cluster-bus messages have no auth/integrity/framing. `DFLYMIGRATE`/
`DFLYCLUSTER` status commands transfer no real data (bookkeeping-only, unlike the real `MIGRATE`/`CLUSTER
SETSLOT` path).

**Full documentation:** [`docs/internal/11_cluster_topology.md`](11_cluster_topology.md)

---

## Component 12: CRDT Data Types & Manual Multi-Region Sync

**Source files:** `src/crdt.rs` (685 lines) — `HlcTimestamp`/`HybridLogicalClock`, `LwwRegister`, `OrSet`,
`PnCounter`, `CrdtStore`; plus touchpoints in `src/shard.rs`, `src/resp.rs`, `src/connection.rs`, `src/aof.rs`

**Key data structures.** `HlcTimestamp{physical_ms:u64, logical:u32, node_id:u16}` (lexicographic Ord);
`LwwRegister{value:Bytes, timestamp, tombstone}`; `OrSet{elements:HashMap<Bytes, HashSet<HlcTimestamp>>,
tombstones:HashSet<HlcTimestamp>}`; `PnCounter{p:HashMap<u16,i64>, n:HashMap<u16,i64>}` — entries never
removed, only grows; `CrdtStore{clock, registers, sets, counters}`, one per `ShardDb`. `HlcTimestamp.node_id`
is the server's **listening port**, not per-shard or per-cluster identity — every shard in one Rudis process
generates HLC timestamps under the identical `node_id`, since all shard workers share one `port` under
`SO_REUSEPORT`.

**AOF/replication coverage finding (as flagged).** Re-verified — **zero coverage for any `CRDT.*`
mutation.** CRDT writes go through `record_change!(cmd)`, which unconditionally bumps `DIRTY_CHANGES` and
touches `WATCH`ed keys, but AOF-append/replica-propagate are gated behind `crate::aof::command_to_resp`
returning `Some(bytes)`. `command_to_resp` (`src/aof.rs`) has literally zero match arms for `Command::Crdt*`
— confirmed via `rg -n "Crdt" src/aof.rs` returning zero matches — falling through to `_ => None`.
Concretely: for every `CRDT.SET`/`DEL`/`INCRBY`/`SADD`/`SREM`/`MERGE`, nothing is ever written to the AOF and
nothing is sent over the replication stream; `rewrite_shard_aof` (BGREWRITEAOF compaction) also never reads
`db.crdt_store` at all. What *does* capture CRDT state: RDB snapshots — `ShardDb::save_rdb_chunk` embeds the
entire `export_sync_payload()` as an extended record (type byte `13`), used by `SAVE`/`BGSAVE`,
`generate_full_rdb` (PSYNC full resync), and `DEBUG RELOAD`; `restore_rdb_chunk` decodes it back via
`merge_sync_payload` but discards the `Result` (`let _ = ...`) — a corrupt record is silently swallowed.
Consequence: if AOF is the authoritative recovery source, CRDT state is lost on restart regardless of how
recently `CRDT.SET` etc. were called; replication only carries CRDT state at the moment of a full PSYNC
resync, with no further CRDT write ever reaching the replica afterward.

**Key algorithm / workflow.** `HybridLogicalClock::now`/`update` are lock-free CAS retry loops with no
skew-bound check, so a badly-skewed remote timestamp can permanently push the local HLC ahead of wall-clock
time. Per-type merges: `LwwRegister::merge` (later HLC wins outright), `PnCounter::merge` (component-wise max
per node_id on `p`/`n`), `OrSet::merge` (union tags/tombstones, drop tombstoned tags, add-wins semantics).
`export_sync_payload`/`merge_sync_payload` form a hand-rolled, uncompressed, unversioned, checksum-free wire
format serializing the **entire** store on every call — nothing inside Rudis schedules, transports, or
discovers peers for it; sync is entirely operator/external-script driven via `CRDT.DUMP`/`CRDT.MERGE`.

**Notable implementation detail / bug.** `merge_sync_payload`'s bounds checking is incomplete: it checks
`offset + 4 > data.len()` only before each item's leading length field, not before subsequent fixed-width
reads — a malformed or truncated payload passed to `CRDT.MERGE` can panic the shard thread rather than
returning a clean `Err`. `CRDT.GC` has no automatic scheduler despite a doc-comment default of 24h TTL;
tombstones and `PnCounter` node-id entries grow unbounded.

**Full documentation:** [`docs/internal/12_crdt_types.md`](12_crdt_types.md)

---

## Component 13: Lua Scripting & Redis 7 Functions Engine

**Source files:** `src/scripting.rs` (739 lines — flat statics/free-functions, no enums/traits/impls)

**Key data structures.** `SCRIPT_CACHE: LazyLock<RwLock<HashMap<String, String>>>` (SHA1 hex → raw source)
and `FUNCTION_LIBS: LazyLock<RwLock<HashMap<String, FunctionLib>>>` are both **process-wide**, not per-shard
— a script loaded or a library `FUNCTION LOAD`ed on one shard's connection is immediately usable from any
other shard, unlike almost everything else in Rudis. `FunctionLib{name, engine: "LUA", raw_code, functions:
Vec<String>}` holds no compiled/cached closure — only source text and discovered function names. `mlua =
"0.12.1"` with `lua54`/`vendored` features (real Lua 5.4, synchronous only); `Lua::new()` uses no sandboxing
feature flags.

**Key algorithm / workflow.** `EVAL`/`EVALSHA` each do their own first-key routing check: if `KEYS[1]`'s shard
differs from the local shard, the whole command is forwarded via `router.target_shard`/`execute_remote` — a
documented behavior change from earlier revisions. `FCALL` has **no such check** and always runs locally
regardless of `KEYS` — a real, verified routing asymmetry; `target_shard_of_cmd` has no arm for
`Eval`/`Evalsha`/`Fcall` at all. `redis.call`/`redis.pcall` convert Lua args to `Vec<Bytes>`, build a real
`Command` via `crate::resp::build_command`, and run it through `execute_local_command` — so every script write
is independently AOF-logged/replicated as its own constituent command (effects replication). `FUNCTION
LOAD`/`FCALL` each re-run the **entire library source** in a fresh `Lua::new()` VM — once at load (discovery
only, closures discarded) and once per `FCALL` (to re-capture the one needed closure) — there is no persisted
callable object.

**Notable implementation detail.** The Redis-facing API surface is exactly 6 functions: `redis.call`,
`redis.pcall`, `redis.status_reply`, `redis.error_reply`, `redis.sha1hex`, plus `redis.register_function`
(load/call context only) — no `redis.log`, `setresp`, `breakpoint`, `replicate_commands`, or `set_repl` exist
anywhere. No execution timeout/instruction budget exists; `FUNCTION KILL` always replies `+OK` and kills
nothing. `FUNCTION DUMP`/`FUNCTION RESTORE` do not exist at all — `FUNCTION_LIBS` has no persistence and is
lost on every restart.

**Verified findings.** `EVAL_RO`/`EVALSHA_RO`/`FCALL_RO` parse to the identical `Command` variants as their
mutating counterparts — no read-only enforcement exists; `redis.call('SET', ...)` succeeds inside an `_RO`
script. A genuine correctness bug: `execute_local_command` (the function `redis.call` runs through) has no
match arm for `Eval`/`Evalsha`/`Fcall`/`ScriptLoad`/`FunctionLoad` — so nested scripting
(`redis.call('EVAL', ...)` from inside a script) silently falls through to the catch-all, producing Lua `nil`
with **no error raised**. No sandboxing: full Lua stdlib access (`os`, `io`, `debug`) via `mlua::Lua::new()`
defaults, so `os.execute`/`io.open` are reachable from any script.

**Full documentation:** [`docs/internal/13_scripting_functions.md`](13_scripting_functions.md)

---

## Component 14: Persistence & Replication Engines

**Source files:** `src/replication.rs` (1,404 lines) · `src/aof.rs` (3,066 lines, ~doubled since last
revision) · RDB save/load in `src/router.rs`/`src/shard.rs`/`src/table.rs` · PSYNC/DFLY-FLOW handlers in
`src/connection.rs`

**Key data structures.** `AofWriter{buffer, spare_buffer, file, path, offset}` and `AofConfig{enabled, dir,
fsync_every_sec}` (unchanged shape). `ReplicationHub{role, backlog: RwLock<ReplicationBacklog>, replicas:
RwLock<HashMap<u64, Arc<ConnectedReplica>>>, shard_flows, ...}`; `ReplicationBacklog` is a genuine fixed 1MB
(hardcoded) ring buffer. `command_to_resp(cmd: &Command) -> Option<Vec<u8>>` (`aof.rs`) is now an
**81-command, ~1,770-line match** — the single gate deciding what gets AOF-appended and replica-propagated,
used by both `record_mutation` and the `record_change!` macro.

**Key algorithm / workflow.** `command_to_resp` grew to cover hash-field TTL (`Hexpire`/`Hpersist`/
`Hgetex`/`Hsetex`), vector sets (`Vadd`/`Vdel`/`Vsetattr`), semantic cache, agent runtime, extended streams,
`Copy`/`Unlink`/`Bitfield`/`Msetex`/`Zrangestore`. `BGREWRITEAOF` (`rewrite_shard_aof`) streams through a
64KB `BufWriter` and now covers JSON, vector-set, semantic-cache, and agent-runtime state in addition to the
base table/HLL/streams — but **never touches probabilistic structures (Bloom/Cuckoo/CMS/TopK) or CRDT
state**, consistent with those families having no AOF support at all.

**Notable implementation detail — the `RudisValue::Tiered` zero-byte bug is RESOLVED at the production path,
but survives as dead code.** `ShardDb::save_rdb_chunk` (the only production-called RDB codec) and
`rewrite_shard_aof` now hydrate `Tiered(ptr)` entries via `tier_manager.read_ptr_sync` and write them as
normal `String` payloads (commit `7c7061e`). But `RudisTable::save_rdb_chunk`/`restore_rdb_chunk`
(`table.rs`) are a second, **dead** (zero callers anywhere, confirmed by grep) implementation that still
contains the old `RudisValue::Tiered(_) => {}` zero-byte bug — confusing for anyone who greps
`save_rdb_chunk` and lands in `table.rs` first. RDB is now a 19-tag format (type bytes 0-18, up from 14
previously) — tags 15-18 (semantic cache, agent memory, agent checkpoints, agent tool leases) are new.

**Verified findings — AOF/replication coverage.** `command_to_resp` has **no arm for any `JSON.*`,
`BF.*`/`CF.*`/`CMS.*`/`TOPK.*`, or `CRDT.*` command** (confirmed via grep returning zero hits for each).
`record_change!`'s AOF/replicate branch therefore never fires for these families — `DIRTY_CHANGES`/WATCH
invalidation still work, but the mutation is **never AOF-appended and never propagated to a live replica**.
These types persist only via point-in-time RDB snapshot (and for JSON only, also via `BGREWRITEAOF`
compaction); any live write after the last save/rewrite is lost on restart or invisible to a connected
replica. Vector-set/semantic-cache/agent-runtime commands are **not** affected — they have explicit
`command_to_resp` arms. Two determinism gaps: `Spop` re-encodes the input `count` (safe only because
`RudisSet`'s hasher is unseeded/deterministic, unlike real Redis which rewrites to `SREM`); `Xautoclaim`
re-encodes original filter params rather than resolved claim IDs, non-deterministic across replay since
`min_idle_time` is evaluated against differing wall-clock "now". `WaitAof`'s `numlocal` is accepted but
ignored. `save N M` is parsed into `extra_directives` and never scheduled anywhere (confirms the previously
documented gap). RDB save remains forkless, uses blocking `std::fs` inside an async fn, and CRC64 is a plain
byte-at-a-time table lookup (no SIMD).

**Full documentation:** [`docs/internal/14_persistence_replication.md`](14_persistence_replication.md)

---

## Component 15: Security, Memory Allocator & TLS

**Source files:** `src/acl.rs` (443 lines) · `src/allocator.rs` (344 lines — jemalloc telemetry plus an
unrelated `SmallCollectionArena`) · `src/tls.rs` (346 lines)

**Key data structures.** `PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<AclManager>>>>>` keyed by **port
number**, process-global — the TLS accept loop reuses the same `router.port` as the plain loop, so plain and
TLS connections on a shard share one identical `AclManager`. `HAS_CUSTOM_ACL: AtomicBool` is a one-way latch
for the whole process, never reset. `AclUser{name, enabled, passwords, password_hashes, nopass, all_commands,
allowed_commands, disallowed_commands, all_keys, allowed_key_patterns}`. `AllocatorStats{allocated, active,
resident, metadata, mapped, fragmentation_ratio}` from real `tikv_jemalloc_ctl::stats::*` reads.
`TlsSession` wraps a `rustls::ServerConnection` (the former `is_ktls_active` flag is gone — kTLS is not
implemented); after the handshake it is wrapped in `TlsTransport`, the `ClientTransport` impl that lets TLS
clients run the shared generic `handle_client` loop (`src/transport.rs` holds the trait, `PlainTransport`, and
`PushTarget`).

**Key algorithm / workflow.** `check_auth` computes **three** hash forms (`hash_password` legacy SHA1 with a
hardcoded global salt, `hash_password_sha256` — unsalted, matching real Redis exactly, `hash_password_salted`
— per-user salted SHA-256, dead on the write side since nothing ever stores a hash in that format) and
compares against stored `password_hashes`, plus a verbatim `h == password` arm for pre-hashed `ACL SETUSER
user #<hash>` syntax. `ACL SETUSER`'s rule-token parser handles `on/off`, `nopass`, `>password`, `#hexhash`,
`+@all`/`-@all`, `+cmd`/`-cmd`, `~*`/`allkeys`/`~pattern` — but `+@category`/`-@category` tokens and
`&channel:*` are **silently accepted and ignored**, always returning `+OK`. `ACL GETUSER` collapses real
command/key-pattern detail to just `"+@all"`/`"-@all"` and `"~*"`/`""`.

**Notable implementation detail.** jemalloc (`tikv_jemallocator::Jemalloc`) is confirmed the sole
`#[global_allocator]`; `mimalloc` remains in `Cargo.toml` but has zero references in `src/` — dead
dependency. `INFO`'s `mem_allocator` field is a **hardcoded literal `"libc"`** despite every other
`allocator_*` field being real jemalloc data.

**Verified findings — the four flagged questions.** (a) **`requirepass` priming is FIXED.** `main.rs` and
`CONFIG SET requirepass` both clear+repopulate `passwords`/`password_hashes`, push the SHA-256 hash, set
`nopass=false` and `HAS_CUSTOM_ACL=true`; `is_auth_required_for_default()` now correctly returns `true` and
both plain/TLS connections bootstrap `authenticated=false` — traced to commit `002086a`, closing a gap a
prior revision explicitly documented. (b) **kTLS is not implemented; the old kTLS plaintext-bypass note is
obsolete.** The partial `enable_ktls` (a `TCP_ULP` attach with no `SOL_TLS` `TLS_TX`/`TLS_RX` key install),
the `is_ktls_active` flag, and the never-taken kTLS branches in `read_plaintext`/`write_plaintext` have been
deleted outright, so every `--tls-port` connection uses userspace rustls and the buggy code path no longer
exists. (c) **TLS cert loading does
NOT do PEM decoding and will likely panic on real PEM files.** `load_certs_and_key_from_files` reads raw file
bytes and passes them directly to `create_server_config` as if already DER — no `pem`/`rustls-pemfile`
dependency exists anywhere, despite the function's own doc comment claiming PEM support. Real
`certbot`/`openssl` PEM output will fail rustls's DER parse, surfacing as a startup `.expect()` panic; only
the self-signed in-memory fallback (already raw DER) is unaffected. (d) **The TLS accept loop does not use
`conn_balance`** — confirmed independently by this doc and by Component 01's own direct finding; no
connection-balancing hookup exists for the TLS listener.

Other findings: `Command::Reset`'s re-auth check diverges from `is_auth_required_for_default` — it only
inspects `passwords.is_empty()`, ignoring `nopass`/`password_hashes`, so a default user secured only via a
pre-hashed credential would incorrectly stay `authenticated=true` after `RESET`.

**Full documentation:** [`docs/internal/15_security_tls.md`](15_security_tls.md)

---

## Component 16: JSON Document Store & JSONPath Engine

**Source files:** `src/json.rs` (991 lines) · `Command::Json*` wiring in `src/resp.rs`/`src/connection.rs`/
`src/router.rs`/`src/shard.rs`/`src/table.rs`/`src/aof.rs`/`src/search.rs`

**Key data structures.** `JsonStore{docs: HashMap<Bytes, Value>}` — documents are plain `serde_json::Value`,
never a `RudisValue` variant, so they're invisible to `GET`/`TYPE`/`OBJECT ENCODING`. `PathSegment{Root,
Field(String), Index(isize), Wildcard, Slice{start,end}}` — no filter-expression (`?(@.price<N)`) support
anywhere.

**Key algorithm/workflow.** `query_json_path`/`query_json_path_mut` are hand-duplicated breadth-first
segment-fan-out functions over a `Vec` of current matches. `set_json_path` walks parent segments,
auto-vivifying: if an intermediate node is the wrong type it is **silently destroyed and replaced** with an
empty container — no type-check error. `delete_json_path`'s wildcard-last-segment clears the whole container
rather than deleting elements individually; `Slice` as a last segment is unhandled and silently deletes
nothing. Mutators split into two groups with inconsistent semantics: `NUMINCRBY`/`NUMMULTBY`/`STRAPPEND`/
`ARRAPPEND`/`TOGGLE`/`CLEAR` act on **every** matched node, while `TYPE`/`STRLEN`/`ARRLEN`/`OBJKEYS`/
`OBJLEN`/`ARRPOP` act on **only the first** match.

**Verified findings — `JSON.NUMMULTBY` multi-match handling (flagged check, stale-doc correction).**
`JSON.NUMMULTBY` now has its **own** standalone `JsonStore` method, `json_nummultby`, structurally identical
to `json_numincrby` but multiplying, and it correctly handles multi-match wildcard paths — confirmed by an
in-file unit test asserting `"[20,50,100]"` for `$.items[*].price` doubled from `[10,25,50]`. This reverses an
earlier characterization: a prior revision composed `JSON.NUMMULTBY` from two separate `json_numincrby` calls
(read-then-apply-delta), which broke on multi-match wildcard paths because the intermediate read returned a
bracketed multi-value string that couldn't parse as `f64` — that composition no longer exists, fixed as part
of commit `784ccb4`.

**Verified findings — `JsonStore` RDB persistence (flagged check, reverses a prior finding).** JSON
documents **do** survive RDB. `save_extended_rdb_chunk` unconditionally writes every `json_store` entry with
extended-record type tag `7`, called unconditionally from `save_rdb_chunk`; two independent restore paths
(`table.rs::load_rdb`/`load_rdb_bytes` and `shard.rs::restore_rdb_chunk`'s tag-7 branch) both decode and
restore it. This covers both on-disk `SAVE`/`BGSAVE`+restart and a replica's initial full-resync snapshot
(commit `63437cb`) — reversing a prior finding that JSON had "no relationship" to RDB. However, AOF append and
incremental replication remain **completely absent** — `aof::command_to_resp` has zero `Json*` arms, so no
JSON mutation is replayed from AOF or propagated incrementally to replicas, even though `record_change!`
still increments `DIRTY_CHANGES` and fires `WATCH` correctly (see Component 14).

**Other gaps.** No recursive descent despite a misleading in-code comment — `$..name` silently degrades to
`$.name` (single-level). `JSON.ARRINSERT`, `ARRTRIM`, `MERGE`, `DEBUG`, `RESP` don't exist anywhere. `JSON.MGET`
is now properly bucketed/parallel-fanned-out per shard via `Router::json_mget` (fixed since last pass,
replacing a prior sequential-per-key gap).

**Full documentation:** [`docs/internal/16_json_store.md`](16_json_store.md)

---

## Component 17: Geospatial Commands

**Source files:** `src/geo.rs` (569 lines — pure algorithm/helper module, no command dispatch) ·
`Command::Geo*` wiring in `src/connection.rs`/`src/resp.rs`

**Key data structures.** `GeoHashBits{bits:u64, step:u8}` and `GeoHashArea{min_lon,max_lon,min_lat,max_lat}`
— a variable-precision interleaved geohash grid, distinct from the fixed-26-bit-per-axis codec used for the
ZSET storage score. `GeoSearchShape{Radius{radius_m}, Box{width_m,height_m}}`. A "geo set" is literally a
`RudisZSet` whose scores are 52-bit Z-order/Morton geohash integers reinterpreted as `f64` — no dedicated geo
storage structure exists.

**Key algorithm/workflow.** Commit `375b237` replaced an earlier custom "power-of-two-cell interval
decomposition" algorithm with a line-for-line port of real Redis's `geohash.c`: bit-twiddling neighbor
stepping, a doubling-radius step-estimation loop with high-latitude precision correction, and
`calculate_search_areas` (center cell + up to 8 neighbors, trimmed to those overlapping the query box — at
most 9 cells, always). Each cell maps to a `[min_score,max_score)` ZSET range, letting `execute_geo_query`
issue a real ordered range query per cell — for `Full`-backed (`BTreeSet`) ZSets this is `O(9*(log n + k))`,
superseding a prior `O(N)` finding that applied only to the now-removed implementation. Final shape membership
uses exact spherical geometry (Haversine) for both `Radius` and `Box`; the only approximation (flat-Earth
`cos(lat)`-style) sizes the candidate-cell search box, never decides final membership, so pruning cannot
produce false positives.

**Flagged finding — `GEORADIUS STORE`/`GEOSEARCHSTORE` can write to an invisible key on a different shard.**
A new, previously undocumented correctness bug: `GEORADIUS ... STORE`/`STOREDIST` and `GEOSEARCHSTORE` write
their destination key on the shard that owns the **source** key, not the shard owning the destination's hash
slot. Rudis always partitions the keyspace by CRC16 hash slot regardless of whether `CLUSTER` mode is
enabled. The `CROSSSLOT` check exists but is gated behind `router.cluster_enabled`, which is **false by
default** in standalone mode. Shard dispatch (`cmd_primary_key`, `target_shard_of_cmd`) extracts only the
source key, ignoring `store`/`storedist`/`dest` entirely, and the handler calls `db.zadd(store_dest, ...)`
directly against the local `ShardDb` of whichever shard owns the source key — no cross-shard forwarding. Net
effect: in standalone multi-shard mode (the default on multi-core machines), if the destination key hashes to
a different shard than the source, the write silently lands on the wrong shard's local keyspace partition; a
later direct lookup of the destination routes to the correct shard by its own hash slot and finds nothing —
the result is invisible except through further STORE-based commands that happen to route through the source
shard. In `CLUSTER` mode this is masked by the `CROSSSLOT` rejection. The suggested fix mirrors
`ZRANGESTORE`, which correctly keys shard routing on `dst`.

**Other findings.** `EARTH_RADIUS_IN_METERS = 6372797.560856` (matches real Redis's `D_R`, not WGS84). Mile
conversion is `1609.34`, confirmed to match real Redis's own table exactly — a prior doc's claim of
`1609.344` was wrong and is corrected. `GEOADD`'s type-check is delegated entirely to `ZADD` (no explicit
`WRONGTYPE` check, unlike the other 7 geo commands) — a plain `ZADD` can silently corrupt a geo set, with
later geo commands decoding garbage coordinates.

**Full documentation:** [`docs/internal/17_geospatial.md`](17_geospatial.md)

---

## Component 18: Probabilistic Data Structures

**Source files:** `src/probabilistic.rs` (417 lines: `BloomFilter`, `CuckooFilter`, `CountMinSketch`,
`TopK`, `ProbabilisticStore`) · `src/hll.rs` (348 lines, brand-new standalone module: `murmur_hash_64a`,
`hll_pat_len`, `hll_validate`, `hll_decode_registers`, `hll_encode_sparse`/`hll_encode_dense`,
`hll_compute_card`, `hll_count`, `hll_add`, `hll_merge`) · `src/table.rs` (`Db::pfadd`/`pfcount`/`pfmerge`,
legacy `RudisValue::HyperLogLog`)

**Key data structures.** `ProbabilisticStore{bloom_filters, cuckoo_filters, cms_sketches, topk_trackers:
HashMap<Bytes,_>}`, a separate per-shard field outside the main `RudisTable` keyspace — these are **not**
`RudisValue` variants. HLL is the opposite: a `PFADD`-created key is an ordinary `RudisValue::String` holding
real Redis `"HYLL"`-magic byte-format data (`HLL_HDR_SIZE=16`, `HLL_REGISTERS=16384`, `HLL_DENSE_SIZE=16304`,
`HLL_SPARSE_MAX_BYTES=3000`), fully subject to `EXPIRE`/`DEL`/`RENAME`/`DUMP`/`RESTORE`/replication/generic
string ops.

**Flagged finding (a) — legacy `RudisValue::HyperLogLog` and a confirmed RESTORE byte-leak bug.**
`RudisValue::HyperLogLog(Box<[u8;16384]>)` is only ever *constructed* via `RESTORE` of a `DUMP` payload with
type byte `5` — it is legacy-only; every `PFADD`-created key today goes through the new `src/hll.rs` byte
format instead (Component 05). `PFADD` on an existing legacy value mutates the register array in place
without ever producing a `"HYLL"` blob. **Confirmed real bug:** once a legacy value exists, `GET`/
`get_with_hash`/`write_get_resp` return the **raw, unwrapped 16384-byte register array with no `"HYLL"`
header**, and replication/`MIGRATE` propagation encodes it the same way as `SET key <raw bytes>`. Any
subsequent `PF*` command against that leaked value then fails validation since it doesn't start with
`"HYLL"`. Cross-shard `PFCOUNT`/`PFMERGE` fan-out has no legacy-variant handling at all.

**Flagged finding (b) — PFCOUNT estimator.** `hll_compute_card` is re-verified as the **classic/original
Flajolet et al. (2007) estimator**: raw harmonic-mean `α·m²/Σ2^-M[i]` (`ALPHA=0.7213475204444817`,
`M=16384.0`), linear counting for small cardinalities, and the large-range correction — explicitly **not**
the modern histogram/bias-corrected estimator real Redis adopted in 4.0+. For identical register state,
`PFCOUNT` produces a numerically different estimate than modern Redis.

**Flagged finding (c) — RDB persistence.** Bloom/Cuckoo/CMS/Top-K: real, verified, unchanged —
`save_extended_rdb_chunk` serializes `bloom_filters` (tag 8), `cuckoo_filters` (tag 10), `cms_sketches` (tag
11), `topk_trackers` (tag 12), with matching load-path reconstruction; these survive `SAVE`/restart like
ordinary keys. HyperLogLog: no dedicated RDB tag needed — since a `PFADD`-created key is a plain
`RudisValue::String`, it rides the generic string-key RDB path.

**Other verified findings.** Cuckoo filter eviction (`MAX_KICKS=500`) is fully deterministic from the
fingerprint, not RNG-driven, despite informal "random kicks" terminology. Count-Min Sketch uses the standard
non-conservative update rule. Top-K eviction is an O(k) linear scan, not a heap. `hll_merge` is **dead code**
— `table.rs::pfmerge` and the cross-shard `PFMERGE` path both reimplement identical decode/max/re-encode logic
manually instead of calling it. Every mutating `PFADD`/`PFMERGE` pays a full O(16384) decode+re-encode rather
than real Redis's in-place sparse-opcode patching. `PFDEBUG SIMD`/`PFSELFTEST` are pure RESP stubs — no SIMD
code exists.

**Full documentation:** [`docs/internal/18_probabilistic.md`](18_probabilistic.md)

---

## Component 19: Pub/Sub Messaging Hub

**Source files:** `src/pubsub.rs` (987 lines — hub, presence table, glob matcher, RESP frame builders) ·
cross-shard routing and CRC16 sharded-channel slot resolution in `src/router.rs` · per-connection state
machine in `src/connection.rs` · inter-shard wire protocol in `src/shard.rs`

**Key data structures.** `PubSubHub` (one per shard, owned by `Router.pubsub: Rc<RefCell<PubSubHub>>`):
`channels`/`patterns`/`shard_channels` plus reverse indices (`client_channels`/`client_patterns`/
`client_shard_channels`) so disconnect cleanup is O(subscriptions held), not O(all registered state).
`ShardedPresenceTable`: `channel_stripes: [[AtomicU64; 4]; 16]` and `pattern_presence: [AtomicU64; 4]` (one
instance per listening port), widened from a single `AtomicU64`/stripe (64-shard ceiling) to 4 words/stripe
(256-shard ceiling) in commit `3adc495`.

**Key algorithm/workflow.** Local delivery (`PubSubHub::publish`) runs two passes: exact-channel
(O(subscribers)) and pattern (O(total registered patterns) — every pattern glob-matched against every
published channel, no prefix trie). Delivery uses `flume::Sender::try_send` (non-blocking); a full queue
drops the message for that subscriber silently — at-most-once delivery. Cross-shard fan-out
(`Router::publish`) delivers locally first, then consults `is_shard_interested` per remote shard candidate and
dispatches all `ShardMessage::Publish` sends before awaiting any reply. Sharded Pub/Sub (`SPUBLISH`/
`SSUBSCRIBE`) instead routes by CRC16/XMODEM slot (identical functions to ordinary key routing) directly to
the one owning shard — no presence-table consultation.

**Notable implementation detail — reverses a prior finding.** `glob_match` was rewritten in commit `3ffffce`
from a simplified two-pointer `*`/`?`-only matcher into a full recursive port of Redis's `stringmatchlen`, now
supporting `[abc]` membership, `[^abc]` negation, `[a-z]` ranges, and `\`-escapes — the complete real-Redis
glob grammar. It is case-sensitive only and guards recursion depth from `*`-backtracking at `nesting > 1000`.
**This directly reverses the prior rollup doc's claim that bracket classes are unsupported — now false.**

**Verified findings.** For shard IDs ≥ 256, presence-table add/remove calls are silent no-ops while
`is_shard_interested` unconditionally returns `true` (safe fail-open, zero pruning benefit above 256 shards).
Once a connection issues `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`, it is permanently handed to `run_pubsub_loop`
and can never return to normal command execution, even after unsubscribing from everything — unlike real
Redis; every command other than the subscribe-mode allowlist is rejected, and RESP3 does not relax this. No
persistence/replay anywhere — an undeliverable message is simply gone. The per-subsystem doc was checked
directly for the "`notify_stream` blocking 1ms sleep" finding flagged for cross-verification: **no such
mention exists anywhere in `19_pubsub.md`** — that finding belongs to Component 06 (`BlockHub::notify_stream`,
`src/block.rs`, see above), not to this pub/sub subsystem; the two are easy to conflate by name but are
different functions in different files.

**Full documentation:** [`docs/internal/19_pubsub.md`](19_pubsub.md)

---

## Component 20: Agent Memory, LLM Quota & Checkpoints

**Source files:** `src/agent.rs` (802 lines) · touchpoints in `src/resp.rs`, `src/connection.rs`,
`src/shard.rs`, `src/aof.rs`

**Key data structures.** Four independent types, no shared base: `AgentMemorySession{session_id, turns:
Vec<AgentTurn>, id_to_pos, index: Option<HnswIndex>, next_turn_id, active_tokens, compactions}` with
`AgentTurn{id, role, content, tokens, meta, compacted}` (no length/count cap on `content` or `turns`);
`LlmQuotaBucket{requests: VecDeque<(Instant,u64)>, reservations: HashMap<u64,(Instant,u64)>,
next_reservation_id, window_ms}` (default 60,000ms); `AgentCheckpointThread{nodes: HashMap<Bytes,
AgentCheckpointNode>, order, head_step_id, next_seq}` with `AgentCheckpointNode{step_id, parent_id, seq,
timestamp_ms, state, metadata}`; `AgentToolRegistry{calls: HashMap<Bytes, ToolCallEntry>}` with
`ToolCallEntry{input, output, attempt, lease_until, expire_at}` and `ToolClaimState::{Claimed, InProgress,
Completed}`. All four live as `ShardDb` fields (`agent_memories`, `llm_quotas`, `agent_checkpoints`,
`agent_tools`), never as `RudisValue` — invisible to `EXPIRE`/`TYPE`/`OBJECT ENCODING`/`KEYS`. Every command
in this subsystem is single-key, single-shard, routed by the normal CRC16 mechanism — unlike CRDT, nothing
here fans out across shards.

**Key algorithm/workflow.** `AgentMemorySession::context` does a greedy knapsack-by-recency scan (newest-
first, stops at the first turn that would overflow `max_tokens`, even if an older smaller one would fit) then
an episodic-recall pass via `HnswIndex::search_filtered` with an in-graph filter excluding ids already
selected for the working window — but a *compacted* turn (moved out of the working window, never deleted) is
always eligible for recall, since compaction intentionally never prunes the HNSW index (lossy for the prompt
budget, not for retrieval). `compact` flips the oldest N active turns to `compacted: true`, splices in one
summary turn, and fully rebuilds `id_to_pos` from scratch every call — O(total_turns). `LlmQuotaBucket::
reserve` is a dual-limit admission check (RPM AND TPM both required) using a two-phase reserve-then-settle
pattern so a caller knows *before* paying for an LLM call whether it would exceed quota; `window_ms` is not
fixed at bucket creation — any `reserve` call with an explicit `WINDOW` argument silently overwrites it for
all future calls. `AgentToolRegistry::claim` is a three-state lease machine (`CLAIMED`/`IN_PROGRESS`/
`COMPLETED`) specifically designed so a caller that retries a call whose previous attempt already finished
gets the original cached result instead of re-executing a possibly non-idempotent side effect.

**Verified findings — the three flagged questions.** (a) **`LLM.QUOTA.*` state has zero persistence**,
confirmed three independent ways: the dispatch arms never call `record_change!`; `aof::command_to_resp` has
zero `Llm*` match arms (no AOF, no BGREWRITEAOF resnapshot); and `save_extended_rdb_chunk` has explicit RDB
record types for semantic caches (15), agent memory (16), checkpoints (17), and tool registries (18) but
**no section or record type for `llm_quotas` at all** — a restart, failover, or `DEBUG RELOAD` silently
resets every quota bucket to empty. (b) **Checkpoint timestamps are non-deterministic across replicas**:
`AgentCheckpointNode.timestamp_ms` is not a field of the RESP command — it is computed via
`SystemTime::now()` inside `put()` at execution time, so AOF replay or a replica re-executing the original
command bytes computes its own `timestamp_ms` that will not match what the primary originally stored/
returned (`seq` and all other fields do replay deterministically). (c) Other gaps: the episodic-memory HNSW
index is hard-coded to `VectorMetric::Cosine` with no per-session choice; `AgentToolRegistry` has no `DEL`/
`CLEAR`/`EXPIRE` command — a `COMPLETE` with no `TTL` caches forever; `estimate_tokens` is a crude
`content.len().div_ceil(4).max(1)` heuristic that undercounts CJK text. By contrast, `agent_memories`/
`agent_checkpoints`/`agent_tools` (unlike `llm_quotas`) **do** have full RDB (types 16/17/18) + AOF coverage
and correctly survive restart/replication.

**Full documentation:** [`docs/internal/20_agent_memory.md`](20_agent_memory.md)

---

## Component 21: MCP Server

**Source files:** `src/mcp.rs` (748 lines, full module) · wire parsing in `src/resp.rs` · handler dispatch in
`src/connection.rs`

**Key data structures.** `McpToolDef{name: &'static str, description: &'static str, input_schema:
serde_json::Value}`. `builtin_mcp_tools()` returns exactly **11** fixed tools (`rudis_kv_get`,
`rudis_kv_set`, `rudis_semantic_set`, `rudis_semantic_get`, `rudis_vector_add`, `rudis_vector_search`,
`rudis_agent_memory_add`, `rudis_agent_memory_context`, `rudis_agent_checkpoint_put`,
`rudis_agent_checkpoint_get`, `rudis_ft_search`). `tools_list_json()` is a pure derived view feeding both
`MCP.TOOLS` and `MCP.RPC {"tools/list"}`, so the two surfaces can't drift from each other. Three `Command`
variants: `McpTools`, `McpCall{tool, args_json}`, `McpRpc(Bytes)`. `mcp.rs` imports no `crate::agent`/
`crate::vector`/`crate::table` — it only constructs `Command` values.

**Key algorithm/workflow.** `plan_tool_command(tool, args)` translates a tool name + JSON args into a native
`Command` via a flat `match` (e.g. `rudis_vector_search` → `Command::Vsim{target: VsimTarget::Vector(vector),
count: count.max(1), ...}` — count 0 silently bumped to 1, never rejected, no upper bound). `MCP.CALL`/
`MCP.RPC`'s `tools/call` both re-enter `execute_command` recursively (`Box::pin`) with the same `router`/
`client_id`/`authenticated`/`auth_user` state as the outer call — the only subsystem in this doc series where
a command handler re-enters the top-level dispatcher rather than calling `execute_local_command` directly
(contrast Component 13's `redis.call`, which bypasses the ACL gate). This means a tool call inherits real
per-command/per-key ACL enforcement, correct cross-shard routing, and normal AOF/replication behavior for
free, at the cost of one extra recursive `execute_command` frame (its own ACL check, command-stat increment,
and slowlog entry run a second time — one MCP tool call shows up as two distinct executions in
`COMMAND.STATS`/`SLOWLOG`).

**Notable implementation detail.** `MCP.RPC` implements only 4 JSON-RPC 2.0 methods (`initialize`, `ping`,
`tools/list`, `tools/call`; anything else returns `-32601 Method not found`) — no `resources/*`, `prompts/*`,
or batch-request support. `initialize`'s `protocolVersion`/`serverInfo` are hardcoded literal constants, not
derived from the actual build. Tool-level failures (unknown tool, missing argument, failing underlying
command) never produce a RESP `-ERR`/JSON-RPC transport error — only a malformed outer payload does; failures
come back as a successful envelope with `"isError": true`, matching MCP convention.

**Verified findings.** The tool catalog (`builtin_mcp_tools()`) and the tool planner (`plan_tool_command`) are
two independently hand-written descriptions of the same contract with at least one confirmed drift instance:
`rudis_semantic_set`'s published schema omits a `tokens` property that `plan_tool_command` reads anyway, and
`rudis_vector_add`'s schema exposes far fewer fields than `Command::Vadd` actually has. `get_req_str` reports
the identical "missing required string argument" message for both an absent field and a present-but-
wrong-typed field, masking type errors as missing-field errors. Roughly half of the related `Command` surface
has no MCP tool at all — no delete/flush/info variants for semantic cache or agent memory, no checkpoint
history, no tool-lease claim/complete, no LLM quota tools, and only 2 of the ~12 Vector-Set commands are
exposed. **Stale-doc correction**: an earlier combined `20_ai_native_runtime.md` doc pair described `mcp.rs`
as exposing 8 different tools and functions (`mcp_tool_definitions()`, `execute_mcp_tool()`,
`handle_mcp_rpc()`) — none of those names exist anywhere in the current source (verified by grep, zero
matches); that document described a superseded shape of the module, now replaced by this document plus
Component 20.

**Full documentation:** [`docs/internal/21_mcp_server.md`](21_mcp_server.md)
