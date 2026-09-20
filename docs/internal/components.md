# Rudis Subsystem Implementation Deep-Dive & Code Reference

This document provides a concise, implementation-focused summary for all 19 core subsystems of **Rudis**:
the concrete Rust data structures, the key algorithm or workflow, and the most important findings from this
session's source-verification pass against the current codebase.

Each entry links to its full per-subsystem internal document under `docs/internal/`, which carries complete
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

**Source files:** `src/main.rs` (process entry: OS tuning, config load, thread spawn) · `src/server.rs`
(`run_shard_worker`, the per-shard event loop)

**Key data structures.** `Args`/`RudisConfig` (CLI + config-file merge); `AofConfig{enabled, dir,
fsync_every_sec}` and `Option<TlsWorkerConfig>`, each cloned once per shard; `ShardMessage` (60+ variants,
matched in the cross-shard receiver loop); `CatchUnwind`/`catch_unwind_async` (a hand-rolled `Future`
combinator wrapping every `poll()` in `std::panic::catch_unwind`).

**Key algorithm / workflow.** `main.rs` disables transparent huge pages, parses config, computes
`num_shards = threads.unwrap_or(num_cores.min(8))`, builds one `flume`-based shard mesh via
`mailbox::create_shard_mesh(num_shards)`, and spawns one OS thread per shard running `run_shard_worker`. Each
shard thread pins its core, builds a `monoio::RuntimeBuilder<FusionDriver>`, opens a `SO_REUSEPORT` listener
(and a second one on `--tls-port` if configured), restores from RDB or replays AOF, opens its tiering manager,
spawns three periodic tasks (100ms expire / 20ms auto-tier / 2s tiering GC) sharing one `Rc<RefCell<ShardDb>>`
with no synchronization, spawns the cross-shard receiver loop, then enters the accept loop. The cross-shard
receiver loop drains up to 64 already-queued messages per wakeup via non-async `try_recv()` before yielding
back to `recv_async().await`.

**Notable implementation detail.** Shutdown polls `shutdown::is_shutting_down()` in both accept loops; after
the plain loop returns, the AOF writer flushes and fsyncs, then the thread joins — but the cross-shard
receiver, the three periodic tasks, and the AF_XDP ingress loop are not explicitly cancelled, and in-flight
connections are not drained.

**Verified findings.** No correctness gaps specific to this subsystem. TLS wiring (config parsing, per-shard
listener setup) is confirmed live here — see Component 15 for the kTLS bug in the handshake path itself.

**Full documentation:** [`docs/internal/01_reactor_runtime.md`](01_reactor_runtime.md)

---

## Component 02: Connection Lifecycle & Command Execution

**Source files:** `src/connection.rs` · reply mailbox primitives in `src/mailbox.rs`

**Key data structures.** `ClientInfo`, `ClientTracker`; `WATCHED_KEYS`/`CLIENT_WATCH_TAINTED` (per-port
static maps, gated by a fast-path `AtomicBool`); `CMD_STATS` (global map, fed from a thread-local buffer
merged every 1024 commands); `BufferLimit`/`ClientClass` (per-class hard/soft output-buffer limits);
`ConnScratch` (pooled per-reactor-thread scratch buffers, up to 32 instances); **`BatchResponder`**
(`src/mailbox.rs`) — `ready: CachePadded<AtomicBool>`, `payload: CachePadded<UnsafeCell<Option<(...)>>>`,
plus a `flume::Sender`/`Receiver<()>` pair used only for the async wake-up fallback.

**Key algorithm / workflow.** `handle_connection` reads pipelined RESP frames, then picks one of four
execution strategies per read: a transaction branch, a blocking-command branch (flushes buffered replies
first), a single-`execute_command` branch, or — for multiple non-blocking commands — `execute_commands_squashed`.
Squashing buckets remote-routed commands by target shard and dispatches one `ShardMessage::Batch` per remote
shard; the reply travels back via `BatchResponder::finish()` (remote shard writes the payload then stores
`ready` with `Release` ordering) and `try_take()` (connection loads `ready` with `Acquire` and takes the
payload without blocking) — confirmed as "the actual mechanism behind cross-shard pipeline squashing, not a
flume-channel-based responder pool." Harvest: up to 256 non-blocking `try_take()` sweeps with `spin_loop()`
between them, falling back to `notify_rx.recv_async().await` only if still not ready.

**Notable implementation detail.** `execute_command` runs an eight-gate sequence (slowlog → NOAUTH → ACL →
ASKING → CROSSSLOT → cluster redirection → READONLY → OOM) and checks **every** key of a command via
`for_each_cmd_key`, not just the primary key — a previously-flagged "only first key checked" ACL gap is
confirmed closed. Inline write fast paths inside `execute_commands_squashed` (`SET`, `INCRBY`, `DEL`, `HSET`,
`SADD`, `ZADD`, `LPUSH`, `LPOP`) are gated on "no AOF and no connected replica," since AOF/replication/
search-index/blocked-wakeup side effects live in the general dispatch path, not duplicated into fast paths.

**Verified findings.** The inline `SET` fast path inside `execute_commands_squashed` does **not** call
`touch_watched_key` or `notify_key_invalidation`, unlike its sibling fast paths (`INCRBY`/`DEL`/`HSET`/
`SADD`/`ZADD`/`LPUSH`/`LPOP`, all of which do). A plain `SET` executed inside a squashed batch under the
fast-path's gating conditions will not taint an active `WATCH` on that key and will not emit a RESP3
client-side-cache invalidation — a real, verified correctness gap with a proposed one-line fix (add the same
call the sibling arms already make).

**Full documentation:** [`docs/internal/02_connection_lifecycle.md`](02_connection_lifecycle.md)

---

## Component 03: RESP Protocol Engine & Command Parser

**Source files:** `src/resp.rs` (9,155 lines)

**Key data structures.** `pub enum Command` (108 variants, ~1,069 lines, derives `Debug, PartialEq, Clone`
but not `Eq`, since several variants carry `f64` fields with no total order); `Del(SmallVec<[Bytes; 1]>)`/
`Exists(SmallVec<[Bytes; 1]>)` (inline capacity 1, so the common single-key case never allocates); Memcached
variants coexist in the same enum as native RESP commands.

**Key algorithm / workflow.** `parse_command` dispatches purely on the first byte: `*` → `parse_resp_array`;
otherwise tries `parse_memcached_storage_command` (a triple-layered `Option` distinguishing "not memcached,"
"memcached but incomplete," and "complete"), falling through to `parse_inline_command`.
`parse_resp_array` is two-pass: pass one scans for frame completeness without mutating the buffer, caching
offsets for arrays of ≤16 elements in a fixed stack array; pass two either does one `split_to` call producing
a single shared `Bytes` frame and dispatches ≤16-element, high-frequency commands directly from cached offsets
(skipping `build_command` entirely), or falls through to `build_command`'s 283-arm match for everything else.

**Notable implementation detail.** `parse_decimal_bytes` is the only overflow guard in the length-decoding
path (`checked_mul`/`checked_add`) — there is still no upper bound on a valid, non-overflowing length.
`parse_redis_f64` tries Rust's `f64::from_str` first, then falls back to libc `strtod` via FFI for exact
Redis-compatible float parsing (accepting `inf`/`+inf`/`-inf`/`infinity`, rejecting `nan` and literals Rust
would silently coerce to `inf`).

**Verified findings.** No maximum bulk-string/array length is enforced beyond `usize`-overflow protection —
a documented gap (no `proto-max-bulk-len` equivalent), not a new discovery. No cross-shard mailbox, routing, or
WATCH logic exists in this file at all — those concerns live entirely in `connection.rs`/`router.rs`/`shard.rs`.

**Full documentation:** [`docs/internal/03_resp_engine.md`](03_resp_engine.md)

---

## Component 04: Sharding Architecture & Cross-Core Mesh

**Source files:** `src/router.rs` (4,197 lines) · `src/shard.rs` (2,668 lines) · `src/mailbox.rs` (728 lines —
the cross-shard IPC primitives)

**Key data structures.** `Router` (grown from 5 fields to ~20+: routing/topology, persistence/tiering,
pub/sub, cross-shard-tx, and several allocation-eliminating object pools, all `Rc<RefCell<...>>`); `ShardMessage`
(66 variants); `SpscQueue<T>` (`#[repr(align(64))]`, lock-free ring, capacity rounded to a power of two, with a
mutex-guarded overflow `VecDeque` for the rare full-ring case); and the **shared-memory descriptor family**:
`FastGetDescriptor`, `FastSetDescriptor`, `BatchResponder`, `ScatterMgetDescriptor`, `ScatterMsetDescriptor` —
all `Arc`'d, `unsafe impl Send + Sync`, single-writer/single-reader `AtomicBool`-gated `UnsafeCell` payloads.

**Key algorithm / workflow.** Two routing schemes: standalone mode is `fxhash::hash64(extract_hash_tag(key))
% num_shards`; cluster mode is CRC16/XMODEM mod 16,384 → slot → `slot_to_shard(slot, n) = (slot * n) / 16384`
(contiguous ranges, not modulo). `Router::get`/`set` acquire a pooled `FastGetDescriptor`/`FastSetDescriptor`,
spin up to 32 iterations on the descriptor's `done` flag before falling back to `.await`. `MGET`/`MSET` bucket
keys by shard once and dispatch a `ScatterMget`/`ScatterMset` per touched shard, each remote shard writing only
its own disjoint result indices (`write_result(global_idx, val)`) before decrementing a shared `AtomicUsize`
pending counter — only the last shard to finish pays for the wake-up signal.

**Notable implementation detail — corrects a materially stale prior revision of this document.** The
`ShardMessage` variants shown in an earlier revision of this file (`Get`/`Set`/`Batch{responder:
flume::Sender<...>}`) are still *defined* and matched in `server.rs`, but grepping confirms production code
constructs them only inside `#[cfg(test)]` — real traffic exclusively uses `FastGet`/`FastSet`/`ScatterMget`/
`ScatterMset`/`Batch{responder: Arc<BatchResponder>}` instead. `flume` channels remain throughout the mesh but,
on these hot paths, carry only a zero-sized wake-up signal, never the payload. Lower-traffic per-key operations
(`Del`, `Exists`, `IncrBy`, `Expire`, `Persist`, `Ttl`, `DumpKey`, `RandomKey`, …) genuinely still allocate a
fresh `flume::bounded(1)` per remote call.

**Verified findings.** Two genuinely different routing entry points coexist, confirmed by call-site grep:
the **static** free functions `target_shard`/`target_shard_and_hash` never consult `slot_owners`, while the
**dynamic** instance method `Router::target_shard` (via `target_shard_for_slot`) indexes `self.slot_owners`,
honoring any live `set_slot_owner` override during migration. Static routing: `get`, `set`, `expire`,
`persist`, `ttl`, `incr_by`, `exists`, `begin_mget_resp`. Dynamic routing: `expiretime`, `del_keys`, `mget`,
`begin_mset`/`mset`, `json_mget`. The same `MGET` command can therefore route differently depending on which
`Router` entry point handles it — a real, currently unresolved routing inconsistency, listed as the top "High"
priority item in the doc's own Future Improvements. Separately, `Router::check_slot_redirection` exists with
correct-looking logic but is never called anywhere in production — `connection.rs` inlines an equivalent check
itself, and `MGET`/`MSET`'s own dispatch never consults `slot_states` at all.

**Full documentation:** [`docs/internal/04_sharding_mesh.md`](04_sharding_mesh.md)

---

## Component 05: Storage Engine & Compact Encodings

**Source files:** `src/table.rs` (11,712 lines)

**Key data structures.** `RudisFlatTable{ctrl: Vec<u8>, slots: Vec<Option<RudisEntry>>, capacity, mask,
items, growth_left, slot_counts: Box<[u32; 16384]>}`; `RudisEntry{key: Bytes (32B), val: RudisValue (40B),
expire_at: Option<Instant> (16B)}` = 88 bytes total (corrects an earlier draft's wrong 24+40+24 split);
`RudisValue` (11 variants — `Hash`/`Set`/`ZSet`/`Stream` are all `Box`ed to keep the enum itself at 40 bytes);
`SmallCollectionArena` (`src/allocator.rs`) — thread-local free-list pools recycling List/SmallHash/small-Set/
small-ZSet backing allocations, undocumented in any prior revision of this doc.

**Key algorithm / workflow.** Hashing is `fxhash::hash64` only (FxHash, not a cryptographic hash);
`fingerprint()` takes the top 7 bits of the hash. Probing uses `_mm_loadu_si128`/`_mm_cmpeq_epi8`/
`_mm_movemask_epi8` — an **unaligned** SIMD load on x86_64, with a portable scalar fallback on other
architectures (no NEON path). `RudisFlatTable::resize` is monolithic: it allocates a fresh table and rehashes
every live entry in one synchronous pass, triggered whenever `growth_left` reaches zero; a same-capacity
tombstone-clearing branch avoids growing memory when the table is tombstone-dominated rather than sparse.

**Notable implementation detail.** Small/full promotion thresholds are inconsistent in kind: `Hash`'s
threshold is runtime-configurable via global atomics, while `Set`'s and `ZSet`'s are hardcoded constants;
`RudisZSet::Full`'s dict uses hashbrown's default `foldhash` hasher, not `FxBuildHasher` — a noted
inconsistency with the rest of the table's hashing choice.

**Verified findings.** The companion deep-dive [`../design/rudis_table.md`](../design/rudis_table.md) has been
corrected against this implementation: its 64-byte-cache-line-aligned bucket layout, ARM/NEON SIMD path, and
segmented/incrementally-splitting DASH-style directory were all **never built** — `RudisFlatTable` is a single
flat structure with no directory, no segments, and no incremental split; growth is handled entirely by the
monolithic `resize` described above. See Component 14 for the related `RudisValue::Tiered` zero-byte
serialization bug (this file's `TieredPointer`/`Cooled` types are the data affected, but the bug itself is in
the AOF/RDB serialization code path).

**Full documentation:** [`docs/internal/05_storage_engine.md`](05_storage_engine.md)

---

## Component 06: Blocking Operations & The Reactive Event Hub

**Source files:** `src/block.rs`

**Key data structures.** `BlockHub{list_waiters: HashMap<Bytes, VecDeque<ListWaiter>>, zset_waiters,
stream_waiters: HashMap<Bytes, Vec<StreamWaiter>>, blocked_clients, blocked_zset_clients, paused_count,
pending_notifies: Vec<Bytes>}`; `ListWaiter{client_id, key, op: WaiterOp, sender: flume::Sender<...>}` (three
separate waiter/result types, not a unified type — `WaiterOp::{Pop, Move}`); global registry
`PORT_BLOCK_HUBS: LazyLock<Mutex<HashMap<u16, Arc<Mutex<BlockHub>>>>>`.

**Key algorithm / workflow.** `notify_list`/`notify_zset` perform the actual pop **inside** the notify call
itself, under the held mutex and the caller's `ShardDb` borrow — not a design where the writer hands the
pushed value directly to a waiting reader. `WaiterOp::Move` recursively calls `notify_list` on the destination
key, so a single `LPUSH`-triggered wakeup can cascade into a second blocked client's wakeup inside one lock
acquisition. `wait_for_blocked_result` loops re-awaiting the result channel capped at 20ms per iteration; each
timeout tick calls `is_fd_closed` (`libc::poll` plus a non-consuming `MSG_PEEK` recv) to detect a vanished
client without a clean FIN, since a channel receive alone cannot detect that.

**Notable implementation detail.** `notify_list_or_defer`/`notify_zset_or_defer` check `hub.is_paused()`;
during a paused (in-`EXEC`) window, writes record their key in `pending_notifies` instead of notifying
immediately, and `resume()` drains that list and fires real (possibly cross-shard, via
`ShardMessage::NotifyList`) wakeups only after the transaction and any cross-shard lock release fully complete
— a blocked client can never observe a partially-applied transaction. `CLIENT PAUSE`/`UNPAUSE`/`NO-TOUCH` is a
literal unconditional `+OK` stub, distinct from this internal pause/resume mechanism.

**Verified findings.** No correctness gaps specific to this subsystem.

**Full documentation:** [`docs/internal/06_blocking_hub.md`](06_blocking_hub.md)

---

## Component 07: NVMe SSD Tiered Storage Engine

**Source files:** `src/tiering.rs, src/tiering/`

**Key data structures.** `TieredPointer{file_id: u32, offset: u64, length: u32, value_type: u8}` (17 bytes,
held inline in `RudisValue`); `ShardTierManager{file: Rc<monoio::fs::File>, current_offset: Cell<u64>, stats:
Arc<TieringStats>, op_manager: Rc<OpManager>, small_bins: RefCell<SmallBinsManager>, is_direct_io}`;
`SmallBinsManager{active_bin: ActiveBin, page_active_counts: HashMap<u64,usize>, dead_pages: Vec<u64>}`;
`OpManager{in_flight_reads, pending_stashes, pending_stash_bytes: AtomicUsize}`; `TieringStats` (21 atomic
counters, including `offload_threshold_pct` (default 60, live) and `upload_threshold_pct` (default 80,
**reported but never consumed by any gate**)).

**Key algorithm / workflow.** `stash_record` branches on size vs. `SMALL_VALUE_LIMIT` (2KB): below it, the
record packs into the shard's single in-progress `ActiveBin`, flushed with one `write_all_at` per page; at or
above it, any open bin is flushed first, then the record is written as its own page-aligned standalone block.
On-disk records are CRC64-checked (`TIER_MAGIC | value_type | key_len | val_len | crc64 | key | payload`) — a
corrupt/torn write surfaces as an explicit I/O error, not silent corruption. `read_tiered_record` checks the
still-open `ActiveBin` first, then `OpManager::read_page_coalesced` for same-page records (collapsing
concurrent same-page reads to one physical read), or a direct `read_exact_at` for standalone blocks.

**Notable implementation detail.** On restart, the write cursor `current_offset` resumes from the file's
current length rounded up to the next 4KB boundary — a shard restarted mid-page never overwrites a partial
page, it leaves a gap and starts fresh. GC (`on_key_deleted`/`run_gc`) decrements a per-page live-record count
for SmallBins-packed records, queuing a page in `dead_pages` once it hits zero; standalone large blocks are
punched immediately via `fallocate(FALLOC_FL_PUNCH_HOLE)`.

**Verified findings.** The companion deep-dive [`../design/tiered_storage.md`](../design/tiered_storage.md)
confirms the original packed-pointer `RudisExternalPtr`/intrusive `CoolRecord` LRU/mimalloc-style
`ExternalAllocator` design was **not implemented** — this simpler `TieredPointer`/`Cooled`/`current_offset`
design shipped instead. **Critically, punched holes are never reused by future writes**: `current_offset`
grows monotonically regardless of how many holes exist behind it, so the backing file's logical size grows
unboundedly under sustained tier-churn even though physical usage stays bounded by the filesystem's
hole-punching support — a real, verified gap relative to the original allocator proposal, not merely an
unimplemented optimization. Separately, `RudisValue::Tiered` values are skipped (written as zero bytes) during
both RDB save and `BGREWRITEAOF` serialization — see Component 14.

**Full documentation:** [`docs/internal/07_nvme_tiering.md`](07_nvme_tiering.md)

---

## Component 08: Vector Search Engine: HNSW, SQ8 & Product Quantization

**Source files:** `src/vector.rs`

**Key data structures.** `HnswIndex{dim, metric, m=16, m0=32, ef_construction=64, ef_search=32,
entry_point, max_layer, nodes: Vec<Option<HnswNode>>, key_to_id, pq_quantizer: Option<ProductQuantizer>,
rng_state}`; `HnswNode{vector: Vec<f32> (always kept), quantized: Option<QuantizedVector>, pq:
Option<PQVector>, neighbors: Vec<Vec<usize>>}`; `ProductQuantizer{dim, m, d_sub, codebooks: Vec<Vec<Vec<f32>>>}`.

**Key algorithm / workflow.** Distance kernels (`dot_product`, `l2_distance_sq`, `cosine_distance`) probe
`is_x86_feature_detected!` at call time in priority order: AVX-512 (two `__m512` accumulators, 32 floats/iter)
→ AVX2+FMA (two `__m256` accumulators, 16 floats/iter) → portable 8-lane-unrolled scalar. Insertion
(`add_quantized_ext`) computes a target layer via `random_level()`, greedily descends layers above it, then
beam-searches (`search_layer`) down to layer 0 keeping the nearest `m`/`m0` neighbors, with `prune_neighbors`
doing plain nearest-distance truncation (not the original HNSW paper's diversity-aware heuristic). Deletion
strips references from every layer's neighbor lists and tombstones the slot (`nodes[id] = None`) rather than
compacting or reusing it.

**Notable implementation detail.** `ProductQuantizer::new` generates each subvector's 256 centroids
deterministically — centroid 0 is the zero vector, the next `d_sub` are positive unit basis vectors, the next
`d_sub` are negative unit basis vectors, and the remainder are filled by a fixed SplitMix64-style hash — there
is no k-means or any training pass over real data, so every `ProductQuantizer` for a given `(dim, m)` is
byte-for-byte identical. `HnswIndex::new` seeds its xorshift64 PRNG with the fixed constant
`0x853c49e6748fea9b` every time, not OS randomness.

**Verified findings.** No data-loss or correctness bugs. `HnswIndex` lives entirely inside one shard's
`ShardDb` with no `ShardMessage` variant to reach another shard's index — unimplemented cross-shard support,
confirmed as the single most important architectural gap in this subsystem's own Future Improvements.

**Full documentation:** [`docs/internal/08_vector_engine.md`](08_vector_engine.md)

---

## Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion

**Source files:** `src/search.rs` (2,488 lines)

**Key data structures — corrects a materially stale prior revision of this document.** `pub type DocId =
u32` (not `String`); `InvertedIndex{schema, inverted: HashMap<String, Vec<Posting>>, numeric_trees:
HashMap<String, RangeTree>, key_to_id, id_to_meta, next_doc_id, free_ids: Vec<DocId>, total_docs,
total_terms}`; `Posting{doc_id: DocId, term_freq: u32}`; `DocMeta{key, doc_len (whole-document, not per-field),
fields, numeric_fields, tag_fields, vector_fields: Vec<f32>, terms}`; `RangeTree{entries:
BTreeMap<OrderedF64, Vec<DocId>>}`. No `vector_index: Option<HnswIndex>` field exists anywhere in
`InvertedIndex` — vector fields are plain `Vec<f32>`, and KNN is a brute-force linear scan using Component 08's
shared SIMD `cosine_distance` kernel, not an HNSW lookup.

**Key algorithm / workflow.** Each index is stored twice. The real query path is per-shard:
`ShardDb.search_indices: HashMap<String, InvertedIndex>`, with `Router::ft_search` querying the local
partition first, then scatter-gathering `ShardMessage::SearchQuery` to every other shard, each of which answers
using *only* its own local partition. A process-wide mirror (`SEARCH_INDICES: LazyLock<RwLock<HashMap<String,
Arc<RwLock<InvertedIndex>>>>>`) is written on every indexed write and read only by `FT.INFO`/metadata queries
and as a defensive fallback — real lock contention exists solely on this mirror. `add_document` re-indexes as
remove-then-re-add if the key exists; the new doc ID comes from `free_ids.pop()` or increments `next_doc_id`.
`remove_document` is O(terms in that document) via `DocMeta.terms`, recycling the ID into `free_ids`.

**Notable implementation detail.** `bm25_score` is textbook Okapi BM25 (k1=1.2, b=0.75) using one
whole-document `doc_len`, not per-field lengths; `FieldType::Text.weight` is parsed by `FT.CREATE` but
confirmed (by grep) never read anywhere in the scoring function. `QueryAst::Exact(String)` is confirmed dead
code — `parse_query` never constructs it.

**Verified findings.** Confirms, precisely: RediSearch indexing is genuinely per-shard partitioned, not one
global lock, and document identity is a dense `u32` `DocId` with free-list reclamation, not the document's
string key — correcting a prior characterization of this subsystem as a single globally-locked,
string-doc-id-keyed index.

**Full documentation:** [`docs/internal/09_redisearch.md`](09_redisearch.md)

---

## Component 10: Kernel Bypass & Zero-Copy Networking

**Source files:** `src/xdp.rs` (706 lines) · `src/zerocopy.rs` (352 lines)

**Key data structures.** `XdpAction{Pass, Drop, Redirect, Tx}`; `XdpMode{Driver (never constructed), Skb,
Simulated}`; `XdpRule{id, action, cidr, network, netmask}` (linear-scan `RwLock<Vec<XdpRule>>`); `TokenBucket`
(continuous refill, default capacity 50,000 / refill 100,000 tokens/sec, keyed per source IPv4, never evicted);
`XskRing<T>{producer: AtomicU32, consumer: AtomicU32, entries: Vec<RwLock<T>>}` (four per `XskSocket`: rx/fill/
tx/comp, mirroring real AF_XDP's ring layout); `XskUmem{frame_size=2048, num_frames: 128 (Simulated) or 4096
(Skb/Driver), frames: RwLock<Vec<Vec<u8>>>}`.

**Key algorithm / workflow.** Every shard's boot sequence spawns a `monoio` background task polling that
shard's `XskSocket` via `rx_burst`; classified `Pass`/`Redirect` packets have their RESP payload extracted and
are genuinely executed against the shard's real database, with the response written back to `tx_ring`/
`comp_ring`. `process_packet` parses the Ethernet/IPv4 frame, extracts the source IP (and TCP dest port,
computed but never branched on — `let _ = payload_offset;`), applies the CIDR rule scan and rate limiter, then
falls back to an unconditional `Redirect` for anything unmatched.

**Notable implementation detail — corrects a materially stale prior revision of this document.** An earlier
revision of this internal doc claimed `XdpEngine` had no socket/ring/UMEM types at all; that was wrong. Those
types do exist, are instantiated once per shard at boot, and are driven by the real polling loop described
above. What remains true: the only producer that ever feeds `rx_ring` is the explicit `XDP.INJECT` admin
command — nothing external (no real NIC, no eBPF program) ever populates it, and nothing consumes `tx_ring`/
`comp_ring` to transmit over a real network. `zerocopy.rs`'s `send_zc` has zero callers anywhere in `src/`
outside its own `#[cfg(test)]` module.

**Verified findings.** No new correctness bugs; the corrected socket/ring/UMEM-existence finding above is
this component's most significant verification-pass result.

**Full documentation:** [`docs/internal/10_kernel_bypass_xdp.md`](10_kernel_bypass_xdp.md)

---

## Component 11: Redis Cluster Topology & Gossip Protocol

**Source files:** `src/cluster.rs` (2,306 lines)

**Key data structures.** `ClusterNodeInfo{id, ip, port, cport (=port+10000), flags, slots: Vec<(u16,u16)>}`
(normalized, sorted/merged range lists — no 16,384-bit slot bitmask anywhere); `ClusterHub{nodes:
RwLock<HashMap<String,ClusterNodeInfo>>, my_slots, pfail_reports: RwLock<HashMap<String,HashSet<String>>>,
active_migration, slot_states: RwLock<HashMap<u16,(String,String)>>}` — one instance per port via a global
registry, the same deliberate shared-nothing exception as `BlockHub`; `ActiveMigration{state, source_id, slots,
keys_migrated}`.

**Key algorithm / workflow.** `cluster_bus_tick` runs every 500ms: opens a fresh blocking `TcpStream` per
peer, sends a `PING` with the full serialized node table; on >5000ms silence it tallies corroborating PFAIL
votes and compares to `quorum = floor(total_masters/2)+1`, broadcasting `FAIL` only once quorum is reached.
`start_election` requests votes from every known master and only promotes after collecting
`>= (masters.len()+1)/2 + 1` acks, gated by a strictly-increasing `last_vote_epoch` — "the one piece of this
file that is a genuine distributed-consensus mechanism, not local bookkeeping."

**Notable implementation detail — the slot-migration data path, in code-level terms.** `execute_rebalance_plans`
(driven by `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD`) marks a slot `Migrating` in both `router.slot_states` and
`hub.slot_states`, sends `SETSLOT IMPORTING` to the target, then fetches up to 100 keys at a time via
`router.get_keys_in_slot` and migrates each one via `migrate_keys_to_node`: a `router.dump_key`-produced
DUMP-equivalent snapshot (value + remaining TTL), replayed on the target as `ASKING` plus a type-appropriate
write command, over a fresh `monoio::net::TcpStream`. This repeats until the slot is empty, then `SETSLOT NODE
myself` finalizes it. By contrast, the Dragonfly-compatible `dfly_migrate_init`/`_flow`/`_ack` family only
maintains an in-memory state string and a counter — "no keys are read, serialized, or transferred by this code
path."

**Verified findings.** Confirms, precisely: cluster slot migration via `SETSLOT`/`REBALANCE`/`RESHARD`
performs a real, working, sequential (not pipelined) key copy — not bookkeeping-only, correcting any prior
characterization to the contrary; the bookkeeping-only characterization remains accurate only for the separate
`DFLYMIGRATE`/`DFLYCLUSTER` compatibility surface. Separately: `router.slot_states` (Component 04, per-shard),
`ClusterHub.my_slots`/`.nodes` (gossip-populated), and `ClusterHub.slot_states` (a second, distinct
migrating/importing tracker) are three independently maintained slot-authority records, all genuinely consulted
by different parts of the redirect/migration path but sharing no storage — documented as a "latent source of
future drift," related to but distinct from Component 04's two-routing-entry-point inconsistency.

**Full documentation:** [`docs/internal/11_cluster_topology.md`](11_cluster_topology.md)

---

## Component 12: CRDT Data Types & Manual Multi-Region Sync

**Source files:** `src/crdt.rs`

**Key data structures.** `HlcTimestamp{physical_ms: u64, logical: u32, node_id: u16}` (lexicographic `Ord`);
`HybridLogicalClock{node_id, latest_physical_ms: AtomicU64, latest_logical: AtomicU32}`;
`LwwRegister{value, timestamp, tombstone}`; `OrSet{elements: HashMap<Bytes, HashSet<HlcTimestamp>>, tombstones:
HashSet<HlcTimestamp>}`; `PnCounter{p: HashMap<u16,i64>, n: HashMap<u16,i64>}`; `CrdtStore{clock, registers,
sets, counters}` — one instance per shard, keyed by node ID = the server's port, so all shards sharing one
`SO_REUSEPORT` port generate HLC timestamps under the *same* node ID.

**Key algorithm / workflow.** `HybridLogicalClock::now`/`update` use lock-free compare-exchange retry loops
(`Acquire`/`Release`), seeding the next physical value from `phys_now.max(cur_phys).max(remote.physical_ms)`.
Merge: `LwwRegister` — strictly later HLC timestamp wins, ties keep the existing value; `PnCounter` —
per-node component-wise max of the `p`/`n` maps; `OrSet` — union the tag sets and tombstones, then retain only
tags not covered by a tombstone. `export_sync_payload`/`merge_sync_payload` use a flat, hand-rolled binary
format (1-byte type tag + little-endian length-prefixed fields, no framing beyond concatenation, no
delta/incremental support) — `CRDT.DUMP` re-serializes the entire store on every call.

**Notable implementation detail.** `gc_tombstones(ttl_ms)` prunes tombstoned `LwwRegister`s and `OrSet`
tombstone entries strictly by wall-clock age; it does not touch `PnCounter` (which has no tombstones), and
nothing in `server.rs` calls it automatically — it is purely on-demand via `CRDT.GC`.

**Verified findings.** `command_to_resp` (`src/aof.rs`, Component 14) has no match arm for any
`Command::Crdt*` variant — CRDT single-key writes are not AOF-appended and not streamed to PSYNC replicas. A
node's CRDT state today survives only in memory plus whatever external process runs `CRDT.DUMP`/`CRDT.MERGE`; a
restart or failover loses it.

**Full documentation:** [`docs/internal/12_crdt_types.md`](12_crdt_types.md)

---

## Component 13: Lua Scripting & Redis 7 Functions Engine

**Source files:** `src/scripting.rs`

**Key data structures.** `static SCRIPT_CACHE: LazyLock<RwLock<HashMap<String, String>>>` (SHA1 → **source
text**, not bytecode); `static FUNCTION_LIBS: LazyLock<RwLock<HashMap<String, FunctionLib>>>`;
`FunctionLib{name, engine: "LUA", raw_code, functions: Vec<String>}` — records only which names a library
registers.

**Key algorithm / workflow.** `eval_script`/`call_function` both call `mlua::Lua::new()` fresh at the top and
let it drop at the end — no persistent VM, no bytecode cache. `redis.call`/`redis.pcall` convert Lua arguments
to `Bytes`, pass them through `crate::resp::build_command` (the same parser as wire commands), and execute via
`crate::connection::execute_local_command` — a script is not a separate command-processing path, so each write
independently triggers `record_change!` (AOF append + replication propagation), exactly as if the client had
sent it directly. `FUNCTION LOAD` runs a library's entire top-level source once in a discovery-only interpreter
with a stubbed `redis.register_function` that just records names; `FCALL` **re-runs the entire library source
again**, in a fresh interpreter, this time with a real `redis.register_function` that captures and invokes the
matching closure.

**Notable implementation detail.** `resp_bytes_to_lua` converts a RESP error reply into a raised Lua error
(not a returned value) — this is what makes a failing `redis.call` raise inside the script, while
`redis.pcall` intercepts it and returns `{err=...}` instead.

**Verified findings.** Explicitly, directly stated by the design doc: Rudis's Lua environment is **not
sandboxed** — no stdlib restriction (`os`/`io`/`package`/`debug` all present), no execution-time limit, no
`SCRIPT KILL`. `EVAL`/`EVALSHA`/`FCALL` never route based on `KEYS` — always run on the connection's local
shard, verified directly against `connection.rs`'s command-routing table. `FCALL` re-executing a function
library's entire top-level source on every call (not just at `FUNCTION LOAD`) is a real, verified
implementation detail with real repeated-parse cost, not a hypothetical one.

**Full documentation:** [`docs/internal/13_scripting_functions.md`](13_scripting_functions.md)

---

## Component 14: Persistence & Replication Engines

**Source files:** `src/replication.rs, src/aof.rs` · RDB save/load lives in `src/router.rs`
(`perform_save_rdb`/`generate_full_rdb`) and `src/table.rs` (`serialize_val_payload`/`load_rdb_bytes`) — see
also the dedicated [`docs/rdbsave.md`](../rdbsave.md) for the full RDB format and save-path deep-dive.

**Key data structures.** `AofWriter{buffer, spare_buffer, file: Option<Rc<monoio::fs::File>>, offset}`;
`ReplicationHub{role, backlog: ReplicationBacklog, replicas: HashMap<u64, Arc<ConnectedReplica>>,
shard_flows: HashMap<usize, HashMap<u64, Arc<ShardReplicaFlow>>>}` (one shared instance per port, behind
`RwLock`s/atomics); `ReplicationBacklog{buffer: Vec<u8> (fixed max_size), write_idx, len, max_size}` — a
genuine fixed-capacity ring buffer (`append` wraps with two `copy_from_slice` calls, `get_diff` reads backward
with wrapping), correcting an earlier revision that described it as a plain growable `Vec`.

**Key algorithm / workflow.** `record_mutation(port, aof, cmd)` calls `command_to_resp` once; if it returns
`Some(bytes)`, those bytes are appended to the local AOF (if present) and handed to `propagate_bytes` — any
command without a `command_to_resp` match arm is invisible to *both* AOF and replication (the mechanism behind
Component 12's CRDT-write gap). `BGREWRITEAOF`/`rewrite_shard_aof` iterates every live table entry, emits
canonical RESP reconstruction commands to a temp file, fsyncs, atomically renames over the live AOF, fsyncs the
containing directory, then rewrites every *other* shard **sequentially, not concurrently**, to bound peak
memory during compaction. Partial resync: the master's `try_partial_resync` checks a requested replid/offset
against the retained backlog window and answers `+CONTINUE <replid>\r\n<diff-bytes>` when eligible, else falls
back to `router.generate_full_rdb()` and `+FULLRESYNC`.

**Notable implementation detail — the RDB/AOF-rewrite serialization gap, in code-level terms.**
`serialize_val_payload`'s match arm for `RudisValue::Cooled` correctly delegates to serializing the wrapped
value, but **the arm for `RudisValue::Tiered` writes nothing at all** — `RudisValue::Tiered(_) => {}`, not even
a type tag. Because `save_rdb_chunk` always writes the key length and key bytes *before* calling
`serialize_val_payload`, a key whose value is `Tiered` at the moment of `SAVE`/`BGSAVE` or `BGREWRITEAOF`
produces a key with zero payload bytes, desynchronizing the reader's cursor for every record that follows it in
that shard's chunk. This has not been observed to be specially handled anywhere in the save path (e.g. by
forcing tiered values back into RAM before a save).

**Verified findings.** The `RudisValue::Tiered` zero-byte serialization bug above is a genuine, currently
unhandled data-loss/corruption edge case for any deployment saving or compacting while tiered storage is
active — not hypothetical. Separately: Rudis's config parser recognizes and stores the classic Redis
`save <seconds> <changes>` directive verbatim (`extra_directives` in `src/config.rs`), but **no code path in
`router.rs` or `server.rs` ever reads it to schedule an automatic `BGSAVE`** — the directive is accepted
without a parse error but has no runtime effect; there is currently no automatic, save-point-triggered
background snapshotting in Rudis. Also worth noting: `INFO`'s `# Persistence` section hardcodes
`rdb_bgsave_in_progress:0` and `rdb_last_save_time:0` regardless of actual state — only the dedicated
`LASTSAVE` command reflects reality.

**Full documentation:** [`docs/internal/14_persistence_replication.md`](14_persistence_replication.md)

---

## Component 15: Security, Memory Allocator & TLS

**Source files:** `src/acl.rs, src/allocator.rs, src/tls.rs`

**Key data structures.** `AclUser{name, enabled, passwords: Vec<String> (plaintext), password_hashes:
Vec<String> (SHA1, fixed salt), nopass, all_commands, allowed_commands/disallowed_commands: HashSet<String>,
all_keys, allowed_key_patterns: Vec<String>}`; `AclManager{users: HashMap<String, AclUser>}` — seeds exactly
one `"default"` user with `nopass: true, all_commands: true, all_keys: true`, so an unauthenticated connection
behaves as this fully-privileged user unless auth is actually enforced; `AllocatorStats{allocated, active,
resident, metadata, mapped, fragmentation_ratio}` (via `tikv_jemalloc_ctl`).

**Key algorithm / workflow.** `execute_command`'s auth/authz sequence: `-NOAUTH` gate for anything but
`AUTH`/`HELLO`/`QUIT`; then, if authenticated, `can_execute_command(cmd_name)` (allow/deny-list lookup, always
allowing `ping|reset|quit|auth|hello`) and, if the command has a primary key, `can_access_key(key)`
(`allowed_key_patterns` prefix/exact match unless `all_keys`) — a denial produces a real `-NOPERM` reply and an
early return. `execute_commands_squashed`'s eligibility loop calls the same two methods; an ACL-denied command
simply falls back to the sequential path where the real denial fires.

**Notable implementation detail — the `requirepass` gap, traced to the exact line.** Auth-required state is
computed as `!HAS_CUSTOM_ACL.load(...) || !get_acl_for_port(port).read().unwrap().is_auth_required_for_default()`
in `connection.rs`. `HAS_CUSTOM_ACL` is set only inside `AclManager::set_user`/`del_user`. `CONFIG SET
requirepass <pw>` mutates the default user's `passwords` field directly, **without calling `set_user`** — it
never sets `HAS_CUSTOM_ACL` and never clears `nopass`, so `is_auth_required_for_default` (`!user.nopass && ...`)
stays `false`. The config-file `requirepass` directive is not applied to the ACL system at all. Only an
explicit `ACL SETUSER default ... >password` reliably turns on enforcement.

**Verified findings — the kTLS plaintext-bypass bug, in code-level terms.** `enable_ktls(raw_fd)` performs
only `setsockopt(IPPROTO_TCP, TCP_ULP, b"tls\0", 4)` — attaching the kernel TLS module, which succeeds on
essentially any modern Linux host regardless of whether key material was ever installed.
`handshake_monoio` does `if enable_ktls(raw_fd).is_ok() { self.is_ktls_active = true; }` after a successful
handshake. The second, actually-required `setsockopt(SOL_TLS, TLS_TX/TLS_RX, ...)` call that installs the
negotiated cipher/key/IV into the kernel socket does not exist anywhere in this file. `TlsSession::
read_plaintext`/`write_plaintext` then branch on `is_ktls_active`: when true, they read/write the raw socket
directly, with no rustls encryption at all. Net effect: on essentially every real Linux deployment that enables
`--tls-port`, application data is transmitted **completely unencrypted** after a genuinely successful handshake
— marked in this document's own Future Improvements as upgraded from "Medium" to "CRITICAL," the single most
urgent item in the whole document. Separately: plaintext password storage was not removed when hashing was
added (`check_auth` accepts either), and the SHA1 hash uses one hardcoded global salt shared by every user and
deployment.

**Full documentation:** [`docs/internal/15_security_tls.md`](15_security_tls.md)

---

## Component 16: JSON Document Store & JSONPath Engine

**Source files:** `src/json.rs`

**Key data structures.** `PathSegment{Root, Field(String), Index(isize), Wildcard, Slice{start, end}}`;
`JsonStore{docs: HashMap<Bytes, Value>}` — plain `serde_json::Value`, no bespoke JSON representation.

**Key algorithm / workflow.** `parse_json_path` is a hand-rolled loop over `Peekable<Chars>` — no
grammar/lexer library. `query_json_path`/`query_json_path_mut` accumulate matches breadth-first, building a new
`Vec` of children satisfying each path segment in turn, with slice bounds independently clamped (negative
counts from the end, `.max(0)`/`.min(len)`). `set_json_path` auto-vivifies `Object`/`Array` containers while
walking parent segments; if an intermediate element exists but is the wrong type, it is silently overwritten
with a fresh empty container rather than erroring (setting through a `Wildcard`/`Slice` parent *does* return a
real error).

**Notable implementation detail.** `JSON.NUMMULTBY` has no dedicated `JsonStore` method at all — it is
composed in `connection.rs` from two `json_numincrby` calls (read current value with delta 0.0, compute
`new = cur * factor`, apply delta `new - cur`), parsing the intermediate result as a single `f64`.
`query_json_path`/`query_json_path_mut` are hand-duplicated for `&`/`&mut` access — every match arm in the
immutable traversal has a corresponding `_mut` arm doing identical navigation logic, verified as real
duplication with no stated design rationale.

**Verified findings.** `JSON.NUMMULTBY` silently fails on multi-match wildcard paths: a path matching more
than one node makes the intermediate `json_numincrby` result a bracketed multi-value string, which fails the
`f64::parse` and returns `-ERR value at path is not a number` instead of multiplying every match. `JSON.SET`
can silently overwrite a differently-typed intermediate value during auto-vivification, as described above.
`JSON.MGET` is dispatched sequentially, one shard round-trip per key, unlike the bucketed fan-out `MGET`/`MSET`
received. `JsonStore` is verified absent from `save_rdb_chunk`/`load_rdb` in `table.rs`/`router.rs` — no RDB
persistence, the same gap Component 08's vector indexes have.

**Full documentation:** [`docs/internal/16_json_store.md`](16_json_store.md)

---

## Component 17: Geospatial Commands

**Source files:** `src/geo.rs`

**Key data structures.** `GeoUnit{Meters, Kilometers, Miles, Feet}` (conversion factors matching real Redis);
`GeoItemResult{member, dist, hash, coord}` with a single shared `format_geo_results` reply formatter across
`GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH`; `GeoSearchShape{Radius{radius_m}, Box{width_m, height_m}}`.

**Key algorithm / workflow.** `encode_geohash`/`decode_geohash` interleave bits over a 26-iteration loop
(standard Z-order/Morton encoding); decoding is lossy, returning the *center* of the encoded cell.
`GEOADD` encodes then delegates entirely to `db.zadd` (Component 05) — no geo-specific conflict handling.
`geohash_search_ranges` computes contiguous 1D geohash score intervals covering the query bounding box *before*
touching the ZSet: degenerate cases handled explicitly (radius ≤ 0 → single exact-point interval; radius ≥
20,000,000m → the full range), otherwise the radius is converted to a local lat/lon box via a 111,320 m/degree
flat-Earth approximation, the largest power-of-two grid cell covering the box is found, and overlapping cells
are enumerated and merged. `execute_geo_query` runs one `RudisTable::zrange` per interval, de-duplicating
members matched by multiple intervals via a `HashSet`, then applies the exact radius/box test
(`GeoSearchShape::is_inside`) to every candidate — pruning narrows the set but introduces no false positives.

**Notable implementation detail.** `GEODIST` does two `zscore` lookups, decodes each, and computes Haversine
distance — it returns `$-1` only if a `zscore` lookup itself misses, not if the decoded coordinates are
nonsensical (e.g. the score came from a non-geo `ZADD` on the same key).

**Verified findings.** No correctness gaps. Approximations (spherical rather than ellipsoidal distance, the
flat-Earth box-width/height conversion, a self-derived interval decomposition rather than real Redis's literal
"9 neighboring cells" technique) are documented as deliberate choices intentionally matching or closely
approximating real Redis behavior.

**Full documentation:** [`docs/internal/17_geospatial.md`](17_geospatial.md)

---

## Component 18: Probabilistic Data Structures

**Source files:** `src/probabilistic.rs`

**Key data structures.** `BloomFilter{capacity, error_rate, num_bits, num_hashes, count, bits: Vec<u64>}`;
`CuckooFilter{capacity, num_buckets, count, buckets: Vec<[u16; 4]>}` (bucket size 4); `CountMinSketch{width,
depth, total_count, table: Vec<Vec<u64>>}`; `TopK{k, items: HashMap<Bytes, u64>}`; `ProbabilisticStore` holds
each structure type in its own separate map, so a Bloom key and a Cuckoo key of the same name are independent
entries (dispatch is by command family — `BF.*` vs `CF.*` — not by a shared namespace).

**Key algorithm / workflow.** Bloom `add` uses the Kirsch-Mitzenmacher trick: only two real hashes (`h1`,
`h2`) are computed, with the `i`-th hash derived as `h1 + i*h2 mod num_bits`. Cuckoo's `indices` function is
standard partial-key cuckoo hashing (`i2 = i1 ^ fnv1a_hash(fingerprint)`), so a bucket is recomputable from the
current bucket plus fingerprint alone during a kick chain (capped at `MAX_KICKS = 500`). CMS `incr_by` uses a
**standard, non-conservative** update rule — every row is unconditionally incremented by the delta on every
call (explicitly not the conservative-update variant); the estimate is the minimum across rows. Top-K's
eviction does a linear scan over all `k` tracked items to find the minimum-count item, not a heap — fine at
realistic small `k`.

**Notable implementation detail.** Command handlers live in `src/table.rs` (e.g. `BF.RESERVE` inserts
directly into `db.probabilistic_store.bloom_filters`), not in `probabilistic.rs` itself, which holds only pure
data structures and algorithms.

**Verified findings.** No correctness gaps. RDB persistence is explicitly verified present:
`ShardDb::save_extended_rdb_chunk` serializes every Bloom/Cuckoo/CMS/Top-K entry with its own type tag, and the
load path in `src/shard.rs` reconstructs each structure verbatim — unlike Components 08, 12, and 16, this
subsystem has no persistence gap.

**Full documentation:** [`docs/internal/18_probabilistic.md`](18_probabilistic.md)

---

## Component 19: Pub/Sub Messaging Hub

**Source files:** `src/pubsub.rs` · cross-shard dispatch lives in `src/router.rs`

**Key data structures.** `PubSubHub` (one per shard, owned by `Router.pubsub: Rc<RefCell<PubSubHub>>`):
`channels`/`patterns`/`shard_channels` maps plus reverse indices (`client_channels`/`client_patterns`/
`client_shard_channels`) so per-client cleanup is O(subscriptions held by that client), not O(every
channel/pattern in the hub). `ShardedPresenceTable` (one per port): `channel_stripes: [AtomicU64; 16]` plus a
single unstriped `pattern_presence: AtomicU64`; `stripe_for(channel) = hash_key(channel) % 16`, reusing the
storage engine's own hash function.

**Key algorithm / workflow.** `PubSubHub::publish` runs two independent passes: exact-channel subscribers
(O(subscribers to that channel)) then pattern subscribers (O(total registered patterns in this shard's hub),
glob-matched per publish — no prefix index or trie). `Router::publish` delivers locally first, then computes
`presence_table.interested_shards(channel) = channel_stripes[stripe] | pattern_presence` and dispatches
`ShardMessage::Publish` only to shards whose bit is set, issuing every send before awaiting any reply. Sharded
Pub/Sub (`SPUBLISH`) computes `slot = key_slot(&channel)` (the same CRC16/XMODEM function used for key
routing, honoring `{hash tag}` syntax) and routes to exactly one shard, consulting no presence state at all.

**Notable implementation detail.** `glob_match` is a hand-written backtracking `*`/`?` matcher (tracks the
last `*` position and resumes from there on a mismatch, linear-time in practice) with **no `[...]`
character-class support** — narrower than real Redis's fuller glob syntax; bracket characters match only
literally. Delivery uses `flume::Sender::try_send` (non-blocking); a full per-subscriber queue causes a silent
drop, not counted in `PUBLISH`'s returned receiver total.

**Verified findings.** No correctness gaps. The presence bitmask is confirmed to be a hint that may
over-approximate (a stripe collision can cause a spurious remote lookup) but never under-approximates (never a
missed delivery, since the receiving shard's own hub still performs the exact match) — a deliberate,
documented precision/cost trade-off, not a bug.

**Full documentation:** [`docs/internal/19_pubsub.md`](19_pubsub.md)
