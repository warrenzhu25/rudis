# Rudis Subsystem Architecture & High-Level Design Guide

This document provides the high-level architectural specifications for all 19 core subsystems of **Rudis**.
It details **what** each subsystem does, **why** it was designed that way, its **key concurrency invariants**, and its **performance characteristics**.

For concrete Rust struct definitions, step-by-step execution algorithms, and source line references, see [`docs/internal/components.md`](../internal/components.md).

---

## Subsystem Index

- [01. Reactor Runtime & Server Lifecycle](#component-01) (`src/main.rs, src/server.rs`)
- [02. Connection Lifecycle & Command Execution](#component-02) (`src/connection.rs`)
- [03. RESP Protocol Engine & Command Parser](#component-03) (`src/resp.rs`)
- [04. Sharding Architecture & Cross-Core Mesh](#component-04) (`src/router.rs, src/shard.rs`)
- [05. Storage Engine & Compact Encodings](#component-05) (`src/table.rs`)
- [06. Blocking Operations & The Reactive Event Hub](#component-06) (`src/block.rs`)
- [07. NVMe SSD Tiered Storage Engine](#component-07) (`src/tiering.rs`)
- [08. Vector Search Engine: HNSW, SQ8 & PQ](#component-08) (`src/vector.rs`)
- [09. RediSearch Full-Text Engine & Reciprocal Rank Fusion](#component-09) (`src/search.rs`)
- [10. Kernel Bypass & Zero-Copy Networking](#component-10) (`src/xdp.rs, src/zerocopy.rs`)
- [11. Redis Cluster Topology & Gossip Protocol](#component-11) (`src/cluster.rs`)
- [12. CRDT Data Types & Manual Multi-Region Sync](#component-12) (`src/crdt.rs`)
- [13. Lua Scripting & Redis 7 Functions Engine](#component-13) (`src/scripting.rs`)
- [14. Persistence & Replication Engines](#component-14) (`src/replication.rs, src/aof.rs`)
- [15. Security, Memory Allocator & TLS](#component-15) (`src/acl.rs, src/allocator.rs, src/tls.rs`)
- [16. JSON Document Store & JSONPath Engine](#component-16) (`src/json.rs`)
- [17. Geospatial Commands](#component-17) (`src/geo.rs`)
- [18. Probabilistic Data Structures](#component-18) (`src/probabilistic.rs`)
- [19. Pub/Sub Messaging Hub](#component-19) (`src/pubsub.rs`)

---

## Component 01: Reactor Runtime & Server Lifecycle

> **Source Files**: `src/main.rs, src/server.rs` | **Internal Reference**: [`docs/internal/01_reactor_runtime.md`](../internal/01_reactor_runtime.md)


---

### 1. Architectural Purpose & Scope

The **Reactor Runtime & Server Lifecycle** subsystem is responsible for bootstrapping the Rudis server process, pinning worker threads to physical CPU cores, setting up Linux `io_uring` instances via the `monoio` asynchronous runtime, and running each shard's event loop for its entire lifetime.

Unlike Redis (single-threaded event loop) or lock-based multi-threaded servers, Rudis uses a **Shared-Nothing Multi-Reactor** pattern: every worker core runs its own independent `monoio` runtime driving an isolated Linux `io_uring` ring, its own `SO_REUSEPORT` listener, and its own thread-local database. `src/main.rs` parses CLI arguments and spawns one OS thread per shard; `src/server.rs::run_shard_worker` is the entire body of that thread — it never returns.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Thread-per-Core Pinning**: Every worker thread is pinned to an exclusive CPU core using `core_affinity::set_for_current`, unless `--no-pin` is passed. No worker thread is migrated by the OS scheduler once pinned.
2. **`SO_REUSEPORT` Ingress Balancing**: Every worker thread opens its own listening socket bound to the same port (`socket.set_reuse_port(true)`). The kernel distributes new incoming connections across all bound sockets by a 4-tuple hash, with zero userspace dispatch.
3. **Thread-local database, no locks in the data path**: `ShardDb` lives in a plain `Rc<RefCell<ShardDb>>` — not `Arc<Mutex<_>>`. Because `Rc`/`RefCell` aren't `Send`, the compiler itself refuses to let a `ShardDb` handle cross a thread boundary.
4. **One real exception to "zero locks": the blocking-command hub.** `crate::block::get_block_hub_for_port(port)` returns an `Arc<Mutex<BlockHub>>` from a process-wide `static` map keyed by port (`PORT_BLOCK_HUBS`), so it *is* shared and mutex-guarded across every shard thread serving that port. This is a deliberate, narrow exception: blocking commands (`BLPOP`, `BZPOPMIN`, ...) need cross-shard wakeups, which the shared-nothing model can't give them for free, so a small locked structure was introduced specifically for that coordination rather than for the data path itself.
5. **No graceful shutdown.** There is no signal handler anywhere in `src/server.rs` or `src/main.rs` — no `SIGINT`/`SIGTERM` trap, no drain, no flush-on-exit logic. The process only stops if every thread's infinite loop is killed externally (the accept loop and the cross-shard receiver loop both run forever).

---

---

### 3. Performance Characteristics

- **Zero-syscall-per-connection ingress**: `SO_REUSEPORT` means the kernel — not userspace — decides which shard's listener gets each new connection.
- **No cross-core cache traffic in the common case**: local key access never leaves the owning thread; only the `ShardMessage` mesh and the shared `BlockHub` mutex cross cores, and both are only exercised on non-local or blocking operations.
- **Bounded periodic work, not full scans**: the 100ms expiration cycle, 20ms auto-tier check, and 2s GC task are all designed to do fixed, small amounts of work per tick rather than scanning the whole shard, so they never show up as a latency spike on the shared single-threaded runtime.
- **The cross-shard receiver loop now amortizes async overhead across up to 64 messages per wakeup** (§4.2's `try_recv` burst-draining) instead of paying one `recv_async().await` suspend/resume cycle per message — a real, measurable win under sustained cross-shard traffic (e.g. many concurrent `MGET`/`MSET` fan-outs or heavy pipeline squashing hitting one shard from many peers at once).
- **Batch-level tiering checks are now gated on whether tiering is enabled at all** (`has_tier_manager`, §4.2) — a shard running with no tiered storage configured skips the per-`Get`-in-batch `is_tiered` lookup entirely rather than paying a cheap-but-nonzero check on every batched read.

---

---

## Component 02: Connection Lifecycle & Command Execution

> **Source Files**: `src/connection.rs` | **Internal Reference**: [`docs/internal/02_connection_lifecycle.md`](../internal/02_connection_lifecycle.md)


---

### 1. Architectural Purpose & Scope

`src/connection.rs` is the single largest module in Rudis (~10,600 lines) and the central
coordination layer for every client session. It owns the per-connection read/parse/execute/write
loop, protocol-mode transitions (Pub/Sub, replica streaming via `PSYNC`), RESP2/RESP3 reply
formatting and client-side caching invalidation, Redis transactions (`MULTI`/`EXEC`/`WATCH`),
blocking commands (`BLPOP`/`BZPOPMIN`/blocking `XREAD`), Redis Cluster slot-migration
redirection (`MOVED`/`ASK`/`ASKING`), ACL authentication gating, and the local-vs-remote
routing decision — plus command-specific execution logic for the full command surface (strings,
hashes, lists, sets, sorted sets, streams with consumer groups, HyperLogLog, bitmaps, geo,
probabilistic structures, JSON, vector search, Lua scripting, and a Memcached text-protocol
gateway). It is genuinely the busiest file in the codebase, not a thin dispatcher.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Pure Single-Threaded Client State**: Every connection is handled exclusively by the
   core that accepted it (`handle_connection`'s locals — `in_multi`, `tx_queue`, `asking`,
   `authenticated`, `auth_user` — are plain stack variables, no `Arc`/`Mutex`).
2. **Blocking Commands Force an Immediate Flush First**: Before executing a command like
   `BLPOP` that may suspend the task for up to its timeout, `handle_connection` flushes any
   already-buffered responses to the socket first — otherwise earlier pipelined replies would
   sit unsent for the entire blocking duration.
3. **Cross-Shard Transactions Use Explicit Locks**: A `MULTI`/`EXEC` block whose queued
   commands touch more than one shard acquires a distributed "VLL" (very-lightweight-locking)
   lock across every touched shard before running the batch, and releases it after — see §4.2.
4. **Squashing Is Gated on More Than Just Routability**: The pipeline-squashing fast path
   (`execute_commands_squashed`) additionally requires the client to already be authenticated,
   have real ACL permission for each command and its key, and (for keyed commands) that this
   node's cluster-gossip table actually confirms ownership of the key's slot — not merely that
   the slot's local `SlotState` is `Stable` — see §4.4.
5. **Non-Blocking Cross-Shard Dispatch**: Remote-shard work is always sent as a
   `ShardMessage::Batch` over pre-allocated `flume` channels and awaited without blocking the
   reactor thread — other connections on the same core keep making progress.

---

---

### 3. Performance Characteristics

- **Pre-allocated responder pool, not one-shot channels**: `ResponderChannel`s are built once
  per connection (`(0..router.num_shards).map(|_| flume::bounded(1))`) and reused for every
  pipeline flush — still the mechanism behind zero-allocation steady-state cross-shard fan-out.
- **`CompactResp` replies**: batched remote responses are carried as `CompactResp` rather than
  a plain `Vec<u8>`, reducing per-reply allocation/copy overhead in the squashed path.
- **Command-name lowercasing/stat tracking is not free**: every executed command updates a
  global `CMD_STATS` map (`record_cmd_stat`) under a `RwLock`, plus per-client `last_cmd`
  bookkeeping — real but modest fixed overhead paid on every command, not just squashed
  batches.
- **`MGET`/`MSET` now genuinely parallelize across shards** (§4.6) via pooled channels and a
  spin-then-block harvest, closing what was previously the single biggest cost on multi-shard
  multi-key workloads — bounded by the slowest remote shard's response time now, not by the
  sum of every remote shard's response time.
- **The spin-then-block harvest trades CPU for latency, with a fairness cost worth naming**:
  up to 128 non-blocking `try_recv` sweeps (`std::hint::spin_loop()` between them) happen
  *without yielding to `monoio`'s cooperative scheduler* — on this shared-nothing,
  single-threaded-per-core design, that means other connections' tasks on the *same core*
  make no progress while an `MGET`/`MSET` is in its spin phase. Fine when remote shards
  reply within microseconds (the common case this was tuned for); a burst of concurrent
  `MGET`/`MSET` calls each waiting on a genuinely slow remote shard could measurably delay
  unrelated connections sharing that core until the 128-iteration cap is hit and each falls
  back to a real `.await`.

---

---

## Component 03: RESP Protocol Engine & Command Parser

> **Source Files**: `src/resp.rs` | **Internal Reference**: [`docs/internal/03_resp_engine.md`](../internal/03_resp_engine.md)


---

### 1. Architectural Purpose & Scope

`src/resp.rs` is Rudis's wire-format decoder. It turns raw bytes read off a TCP socket into
a single, strongly-typed `Command` enum value, one command at a time, and nothing else — it
does **not** serialize replies. Reply formatting (RESP2 bulk strings, integers, arrays, and
RESP3 maps/booleans where applicable) is hand-written directly into the output buffer in
`src/connection.rs`, not in this file. There is no `write_resp_*`/serialization module here.

The file is large (~7,500 lines) almost entirely because of the size of the `Command` enum
and its parser (`build_command`), which now covers well over 200 distinct top-level command
names spanning strings, hashes, lists, sets, sorted sets, streams, bitmaps, HyperLogLog,
pub/sub, transactions, cluster/gossip, ACL, scripting, vector search, geospatial, probabilistic
structures, RDB serialization, tiered-storage control commands, and a Memcached text-protocol
gateway — not because the core parsing algorithm itself grew complex. That algorithm (the
two-pass zero-copy RESP array parser) is unchanged from the original implementation.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Zero-Copy RESP Array Parsing**: Bulk string arguments inside a `*N\r\n...` frame are
   extracted via `BytesMut::split_to(len).freeze()` — a reference-count bump on the
   underlying buffer, never a byte-for-byte copy.
2. **All-or-Nothing Frame Consumption**: A partially buffered command (still waiting on more
   bytes from the socket) leaves the input buffer completely untouched (`Ok(None)`); a
   complete command is parsed and fully consumed in one call. There's no partial-consumption
   state to track between calls.
3. **Three Independent Input Grammars, One Entry Point**: `parse_command` recognizes RESP
   arrays (`*...`), plain space-separated inline text (`GET foo\r\n`), and Memcached's ASCII
   storage-command grammar (`set key flags exptime bytes\r\n<data>\r\n`) — dispatched purely
   by the first byte of the buffer, or by trial-parsing for the Memcached case (see §4.1).
4. **No RESP3 wire-format parsing in this file.** `HELLO` is recognized and parsed as a
   `Command` (so a client can request protocol v3), but nothing in `resp.rs` parses RESP3
   input types (maps `%`, sets `~`, doubles `,`, booleans `#`, nulls `_`, pushes `>`) — every
   incoming command is still a flat array of `$`-prefixed bulk strings. RESP3 is purely an
   *output*-side concern implemented in `connection.rs` (a per-client `is_resp3` flag and a
   thread-local `CURRENT_CLIENT_RESP3` cell gate which reply format gets written).

---

---

### 3. Performance Characteristics

- **Zero-copy on the hot (RESP array) path**: every bulk-string argument is a `Bytes` slice
  sharing the original read buffer's allocation, not a fresh heap copy.
- **The inline and Memcached paths copy**: both exist for compatibility/interactive use, not
  throughput, and neither is on the benchmarked pipeline path.
- **`build_command`'s dispatch is a single string match, not a lookup table**: with 200+ arms,
  this is a large `match` the compiler is left to optimize (typically into some mix of
  length-bucketed comparisons/jump tables); no bespoke perfect-hash or trie dispatch was
  built for it.

---

---

## Component 04: Sharding Architecture & Cross-Core Mesh

> **Source Files**: `src/router.rs, src/shard.rs` | **Internal Reference**: [`docs/internal/04_sharding_mesh.md`](../internal/04_sharding_mesh.md)


---

### 1. Architectural Purpose & Scope

`src/router.rs` and `src/shard.rs` implement Rudis's data partitioning and inter-thread
messaging system. `router.rs` defines the `Router` struct — the per-shard facade every
command goes through to decide "is this key mine, or do I need to hop to a peer core" —
plus CRC16-based key-to-slot-to-shard mapping. `shard.rs` defines the thread-local
`ShardDb` (the actual per-core state container: `RudisTable` plus every other per-shard
subsystem — tiering, vector search, CRDTs, JSON, probabilistic structures, sticky-key
pinning) and the `ShardMessage` enum that is the entire cross-core wire format.

Beyond routing, `Router` has grown into the coordination point for nearly every
multi-shard concern in the codebase: NVMe tiering orchestration, RDB snapshotting,
AOF fsync fan-out, pub/sub broadcast, cross-shard transaction locking, cluster
administration passthroughs, and replication-stream application. Each of these is
documented below because they all live in this file, even though several belong more to
persistence/replication/tiering conceptually.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Deterministic Key Ownership (static formula)**: `target_shard(key, num_shards)` maps
   every key to exactly one shard via `crc16(hash_tag(key)) % 16384` → contiguous slot
   range → shard index. This is unchanged from the original design and is what nearly
   every command actually routes through (see §4.4 for the caveat).
2. **Lock-Free Asynchronous Mesh**: cross-shard communication is exclusively `flume`
   channels (unbounded senders held by every shard, one per-shard receiver drained in
   that shard's own event loop). No shared memory, no mutex, no `oneshot` crate — despite
   what an earlier, inaccurate draft of this document claimed.
3. **Hash Tag Compatibility**: `extract_hash_tag` — only the substring between the first
   `{` and the next non-empty `}` is hashed, so `{user:100}:profile` and
   `{user:100}:orders` land on the same shard.
4. **`Router` is `Clone`, not a singleton reference**: cloning just bumps `Rc`/`Arc`
   refcounts on its fields (see §3) — cheap, and necessary because async tasks spawned
   off a `Router` method (e.g. `bgsave`'s background save) need an owned copy to move into
   `monoio::spawn`.

---

---

### 3. Performance Characteristics

- **Lock-Free Communication**: unchanged — `flume` channels, no mutexes, no atomics on
  the per-key data path itself.
- **Per-op remote calls still allocate**: as documented in §4.2, the non-batched router
  methods allocate a fresh one-shot channel per remote call; only the pipelined
  `ShardMessage::Batch` path (Component 02) uses a pre-allocated pool.
- **`CompactResp`'s 30-byte inline buffer** removes a heap allocation from the
  overwhelmingly common case (short RESP replies) of the cross-shard batch response path.
- **`MGET`/`MSET` now parallelize across shards, with pooled channels and a busy-poll
  fast-harvest phase** (§4.3, resolved) — one `ShardMessage::Mget`/`Mset` per remote
  shard touched, dispatched together and harvested via non-blocking `try_recv` before
  falling back to a real `.await`, so throughput on multi-shard keysets is now bounded by
  the slowest remote shard's response, not by the sum of every key's round-trip.

---

---

## Component 05: Storage Engine & Compact Encodings

> **Source Files**: `src/table.rs` | **Internal Reference**: [`docs/internal/05_storage_engine.md`](../internal/05_storage_engine.md)


---

### 1. Architectural Purpose & Scope

`src/table.rs` is Rudis's core in-memory associative storage engine. It provides the
dictionary implementation (`RudisTable`), the definitions and per-command logic for every
`RudisValue` data type (strings, hashes, lists, sets, sorted sets, streams, bitmaps-as-strings,
HyperLogLog), active/passive key expiration, and the bookkeeping hooks that let
`src/tiering.rs` move cold values out to NVMe storage and back.

The dictionary itself is still the custom SIMD flat hash table (`RudisFlatTable`) originally
designed for this project — it has **not** been replaced by `hashbrown` or any Listpack/
Intset/skiplist-based structure. What has grown substantially since the original design is
everything built on top of it: `RudisValue` now has 11 variants instead of 2, several of
which have their own adaptive small/full representations, and `RudisTable` now tracks live
memory usage and NVMe-tiering state per key.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Thread-Isolation**: Each `RudisTable` belongs to a single shard thread. It contains
   **no mutexes, atomic operations for its own data, or lock-free concurrency wrappers**
   (the two global counters described in §5 are process-wide atomics, but they're simple
   monotonic stats, not synchronization for the table itself).
2. **Inlined Metadata & Expiration**: `expire_at: Option<Instant>` lives directly inside
   `RudisEntry`. Checking key validity never requires a secondary hash lookup.
3. **Adaptive Small/Full Promotion — but not for every type**: `Hash`, `Set`, and `ZSet`
   values each start in a compact linear form and promote to an indexed form once they
   cross a size threshold (§4). Hash's thresholds are runtime-configurable via global
   atomics; Set's and ZSet's are hardcoded constants. `List` and `Stream` have no compact
   form at all — they use the same representation regardless of size.
4. **Passive Inline Eviction, with an escape hatch**: any operation encountering an expired
   key immediately frees it and returns `None`/empty, unless the process-wide
   `crate::connection::ALLOW_ACCESS_EXPIRED` atomic flag is set, in which case expiration
   checks are skipped entirely (see §5.1).
5. **Cold-storage awareness**: a value can be partially or fully moved to NVMe by
   `src/tiering.rs`. `table.rs` doesn't perform that I/O itself, but `RudisValue::Tiered`
   and `RudisValue::Cooled` exist specifically so the table can represent "this key's value
   lives on disk" or "this key's value lives on disk *and* is still cached in RAM" (§5.3).

---

---

### 3. Performance Characteristics

- **SIMD group probing is unchanged**: still one 128-bit load and compare per 16-slot group,
  triangular-step probing to avoid primary clustering.
- **Zero-allocation `INCR`/`DECR`** when the value is already `Int`-encoded — mutates the
  `i64` in place instead of round-tripping through string formatting.
- **Hand-written integer/byte conversions** (`parse_i64_bytes`, `format_i64`) avoid
  `std::str`/`format!` overhead on the hottest string-command paths.
- **Resize is still monolithic**: `RudisFlatTable::resize` still rehashes the entire table
  into a fresh allocation at 7/8 load factor — the segmented/incremental resize from the
  original design's roadmap was never built.
- **Small-form promotions trade O(n) linear scans for cache-friendly `Vec` access** below
  their thresholds — real for `Hash`/`Set`/`ZSet`, applied inconsistently (Hash's threshold
  is live-configurable; Set's and ZSet's are compile-time constants).
- **`used_memory` is an estimate, not exact accounting** — it's derived from
  `RudisValue::approx_bytes()` (a fixed per-variant heuristic, e.g. `+16`/`+32` bytes of
  assumed overhead per element) plus a flat `+64` bytes per entry, not a precise allocator
  measurement.

---

---

## Component 06: Blocking Operations & The Reactive Event Hub

> **Source Files**: `src/block.rs` | **Internal Reference**: [`docs/internal/06_blocking_hub.md`](../internal/06_blocking_hub.md)


---

### 1. Architectural Purpose & Scope

`src/block.rs` implements Rudis's waiter registration and wakeup engine (**`BlockHub`**). It
powers the blocking list/zset/stream commands — `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPOP`-style
moves, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, and `XREAD ... BLOCK` — plus `CLIENT UNBLOCK` and the
blocked-flag reported by `CLIENT LIST`/`CLIENT INFO`. Unlike the rest of Rudis, `BlockHub` is
**not** thread-local: it is one process-wide, mutex-guarded structure per listening port, shared
by every shard thread serving that port.

---

### 2. Key Invariants & Concurrency Constraints

1. **The one deliberate exception to "zero locks."** `BlockHub` lives behind a real
   `std::sync::Mutex`, reachable from any shard via `get_block_hub_for_port(port)`:
   ```rust
   pub static PORT_BLOCK_HUBS: LazyLock<Mutex<HashMap<u16, Arc<Mutex<BlockHub>>>>> =
       LazyLock::new(|| Mutex::new(HashMap::new()));

   pub fn get_block_hub_for_port(port: u16) -> Arc<Mutex<BlockHub>> {
       let mut map = PORT_BLOCK_HUBS.lock().unwrap();
       map.entry(port)
           .or_insert_with(|| Arc::new(Mutex::new(BlockHub::new(port))))
           .clone()
   }
   ```
   Every caller does `get_block_hub_for_port(port).lock().unwrap()` around a short, synchronous
   critical section (register a waiter, or walk a key's waiter queue and pop values). This one
   `Mutex` is the price paid for cross-shard wakeups: a client blocked on shard A must be
   wakeable by a write executed on shard B, which the thread-local `Rc<RefCell<ShardDb>>` model
   can't do on its own.
2. **Reactor threads never sleep on a plain timer.** A blocked command doesn't `sleep()` the
   whole event loop — it registers a waiter (a `flume::Sender`), yields by `.await`ing a
   polling helper (`wait_for_blocked_result`, §4.2) that also has to actively watch for client
   disconnect, since a channel receive alone can't detect a dead TCP socket (see §4.2).
3. **FIFO fairness per key.** Waiter queues are `VecDeque`, and `notify_*` always
   `pop_front()`s — the client that has been waiting longest on a key is served first.
4. **Guaranteed cleanup via RAII.** `BlockedClientGuard`'s `Drop` impl calls
   `hub.unregister_blocked_client(client_id)` unconditionally, so a waiter registration can
   never outlive the `.await` that registered it — whether it resolved by pop, by timeout, by
   `CLIENT UNBLOCK`, or by the connection task itself being dropped.
5. **Transaction-aware deferral, not `CLIENT PAUSE`.** `BlockHub::pause()`/`resume()` exist —
   but they're wired to `MULTI`/`EXEC`, not to Redis's `CLIENT PAUSE` command (which is a
   complete no-op stub in Rudis, see §4.4).

---

---

### 3. Performance Characteristics

- **Not zero-overhead while blocked**: unlike a pure channel-based design, each blocked client
  costs a wakeup-and-poll cycle at most every 20ms (`wait_for_blocked_result`'s cap) purely to
  detect disconnection via `libc::poll`, in addition to being woken immediately (no polling
  delay) whenever a real `notify_list`/`notify_zset`/`notify_stream` fires.
- **One global mutex per port, held briefly**: every register/notify/unblock operation takes
  `PORT_BLOCK_HUBS`'s per-port `Mutex<BlockHub>` for a short, synchronous, non-`.await`-ing
  critical section (no lock is ever held across an `.await` point) — contention scales with how
  many shards are simultaneously registering or notifying blocking waiters, not with the number
  of ordinary (non-blocking) commands, which never touch this lock at all.
- **Transaction-batched wakeups**: the `pause`/`resume` mechanism (§4.4) turns what could be up
  to one wakeup attempt per write inside a large `MULTI`/`EXEC` into a single deferred batch
  processed once, after the transaction (and any cross-shard lock release) fully completes.

---

---

## Component 07: NVMe SSD Tiered Storage Engine

> **Source Files**: `src/tiering.rs` | **Internal Reference**: [`docs/internal/07_nvme_tiering.md`](../internal/07_nvme_tiering.md)


---

### 1. Architectural Purpose & Scope

`src/tiering.rs` implements Rudis's per-shard NVMe/disk offload engine. Each shard owns one
private tiered-storage file (`tier_shard_{id}.db` under a configured directory) and one
`ShardTierManager` that packs small values into 4KB pages (`SmallBins`), writes larger values
as their own aligned blocks, and lets `RudisTable` (`src/table.rs`) replace a hot in-RAM value
with a small pointer (`TieredPointer`) once it has been written to disk. Orchestration (when to
spill, when to reload, the auto-tiering trigger) lives in `src/router.rs`, not here — this file
is the disk I/O and page-packing layer underneath it.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Thread-local, `Rc`-based, not `Arc`/`Mutex`**: `ShardTierManager` holds `file: Rc<monoio::fs::File>` and uses `RefCell`/`Cell` internally — it is only ever used by the one shard that owns it, consistent with the rest of the shared-nothing architecture. Cross-shard tiering requests go through `ShardMessage::Tier*` variants (Component 04), not by sharing a `ShardTierManager` across threads.
2. **`O_DIRECT` is opt-in and falls back automatically.** `ShardTierManager::open` only attempts `O_DIRECT` if the `RUDIS_DIRECT_IO` environment variable is set to something other than `"0"`; if the `O_DIRECT` open call fails (common on filesystems/kernels that don't support it), it silently retries with a normal buffered open. There is no page-cache-bypass guarantee unless that env var is set *and* the underlying filesystem actually supports it.
3. **Global, per-port shared statistics behind a lock.** `TieringStats` (23 atomic counters) is stored in a process-wide `static TIER_STATS: RwLock<Option<HashMap<u16, Arc<TieringStats>>>>`, one entry per listening port, shared by every shard on that port. This is a real (small, read-mostly) synchronization point outside the shared-nothing data path, used purely for reporting (`INFO`-style stats), not for coordinating storage itself.
4. **Read coalescing, not hole punching, is the concurrency-sensitive part.** `OpManager::read_page_coalesced` ensures that if two in-flight reads target the same 4KB page, only one physical `read_exact_at` happens; the second caller waits on a `flume::bounded(1)` channel fed by the first.
5. **Write backpressure via a byte counter, not a queue depth.** `OpManager::check_write_backpressure` returns `true` once `pending_stash_bytes` (tracked via `AtomicUsize`, incremented in `start_pending_stash`/decremented in `finish_pending_stash`/`cancel_pending_stash`) exceeds a hardcoded 16MB; `ShardTierManager::stash_record` refuses new stashes (`io::ErrorKind::WouldBlock`) while over that limit.

---

---

### 3. Performance Characteristics

- **`O_DIRECT` is conditional, not guaranteed** (§2.2) — actual page-cache-bypass behavior
  depends on `RUDIS_DIRECT_IO` being set and the filesystem/kernel actually honoring the flag;
  silently falls back to normal buffered I/O otherwise.
- **Read coalescing collapses concurrent hot-page reads** to one physical read plus N
  in-memory channel deliveries, avoiding redundant disk I/O when several keys on the same
  4KB SmallBin page are accessed close together.
- **Write backpressure is a simple byte-budget gate** (16MB of in-flight stash data), not a
  queue-depth or per-key limit — a burst of large concurrent spills can hit it and get
  `WouldBlock` back to the caller.
- **GC reclaims whole 4KB pages, not individual records** — a page with even one surviving
  record can't be punched; deletion-heavy small-value workloads can accumulate dead-but-unfreed
  bytes (`dead_bytes` stat) until every record sharing a page happens to be deleted.
- **Snapshotting cost depends entirely on filesystem reflink support** — instant on
  btrfs/XFS-with-reflink, an in-kernel `copy_file_range` loop otherwise (still avoiding a
  full userspace read+write round trip), and only falls all the way back to `std::fs::copy`
  if both kernel-assisted paths are unavailable.

---

---

## Component 08: Vector Search Engine: HNSW, SQ8 & PQ

> **Source Files**: `src/vector.rs` | **Internal Reference**: [`docs/internal/08_vector_engine.md`](../internal/08_vector_engine.md)


---

### 1. Architectural Purpose & Scope

`src/vector.rs` implements an in-memory approximate nearest-neighbor (ANN) vector index:
a **Hierarchical Navigable Small World (HNSW)** graph (`HnswIndex`), an **8-bit scalar
quantization** scheme (`QuantizedVector`), and a **Product Quantization with Asymmetric
Distance Computation** scheme (`ProductQuantizer`/`PQVector`). It is exposed to clients
through five bespoke commands parsed in `src/resp.rs` and dispatched in `src/connection.rs`:
`VADD`, `VQUERY`, `VSIM`, `VDEL`, `VINFO`. There is no `FT.SEARCH ... KNN` integration —
that syntax does not exist anywhere in this codebase; full-text search (`src/search.rs`,
Component 09) is a separate engine with no code-level link to this one.

**Each shard owns a completely independent set of named indexes** (`ShardDb.vector_indexes:
HashMap<String, HnswIndex>`), and every vector command only ever touches
`router.local_db` — there is no cross-shard routing for `VADD`/`VQUERY`/`VSIM`/`VDEL`/`VINFO`
at all (confirmed: none of the five appear in `target_shard_of_cmd`, and none of `Router`'s
methods reference the vector engine). This means an index named `"products"` on shard 0 and
an index named `"products"` on shard 1 are two entirely separate, unrelated HNSW graphs —
which shard a given connection lands on (decided by the kernel via `SO_REUSEPORT`, per
Component 01) silently determines which index a `VADD`/`VQUERY` actually reads or writes.
There is no fan-out, no merge, and no consistency check across shards. Treat this as the
single most important operational caveat for this subsystem.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Thread-local, not cross-shard**: consistent with the rest of the codebase, `HnswIndex`
   instances live inside one shard's `ShardDb` with no locks — but unlike the key-value
   store, there is no `ShardMessage` variant to reach a vector index on another shard at all
   (see §1). This isn't a locking decision, it's simply unimplemented cross-shard support.
2. **Runtime AVX2 detection, x86_64 only**: `dot_product`/`l2_distance_sq` check
   `is_x86_feature_detected!("avx2")`/`"fma"` at call time and fall back to a portable
   8-lane-unrolled scalar implementation otherwise. There is **no ARM/NEON code path** —
   only `#[cfg(target_arch = "x86_64")]` SIMD kernels exist; any other architecture always
   takes the portable path.
3. **All three metrics return a "smaller is closer" distance, not a raw similarity score**:
   `VectorMetric::IP` (inner product) returns `-dot_product(a, b)` specifically so that, like
   `L2` and `Cosine`, a smaller returned value always means "more similar" — letting
   `search_layer`'s single min/max-heap logic work identically regardless of metric.
4. **Product Quantization codebooks are not trained on data.** `ProductQuantizer::new`
   generates each subvector's 256 centroids deterministically: centroid 0 is the zero
   vector, centroids `1..=d_sub` are positive unit basis vectors, `d_sub+1..=2*d_sub` are
   negative unit basis vectors, and the remainder are filled by a fixed SplitMix64-style
   hash of `(centroid_id, subvector_id, dim_id)` mapped into `[-1, 1]`. There is no k-means
   or any training pass over real vectors — every `ProductQuantizer` for a given `(dim, m)`
   produces byte-for-byte identical codebooks. Real PQ implementations cluster the actual
   data distribution; this one does not, which will cost recall accordingly.
5. **HNSW layer assignment is deterministic across index instances.** `HnswIndex::new`
   seeds a custom xorshift64 PRNG (`rng_state`) with the fixed constant
   `0x853c49e6748fea9b` every time — not from OS randomness, the clock, or the index name.
   Two indexes built by inserting the same vectors in the same order will have identical
   graph topology.

---

---

### 3. Performance Characteristics

- Distance kernels are genuinely AVX2+FMA accelerated at 16 floats/iteration when the CPU
  supports it, with a correct portable fallback otherwise — no unconditional `unsafe` on
  unsupported hardware.
- SQ8 scoring reuses the same AVX2 kernels against `u8` data, so approximate scoring during
  graph traversal is not meaningfully slower per-comparison than exact float scoring.
- No numbers in this document are benchmarked — the previous version's "\>3,500 vectors/sec",
  "\<400µs p99", and "75% RAM reduction" figures were unsourced and have been removed rather
  than repeated unverified. SQ8's memory reduction ratio (4 bytes/dim -> 1 byte/dim, i.e. 4x
  smaller for the quantized copy, kept *alongside* the original `Vec<f32>` per §3's note) is
  the one ratio derivable directly from the type definitions, not from measurement.

---

---

## Component 09: RediSearch Full-Text Engine & Reciprocal Rank Fusion

> **Source Files**: `src/search.rs` | **Internal Reference**: [`docs/internal/09_redisearch.md`](../internal/09_redisearch.md)


---

### 1. Architectural Purpose & Scope

`src/search.rs` provides an in-memory full-text search and indexing engine compatible with a
subset of RediSearch (`FT.CREATE`, `FT.SEARCH`, `FT.INFO`, `FT.DROPINDEX`, `FT.EXPLAIN`,
`FT.ADD`). It supports multi-field schema definitions (`TEXT`, `TAG`, `NUMERIC`, `VECTOR`), a
hand-written inverted-index posting-list structure, real Okapi BM25 relevance scoring, a small
RediSearch-like query-string parser (`parse_query`/`QueryAst`), and Reciprocal Rank Fusion for
merging two ranked result lists. Auto-indexing is wired into `HSET`/`HMSET`/`JSON.SET` (root
path only) in `src/connection.rs`.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Automatic Document Ingestion, but only for `HSET`/`HMSET`/`JSON.SET`**: every `HSET`,
   `HMSET`, and `JSON.SET key $ ...` call in `connection.rs` calls
   `crate::search::index_document_hook(key, str_fields)` after the write succeeds, converting
   whatever field values it has into `HashMap<String, String>` and re-indexing that document
   against every index whose prefix matches. Other write paths (`SET`, `LPUSH`, ...) do **not**
   trigger re-indexing.
2. **The index registry is a real, global, cross-shard-shared data structure — not
   thread-local.** Despite this looking like a per-shard subsystem, `SEARCH_INDICES` is a
   single process-wide `static LazyLock<RwLock<HashMap<String, Arc<RwLock<InvertedIndex>>>>>`.
   Every shard thread that handles an `HSET`/`FT.SEARCH`/etc. call takes a real
   `std::sync::RwLock` read or write lock on it. This is a genuine, deliberate exception to
   the shared-nothing/zero-lock architecture — the same category of exception as `BlockHub`
   (Component 06) — needed because a search index has to see writes from every shard, not just
   the one that happens to own a given key's slot.
3. **Deterministic Tokenization with a real (small) stop-word list and a heuristic stemmer**:
   `tokenize_text` splits on non-alphanumeric characters, lowercases, drops any token in the
   ~170-word `ENGLISH_STOP_WORDS` set, and optionally passes survivors through `simple_stem` — a
   handful of suffix-stripping rules (`-ing`, `-ies`→`-y`, `-es`, `-ed`, trailing `-s`), not a
   real Porter/Snowball stemmer.
4. **Real BM25, but with a single whole-document length, not one length per field.** `DocMeta`
   stores one `doc_len: usize` — the total token count across *all* indexed text fields of a
   document combined — and `InvertedIndex::avg_doc_len()` is `total_terms / total_docs` across
   the whole index, not per field. This is architecturally simpler than genuine multi-field
   BM25F (which needs per-field lengths/weights) despite `FieldType::Text` carrying a `weight`
   field — that `weight` is accepted by `FT.CREATE`'s parser but **`bm25_score` never reads
   it** (verified: no reference to any field's `weight` anywhere in the scoring function).

---

---

### 3. Performance Characteristics

- **Global `RwLock` contention, not per-shard isolation** (§2.2): every indexed write and every
  `FT.SEARCH` call takes a real lock on the process-wide index (a write lock for indexing, a
  read lock for search) — under concurrent writers across many shards to prefixed keys, this is
  a real, shared contention point unlike the rest of the storage engine.
- **`And`/`Or`/`Not` evaluation re-runs `execute_search` recursively per sub-clause** with
  `limit: usize::MAX`, materializing a full intermediate `Vec<SearchHit>`/`HashMap` at every AST
  node rather than streaming or short-circuiting — fine for the small corpora this has been
  exercised against, not optimized for deep or wide boolean queries.
- **No compression**: posting lists are plain `Vec<Posting>` (doc_id `String` + `u32` term
  frequency + `Vec<u32>` positions per entry) — no delta-encoding, no compression, and the
  tracked term `positions` are never actually read by anything (no phrase-query support uses
  them).

---

---

## Component 10: Kernel Bypass & Zero-Copy Networking

> **Source Files**: `src/xdp.rs, src/zerocopy.rs` | **Internal Reference**: [`docs/internal/10_kernel_bypass_xdp.md`](../internal/10_kernel_bypass_xdp.md)


---

### 1. Architectural Purpose & Scope

This component is two independent, mostly-unconnected pieces of code, neither of which does
what its name and the previous version of this document claimed:

1. **`src/xdp.rs`**: **Not real AF_XDP/eBPF kernel bypass.** There is no `aya`/`libbpf`/`xsk`
   dependency in `Cargo.toml`, no `bpf()` syscall, no raw socket, no UMEM ring buffers, and no
   attachment of any program to a NIC driver. What actually exists is a pure-userspace
   `XdpEngine`: a CIDR-based allow/drop/redirect rule table plus a per-source-IP token-bucket
   rate limiter, driven entirely by a Redis command (`XDP.PACKET <payload>`) that lets a client
   hand it a raw byte buffer to run through the simulated pipeline. It never touches real
   inbound network traffic.
2. **`src/zerocopy.rs`**: **Real Linux zero-copy syscalls, but entirely disconnected from the
   live request path.** `SO_ZEROCOPY`/`MSG_ZEROCOPY` usage here is genuine and correctly
   implemented (real `libc` FFI, real `ENOBUFS` fallback handling), and there's a real
   page-aligned `RegisteredBufferPool` with `io_uring`-crate-compatible `iovec`s. But grepping
   the entire codebase shows **zero call sites** for any of it outside this file's own unit
   tests — `server.rs`/`connection.rs`/`main.rs` never construct a `ZeroCopyEngine` or call
   `send_zc`. The actual connection path (`connection.rs`, via `monoio`'s `io_uring` driver)
   never uses this code.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **`XdpEngine` is a single global, not per-shard**: `get_xdp_engine()` returns a clone of an
   `Arc<XdpEngine>` behind a process-wide `static GLOBAL_XDP_ENGINE: LazyLock<Arc<XdpEngine>>`
   — every shard thread that calls `XDP.*` commands shares the exact same instance, coordinated
   via `RwLock<Vec<XdpRule>>` and `RwLock<HashMap<u32, TokenBucket>>` (a real, if narrow,
   exception to the shared-nothing model, similar in shape to `BlockHub` in Component 06).
2. **`XdpMode` is cosmetic, not functional**: the global engine picks `XdpMode::Skb` if
   `/sys/class/net` exists on the host, else `XdpMode::Simulated` — this only changes what
   `XDP.INFO` reports as a string; it does not change `process_packet`'s behavior or attach
   anything to a real interface in either mode.
3. **`RegisteredBufferPool` owns raw allocated memory directly** (`alloc_zeroed`/`dealloc` via
   `std::alloc`, not a `Vec`), page-aligned via `Layout::from_size_align(total_size, PAGE_SIZE)`,
   with a manual `unsafe impl Send + Sync` — correct in isolation, but again: nothing in the
   codebase actually constructs one outside its own test.
4. **`ZeroCopyEngine::send_zc` degrades gracefully**: only applies `MSG_ZEROCOPY` for payloads
   `>= PAGE_SIZE` (4KB, per a comment citing page-locking overhead for smaller sends), and on
   `ENOBUFS` (kernel zero-copy completion queue full) or general zero-copy failure, retries once
   with a plain blocking `send()` — this fallback logic is real and correct, it's just never
   invoked by anything.

---

---

### 3. Performance Characteristics

- **No measured network-layer performance benefit exists from either file.** `xdp.rs`'s cost is
  whatever it costs to run `process_packet` once per `XDP.PACKET` command a client explicitly
  sends — i.e., it's exercised at whatever rate a test or admin script chooses to call it, not
  at line rate against real traffic. `zerocopy.rs` is entirely inert in the running server.
- **The token-bucket rate limiter and CIDR rule table are real, correct, O(1)-per-packet
  (rules are a linear scan, but the rule list is expected to be small) userspace logic** — useful
  as a testable filter-policy engine, just not connected to anything that would make it a DDoS
  defense in practice.
- Any performance claims in the previous version of this document (100GbE line-rate, "28 million
  packets/sec", "&lt;5% CPU utilization" for `MSG_ZEROCOPY`) were invented and have been removed;
  none of it has ever been benchmarked because none of it runs on the real request path.

---

---

## Component 11: Redis Cluster Topology & Gossip Protocol

> **Source Files**: `src/cluster.rs` | **Internal Reference**: [`docs/internal/11_cluster_topology.md`](../internal/11_cluster_topology.md)


---

### 1. Architectural Purpose & Scope

`src/cluster.rs` implements a simplified Redis Cluster control plane: per-node slot
ownership tracked as `(start, end)` ranges (not a real Redis Cluster deployment's
16,384-bit bitmask), a plain-text line-oriented gossip protocol between nodes on
`port + 10000`, unilateral (non-consensus) failure detection based on ping/pong
staleness, and a real majority-vote replica election for failover. It is a single
process-wide singleton per listening port (`get_cluster_hub(port)`), and only the
shard-0 worker thread ever starts the cluster-bus listener for that port
(`start_cluster_bus`, called from `run_shard_worker` — see Component 01).

---

---

### 2. Key Invariants & Concurrency Constraints

1. **One `ClusterHub` per port, shared via a global registry**: `CLUSTER_HUBS:
   LazyLock<RwLock<HashMap<u16, Arc<ClusterHub>>>>`. `get_cluster_hub(port)`
   lazily creates and caches one `Arc<ClusterHub>` per port — this is process-wide
   shared, mutex/rwlock-guarded state, not thread-local (a deliberate, narrow
   exception to the shared-nothing model, same category as `BlockHub` in
   Component 06).
2. **Plain-text wire protocol, not binary framing**: every cluster-bus message
   (`MEET`, `PING`, `FAIL`, `FAILOVER`, `FAILOVER_AUTH_REQUEST`,
   `FAILOVER_ANNOUNCE`) is a `\r\n`-terminated space-separated ASCII line, parsed
   with `split_whitespace()`. There is no binary struct, no magic-byte signature,
   no `#[repr(C, packed)]` header of any kind.
3. **Synchronous blocking I/O on dedicated OS threads, not `io_uring`/`monoio`**:
   the cluster bus listener runs on its own `std::thread`, using plain
   `std::net::TcpStream`/`TcpListener` with short (200-500ms) read/write
   timeouts — completely separate from the rest of Rudis's async, io_uring-based
   networking. Each inbound connection also gets its own `std::thread::spawn`.
4. **Quorum-based failure detection with distributed gossip corroboration**: a peer is
   locally marked `"fail?"` (PFAIL) after 5s of missed PONGs. That opinion is piggybacked
   in gossip payloads to peer nodes, which record it in `pfail_reports: HashMap<String, HashSet<String>>`.
   Escalation to confirmed `"fail"` requires corroborating PFAIL votes from a strict majority
   of masters (`total_votes >= quorum`), at which point a `FAIL <node_id>` broadcast is sent to
   notify all peers. If connectivity recovers, PFAIL opinions are retracted via gossip.
5. **Replica election *is* a real majority vote**: `start_election` does send
   `FAILOVER_AUTH_REQUEST` to every known master and only promotes itself after
   collecting `>= (total_masters / 2) + 1` `FAILOVER_AUTH_ACK` replies, gated by
   `last_vote_epoch` (one vote per epoch per master) — this part matches the
   Architectural Purpose's claim, unlike the failure-detection consensus.

---

---

### 3. Performance Characteristics

- **Not zero-allocation, not io_uring-based**: every gossip tick and every
  `CLUSTER MEET`/`FAILOVER` opens a brand-new blocking `TcpStream` per peer
  (connect + write + read, each with its own 200-500ms timeout) on a plain OS
  thread — the opposite of the rest of Rudis's zero-copy/`monoio` design.
  Acceptable for a control-plane path that runs a few times a second, not
  something to model the data-path invariants on.
- **Full-state gossip, not incremental**: `cluster_bus_tick` re-sends the entire
  known node table to every peer on every 500ms tick — bandwidth is O(peers²)
  per tick, not the randomized-sample gossip real Redis Cluster uses. Fine at
  small cluster sizes (a handful of nodes), not validated or designed for
  hundreds of nodes despite what an earlier draft of this doc claimed about
  "100-node cluster convergence."
- **Failure detection is local and synchronous, not a distributed vote** (§2.4)
  — a node can mark a peer `"fail"` purely from its own missed-PONG timer, with
  no corroboration from other nodes, unlike the real replica-election step which
  does require a genuine majority.

---

---

## Component 12: CRDT Data Types & Manual Multi-Region Sync

> **Source Files**: `src/crdt.rs` | **Internal Reference**: [`docs/internal/12_crdt_types.md`](../internal/12_crdt_types.md)


---

### 1. Architectural Purpose & Scope

`src/crdt.rs` (645 lines) implements a small, self-contained library of three
**Conflict-Free Replicated Data Types (CRDTs)** — a Last-Write-Wins Register, an
Observed-Remove Set, and a Positive-Negative Counter — each ordered by a **Hybrid Logical
Clock (HLC)**, plus a binary export/import format for merging one instance's CRDT state
into another's.

**What this is not**: there is no automatic cross-region network replication. There is no
peer/region configuration anywhere in `main.rs`, no background sync task, and no wiring
into `src/replication.rs` (which handles the unrelated primary/replica `PSYNC` stream).
`CRDT.MERGE` takes its payload as a plain command argument (`Command::CrdtMerge(Bytes)`),
which means getting CRDT state from one Rudis instance to another is entirely
operator/client-driven: read it out with `CRDT.DUMP`, transport those bytes yourself
(script, sidecar, whatever), and feed them into the target instance with `CRDT.MERGE
<payload>`. The "multi-region" framing in this file's doc comments describes the
data types' *convergence properties*, not a built network protocol.

**Update — the routing gap below is now fixed.** An earlier version of this document found
that every `Command::Crdt*` handler called `router.local_db.borrow_mut().crdt_*(...)`
directly, bypassing normal key-based routing entirely, so the same key name could hold
completely independent state on different shards. As of the current source, the single-key
CRDT commands (`CrdtSet`/`CrdtGet`/`CrdtDel`/`CrdtIncrby`/`CrdtSadd`/`CrdtSmembers`/
`CrdtSrem`) are now included in both `cmd_primary_key` and `target_shard_of_cmd`
(`connection.rs`) and dispatch through the same local-vs-`execute_remote` fork every other
keyed command uses — a `CRDT.SET foo bar` now always lands on the one shard `foo` actually
hashes to, regardless of which shard's connection issued it. Separately, `CRDT.DUMP`,
`CRDT.MERGE`, and `CRDT.GC` — which operate on an entire store, not one key — now
explicitly fan out to *every* shard (`for sid in 0..router.num_shards { ... }`, via
`router.execute_remote`) and aggregate the results: `CrdtDump` concatenates every shard's
exported payload into one response, `CrdtMerge` sums the per-shard merged-item counts, and
`CrdtGc` sums the per-shard tombstones-pruned counts. In effect, `CrdtStore` is still a
genuinely separate `CrdtStore` instance per shard (the underlying data structure hasn't
changed — see §3), but the command layer now presents it as one logical whole-node store:
single-key operations are correctly routed to the one shard that owns the key, and
whole-store operations correctly touch every shard rather than just the connection's local
one.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Deterministic Convergence (real, and tested)**: `LwwRegister::merge`, `OrSet::merge`,
   and `PnCounter::merge` are each commutative/idempotent by construction (see §4) — the
   file's own `#[cfg(test)]` module (`test_lww_register_convergence`,
   `test_pn_counter_convergence`, `test_orset_add_wins`) exercises exactly this.
2. **HLC via lock-free CAS, not a mutex**: `HybridLogicalClock` stores
   `latest_physical_ms: AtomicU64` / `latest_logical: AtomicU32` and advances them with a
   compare-exchange retry loop (§4.1) — real lock-free code, not a fabrication.
3. **Add-Wins semantics for `OrSet`**: a concurrent add and remove of the same element
   resolve in favor of the add, because `remove` only tombstones the specific add-tags
   (`HlcTimestamp`s) it has *observed so far* — a later add carries a fresh tag the remove
   never saw, so it survives merge. Verified by `test_orset_add_wins`.
4. **No consensus, because there's no network layer to reach consensus over**: with sync
   entirely manual (§1), there's no Paxos/Raft and also no automatic conflict detection —
   whoever runs `CRDT.MERGE` decides when and with what payload merging happens.

---

---

### 3. Performance Characteristics

- **Lock-free clock advancement**: `HybridLogicalClock::now`/`update` use CAS retry loops,
  not a mutex — cheap even under contention from multiple connections on the same shard.
- **Export is O(total CRDT state size) and single-threaded**: `export_sync_payload` builds
  one `Vec<u8>` for the *entire* store in one call; there's no incremental/delta export —
  every `CRDT.DUMP` re-serializes everything currently held.
- **No network cost inside Rudis**: since sync is manual (§1), there's no WAN traffic,
  retry logic, or delta-batching to account for here at all — that cost (if any) lives
  entirely in whatever external process actually transports the dump/merge payloads.

---

---

## Component 13: Lua Scripting & Redis 7 Functions Engine

> **Source Files**: `src/scripting.rs` | **Internal Reference**: [`docs/internal/13_scripting_functions.md`](../internal/13_scripting_functions.md)


---

### 1. Architectural Purpose & Scope

`src/scripting.rs` embeds Lua via `mlua` (`lua54`, vendored) to run `EVAL`/`EVALSHA`/`SCRIPT
LOAD`/`SCRIPT EXISTS`/`SCRIPT FLUSH` and Redis 7 Functions (`FUNCTION LOAD`, `FCALL`,
`FUNCTION LIST`, `FUNCTION DELETE`, `FUNCTION FLUSH`). There is no persistent `ScriptEngine`
struct — every `EVAL`/`EVALSHA`/`FCALL` call creates a **brand-new `mlua::Lua` instance**,
runs once, and drops it. Script *source* is cached (by SHA1, and by function-library name);
compiled bytecode and the Lua VM itself are not.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Fresh interpreter per call, no persistent VM or bytecode cache.** `eval_script` and
   `call_function` both call `Lua::new()` at the top and let it drop at the end of the
   function. There is nothing analogous to the old doc's `ScriptEngine`/`script_cache:
   HashMap<String, Vec<u8>>` (compiled bytecode) — what's cached is the **script's source
   text**, keyed by its SHA1 hex digest, in a global `RwLock<HashMap<String, String>>`.
2. **No sandboxing.** Grepping the file for `io`/`os`/`debug`/`package` global stripping
   finds nothing — `mlua::Lua::new()` is used unmodified, so a script has full access to
   whatever the default Lua 5.4 standard library exposes (the old doc's "Security
   Sandboxing" invariant does not exist in the real code).
3. **Deterministic execution w.r.t. the shard, by construction, not by an explicit lock.**
   Because `redis.call`/`redis.pcall` synchronously call `crate::connection::
   execute_local_command` against the same `Rc<RefCell<ShardDb>>` the calling connection
   task already holds, and Rudis's per-core execution model means nothing else touches
   that `RefCell` concurrently, a script's commands execute atomically relative to other
   traffic on the shard for free — not because scripting.rs added any synchronization of
   its own.
4. **Global, cross-shard-visible caches.** Both `SCRIPT_CACHE` (source by SHA1) and
   `FUNCTION_LIBS` (function libraries) are `static LazyLock<RwLock<HashMap<...>>>` —
   process-wide, not per-shard. A script loaded via `SCRIPT LOAD`/`FUNCTION LOAD` on one
   shard's connection is immediately visible to `EVALSHA`/`FCALL` calls arriving on any
   other shard, because it's the same global map — this is a deliberate (if easy-to-miss)
   departure from the rest of the codebase's thread-local, shared-nothing design.

---

---

### 3. Performance Characteristics

- **No bytecode caching, despite the SHA1 cache's name.** `SCRIPT_CACHE` only saves
  re-transmission of the script *text* for `EVALSHA`; Lua source is re-parsed by `mlua` on
  every single `EVAL`/`EVALSHA` call, and a fresh `Lua::new()` VM is constructed and torn down
  per call — there is no persistent interpreter or precompiled-chunk reuse the old doc's
  "Bytecode Caching" section claimed.
- **In-process, zero-IPC command execution**: real and accurate from the old doc — `redis.call`
  invokes `execute_local_command` directly against the shard's own `Rc<RefCell<ShardDb>>`,
  with no network or channel hop, since scripts only ever run against the local shard.
- **Process-wide global locks on every script/function load or lookup**: `SCRIPT_CACHE`/
  `FUNCTION_LIBS` are `RwLock`s taken on every `EVAL` (write lock, unconditionally, via
  `load_script`), `EVALSHA` (read lock), and `FCALL` (read lock) — a real (if narrow and
  presumably low-contention) departure from the rest of the codebase's lock-free, thread-local
  design, shared with the `BlockHub` exception documented in Component 06/01.

---

---

## Component 14: Persistence & Replication Engines

> **Source Files**: `src/replication.rs, src/aof.rs` | **Internal Reference**: [`docs/internal/14_persistence_replication.md`](../internal/14_persistence_replication.md)


---

### 1. Architectural Purpose & Scope

This subsystem covers two related but independent durability mechanisms:

1. **Append-Only File (AOF) Engine (`src/aof.rs`)**: converts mutating `Command`s back into
   RESP bytes (`command_to_resp`) and appends them to a per-shard file via a buffered,
   periodically-flushed `AofWriter`. On restart, `replay_aof` re-parses the file and replays
   every command through the normal command-execution path.
2. **Replication Hub (`src/replication.rs`)**: a per-port, process-wide `ReplicationHub`
   (master or slave role) that fans out every mutating command to connected replicas over
   plain `flume` channels. The master side now supports **real partial resync** (`+CONTINUE`
   from the backlog) when a reconnecting client presents a valid replid+offset, falling back
   to a full RDB snapshot otherwise — see §4.3 for the update to this (this doc previously,
   correctly, documented this as entirely unimplemented; it has since been built).

**Update**: AOF rewrite/compaction via `BGREWRITEAOF` is fully implemented across shards
(§4.1), snapshotting non-expired table entries, JSON documents, sets, lists, hashes, streams,
and preserving TTLs, with atomic temp-file rename and live reopen on active writers. Partial
resynchronization is real on both the **master** and **replica** sides
(§4.3) — `run_replica_worker` tracks its `master_replid` and `master_repl_offset`,
reconnects automatically with `PSYNC <replid> <offset>`, and applies `+CONTINUE` diffs
without full RDB snapshots.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **AOF compaction via `BGREWRITEAOF` is supported.** `AofWriter::append` grows `self.buffer`
   (later flushed to disk via `write_all_at` at the current end-of-file `offset`). Periodic
   or explicit `BGREWRITEAOF` snapshots memory state to a temporary file, syncs it, atomically
   renames it to replace the AOF file, and reopens `AofWriter` on the new file at the new offset.
2. **Partial resync is supported on both master and replica sides.**
   `run_master_replica_stream` (`src/connection.rs`) inspects the `PSYNC` command's
   replid/offset arguments via `ReplicationHub::try_partial_resync`, and replies `+CONTINUE <replid>\r\n<backlog-diff-bytes>`
   when the requested offset falls inside the retained backlog window for a matching
   replid (or `replid2`), falling back to `+FULLRESYNC <replid> <offset>\r\n$<len>\r\n<rdb-bytes>` otherwise
   (§4.3). `run_replica_worker` tracks its `master_repl_offset` and `master_replid`, sending
   `PSYNC <cached_replid> <cached_offset>` on reconnects, receiving `+CONTINUE`, and executing
   the backlog diff commands without requesting a full RDB snapshot.
3. **The replication backlog is maintained, and now genuinely read back — by the master
   serving a partial resync.** `ReplicationHub::propagate` still appends every propagated
   command to `self.backlog` (a `ReplicationBacklog`), and that data is no longer write-only:
   `try_partial_resync`/`ReplicationBacklog::get_diff` (§4.3) slice it to answer a
   `+CONTINUE` request. It's also still consulted for `INFO replication`'s `repl_backlog_*`
   fields, as before.
4. **Replication uses shared, cross-thread state (`Arc`/`RwLock`), unlike the request path.**
   `ReplicationHub` is looked up via `get_replication_hub(port)` from a process-wide
   `LazyLock<RwLock<HashMap<u16, Arc<ReplicationHub>>>>` — every shard on a given port shares
   the *same* hub instance, guarded by `RwLock`s and atomics throughout. This is a second,
   independent exception to the "zero locks" architecture (the first being the blocking-op
   `BlockHub` documented in Component 06), needed because replication fan-out is inherently
   cross-shard: any shard's write must reach every connected replica, not just the shard that
   handled it.
5. **AOF writing and replication propagation are decoupled but both driven from the same
   call site.** `record_mutation` (in `replication.rs`) is the single function that both
   appends to a shard's local `AofWriter` (if present) and calls `propagate_bytes` — but nothing
   requires both to be enabled together; a node can run with AOF on and no replicas, or
   replication with AOF disabled, independently.

---

---

### 3. Performance Characteristics

- **AOF write cost is O(1) amortized per command**, bounded by the 50ms/~1s flush-fsync
  cadence — but **AOF file size and restart replay time are both unbounded** relative to
  total lifetime write volume, since nothing ever compacts the file (§4.1). A long-running,
  write-heavy node with AOF enabled will have an ever-growing file and an ever-growing
  startup replay cost.
- **Full resync still re-transfers the entire dataset** via `generate_full_rdb` (fans out to
  every shard, waits for all chunks) — but this is no longer the *only* path (§4.3): a
  client presenting a still-in-backlog offset gets a `+CONTINUE` and just the missing bytes
  instead. The catch, per §4.3, is that this codebase's own replica never actually asks for
  the cheap path — a rudis-to-rudis pair still pays full-dataset-transfer cost on every
  reconnect today, even though the master is capable of doing better.
- **Replication fan-out itself is cheap per write**: `propagate` is an `O(num_replicas)`
  loop of non-blocking `flume` sends per mutating command, independent of dataset size.

---

---

## Component 15: Security, Memory Allocator & TLS

> **Source Files**: `src/acl.rs, src/allocator.rs, src/tls.rs` | **Internal Reference**: [`docs/internal/15_security_tls.md`](../internal/15_security_tls.md)


---

### 1. Architectural Purpose & Scope

> **Update note**: since this doc was last verified, a real batch of fixes landed — salted
> password hashing, real per-command/per-key ACL enforcement, and a genuinely-wired `--tls-port`
> listener. This revision re-verifies all three against the current source. Two of the three are
> improvements as advertised; the TLS wiring introduced a new, more severe problem than the
> "dead code" state it replaced — see §2.4/§4.4's ⚠️ for a real plaintext-over-the-wire bug in the
> kTLS path. Read that section before treating `--tls-port` as safe to enable.

Three unrelated system-services modules bundled under one doc:
1. **Access control (`src/acl.rs`)**: a per-port, multi-user authentication *and, as of this update, real authorization* store (`AUTH user pass`, `ACL SETUSER/GETUSER/LIST/USERS/DELUSER/WHOAMI`) modeled loosely on Redis ACL syntax. Per-command and per-key checks are now genuinely enforced — see §2.2 — though password storage still has real weaknesses, see §2.3.
2. **Jemalloc telemetry (`src/allocator.rs`)**: read-only statistics via `tikv-jemalloc-ctl`, surfaced through `INFO`'s memory section. No profiling/heap-dump capability. Unchanged by this update.
3. **TLS certificate/handshake plumbing (`src/tls.rs`)**: a real `rustls` handshake wrapper with genuine in-memory self-signed cert generation (`rcgen`), now genuinely wired to a `--tls-port` listener (§4.4) — but the `kTLS` fast-path it also wires up has a real bug that causes it to silently transmit **unencrypted** application data once activated (§2.4). This is worse than the previous "dead code" state, not better, for anyone who enables `--tls-port` on Linux with the kernel `tls` module available.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **One `AclManager` per listening port, not global**: `PORT_ACLS: LazyLock<Mutex<HashMap<u16, Arc<RwLock<AclManager>>>>>`, looked up via `get_acl_for_port(port)`. Every shard thread serving the same port shares the same `Arc<RwLock<AclManager>>` — this is, like `BlockHub` (Component 06), a deliberate exception to the shared-nothing/zero-lock architecture, needed because auth state has to be consistent across every shard's independently-accepted connections on that port.
2. **Authentication is enforced, and authorization is now real too (updated).** `execute_command` gates on `authenticated: bool` exactly as before (`-NOAUTH` for anything but `AUTH`/`HELLO`/`QUIT` while unauthenticated), but immediately after that gate it now looks up the authenticated user's `AclUser` by `auth_user` and calls two new real methods before dispatching: `can_execute_command(cmd_name)` (checks `disallowed_commands`/`allowed_commands` depending on `all_commands`, always allowing `PING`/`RESET`/`QUIT`/`AUTH`/`HELLO`) and, if the command has a primary key, `can_access_key(key)` (checks `allowed_key_patterns`, a `prefix*`/exact-match list, unless `all_keys`). A denied command gets a real `-NOPERM` reply and returns without executing (§4.1). The pipelined squash-eligibility check (`execute_commands_squashed`) also consults these two methods per command — a command an ACL would deny just falls back to the sequential path, where the real `-NOPERM` denial happens.
3. **Passwords are now hashed, but plaintext storage was not removed — this is a real gap, not full resolution.** `AclUser` gained a `password_hashes: Vec<String>` field, and `hash_password` computes `SHA1("rudis_acl_salt_v1:" + password)`. But `ACL SETUSER user >password` still pushes the plaintext into `passwords` *and* the hash into `password_hashes` (§4.2) — the plaintext field was never removed, so anyone who could previously read plaintext passwords from memory/a core dump still can. The hash itself is also weak by password-hashing standards: SHA1 is a fast general-purpose hash (not a slow KDF like Argon2/bcrypt/scrypt, so no work-factor resistance to offline brute force), and the salt (`"rudis_acl_salt_v1:"`) is a single hardcoded constant shared by every user and every deployment, not a per-user random salt — identical passwords across users or across a fleet of Rudis instances produce identical hashes, and the fixed salt is trivially precomputable into a rainbow table once known. `check_auth` accepts a match against either the plaintext or the hash (`§4.1`), so both weaknesses are live simultaneously.
4. **TLS is now genuinely wired up — and its kTLS fast-path has a real, severe bug: it silently transmits plaintext.** A `--tls-port` listener now exists (§4.4) and performs a real `rustls` handshake via `TlsSession::handshake_monoio`. After a successful handshake, it unconditionally attempts `enable_ktls`, which only calls `setsockopt(IPPROTO_TCP, TCP_ULP, "tls")` — attaching the kernel's TLS upper-layer-protocol module — and, if that syscall merely *succeeds* (which it will on any Linux host with the `tls` kernel module loadable, regardless of whether any key material was ever installed), sets `is_ktls_active = true`. Both `TlsSession::read_plaintext` and `write_plaintext` then branch on `is_ktls_active`: when true, they read/write **raw socket bytes directly, with no rustls encryption/decryption at all**, on the assumption the kernel is doing it. But the second, actually-required `setsockopt(SOL_TLS, TLS_TX/TLS_RX, ...)` call that installs the negotiated key into the kernel socket is still missing (this exact gap was already flagged when the code was dead — see §4.4) — so the kernel never encrypts anything, and **every byte of application data after the handshake goes out on the wire in cleartext**, while both client and server believe they completed a real TLS session. This is worse than TLS simply not existing: a client connecting to `--tls-port` gets a real, correctly-negotiated handshake and then a false sense of security for the rest of the connection.
5. **Allocator stats are read-only telemetry**, gated behind no lock beyond jemalloc's own internal epoch counter (`tikv_jemalloc_ctl::epoch::advance()`), safely callable from any thread without coordination with the rest of Rudis.

---

---

### 3. Performance Characteristics

- **Auth check cost**: one `RwLock::read()` acquisition plus a linear scan over `passwords`/`password_hashes` (typically 0-1 entries each) plus one SHA1 computation per `AUTH` call — negligible, and only paid once per connection lifetime in the common case.
- **Real per-command ACL overhead now exists (updated)**: every command after authentication takes an `AclManager` read-lock and a `HashMap`/`HashSet` lookup via `can_execute_command`/`can_access_key` (§4.1) — small, but no longer zero as the old doc stated; this is a real, permanent per-command cost on every connection now, not just at `AUTH` time.
- **Allocator stats are cheap but not free**: unchanged — `epoch::advance()` triggers jemalloc to refresh its internal counters, more than a simple atomic load; only invoked from `INFO`, not a hot-path command.
- **TLS has real handshake and per-byte I/O cost now that it's wired up**: the `rustls` handshake (§4.4) is genuine CPU work paid once per TLS connection; ongoing traffic either goes through real `rustls` encrypt/decrypt (the safe, intended path) or — per the §4.4 bug — bypasses encryption entirely once `is_ktls_active` is (incorrectly) set, which is *faster* than real encryption precisely because it isn't doing any. Do not read that speed as a feature.

---

---

## Component 16: JSON Document Store & JSONPath Engine

> **Source Files**: `src/json.rs` | **Internal Reference**: [`docs/internal/16_json_store.md`](../internal/16_json_store.md)


---

### 1. Architectural Purpose & Scope

`src/json.rs` implements a RedisJSON-compatible document store: a hand-written JSONPath
parser/evaluator operating directly on `serde_json::Value` trees, plus `JsonStore`, the
per-shard map of key → JSON document that backs `JSON.SET`/`GET`/`DEL`/`TYPE`/`NUMINCRBY`/
`STRAPPEND`/`STRLEN`/`ARRAPPEND`/`ARRLEN`/`ARRPOP`/`OBJKEYS`/`OBJLEN`/`TOGGLE`/`CLEAR`/`MGET`.
Unlike `src/vector.rs` (Component 08) and unlike `src/crdt.rs` before its fix (Component 12),
single-key JSON commands are **genuinely routed per-key across shards** — verified directly
in `connection.rs`: every `Command::Json*` variant (except `JsonMget`, see §4.5) appears in
the same `target_shard_of_cmd`/local-vs-`execute_remote` dispatch arm as ordinary string/hash/
list commands, so a `JSON.SET`/`GET` on a given key always lands on the one shard that key
actually hashes to, regardless of which shard's connection issued it.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **A real, but partial, JSONPath implementation.** `parse_json_path` hand-parses `$`, bare
   `.field` traversal, `[idx]` (including negative indices), `[*]` wildcards, `[start:end]`
   slices (including negative/omitted bounds), and `["quoted"]`/`['quoted']` field names. There
   is **no recursive descent (`$..field`) and no filter-expression syntax (`?(@.price < 10)`)**
   — both real RedisJSON/JSONPath features. A path using either silently fails to match
   anything (parses as a literal field name containing those characters) rather than erroring.
2. **Whole-document storage, no incremental structure.** `JsonStore.docs: HashMap<Bytes,
   Value>` stores one complete `serde_json::Value` tree per key. A `JSON.SET`/`NUMINCRBY`/etc.
   on a deeply nested path still has to parse the target's own sub-value in place (via
   `query_json_path_mut`, no full-document re-parse), but `JSON.GET` always calls
   `serde_json::to_string` fresh on whatever subtree matched — there's no cached serialized
   form, and a `JSON.GET key $` on a huge document re-serializes the entire thing every call.
3. **`query_json_path`/`query_json_path_mut` are structurally identical, hand-duplicated for
   `&`/`&mut`.** Every match arm in the immutable traversal (§4.2) has a corresponding
   `_mut` arm doing the identical navigation logic against `.get`/`.get_mut`,
   `.values()`/`.values_mut()`, `&arr[i]`/`&mut arr[i]`. This is a real, verified
   duplication (not a design choice with a stated rationale) — a bugfix to one traversal
   rule (e.g. how negative slice bounds clamp) has to be applied to both copies by hand.
4. **Auto-vivification on `SET`, not on read.** `set_json_path` creates intermediate
   `Object`/`Array` containers as needed when writing to a path whose parents don't exist yet
   (§4.3) — real Redis JSON has the same behavior. `NX`/`XX` are checked once, up front,
   against whether the *target* path already resolves to something, before any mutation.

---

---

### 3. Performance Characteristics

- **`JSON.GET` cost scales with matched-subtree size, not query specificity** — every call
  does a fresh `serde_json::to_string` of whatever `query_json_path` returned, with no
  memoization; repeatedly reading the same small field from a large sibling-heavy document is
  cheap, but repeatedly reading `$` on a large document is not.
- **Path traversal is O(document breadth) per segment, not indexed** — `Field` lookups on an
  `Object` are O(1) (backed by `serde_json`'s own map), but `Wildcard`/`Slice` segments
  necessarily visit every child at that level; there's no precomputed path index.
- **`JSON.MGET`'s sequential fan-out (§4.5) is the single biggest addressable cost** on
  multi-key JSON reads spread across shards — see Future Improvements.

---

---

## Component 17: Geospatial Commands

> **Source Files**: `src/geo.rs` | **Internal Reference**: [`docs/internal/17_geospatial.md`](../internal/17_geospatial.md)


---

### 1. Architectural Purpose & Scope

`src/geo.rs` is pure math and reply-formatting — it owns **no storage of its own**. Every
`GEOADD`/`GEODIST`/`GEOPOS`/`GEOHASH`/`GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH` command is
implemented directly in `src/connection.rs` on top of the existing sorted-set (`RudisZSet`,
Component 05) API — `GEOADD` is a `ZADD` whose "score" is a 52-bit interleaved geohash encoding
of (longitude, latitude), and every other geo command decodes that score back into
coordinates. This is architecturally identical to how real Redis implements its own `GEO*`
command family as a thin layer over `ZSET`, and it means `ZRANGE`/`ZSCORE`/any other ZSET
command works unmodified against a "geo set" key too — a real compatibility feature and a real
footgun (an arbitrary `ZADD` against a geo key can insert a member with a score that isn't a
valid geohash at all, and nothing rejects it).

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Encoding is 52-bit interleaved (26 bits longitude + 26 bits latitude), matching real
   Redis's internal encoding** — not the 5-bit-alphabet, 11-character textual geohash;
   that (`geohash_to_base32`) is only computed on demand for the `GEOHASH` command's text
   output, never used as the stored representation.
2. **Latitude is clamped to the real Web Mercator-projectable range**
   (`GEO_LAT_MIN`/`MAX` = ±85.05112878°, not ±90°) — this matches real Redis's own
   documented limitation exactly (values interleave cleanly only within this range); a
   `GEOADD` outside it is rejected with `"ERR invalid latitude"`.
3. **Distance is Haversine (great-circle on a sphere), not an ellipsoidal (Vincenty) model** —
   using `EARTH_RADIUS_METERS = 6372797.560856`, the same constant real Redis's own Haversine
   implementation uses. This is an approximation (Earth isn't a perfect sphere) but is exactly
   what real Redis does too, so behavior matches rather than diverges.
4. **Geo commands route per-key across shards exactly like ordinary ZSET commands** (verified:
   `Geoadd`/`Geodist`/`Geopos`/`Geosearch`/etc. all appear in the same `target_shard_of_cmd`
   dispatch arm as other keyed commands, Component 02 §4.5) — there is no geo-specific routing
   concern; a "geo set" is routed by the same CRC16 key-slot mechanism as any other key.

---

---

### 3. Performance Characteristics

- **`GEOADD`/`GEODIST`/`GEOPOS` are O(1)-ish**, bounded by the underlying `ZADD`/`ZSCORE` cost
  (Component 05) plus a fixed amount of bit-interleaving/Haversine math — no scan involved.
- **`GEORADIUS`/`GEORADIUSBYMEMBER`/`GEOSEARCH` are all O(N) in the size of the geo set**
  (§4.4), not O(matches) or O(log N + matches) the way a geohash-neighborhood-aware
  implementation would be — a large geo set with a small-radius search still decodes and
  distance-checks every member.
- **No caching of decoded coordinates** — every scan re-runs `decode_geohash` (cheap bit
  extraction) per member per call; not a measurable cost relative to the Haversine
  trigonometry, which dominates.

---

---

## Component 18: Probabilistic Data Structures

> **Source Files**: `src/probabilistic.rs` | **Internal Reference**: [`docs/internal/18_probabilistic.md`](../internal/18_probabilistic.md)


---

### 1. Architectural Purpose & Scope

`src/probabilistic.rs` implements four independent approximate-membership/frequency data
structures — a **Bloom Filter**, a **Cuckoo Filter**, a **Count-Min Sketch**, and a **Top-K
frequency tracker** (Space-Saving algorithm) — exposed via RedisBloom-compatible commands
(`BF.*`, `CF.*`, `CMS.*`, `TOPK.*`). Each structure type has its own per-key map inside
`ProbabilisticStore`, which lives in `ShardDb` alongside `vector_indexes`/`crdt_store`/
`json_store`. Like `src/json.rs` (Component 16) and `src/geo.rs` (Component 17) and unlike
`src/vector.rs` (Component 08), every single-key command here is genuinely routed per-key
across shards — confirmed directly in `connection.rs`: `BfAdd`/`CfAdd`/`CmsIncrby`/`TopkAdd`/
etc. all appear in the same `target_shard_of_cmd`/local-vs-`execute_remote` dispatch arm as
ordinary keyed commands.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **A single custom hash function underlies all four structures.** `fnv1a_hash` (a
   seeded 64-bit FNV-1a) and `double_hash` (two independent FNV-1a calls with different fixed
   seeds, used for Kirsch-Mitzenmacher double-hashing) are shared by the Bloom filter, Cuckoo
   filter, and Count-Min Sketch — there is no per-structure hash family, and no cryptographic
   hash anywhere in this file (not a concern for these structures' intended use, unlike an
   auth-adjacent context).
2. **Bloom filter sizing follows the standard formulas, computed once at creation.**
   `BloomFilter::new(capacity, error_rate)` derives bit-array size via
   $m = \lceil -n \ln(p) / (\ln 2)^2 \rceil$ and hash count via $k = \text{round}((m/n)\ln 2)$,
   clamped to `[1, 30]` hashes — real, textbook Bloom filter parameter derivation, not
   hardcoded constants.
3. **The Cuckoo filter is a real, complete implementation including eviction ("cuckoo
   kicks").** `add` tries both candidate buckets first, and only falls back to the
   randomized-kick relocation loop (`MAX_KICKS = 500`) if both are full — a genuine cuckoo
   hashing insert, not a simplified always-fails-when-full variant. `delete` is also real
   (removes a matching fingerprint from either candidate bucket), which is one of the
   Cuckoo filter's actual advantages over a Bloom filter (Bloom filters can't support
   deletion at all without a counting variant, which isn't implemented here).
4. **The Top-K tracker is a real Space-Saving algorithm, not an exact top-K.** Once at
   capacity, `TopK::add` evicts the *minimum-count* tracked item and gives the new item that
   evicted item's count plus the increment — the standard Space-Saving guarantee (every
   tracked count is an overestimate, bounded by the true frequency of whatever was evicted
   last), not an exact frequency count.
5. **No structure ever shrinks or is auto-resized.** A Bloom/Cuckoo filter's bit array or
   bucket count is fixed at creation time (`BF.RESERVE`/`CF.RESERVE`'s capacity argument); a
   Count-Min Sketch's width/depth are likewise fixed at `CMS.INITBYDIM`/`INITBYPROB` time.
   There is no `BF.INSERT ... EXPANSION` auto-scaling behavior — once a filter created with a
   given capacity is over-inserted, its false-positive rate silently degrades rather than the
   structure growing.

---

---

### 3. Performance Characteristics

- **Bloom/Cuckoo `add`/`contains` are O(num_hashes) / O(1)** respectively — a Bloom filter
  check costs up to 30 bit-array probes (bounded, per §2.2's clamp), a Cuckoo filter check is
  two fixed-size (4-slot) bucket scans regardless of fill level.
- **Cuckoo insertion degrades under high load factor** — the `MAX_KICKS = 500` eviction chain
  only triggers once both candidate buckets are full, and a filter approaching its rated
  capacity will trigger it increasingly often before either succeeding or returning
  `"ERR Cuckoo filter is full"` — a real, expected cuckoo-hashing characteristic, not a bug.
- **Count-Min Sketch `incr_by`/`query` are O(depth)**, independent of how many distinct items
  have been tracked — the whole point of a fixed-size sketch over an exact per-item counter map.
- **Top-K's eviction is O(k) per new distinct item once at capacity** (§4.4) — negligible at
  small `k`, would matter if `TOPK.RESERVE` were ever used with a very large `k`.

---

---

## Component 19: Pub/Sub Messaging Hub

> **Source Files**: `src/pubsub.rs` | **Internal Reference**: [`docs/internal/19_pubsub.md`](../internal/19_pubsub.md)


---

### 1. Architectural Purpose & Scope

`src/pubsub.rs` implements `PubSubHub`, the per-shard channel/pattern subscription registry
backing `SUBSCRIBE`/`UNSUBSCRIBE`/`PSUBSCRIBE`/`PUNSUBSCRIBE`/`PUBLISH`/`PUBSUB CHANNELS`/
`NUMSUB`/`NUMPAT`. Like `BlockHub` (Component 06), a client that issues `SUBSCRIBE` hands its
connection off to a dedicated, permanent mode-switch loop (`run_pubsub_loop` in
`connection.rs`) that never returns to ordinary command processing for the lifetime of that
TCP connection. Unlike `BlockHub`, `PubSubHub` is genuinely **per-shard** (one instance per
shard, owned by `Router.pubsub`, not a process-wide `Arc<Mutex<_>>`) — cross-shard delivery
(a publisher on shard A reaching a subscriber connected via shard B) is handled by `Router::
publish` fanning the message out to every other shard's own `PubSubHub`, not by sharing one
hub across shards.

---

---

### 2. Key Invariants & Concurrency Constraints

1. **Genuinely per-shard, not a `BlockHub`/`ACL`/search-registry-style global exception.**
   `Router.pubsub: Rc<RefCell<PubSubHub>>` — a plain `Rc`/`RefCell`, exactly like `ShardDb`
   itself, with no `Arc`/`Mutex` anywhere. A subscriber's registration (`channels`,
   `patterns`, `clients`, `client_channels`, `client_patterns`) only ever exists in the
   `PubSubHub` of the one shard that accepted that subscriber's connection.
2. **Cross-shard delivery is a real, parallel fan-out — send-then-await, matching the
   squashed-pipeline pattern.** `Router::publish` (Component 04) delivers to local
   subscribers immediately, then dispatches one `ShardMessage::Publish` to every *other*
   shard (all sends issued before any await), and sums each shard's returned delivery count —
   the same "dispatch all, then await all" shape used throughout the codebase for
   parallelism (Components 02/04).
3. **A hand-written glob matcher, not a regex or a crate.** `glob_match` implements `*`/`?`
   wildcard matching via an explicit backtracking scan (tracking the last `*` position and
   resuming from there on a mismatch) rather than compiling a regex or pulling in a glob
   crate — used both for `PSUBSCRIBE` pattern matching against published channels and for
   `PUBSUB CHANNELS <pattern>`'s filtering.
4. **No RESP3 push-type framing anywhere in this file — verified.** Every delivered message
   (`message`/`pmessage`) and every subscribe/unsubscribe confirmation is hard-coded RESP2
   array framing (`*3\r\n$7\r\nmessage\r\n...`); grepping this file for `is_resp3`/`resp3`
   finds zero matches. A RESP3-negotiated client (Component 02's `CURRENT_CLIENT_RESP3`
   machinery) still receives ordinary array-type frames for pub/sub messages instead of the
   RESP3 push type (`>3\r\n...`) real Redis sends once a client has opted into RESP3 — see §7.
5. **A subscriber count of zero triggers cleanup, not a lingering empty entry.** Every
   `unsubscribe`/`punsubscribe`/`unsubscribe_all`/`punsubscribe_all` path removes the
   channel/pattern's `HashSet` entirely once it's empty (not left as an empty set), and
   removes the client's `flume::Sender` from `clients` once its last subscription anywhere
   drops to zero (`total_subscriptions(client_id) == 0`) — no unbounded growth from
   subscribe/unsubscribe churn.

---

---

### 3. Performance Characteristics

- **Direct-channel publish is O(subscribers to that channel)** — no overhead from unrelated
  channels or patterns.
- **Pattern publish is O(total registered patterns) per publish**, not O(matching patterns)
  (§4.2) — a deployment with many distinct active `PSUBSCRIBE` patterns pays a glob-match per
  pattern on every single `PUBLISH`, regardless of how many (if any) actually match.
- **Cross-shard fan-out cost is O(num_shards) per `PUBLISH`, done in parallel** (§2.2) — every
  publish touches every other shard's `PubSubHub` once via a `flume` message, dispatched
  concurrently rather than sequentially, matching the codebase's general cross-shard fan-out
  pattern.
- **One frame is built once and cloned per subscriber**, not re-serialized per recipient —
  the `Vec<u8>` frame construction cost is paid once regardless of subscriber count.

---
