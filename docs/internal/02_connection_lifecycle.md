# Component 02: Connection Lifecycle & Command Execution (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/connection.rs` (14,542 lines)
> **High-Level Design Spec**: [`docs/design/02_connection_lifecycle.md`](../design/02_connection_lifecycle.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/connection.rs` | Per-connection read loop, command dispatch, transactions, pipeline squashing, mode switches (pub/sub, replication) | `handle_connection`, `execute_command`, `execute_local_command`, `execute_commands_squashed`, `ClientInfo`, `ConnScratch` |
| `src/mailbox.rs` | Lock-free cross-shard response mailboxes and SPSC queues consumed by this file | `BatchResponder`, `SpscQueue`, `ShardSender`/`ShardReceiver` |
| `src/shard.rs` | Reply payload representation and shard-local message envelope | `CompactResp`, `ShardMessage`, `ShardDb` |
| `src/router.rs` | Local-vs-remote routing, cross-shard `MGET`/`MSET` dispatch, VLL transaction locks | `Router`, `MgetInFlight`, `MsetInFlight` |

All line numbers below were confirmed against the current `src/connection.rs` at the time of writing and may drift by a handful of lines as the file evolves; function names are stable references.

---

## 2. Component Architecture & Data Structures

```
                 Client TCP Stream
                        │
                        ▼
              handle_connection() loop
                        │
        ┌───────────────┼──────────────┬────────────────┬───────────────────┐
        ▼               ▼               ▼                ▼                   ▼
  SUBSCRIBE/       PSYNC seen?     DFLY FLOW seen?   MULTI/WATCH/       Blocking cmd
  PSUBSCRIBE/      → replica       → shard-to-shard   EXEC active?      (BLPOP/…)?
  SSUBSCRIBE?      stream mode     replication flow   → transaction     → flush out_buf
  → run_pubsub_loop  (mode switch,  (mode switch,       branch (§4.2)     first, then
    (mode switch,     never returns) never returns)         │              execute
    never returns)         │              │                 │                 │
        │                  └──────────────┴─────────────────┴─────────────────┘
        │                                          ▼
        │                            1 command? execute_command()
        │                            N commands? execute_commands_squashed()
        │                                          │
        │                              ┌───────────┴────────────┐
        │                              ▼                        ▼
        │                       Local shard key           Remote shard key
        │                   execute_local_command()   ShardMessage::Batch over an
        │                     direct on ShardDb        Arc<BatchResponder> mailbox,
        │                                               awaited via spin-then-notify
        │                                          │
        │                                          ▼
        │                            out_buf (RESP2/RESP3/Memcached text)
        │                                          │
        │                                          ▼
        │                       one coalesced buffer flushed via non-blocking
        │                       libc::send, falling back to io_uring write_all
        ▼
  dedicated writer task (bounded flume queue,
  independent output-buffer-limit accounting)
```

Four distinct execution strategies are chosen per read, in priority order (`handle_connection`, ~line 1259 onward): **(a)** a transaction is already open or this batch contains `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` → the transaction branch (§4.2); **(b)** the batch contains a blocking command → sequential execution with a pre-block flush (§4.3); **(c)** exactly one command → `execute_command` directly; **(d)** more than one command, none of the above → `execute_commands_squashed` (§4.4). A single connection can also permanently switch out of this loop into one of three special, non-returning modes: pub/sub (`run_pubsub_loop`), master→replica streaming (`run_master_replica_stream`), or Dragonfly-protocol shard-to-shard replication flow (`run_shard_replication_flow`).

#### Real Per-Connection & Per-Shard-Port State

```rust
// line 27
#[derive(Clone, Debug)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub name: Option<String>,
    pub connected_at: Instant,
    pub last_active: Instant,
    pub last_cmd: &'static str,          // interned command name, not an owned String
    pub is_resp3: bool,
    pub track_tx: Option<flume::Sender<Vec<u8>>>,
    pub raw_fd: std::os::unix::io::RawFd,
    pub omem: usize,                     // bytes currently queued in this client's output buffer
}

// line 163
#[derive(Clone, Debug)]
pub struct ClientTracker {
    pub port: u16,
    pub client_id: u64,
    pub bcast: bool,
    pub prefixes: Vec<Bytes>,
    pub tracked_keys: hashbrown::HashSet<Vec<u8>>,
    pub sender: flume::Sender<Vec<u8>>,
    pub is_resp3: bool,
}

// line 173
thread_local! {
    pub static CURRENT_CLIENT_RESP3: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub static IN_TX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
```

`last_cmd` is a `&'static str` (the interned name returned by `get_cmd_name`), not an owned `String` — there is no per-command string allocation for this bookkeeping. `omem` (added alongside client output-buffer-limit enforcement — see §4.8) mirrors the size of the connection's pending output so `CLIENT LIST`/`CLIENT INFO` can report it without re-measuring `out_buf`.

There is no `ClientContext`/`ClientTxState`/`ClientProtocol` struct anywhere in the file. Per-connection transaction state (`in_multi`, `tx_queue: Vec<Command>`, `tx_has_error`) lives as plain stack locals inside `handle_connection`, and RESP3-vs-RESP2 mode is tracked via the `CURRENT_CLIENT_RESP3` thread-local (set at the top of every dispatch function from `ClientInfo.is_resp3`) rather than an enum.

Three `RwLock`-guarded static maps implement optimistic-locking `WATCH`, RESP3 client-side caching invalidation, and are all keyed so state stays scoped to one shard's listening port even though the maps are process-wide statics. Each is paired with a cheap `AtomicBool` fast-path flag so the hot write path can skip the lock entirely when the feature is unused:

```rust
// line 430
static WATCHED_KEYS: LazyLock<RwLock<HashMap<u16, HashMap<Bytes, HashSet<u64>>>>> = ...;
static CLIENT_WATCH_TAINTED: LazyLock<RwLock<HashMap<(u16, u64), bool>>> = ...;
pub static HAS_WATCHED_KEYS: AtomicBool = AtomicBool::new(false);     // line 483

// line 550
static TRACKING_CLIENTS: LazyLock<RwLock<HashMap<(u16, u64), ClientTracker>>> = ...;
pub static HAS_TRACKING_CLIENTS: AtomicBool = AtomicBool::new(false); // line 554
```

`touch_watched_key`, `record_client_read`, and `notify_key_invalidation` all check the corresponding `AtomicBool` (`Ordering::Relaxed`) before touching the `RwLock`, so a deployment that never issues `WATCH` or `CLIENT TRACKING` pays only an atomic load per write, not a lock acquisition.

`CMD_STATS` (a process-wide `RwLock<HashMap<String, u64>>`, line 438) backs `INFO commandstats`. Per-command increments do **not** touch it directly: `record_cmd_stat` (line 447) increments a thread-local `LOCAL_CMD_STATS: HashMap<&'static str, u64>` and only merges into the global map every 1024 calls (`flush_local_cmd_stats`, also called on connection teardown), keeping the global `RwLock` off the per-command hot path.

#### Client Output Buffer Limits (`ClientClass` / `BufferLimit`)

```rust
// line 40
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferLimit { pub hard_limit: u64, pub soft_limit: u64, pub soft_seconds: u64 }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientClass { Normal, Replica, Pubsub }

pub static NORMAL_BUFFER_LIMIT: RwLock<BufferLimit> = RwLock::new(BufferLimit::new(0, 0, 0));
pub static SLAVE_BUFFER_LIMIT:  RwLock<BufferLimit> = RwLock::new(BufferLimit::new(268_435_456, 67_108_864, 60));
pub static PUBSUB_BUFFER_LIMIT: RwLock<BufferLimit> = RwLock::new(BufferLimit::new(33_554_432, 8_388_608, 60));
```

These mirror Redis's `client-output-buffer-limit` classes and defaults (normal: unlimited; replica: 256MiB hard / 64MiB sustained-60s soft; pubsub: 32MiB hard / 8MiB sustained-60s soft), configurable at runtime via `set_client_output_buffer_limit_str` (§4.8).

#### `ConnScratch`: Pooled, Cross-Connection Reusable Per-Connection Buffers

```rust
// line 1685
struct ConnScratch {
    buf: BytesMut,                                        // inbound read buffer
    out_buf: Vec<u8>,                                      // outbound response buffer
    responders: Vec<Arc<crate::mailbox::BatchResponder>>,  // one mailbox per shard
    remote_batches: Vec<Vec<(usize, u64, Command)>>,        // per-shard outgoing command buckets
    items_pool: Vec<Vec<(usize, u64, Command)>>,            // recycled Vecs for the above
    results_pool: Vec<Vec<(usize, CompactResp)>>,           // recycled result Vecs
    squashed_responses: Vec<CompactResp>,
    commands: Vec<Command>,
}

thread_local! {
    static CONN_SCRATCH_POOL: RefCell<Vec<ConnScratch>> = const { RefCell::new(Vec::new()) };
}
```

`take_conn_scratch`/`recycle_conn_scratch` (lines 1700–1766) pool up to 32 `ConnScratch` instances **per reactor thread, shared across successive connections on that core** — not merely reused within one connection's lifetime. A scratch instance is only returned to the pool if every one of its `BatchResponder`s is provably idle and unreferenced elsewhere:

```rust
let reusable = s.responders.iter().all(|r| {
    Arc::strong_count(r) == 1 && !r.ready.load(Ordering::Acquire)
});
if !reusable { return; }  // drop it; a straggling remote shard still holds an Arc clone
```

This guards against a subtle cross-connection hazard: a remote shard holds an `Arc<BatchResponder>` clone for as long as its batch is in flight and writes the reply through an `UnsafeCell` in `BatchResponder::finish`. Recycling a scratch whose responder is still referenced by a slow remote shard would let that late write land in a different client's connection state.

#### `BatchResponder`: Lock-Free Single-Slot Cross-Shard Mailbox (`src/mailbox.rs`, line 244)

```rust
pub struct BatchResponder {
    pub ready: CachePadded<AtomicBool>,
    pub payload: CachePadded<UnsafeCell<Option<(
        Vec<(usize, u64, Command)>,
        Vec<(usize, CompactResp)>,
    )>>>,
    pub notify_tx: flume::Sender<()>,
    pub notify_rx: flume::Receiver<()>,
}
```

This is the actual mechanism behind cross-shard pipeline squashing — **not** a `flume`-channel-based responder pool. `finish()` (called by the remote shard) writes the payload into the `UnsafeCell` and sets `ready` (`Release`); `try_take()` (called by the connection) checks `ready` (`Acquire`) and, if set, takes the payload without blocking. The paired `flume::bounded(1)` channel carries no payload — `notify_tx.try_send(())` / `notify_rx.recv_async().await` exist purely as an async wake-up for the fallback path once the harvesting connection stops spinning (§4.4). A `pub type ResponderChannel = (flume::Sender<...>, flume::Receiver<...>)` alias is still declared near the top of `connection.rs` (line 21) but is **dead code** — it is not referenced anywhere in the crate; `ConnScratch.responders: Vec<Arc<BatchResponder>>` is what is actually built and reused.

#### `CompactResp`: Small-Reply Inline Storage (`src/shard.rs`, line 9)

```rust
pub enum CompactResp {
    Small { len: u8, data: [u8; 30] },  // inline, no heap allocation
    Big(Vec<u8>),
    Bulk(Bytes),
    Array1Bulk(Bytes),
}
```

Common short replies (`+OK\r\n`, small integers, short bulk strings) are stored inline in a 30-byte array with no heap allocation; only replies that exceed that inline capacity fall back to a heap-backed variant. This is the reply payload type carried through `ShardMessage::Batch` and `BatchResponder`.

---

## 3. Execution Algorithms & Code Logic

### 3.1 `handle_connection` (line 968): read loop, mode switches, then one of four execution strategies

Each iteration of the read loop:

1. **Zero-copy read**: the connection's pooled `BytesMut` is rented directly to monoio's `io_uring` driver (`stream.read(RecvBytesMut(buf))`), avoiding an intermediate `read_buf` memcpy.
2. **Kernel-buffer drain**: if the io_uring read filled the entire spare capacity, or a subsequent `parse_command` call returns `Ok(None)` (an incomplete frame), the loop opportunistically issues a non-blocking `libc::recv(..., MSG_DONTWAIT)` directly on the raw fd to absorb any additional bytes the kernel already has queued, without an extra reactor round-trip.
3. **Parse every complete frame** currently in the buffer via `parse_command`, tracking `has_special` — a single pre-computed flag (from the cheap classifier `is_special_pipeline_cmd`, line 13169) that is true if the batch contains any command requiring special handling: `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`, `PSYNC`, `DFLY FLOW`, transaction-control commands, any blocking command, `HELLO`/`RESET`/`AUTH`/`ACL`/`TIER`/`CONFIG GET`/`CONFIG SET`, `DFLY CLUSTER`/`DFLY MIGRATE`, `STICK`/`UNSTICK`, or the non-storage Memcached commands (`STATS`/`VERSION`/`QUIT`). This flag lets the four mode-switch checks and the transaction/blocking-command branches short-circuit with a single boolean test instead of re-scanning the batch each time.
4. **Mode switches** (checked only if `has_special`), each a one-way, non-returning hand-off of the socket: `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE` → `run_pubsub_loop`; `PSYNC` → `run_master_replica_stream`; `DFLY FLOW` → `run_shard_replication_flow` (any commands preceding the mode-switching command in the same batch are executed first via `execute_command`, then any buffered output is flushed before the hand-off).
5. **Execution strategy selection** — see §2's priority order (a)–(d).
6. **RESP3 client-side-cache invalidation drain**: if `HAS_TRACKING_CLIENTS` is set, any invalidation messages queued on this client's `track_rx` channel by other connections are appended to `out_buf`.
7. **Client output buffer limit enforcement** (§3.5) against `out_buf.len()`.
8. **Flush**: a direct non-blocking `libc::send(..., MSG_DONTWAIT | MSG_NOSIGNAL)` is attempted first; on a full send it clears `out_buf` with no `io_uring` round-trip at all. A partial send copies the remainder into a fresh `Vec` and completes it with `stream.write_all`. A failed/blocked `send` (e.g. `EAGAIN`) falls back to a full `stream.write_all` through `io_uring`. This is a **single coalesced buffer** — not scatter-gather/vectored I/O across multiple buffers.

On disconnect, `ConnScratch` is returned to the pool (if reusable) and `unwatch_keys` clears this client's `WATCH` state.

### 3.2 Transactions: `MULTI`/`EXEC`/`WATCH`, with real cross-shard locking

Unchanged in shape from prior versions of this document and re-verified against the current source (`handle_connection`, ~lines 1319–1427). Queued commands are buffered per-connection (`tx_queue: Vec<Command>`); `WATCH` registers keys in `WATCHED_KEYS`, and any write to a watched key anywhere flips `CLIENT_WATCH_TAINTED` for that client (checked at `EXEC` time to abort optimistically, matching Redis `WATCH` semantics). When `EXEC` runs the queue, if it spans more than one shard it acquires an explicit cross-shard lock before executing any queued command:

```rust
let mut shards = hashbrown::HashSet::new();
for cmd in &tx_queue {
    for k in cmd_keys(cmd) { shards.insert(router.target_shard(k)); }
}
let mut sorted_shards: Vec<usize> = shards.into_iter().collect();
sorted_shards.sort_unstable();          // lock-ordering discipline: avoids deadlock against
                                          // a concurrent transaction touching an overlapping,
                                          // differently-ordered shard set
let use_vll = sorted_shards.len() > 1;
let tx_id = if use_vll {
    static NEXT_TX: AtomicU64 = AtomicU64::new(1);
    let id = NEXT_TX.fetch_add(1, Ordering::Relaxed);
    router.acquire_tx_locks(&sorted_shards, id).await;   // Router::acquire_tx_locks, src/router.rs
    id
} else { 0 };

let hub_arc = crate::block::get_block_hub_for_port(router.port);
hub_arc.lock().unwrap().pause();        // suppress BlockHub wakeups mid-transaction
IN_TX.set(true);
for q_cmd in queued { execute_command(q_cmd, ...).await; }
IN_TX.set(false);
if use_vll { router.release_tx_locks(&sorted_shards, tx_id).await; }
let pending = hub_arc.lock().unwrap().resume();   // replay any list/zset-push notifications
                                                    // that arrived while paused, routing each
                                                    // to its owning shard (local: notify_list/
                                                    // notify_zset inline; remote: ShardMessage::NotifyList)
```

The `BlockHub` (`src/block.rs`) is explicitly paused for the transaction's duration; any list/zset-push notifications that would otherwise wake a blocked client mid-transaction are queued (`add_pending_notify`) and replayed after the transaction commits, rather than firing while the transaction's own writes are still in flight.

### 3.3 Blocking commands: flush before you block

Blocking commands — `BLPOP`, `BRPOP`, `BLMOVE`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, `XREAD` with `block_ms: Some(_)`, `XREADGROUP` with `block_ms: Some(_)` — are detected up front via `has_special` and executed strictly sequentially (never squashed). Each is preceded by a real `io_uring` `write_all` flush of anything already queued in `out_buf`, necessary because a blocking command can legitimately suspend the connection's task for seconds while waiting on `BlockHub`, and earlier pipelined replies should not be delayed by that wait.

### 3.4 `execute_command` (line 3802, ~5,013 lines): per-command gate sequence, then dispatch

Every single-command execution (from the sequential path, the blocking-command path, the squashed path's ineligible-batch fallback, and the squashed path's per-command safe-list fallback) runs through the same gate sequence before reaching its command-specific match arm:

1. **Slowlog timing**: a `SlowlogTracker` RAII guard captures a cloned `Command` and start `Instant` (only when `SLOWLOG_LOG_SLOWER_THAN >= 0` and the command isn't `SLOWLOG`/`QUIT`), and on `Drop` calls `crate::slowlog::log_command_if_slow` with the elapsed microseconds.
2. **`NOAUTH`**: rejected unless `authenticated` or the command is `AUTH`/`HELLO`/`QUIT`.
3. **ACL** (`-NOPERM`): checked only when `HAS_CUSTOM_ACL` is set or the session isn't the `default` user (a real fast-path skip, not just a documentation simplification) — both `user.can_execute_command(cmd_name)` and, over **every** key returned by `cmd_keys(&cmd)` (not just the primary key — see below), `user.can_access_key(key)`.
4. **`ASKING`**: sets a one-shot `asking` flag and returns immediately with `+OK`.
5. **`CROSSSLOT`**: when `router.cluster_enabled` and the command has more than one key, all keys must hash to the same slot or the command is rejected with `-CROSSSLOT`.
6. **Cluster slot-state redirection**: for `cmd_primary_key(&cmd)`'s key, `router.get_slot_state(slot)` drives `MOVED` (state `Moved`), `MOVED` on `Importing` unless `ASKING` was just set, `ASK` on `Migrating` unless the key already exists locally, and — even when `Stable` — a `MOVED` if `cluster_enabled` routing says a different local shard owns the slot, or (single-node-router path) if the gossiped cluster hub says a peer node now owns it.
7. **`READONLY`**: rejected (fast-path gated on `HAS_SLAVE_INSTANCE`) if this node is a replica and the command has a write form per `crate::aof::command_to_resp`.
8. **`OOM`**: for any write command, if `maxmemory` is set, the policy is `noeviction`, and NVMe tiering is inactive, the command is rejected with `-OOM` when this shard's `used_memory` exceeds `maxmemory / num_shards`.

Only after all eight gates pass does control reach the `match cmd { ... }` block. `execute_local_command` (line 9115, ~4,054 lines) is the shard-local, synchronous counterpart, called both for genuinely-local keys and for remote batches arriving via `ShardMessage::Batch`; it shares a `record_change!` macro that bumps `DIRTY_CHANGES`, taints `WATCH`ed keys via `for_each_cmd_key`, and — under one combined `need_aof || need_rep` check — calls `crate::aof::command_to_resp` once and forwards the result to both the AOF writer and shard-scoped replication propagation. The two match statements are independent and kept in sync by hand (see §5, §6).

**Verified correction to prior documentation**: `cmd_primary_key` (line 2334) genuinely does return only the *first* key for multi-key commands (`MGET`/`MSET`/`TOUCH`/`SINTER`/etc. — used for the routing and slot-redirection decision, where a single representative key is the correct unit). But the *ACL* key check no longer uses it: it uses `cmd_keys`/`for_each_cmd_key` (line 2537/2814), which explicitly enumerate every key of every multi-key command (confirmed for `MGET`/`TOUCH`/`MSET`/`MSETNX`/`MSETEX`/`EVAL`/`EVALSHA`/`BITOP`/`PFMERGE` and others). A client permitted on an `MGET`'s first key but forbidden on its second key **is** now rejected — this closes the "only the first key is checked for ACL" gap a previous revision of this document flagged as still open.

### 3.5 Client Output Buffer Limits & Slow-Consumer Protection

Every pass through the main read loop measures `out_buf.len()` against `get_client_output_buffer_limit(ClientClass::Normal)`:

```rust
if norm_limits.hard_limit > 0 && out_buf.len() as u64 >= norm_limits.hard_limit { break; }  // disconnect immediately
if norm_limits.soft_limit > 0 && out_buf.len() as u64 >= norm_limits.soft_limit {
    // disconnect only after the soft limit has been sustained for `soft_seconds`
    ...
}
```

`ClientInfo.omem` is updated before and after the flush so `CLIENT LIST`/`INFO` reflect the live queue depth. Pub/sub connections enforce the `Pubsub` class independently: `run_pubsub_loop` (line 1768) splits the socket into reader/writer halves and spawns a dedicated `monoio::spawn`ed writer task that drains a bounded (`flume::bounded(4096)`) channel; that task applies the same hard/soft-limit policy against its own running `queued_bytes` total and breaks (closing the writer, which tears down the connection) if a slow subscriber can't keep up. This is the mechanism behind Redis-compatible `client-output-buffer-limit normal|slave|pubsub <hard> <soft> <seconds>` (`CONFIG SET`, parsed by `set_client_output_buffer_limit_str`, line 130).

### 3.6 `execute_commands_squashed` (line 13215, ~730 lines): eligibility gate, inline fast paths, dispatch, harvest

**Eligibility gate.** `can_squash` starts as `*authenticated` and, unlike a prior revision of this document described, the ACL and cluster-ownership checks are **conditionally skipped entirely** when there is no reason to run them — `if has_special || user.is_some() || is_cluster`. When they do run, every key of every command is checked via `for_each_cmd_key` (not `cmd_primary_key`):

```rust
if let Some(user) = &user {
    let cmd_name = get_cmd_name(cmd);
    if !user.can_execute_command(cmd_name) { can_squash = false; break; }
    let mut forbidden = false;
    for_each_cmd_key(cmd, |k| { if !user.can_access_key(k) { forbidden = true; } });
    if forbidden { can_squash = false; break; }
}
if is_cluster {
    let mut first_slot = None; let mut invalid_slot = false; let mut has_keys = false;
    for_each_cmd_key(cmd, |k| {
        has_keys = true;
        let slot = key_slot(k);
        if let Some(fs) = first_slot { if fs != slot { invalid_slot = true; } } else { first_slot = Some(slot); }
        if router.get_slot_state(slot) != SlotState::Stable { invalid_slot = true; }
        if let Some(my_slots) = &my_slots_guard {
            if !my_slots.iter().any(|&(s, e)| slot >= s && slot <= e) { invalid_slot = true; }
        }
    });
    if invalid_slot { can_squash = false; break; }
    if !has_keys && !matches!(cmd, Command::Ping(_) | Command::CommandDocs | Command::Quit | Command::Time | Command::Echo(_)) {
        can_squash = false; break;
    }
}
```

Any single ineligible command still forces the *entire* batch to the sequential `execute_command` fallback, applying the real `-NOPERM`/`-MOVED`/`-ASK`/`-CROSSSLOT` behavior per command.

**Inline local fast paths.** For eligible batches, local-shard commands whose target is this shard are handled inline using precomputed-hash table accessors (`*_with_hash`) rather than a second key hash. `GET` and single-key `EXISTS` are always eligible for this path (including the NVMe cold-tier fallback via `router.stream_cold_read_local` when the key is on the tier but not in RAM). Several write commands — plain `SET` (no condition/`GET`/`KEEPTTL`), `INCRBY`, `DEL` (single key), `HSET`, `SADD`, `ZADD`, `LPUSH`, `LPOP` — also get this treatment, but **only when it is provably safe to skip the normal AOF/replication/search/blocking side-effect machinery**: each is additionally gated on `router.aof.is_none() && !crate::replication::has_connected_replicas(router.port)`, and `DEL`/`HSET` further require `!crate::search::has_active_search_indices()`, and `LPUSH` further requires `!crate::block::has_blocked_waiters(router.port)`. When any of those conditions is false, the command falls through to the generic `execute_command`/`execute_local_command` path instead, which does perform AOF append, replication propagation, search-index updates, and blocked-client wakeups. A batch containing any local write via this fast path sets `has_local_writes`, and `router.check_auto_tier_after_write()` runs once after the loop.

**Verified gap, not present in the general dispatch path**: the inline `SET` fast path (line ~13404) does not call `touch_watched_key` or `notify_key_invalidation`, unlike its sibling fast paths (`INCRBY`/`DEL`/`HSET`/`SADD`/`ZADD`/`LPUSH`/`LPOP`, which do call `touch_watched_key`). A plain `SET` executed inside a squashed pipeline batch under the AOF-less/replica-less conditions above will not taint an active `WATCH` on that key, and will not emit a RESP3 client-side-cache invalidation to other tracking clients. See §6.

**Remote dispatch and cross-shard `MGET`/`MSET`.** Commands targeting a remote shard are bucketed into `remote_batches[target_shard]`. `MGET`/`MSET` are dispatched eagerly via `Router::begin_mget_resp`/`Router::begin_mset` (returning an `MgetInFlight`/`MsetInFlight` handle) *before* the remote-batch dispatch loop below, so they run concurrently with the ordinary cross-shard batches rather than stalling the pipeline; their results are gathered via `Router::finish_mget_resp`/`Router::finish_mset` after the batch harvest. Each non-empty `remote_batches[target_shard]` is sent as one `ShardMessage::Batch { items, results, responder: responders[target_shard].clone(), is_resp3 }` over `router.senders[target_shard]`.

**Harvest**: up to 256 iterations of a non-blocking sweep call `BatchResponder::try_take()` on every still-pending responder, with `std::hint::spin_loop()` between sweeps; any responder not ready after 256 spins falls back to `responder.notify_rx.recv_async().await`. This trades a bounded amount of CPU spinning to avoid a scheduler round-trip when remote shards reply within microseconds (the common case), at the cost of other tasks on the same reactor core making no progress during the spin phase.

Responses are written to `out` in original pipeline order once every local, remote, and `MGET`/`MSET` result has been collected (`resp.write_to(out)` per `CompactResp`, with `out` pre-reserved via `estimated_len()`).

### 3.7 `MGET`/`MSET` shard targeting — no fixed shard-count limit

`target_shard_of_cmd` (line 8815) returns `Some(shard)` for `MGET`/`MSET` when *every* key in the command happens to hash to the same shard (the common case in a single-shard deployment, or with hash-tagged keys) — in that case the command rides the ordinary single-target local/remote path with none of the cross-shard machinery below. Only when keys genuinely span multiple shards does `Router::begin_mget_resp`/`begin_mset` engage, using an `Arc<ScatterMgetDescriptor>`/`Arc<ScatterMsetDescriptor>` (`src/mailbox.rs`) with an `AtomicUsize` pending-shard counter. A previous revision of this document flagged a `u64`-bitmask response-collection bug that silently dropped replies from shards numbered 64 and above; that bitmask-based mechanism (`sent_mask`/`remaining_mask`) no longer exists anywhere in `src/connection.rs` or `src/router.rs` — the current descriptor/counter design has no fixed-width shard-count ceiling.

### 3.8 `format_score`: exact `%.17g` compatibility

Unchanged and re-verified accurate:

```rust
// line 229
pub fn format_score(val: f64) -> String {
    if val.is_nan() { "nan".to_string() }
    else if val.is_infinite() { if val.is_sign_positive() { "inf".to_string() } else { "-inf".to_string() } }
    else if val == 0.0 { "0".to_string() }
    else {
        let mut buf = [0u8; 64];
        let len = unsafe { libc::snprintf(buf.as_mut_ptr() as *mut libc::c_char, buf.len(), c"%.17g".as_ptr(), val) };
        if len > 0 && (len as usize) < buf.len() { unsafe { std::str::from_utf8_unchecked(&buf[..len as usize]) }.to_string() }
        else { val.to_string() }
    }
}
```

Sorted-set scores are formatted via a direct `libc::snprintf(..., "%.17g", ...)` FFI call specifically to match the exact string Redis's C implementation produces, which matters for the vendored Redis test suite. `write_resp_score` picks between this and RESP3's native `,<double>\r\n` type depending on `CURRENT_CLIENT_RESP3`.

### 3.9 Memcached text protocol detection

`parse_command` (`src/resp.rs`, line 1336) dispatches purely on the first byte: `*` → RESP multi-bulk array; anything else → first try `parse_memcached_storage_command` (which only recognizes the storage verbs `set`/`add`/`replace` by matching the first whitespace-delimited word, since those carry a following raw data block whose length must be parsed before the frame is complete), and if that returns "not a storage command," fall back to `parse_inline_command` (which handles the remaining Memcached verbs — `get`/`gets`/`delete`/`incr`/`decr`/`stats`/`version`/`quit` — as well as plain RESP inline commands such as a bare `PING` used by some minimal clients). There is no fixed first-byte lookup table gating detection; the only hard branch is the presence or absence of a leading `*`.

---

## 4. TLS Connections Are Not Pipeline-Squashed

`handle_tls_connection` (line 842) is a materially simpler loop than `handle_connection`: it decrypts into a plaintext buffer via `crate::tls::TlsSession`, then executes each parsed command through `execute_command` one at a time. It does not participate in `ConnScratch` pooling, does not attempt pipeline squashing, and does not enforce the output-buffer-limit checks described in §3.5. TLS connections therefore never take the cross-shard fan-out fast path described in §3.6 — every multi-command pipeline over TLS pays one `execute_command` call (and, for remote keys, one full `.await` round-trip) per command.

---

## 5. Cross-Component Interactions

- **`src/resp.rs`**: `parse_command` decodes buffered bytes into `Command` values (RESP2/RESP3 multi-bulk, Memcached text, and plain inline commands — §3.9).
- **`src/router.rs`**: `Router` provides `target_shard`/`key_slot`-based local/remote decisions, `get_slot_state`/`target_shard_for_slot` (cluster migration/ownership), the `senders` mesh, `acquire_tx_locks`/`release_tx_locks` (VLL transaction locking), `write_mget_resp`/`mset` (sequential path) and `begin_mget_resp`/`finish_mget_resp`/`begin_mset`/`finish_mset` (squashed-path concurrent dispatch), and `stream_cold_read_local`/`check_auto_tier_after_write` (tiered-storage integration).
- **`src/mailbox.rs`**: `BatchResponder` (§2), `ScatterMgetDescriptor`/`ScatterMsetDescriptor` (backing `MgetInFlight`/`MsetInFlight`), and the `ShardSender`/`ShardReceiver` mesh primitives.
- **`src/table.rs`** / **`src/shard.rs`**: `execute_local_command` and the squashed path's inline fast paths mutate `ShardDb`/`RudisTable` directly via `*_with_hash` accessors; `CompactResp` (`shard.rs`) is the reply payload type carried through `ShardMessage::Batch` and `BatchResponder`.
- **`src/block.rs`**: `BlockHub` (`get_block_hub_for_port`) — registration/wakeup for `BLPOP`/`BZPOPMIN`/blocking `XREAD`, and the `pause`/`resume`/`add_pending_notify`/`clear_pending_notifies` mechanism transactions use (§3.2), plus `has_blocked_waiters` (gates the squashed `LPUSH` fast path).
- **`src/pubsub.rs`**: `PubSubHub`, entered via `run_pubsub_loop` on `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`; the pub/sub writer task independently enforces the `Pubsub` output-buffer-limit class (§3.5).
- **`src/replication.rs`**: replica-stream mode (`run_master_replica_stream`), the `is_slave()`/`HAS_SLAVE_INSTANCE` write-rejection check, `has_connected_replicas`/`propagate_shard_bytes`/`propagate_bytes` (consulted both by `record_change!` and by the squashed fast-path gates).
- **`src/cluster.rs`**: `get_cluster_hub`/`HAS_ACTIVE_CLUSTER` supply the gossiped slot-ownership table consulted during `MOVED` redirection and the squashed-path cluster-ownership check.
- **`src/acl.rs`** (Component 15): `HAS_CUSTOM_ACL`/`get_acl_for_port`; both `execute_command` and `execute_commands_squashed` enforce real per-command/per-key authorization (`-NOPERM`) via `AclUser::can_execute_command`/`can_access_key`, checked against every key (`cmd_keys`/`for_each_cmd_key`), not just the routing-primary key.
- **`src/aof.rs`**: `execute_local_command` takes an `Option<&RefCell<AofWriter>>` to append write commands for persistence; `command_to_resp` is reused to detect "is this command a write" for the replica read-only guard, the `OOM` gate, and the squashed fast-path AOF-emptiness gate.
- **`src/slowlog.rs`**: `execute_command`'s `SlowlogTracker` RAII guard reports every command's wall-clock duration via `log_command_if_slow` when `SLOWLOG_LOG_SLOWER_THAN >= 0`.
- **`src/tls.rs`**: `handle_tls_connection` (§4) — the non-squashed TLS execution path.

---

## 6. Future Improvements & Known Gaps

- **New — squashed-path `SET` fast path skips `WATCH` tainting and client-side-cache invalidation.** In `execute_commands_squashed` (~line 13404), the inline fast path for a plain `SET` (no condition, no `GET`, no `KEEPTTL`, no AOF, no connected replicas) does not call `touch_watched_key` or `notify_key_invalidation`, while every other inline write fast path in the same function (`INCRBY`, `DEL`, `HSET`, `SADD`, `ZADD`, `LPUSH`, `LPOP`) calls `touch_watched_key`. A concurrent `WATCH` on a key that is subsequently `SET` through a squashed pipeline batch under those conditions will not be tainted, and a RESP3 client with key tracking enabled on that key will not receive an invalidation message. Fix: add the same `touch_watched_key` (and, for parity with the `MSET` special case at line 13817, a `notify_key_invalidation` call gated on `HAS_TRACKING_CLIENTS`) to the `SET` fast-path arm.
- **Resolved since the previous revision of this document — ACL/slot-eligibility now checks every key, not just the first.** Both `execute_command`'s ACL gate and `execute_commands_squashed`'s eligibility gate now iterate all of a multi-key command's keys via `cmd_keys`/`for_each_cmd_key`, rather than `cmd_primary_key`'s single representative key. The previously-documented gap (a command whose first key was permitted/stable but whose second key was forbidden/migrating would slip through) is closed.
- **Resolved since the previous revision of this document — the `> 64`-shard `MGET`/`MSET` bitmask bug no longer applies.** The `u64` `sent_mask`/`remaining_mask` mechanism this document previously flagged does not exist in the current source; cross-shard `MGET`/`MSET` now uses `Arc<ScatterMgetDescriptor>`/`Arc<ScatterMsetDescriptor>` with an `AtomicUsize` pending-shard counter (§3.7), which has no fixed-width shard-count ceiling.
- **De-risk `execute_command`/`execute_local_command`'s hand-synced duplication (§3.4).** The two independent `match` statements over the same `Command` enum have grown to roughly 5,013 and 4,054 lines respectively (up from the ~3,900/~3,750 lines noted previously) and are still kept in sync by hand — exactly the surface where a new command variant gets full local semantics but is forgotten in the remote-batch arm, or vice versa. A macro generating both arms from one command-behavior definition, or at minimum a test asserting both matches are exhaustive over the same variant set, would catch that class of bug before it reaches production.
- **TLS connections never take the pipeline-squashing fast path (§4).** This is a real, current scope limitation rather than a bug: `handle_tls_connection` executes every pipelined command sequentially through `execute_command`, so a TLS client pipelining a batch of local-shard commands does not get the zero-hash-recompute inline fast paths or the cross-shard parallel fan-out that a plaintext connection on the same workload would.
- **`CMD_STATS`/`record_cmd_stat` is a single global `RwLock`ed map (§2).** The thread-local batching (flush every 1024 commands) already keeps this off the hot path in the common case, but it remains one more process-wide lock alongside `BlockHub`/ACL/search/scripting; sharding or per-core aggregation would keep the "how many process-wide locks exist" count from growing unnoticed as command volume grows.

---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: Memcached text-protocol detection is *not* a fixed first-byte lookup table. `parse_command` treats any frame not starting with `*` as non-RESP, tries the three storage verbs (`set`/`add`/`replace`) first, then falls back to inline parsing for `get`/`gets`/`delete`/`incr`/`decr`/`stats`/`version`/`quit` and bare inline RESP commands (§3.9).
* **Gotcha 2**: Cross-shard fan-out reuses `Arc<mailbox::BatchResponder>` mailboxes pooled per-reactor-thread in `ConnScratch` (recycled across connections, not just within one connection's lifetime), not a `flume`-channel responder pool. The `ResponderChannel` type alias in this file is dead code (§2).
* **Gotcha 3**: The outbound socket buffer starts at a 64KiB (`READ_BUFFER_SIZE = 65536`) capacity and accumulates one contiguous `Vec<u8>` per read-loop iteration before flushing — this is a single coalesced write, not `writev`/vectored I/O across multiple buffers, and is normally sent via a direct non-blocking `libc::send` before falling back to `io_uring write_all` (§3.1).
* **Gotcha 4**: The squashed pipeline path's inline write fast paths (`SET`/`INCRBY`/`DEL`/`HSET`/`SADD`/`ZADD`/`LPUSH`/`LPOP`) are only taken when there is no AOF writer and no connected replica (and, for some commands, no active search index or blocked waiter) — otherwise the command falls through to the generic dispatch path so persistence/replication/indexing/blocking side effects are not silently skipped (§3.6).

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
