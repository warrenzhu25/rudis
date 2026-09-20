# Component 04: Sharding Architecture & Cross-Core Mesh (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/router.rs` (4,197 lines), `src/shard.rs` (2,668 lines),
> `src/mailbox.rs` (728 lines, cross-shard IPC primitives)  
> **High-Level Design Spec**: [`docs/design/04_sharding_mesh.md`](../design/04_sharding_mesh.md)  
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

> **Correction notice**: an earlier draft of this document described the cross-shard
> reply mechanism as exclusively `flume` channels. That is no longer (and in some cases
> never was) fully accurate: the hottest cross-shard paths — single-key `GET`/`SET`,
> pipelined `Batch` replies, and `MGET`/`MSET` scatter-gather — use purpose-built
> shared-memory completion cells and descriptors defined in `src/mailbox.rs`
> (`FastGetDescriptor`, `FastSetDescriptor`, `BatchResponder`, `ScatterMgetDescriptor`,
> `ScatterMsetDescriptor`). `flume` channels are still used throughout, but for these hot
> paths they carry only a zero-sized wake-up signal, never the actual payload. Lower
> traffic per-operation methods (`DEL`, `EXISTS`, `EXPIRE`, …) do still allocate a fresh
> `flume::bounded(1)` request/reply channel per remote call — see §4.2 for exactly which
> operations fall into which category, verified against the current source.

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/router.rs` | Key routing, the `Router` struct, one method per Redis command family, descriptor/channel pooling | `Router`, `target_shard`, `target_shard_and_hash`, `key_slot`, `slot_to_shard`, `extract_hash_tag`, `get`/`set`/`del`/…, `mget`/`mset`/`begin_mget_resp`/`finish_mget_resp`, `acquire_tx_locks`/`release_tx_locks`, `scan` |
| `src/shard.rs` | Per-shard state container and the cross-shard wire format | `ShardDb`, `ShardMessage` (66 variants), `CompactResp`, `SlotState` |
| `src/mailbox.rs` | Lock-free cross-shard transport: SPSC ring buffers, the shard mesh, and every shared-memory reply descriptor | `SpscQueue<T>`, `ShardSender`, `ShardReceiver`, `create_shard_mesh`, `CachePadded<T>`, `FastGetDescriptor`, `FastSetDescriptor`, `BatchResponder`, `ScatterMgetDescriptor`, `ScatterMsetDescriptor` |

---

### 3. Component Architecture & Data Structures

```
                        Client Request (any shard's connection)
                                     │
                                     ▼
                    cluster mode active?  (HAS_ACTIVE_CLUSTER /
                          Router::cluster_enabled)
                    ┌────────No─────────┴─────────Yes────────┐
                    ▼                                         ▼
     FxHash64(extract_hash_tag(key))               extract_hash_tag + CRC16/XMODEM
        % num_shards  = target shard                    → slot (0..16383)
                    │                             slot_to_shard(slot, num_shards)
                    │                              (contiguous range, NOT modulo)
                    │                                         │
                    └────────────────┬────────────────────────┘
                                     ▼
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
              Local Shard?                      Remote Shard?
                    │                                 │
       Direct ShardDb mutation             ShardMessage pushed onto a
     (+ AOF append, + tiering hooks)       dedicated mailbox::SpscQueue,
                                          target shard notified via flume
                                                        │
                                                        ▼
                                          Peer shard's event loop pops the
                                          message from its incoming ring
                                          (ShardReceiver::recv_async),
                                          executes it against its own
                                          ShardDb, and replies either via
                                          the message's own
                                          `flume::Sender<T>` or by writing
                                          into a shared mailbox.rs
                                          descriptor and flipping its
                                          completion flag
```

#### `Router` (`src/router.rs`, current field list)

```rust
#[derive(Clone)]
pub struct Router {
    pub shard_id: usize,
    pub num_shards: usize,
    pub port: u16,
    pub base_port: u16,
    pub cluster_enabled: bool,
    pub local_db: Rc<RefCell<ShardDb>>,
    pub senders: Vec<crate::mailbox::ShardSender>,
    pub slot_states: Rc<RefCell<hashbrown::HashMap<u16, crate::shard::SlotState>>>,
    pub slot_owners: Rc<RefCell<Vec<usize>>>,
    pub aof: Option<Rc<RefCell<crate::aof::AofWriter>>>,
    pub pubsub: Rc<RefCell<crate::pubsub::PubSubHub>>,
    pub tx_lock: Rc<RefCell<Option<u64>>>,
    pub tx_waiters: Rc<RefCell<std::collections::VecDeque<(u64, flume::Sender<()>)>>>,
    pub is_saving: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub last_save_time: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub db_dir: std::path::PathBuf,
    pub is_auto_tiering: Rc<Cell<bool>>,
    pub notify_channel_pool: Rc<RefCell<Vec<(flume::Sender<()>, flume::Receiver<()>)>>>,
    pub remote_responder_pool: Rc<RefCell<Vec<std::sync::Arc<crate::mailbox::BatchResponder>>>>,
    pub mget_batch_pool: Rc<RefCell<Vec<Vec<Vec<(usize, Bytes)>>>>>,
    pub mset_batch_pool: Rc<RefCell<Vec<Vec<Vec<(Bytes, Bytes)>>>>>,
    pub mget_desc_pool: Rc<RefCell<Vec<std::sync::Arc<crate::mailbox::ScatterMgetDescriptor>>>>,
    pub mset_desc_pool: Rc<RefCell<Vec<std::sync::Arc<crate::mailbox::ScatterMsetDescriptor>>>>,
    pub pubsub_responder_pool: Rc<RefCell<Vec<(flume::Sender<usize>, flume::Receiver<usize>)>>>,
    pub presence_table: std::sync::Arc<crate::pubsub::ShardedPresenceTable>,
    pub tier_stats: std::sync::Arc<crate::tiering::TieringStats>,
}
```

The struct has grown considerably beyond the original five fields
(`shard_id`/`num_shards`/`port`/`local_db`/`senders`). Grouped by purpose:

- **Routing/topology**: `base_port` (the port of shard 0, used to compute `-MOVED`
  targets as `base_port + target_shard`), `cluster_enabled` (per-instance flag, set once
  in `run_shard_worker`/`server.rs`, checked alongside the process-wide
  `crate::cluster::HAS_ACTIVE_CLUSTER` atomic — see §4.1), `slot_states` (now a **sparse**
  `hashbrown::HashMap<u16, SlotState>` rather than a dense 16,384-entry `Vec` — slots not
  present in the map are implicitly `Stable`, avoiding ~8 KB × 16,384 = redundant memory
  across shards for the overwhelming majority of deployments that never enter cluster
  mode), `slot_owners` (a dense per-slot shard-ownership override, seeded from
  `slot_to_shard` and mutated by live-migration `SetSlotOwner`).
- **`senders: Vec<crate::mailbox::ShardSender>`**: one send handle per shard (including
  this one, though a shard never sends to itself), each wrapping a dedicated SPSC ring —
  see "The mesh transport" below.
- **Persistence/tiering**: `aof`, `is_saving`, `last_save_time`, `db_dir`,
  `is_auto_tiering` (re-entrancy guard for the auto-tiering trigger), `tier_stats`
  (process-wide, `Arc`-shared tiering counters).
- **Pub/sub**: `pubsub`, `presence_table`.
- **Cross-shard transactions**: `tx_lock`, `tx_waiters` — see §4.6.
- **Pools that eliminate per-call allocation on hot paths** (all `Rc<RefCell<...>>`,
  since a shard is single-threaded there is no need for interior synchronization beyond
  the borrow check): `notify_channel_pool` (spare `flume::bounded(1)` wake-up channel
  pairs reused by `FastGet`/`FastSet`/`MGET`/`MSET`), `remote_responder_pool` (spare
  `BatchResponder`s for the single-command `execute_remote` fallback), `mget_batch_pool`/
  `mset_batch_pool` (spare per-shard bucket vectors for partitioning multi-key
  commands), `mget_desc_pool`/`mset_desc_pool` (spare `ScatterMget`/`ScatterMsetDescriptor`
  instances), `pubsub_responder_pool`.

#### `ShardMessage` (`src/shard.rs`) — the wire format, 66 variants

`ShardMessage` is the payload type carried over the mesh (see "The mesh transport" below).
It has grown from an
original ~12 variants to 66, one per cross-shard operation family. Representative slice,
grouped by reply mechanism:

```rust
pub enum ShardMessage {
    // Connection load-balancing (SO_REUSEPORT is uneven; see src/conn_balance.rs)
    AdoptConnection { fd: std::os::unix::io::RawFd, peer: std::net::SocketAddr },

    // Legacy per-key ops: still a fresh flume::bounded(1) reply per call (§4.2)
    Del { key: Bytes, responder: flume::Sender<bool> },
    Exists { key: Bytes, responder: flume::Sender<bool> },
    IncrBy { key: Bytes, delta: i64, responder: flume::Sender<Result<i64, String>> },
    Expire { key: Bytes, duration: Duration, responder: flume::Sender<bool> },
    // ...Persist/Ttl/DumpKey/RandomKey/ExpireTime/Keys/Scan/ClientList/CountKeysInSlot/
    // GetKeysInSlot/DelKeys/ActiveDefrag follow the same shape.

    // Also present for backward compatibility / the unit-test harness: constructed only
    // in #[cfg(test)] code today (see the correction notice in §4.3).
    Get { key: Bytes, responder: flume::Sender<Option<Bytes>> },
    Set { key: Bytes, value: Bytes, expire_in: Option<Duration>, responder: flume::Sender<()> },
    Mget { keys: Vec<(usize, Option<Bytes>)>, responder: flume::Sender<Vec<(usize, Option<Bytes>)>> },
    Mset { pairs: Vec<(Bytes, Bytes)>, responder: flume::Sender<Vec<(Bytes, Bytes)>> },

    // Shared-memory descriptor variants — the production hot paths (§4.2, §4.3)
    FastGet { descriptor: std::sync::Arc<crate::mailbox::FastGetDescriptor> },
    FastSet { descriptor: std::sync::Arc<crate::mailbox::FastSetDescriptor> },
    ScatterMget { shard_id: usize, keys: Vec<(usize, Bytes)>, descriptor: std::sync::Arc<crate::mailbox::ScatterMgetDescriptor> },
    ScatterMset { shard_id: usize, pairs: Vec<(Bytes, Bytes)>, descriptor: std::sync::Arc<crate::mailbox::ScatterMsetDescriptor> },
    Batch {
        items: Vec<(usize, u64, Command)>,
        results: Vec<(usize, CompactResp)>,
        responder: std::sync::Arc<crate::mailbox::BatchResponder>,
        is_resp3: bool,
    },

    // Cluster/migration, replication, tiering, pub/sub, search, and sticky-key control
    // messages, each with its own flume::Sender<T> reply — SetSlotState, SetSlotOwner,
    // AcquireTxLock, ReleaseTxLock, ExecuteReplicaCmd, TierSpill/TierLoad/TierCool/
    // TierDecommit/TierGc/TierSnapshot/TierSpillAll, Publish/Spublish/Ssubscribe/
    // Sunsubscribe/PubsubChannels/PubsubShardchannels/PubsubNumsub/PubsubShardnumsub/
    // PubsubNumpat/RemoveClientPubSub, InitSearchIndex/DropSearchIndex/SearchQuery,
    // SyncAof, SaveRdbChunk, RestoreRdbChunk, RewriteAof, JsonMget, FlushCommandStats,
    // ResetCommandStats, GetUsedMemory, StreamColdRead, FlushSlots, Stick, Unstick,
    // IsSticky, Delex, NotifyList (fire-and-forget, no responder at all).
}
```

**Important nuance verified against the current source**: `ShardMessage::Get`/`Set`/
`Mget`/`Mset` (the plain, `flume::Sender`-based variants) are still defined and still
matched in `src/server.rs`'s receive loop, but grepping the codebase shows the *sending*
side (`self.senders[target].send(...)`) constructs them only inside `#[cfg(test)]` code
in `src/router.rs`. Production code paths (`Router::get`/`set`/`mget`/`mset`/
`begin_mget_resp`/`begin_mset`) exclusively construct `FastGet`/`FastSet`/`ScatterMget`/
`ScatterMset` instead. The older variants are effectively legacy/test-only today, not
dead code (they're still compiled and reachable) but not exercised by real traffic.

**`CompactResp` (`src/shard.rs`)** — a small-buffer-optimized RESP reply type, now four
variants (grown from the original two):

```rust
pub enum CompactResp {
    Small { len: u8, data: [u8; 30] },   // inline: :123\r\n, +OK\r\n, $-1\r\n, ...
    Big(Vec<u8>),                        // fallback for anything > 30 bytes
    Bulk(Bytes),                         // a bulk string backed by a refcounted Bytes
    Array1Bulk(Bytes),                   // a one-element RESP array wrapping one bulk string
}
```

`Small` covers the overwhelming majority of squashed-batch replies (`OK`, small
integers, `NULL`, short bulk strings) with associated `const` values (`CompactResp::OK`,
`INT_0`, `INT_1`, `NULL`, `EMPTY_ARRAY`) built at compile time to skip formatting
entirely on those paths. `Bulk`/`Array1Bulk` avoid re-copying an already-`Bytes`-backed
value (e.g. a `GET` hit) into a fresh heap buffer just to satisfy the enum; `Big` is the
fallback for anything that doesn't fit either shape.

**`is_resp3: bool` on `Batch`**: a remote shard executing a batched command has no direct
knowledge of which protocol the originating client negotiated (RESP2 vs. RESP3 differ in
null/boolean/double encoding), so the flag is read from the thread-local
`crate::connection::CURRENT_CLIENT_RESP3` at send time and carried in the message so the
*executing* shard serializes the reply correctly, then the flag is re-applied
(`CURRENT_CLIENT_RESP3.set(is_resp3)`) on the receiving shard before formatting.

#### `SlotState` — live cluster migration states

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotState {
    Stable,
    Migrating(String),
    Importing(String),
    Moved(String),
}
```

Stored sparsely in `Router::slot_states: HashMap<u16, SlotState>`; a slot absent from the
map is `Stable` by convention (`Router::get_slot_state` returns `SlotState::Stable` on a
miss). See §4.5 for which code paths consult it.

#### `ShardDb` (`src/shard.rs`) — the per-core state container

```rust
pub struct ShardDb {
    pub table: crate::table::RudisTable,
    pub port: u16,
    pub shard_id: usize,
    pub tier_manager: Option<std::rc::Rc<crate::tiering::ShardTierManager>>,
    pub vector_indexes: std::collections::HashMap<String, crate::vector::HnswIndex>,
    pub crdt_store: crate::crdt::CrdtStore,
    pub json_store: crate::json::JsonStore,
    pub probabilistic_store: crate::probabilistic::ProbabilisticStore,
    pub sticky_keys: hashbrown::HashSet<Bytes>,
    pub search_indices: std::collections::HashMap<String, crate::search::InvertedIndex>,
}
```

Two fields beyond the previously documented set: `shard_id` (so a `ShardDb` can identify
itself without threading the id through every method) and `search_indices` (RediSearch
inverted indexes, Component 09).

`ShardDb` is mostly a thin delegate layer — well over 100 `#[inline] pub fn` methods
(`hset`, `lpush`, `zadd`, `xadd`, `pfadd`, `bitcount`, ...) that just forward to
`self.table.*`. The exceptions are `set`/`set_extended`/`del`, which additionally check
`self.table.is_tiered(&key)` / `is_cooled(&key)` first and update `tier_manager`'s stats
(and cancel any in-flight async stash operation via `op_manager.cancel_pending_stash`)
when overwriting or deleting a key that currently lives (partially) on NVMe — the
storage engine (`RudisTable`, documented in Component 05) and the tiering engine have to
stay in sync on every mutation, and `ShardDb::set`/`del` is where that happens.

#### The mesh transport (`src/mailbox.rs`)

```rust
#[repr(align(64))]
pub struct CachePadded<T>(pub T);   // pads a value to its own 64-byte cache line

#[repr(align(64))]
pub struct SpscQueue<T> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    buffer: Box<[UnsafeCell<Option<T>>]>,
    capacity: usize,          // next_power_of_two(requested capacity)
    mask: usize,
    overflow: std::sync::Mutex<std::collections::VecDeque<T>>,
    has_overflow: AtomicBool,
}

#[derive(Clone)]
pub struct ShardSender {
    pub target_shard: usize,
    pub ring: std::sync::Arc<SpscQueue<crate::shard::ShardMessage>>,
    pub target_notify: flume::Sender<()>,
}

#[derive(Clone)]
pub struct ShardReceiver {
    pub shard_id: usize,
    pub incoming_rings: Vec<std::sync::Arc<SpscQueue<crate::shard::ShardMessage>>>,
    pub notify_rx: flume::Receiver<()>,
}

pub fn create_shard_mesh(num_shards: usize) -> (Vec<Vec<ShardSender>>, Vec<ShardReceiver>);
```

`SpscQueue<T>` is a cache-line-aligned lock-free ring buffer sized to the next power of
two above the requested capacity (`mailbox::create_shard_mesh` requests **256** per
ring), with head/tail cursors each padded to their own 64-byte cache line to prevent
false sharing between the producer and consumer. `push`/`pop` are lock-free in the common
case (plain relaxed/acquire/release atomic loads and stores on the cursors); if the ring
is momentarily full, `push` falls back to a `std::sync::Mutex`-guarded
`VecDeque` **overflow queue** so a traffic burst is buffered rather than dropped or
blocked — `has_overflow` is an `AtomicBool` fast-path check so `pop` only pays the mutex
cost while the overflow path is actually active.

`create_shard_mesh(num_shards)` builds a full `num_shards × num_shards` matrix of
`SpscQueue`s — `rings[i][j]` carries messages from producer shard `i` to consumer shard
`j` — plus one `flume::bounded::<()>(1)` notification channel per **consumer** shard,
shared by every producer that targets it. `ShardSender::send` pushes onto its dedicated
ring and calls `target_notify.try_send(())` (never blocks: a full notification channel
just means the target hasn't consumed the previous wake-up yet, which is fine since it
will drain every pending ring entry once it wakes). `ShardReceiver::recv`/`recv_async`
round-robin `try_recv` across every incoming ring before parking on `notify_rx`, and drain
any queued-up notifications (`while notify_rx.try_recv().is_ok() {}`) after waking so a
burst of sends that produced several notifications only causes one wake cycle.

Every shared-memory reply descriptor below is `unsafe impl Send + Sync` by construction:
each is wrapped in an `Arc` and handed to exactly one other shard, which writes only to
disjoint fields/slots that the awaiting shard does not touch until it observes the
corresponding `Ordering::Release`-stored completion flag with an `Ordering::Acquire`
load — the standard single-writer/single-reader handoff pattern, applied per descriptor
rather than through a generic channel:

- **`FastGetDescriptor { key, val: CachePadded<UnsafeCell<Option<Bytes>>>, done: AtomicBool, notify: flume::Sender<()> }`**
  — one remote `GET`. `finish(val)` writes `val`, stores `done = true` (Release), and
  `notify.try_send(())`.
- **`FastSetDescriptor { key, value, expire_in, done: AtomicBool, notify }`** — one remote
  `SET`; `finish()` just flips `done`.
- **`BatchResponder { ready: CachePadded<AtomicBool>, payload: CachePadded<UnsafeCell<Option<(Vec<(usize, u64, Command)>, Vec<(usize, CompactResp)>)>>>, notify_tx, notify_rx }`**
  — the reply slot for a squashed pipeline `Batch` sent to one remote shard.
  `finish(items, results)` stores the payload then `ready = true` (Release);
  `try_take()` acquire-loads `ready` and, if set, takes the payload and clears `ready`
  (used by both the 256-iteration spin-harvest and the async fallback in
  `connection.rs::execute_commands_squashed`, §4.3).
- **`ScatterMgetDescriptor { results: Box<[CachePadded<UnsafeCell<Option<Bytes>>>]>, pending: AtomicUsize, notify: CachePadded<UnsafeCell<flume::Sender<()>>>, recycled_keys: Box<[CachePadded<UnsafeCell<Vec<(usize, Bytes)>>>]>> }`**
  — one descriptor shared by *every* remote shard touched by a single `MGET`. Each remote
  shard calls `write_result(global_idx, val)` for only the indices it owns (a disjoint
  subset of `0..total_keys`), then `finish_shard()`, which does
  `pending.fetch_sub(1, Release)` and signals `notify` only when the count reaches zero —
  i.e. only the *last* shard to finish pays for the wake-up. `recycled_keys` lets each
  shard also hand back the `Vec` it drained its batch into, for pool reuse.
- **`ScatterMsetDescriptor`** — the `MSET` analog, without a `results` array (an `MSET`
  reply is just "done", no per-key payload).

All four descriptor types are pooled per-`Router` (`mget_desc_pool`/`mset_desc_pool`)
rather than allocated fresh per call; `acquire_mget_descriptor`/`acquire_mset_descriptor`
additionally spin briefly (`Arc::strong_count(&desc) == 1`) to confirm no remote shard is
still holding a reference to a pooled descriptor before reusing it — the same "provably
unreferenced and idle" check `connection.rs` applies to pooled `BatchResponder`s before
reuse (see `recycle_conn_scratch`).

---

### 4. Execution Algorithms & Code Logic

#### 4.1 Key Routing

```rust
/// Extracts the hash tag from a key if present (e.g. "{user:1}:profile" -> "user:1").
pub fn extract_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{')
        && let Some(close) = key[open + 1..].iter().position(|&b| b == b'}')
        && close > 0
    {
        return &key[open + 1..open + 1 + close];
    }
    key
}

pub fn key_slot(key: &[u8]) -> u16 {
    let tag = extract_hash_tag(key);
    crc16::State::<crc16::XMODEM>::calculate(tag) % 16384
}

pub fn slot_to_shard(slot: u16, num_shards: usize) -> usize {
    if num_shards <= 1 { 0 } else { ((slot as usize) * num_shards) / 16384 }
}

/// Standalone mode: high-performance 64-bit hashing for key distribution across cores.
/// Cluster mode: real Redis Cluster CRC16 slot mapping.
pub fn target_shard(key: &[u8], num_shards: usize) -> usize {
    if num_shards <= 1 {
        0
    } else if crate::cluster::HAS_ACTIVE_CLUSTER.load(Ordering::Relaxed) {
        slot_to_shard(key_slot(key), num_shards)
    } else {
        let tag = extract_hash_tag(key);
        (crate::table::hash_key(tag) as usize) % num_shards
    }
}
```

**This is the single most important correction to make to this document: routing is not
unconditionally CRC16/16384-slot.** `Router::target_shard(&self, key)` (the instance
method used by most call sites) branches the same way, additionally checking the
per-instance `self.cluster_enabled` flag:

```rust
pub fn target_shard(&self, key: &[u8]) -> usize {
    if self.cluster_enabled || crate::cluster::HAS_ACTIVE_CLUSTER.load(Ordering::Relaxed) {
        let slot = key_slot(key);
        self.target_shard_for_slot(slot)   // slot_owners[slot] — dynamic, migration-aware
    } else {
        target_shard(key, self.num_shards) // free function above — static FxHash % num_shards
    }
}
```

- **Standalone mode (the default — no `cluster-enabled yes`, no active cluster bus)**:
  `crate::table::hash_key(tag)` is `fxhash::hash64(tag)` (the `fxhash` crate, the same
  hasher `RudisTable` itself uses internally), reduced modulo `num_shards`. There is no
  16,384-slot space involved at all in this mode — `key_slot`/`slot_to_shard` are simply
  not called.
- **Cluster mode**: real CRC16/XMODEM (the `crc16` crate) over the hash tag, reduced
  modulo 16,384 to a slot, then `slot_to_shard` maps the slot to a shard via
  multiply-then-divide (`(slot * num_shards) / 16384`), **not modulo**, so each shard
  owns one contiguous slot range — matching what Redis Cluster clients expect from
  `CLUSTER SLOTS`. The instance method additionally routes through the *dynamic*
  `slot_owners` array (`target_shard_for_slot`) rather than the static formula, so a slot
  whose ownership has been overridden by live migration (`set_slot_owner`) routes to the
  new owner.
- A third free function, `target_shard_and_hash(key, num_shards) -> (usize, u64)`, computes
  the routing decision and the storage engine's key hash in one pass (used by `MGET` and
  the pipeline-squashing fast path in `connection.rs`, both of which need the hash anyway
  for a hash-avoiding table lookup) — same cluster/standalone branch, same result as
  calling `target_shard` and `hash_key` separately.

This branch did not exist in earlier revisions of this subsystem, where every mode used
the CRC16/slot scheme unconditionally; it was introduced so that non-cluster deployments
(the common case) do not pay for a 16,384-entry conceptual slot space or produce
Redis-Cluster-shaped hashing behavior they never asked for.

#### 4.2 The Local/Remote Fork — three different reply mechanisms depending on operation

Not every per-key `Router` method uses the same cross-shard reply mechanism today.
Verified against the current source:

**`get`/`set` use pooled shared-memory descriptors (`FastGetDescriptor`/
`FastSetDescriptor`, §3), not a channel per call:**

```rust
pub async fn get(&self, key: Bytes) -> Option<Bytes> {
    let target = target_shard(&key, self.num_shards);
    if target == self.shard_id {
        self.get_local_direct(&key).await
    } else {
        let (tx, rx) = self.acquire_notify_channel();      // pooled, not fresh
        let desc = Arc::new(crate::mailbox::FastGetDescriptor::new(key, tx.clone()));
        let msg = ShardMessage::FastGet { descriptor: desc.clone() };
        let res = if self.senders[target].send(msg).is_ok() {
            if !desc.done.load(Ordering::Acquire) {
                for _ in 0..32 {                            // spin before parking
                    std::hint::spin_loop();
                    if desc.done.load(Ordering::Acquire) { break; }
                }
                if !desc.done.load(Ordering::Acquire) {
                    let _ = rx.recv_async().await;
                }
            }
            while rx.try_recv().is_ok() {}
            unsafe { (*desc.val.get()).take() }
        } else {
            None
        };
        self.release_notify_channel(tx, rx);
        res
    }
}
```

`set` follows the identical shape with `FastSetDescriptor`. `acquire_notify_channel`/
`release_notify_channel` pop/push a `(flume::Sender<()>, flume::Receiver<()>)` pair from
`Router::notify_channel_pool` — since a `Router`'s pools are `Rc`-shared across every
connection on the same shard (never touched from another thread), reuse across calls and
across connections is safe without any additional synchronization.

**`del`/`exists`/`incr_by`/`expire`/`persist`/`ttl`/`dump_key`/`expiretime`/`random_key`/
`scan`/the `Tier*` operations/... still allocate a fresh `flume::bounded(1)` channel per
remote call** — e.g. `del`:

```rust
pub async fn del(&self, key: Bytes) -> bool {
    let target = target_shard(&key, self.num_shards);
    if target == self.shard_id {
        let mut db = self.local_db.borrow_mut();
        let deleted = db.del(&key);
        if deleted {
            db.delete_document_local(&String::from_utf8_lossy(&key));
            if let Some(aof) = &self.aof
                && let Some(bytes) = crate::aof::command_to_resp(&Command::Del(smallvec![key]))
            {
                aof.borrow_mut().append(&bytes);
            }
        }
        deleted
    } else {
        let (tx, rx) = flume::bounded(1);   // fresh allocation, unpooled
        let msg = ShardMessage::Del { key, responder: tx };
        if self.senders[target].send(msg).is_ok() {
            rx.recv_async().await.unwrap_or(false)
        } else {
            false
        }
    }
}
```

This is the accurate, current split: `GET`/`SET` (the two highest-volume operations) got
the pooled shared-memory treatment; the remaining single-key operations did not, and pay
one `flume::bounded(1)` allocation plus a full async suspend/resume per remote call. The
write-path methods (`set`, `del`, `incr_by`, `expire`, `persist`) each append their own
AOF record inline on the local-execution branch, via `crate::aof::command_to_resp`.

**`execute_remote` — the generic single-command fallback used by call sites without a
dedicated `Router` method (e.g. `BZPOPMIN`'s remote retry, `CRDT.DUMP`/`CRDT.MERGE`) —
uses a pooled `BatchResponder` (§3), wrapping the command as a one-item `ShardMessage::Batch`:**

```rust
pub async fn execute_remote(&self, target: usize, cmd: Command) -> Vec<u8> {
    let responder = self.remote_responder_pool.borrow_mut().pop()
        .unwrap_or_else(|| Arc::new(crate::mailbox::BatchResponder::new()));
    let h = crate::connection::cmd_primary_key(&cmd).map(|k| crate::table::hash_key(k)).unwrap_or(0);
    let msg = ShardMessage::Batch {
        items: vec![(0, h, cmd)],
        results: Vec::with_capacity(1),
        responder: responder.clone(),
        is_resp3,
    };
    // ...send, spin up to 48 iterations on responder.try_take(), then await
}
```

`get` additionally has a tiering-aware fast/slow split on the local branch — it checks
the fast in-RAM path first, then (if the key is tiered) whether the shard is under enough
memory pressure to warrant a zero-copy streaming cold read (`stream_cold_read_local`)
versus loading the value back into RAM (`load_local`) before returning it.

#### 4.3 `MGET`/`MSET` — shared-memory scatter-gather via `ScatterMget`/`ScatterMset`

Two entry points exist for cross-shard `MGET`, both ending in the same dispatch shape:
`Router::mget(keys) -> Vec<Option<Bytes>>` (a synchronous dispatch-then-wait call used by,
e.g., Lua scripting) and the split `begin_mget_resp`/`finish_mget_resp` pair (used by the
connection-handling layer so a pipeline can fire several `MGET`s before stalling on the
first — see `docs/design/02_connection_lifecycle.md`). `Router::mset`/`begin_mset`/
`finish_mset` are the `MSET` analogs. All four route each key/pair with
`target_shard_and_hash`/`self.target_shard` respectively (§4.1) — i.e. **through the same
cluster-aware/standalone routing decision every other command uses**, not a separate
scheme.

```rust
// begin_mget_resp — dispatch phase, does not await remote shards
for (idx, key) in keys.into_iter().enumerate() {
    let (target, key_hash) = target_shard_and_hash(key.as_ref(), self.num_shards);
    if target == self.shard_id {
        local_keys.push((idx, key, key_hash));
    } else {
        has_remote = true;
        remote_batches[target].push((idx, key));
    }
}
// Fast path: every key local -> zero channel/descriptor operations at all.
if !has_remote { /* ...write RESP directly, return None... */ }

let (notify_tx, notify_rx) = self.acquire_notify_channel();
let descriptor = self.acquire_mget_descriptor(total_keys, num_remote_shards, notify_tx.clone());
for (target_shard, batch) in remote_batches.iter_mut().enumerate() {
    if !batch.is_empty() {
        let msg = ShardMessage::ScatterMget {
            shard_id: target_shard,
            keys: std::mem::take(batch),
            descriptor: descriptor.clone(),   // one Arc clone per remote shard touched
        };
        if self.senders[target_shard].send(msg).is_err() { descriptor.finish_shard(); }
    }
}
// Local keys execute concurrently with the in-flight remote batches, not after them.
for (idx, key, key_hash) in local_keys {
    descriptor.write_result(idx, self.local_db.borrow_mut().get_with_hash(key.as_ref(), key_hash));
}
```

Each targeted remote shard, on receiving `ScatterMget`, writes its results **directly
into its own disjoint slice of the shared `descriptor.results` array** (via
`write_result(global_idx, val)` — no serialization back over a channel), handles any
tiered/cold keys asynchronously if needed, then calls `descriptor.finish_shard()`, which
decrements `pending: AtomicUsize` and signals the notify channel *only when the count
reaches zero* — so only the last shard to finish pays for a wake-up, and every other
shard's completion is a silent atomic decrement. `finish_mget_resp` spins briefly on
`descriptor.pending == 0` before falling back to `notify_rx.recv_async().await`, then
serializes `descriptor.results` directly into the output buffer and returns both the
descriptor and the notify channel to their pools. `MSET` follows the identical shape with
`ScatterMsetDescriptor` (no `results` array — an `MSET` reply is just "done").

**Pooling, not fresh allocation, throughout**: `mget_batch_pool`/`mset_batch_pool` recycle
the per-shard bucket `Vec`s, `mget_desc_pool`/`mset_desc_pool` recycle the descriptors
themselves (with an `Arc::strong_count(&desc) == 1` spin-check before reuse, to be sure no
straggling remote shard still holds a reference — §3), and `notify_channel_pool` recycles
the wake-up channel. A connection issuing repeated `MGET`/`MSET` calls therefore performs
no heap allocation on the steady-state path once the pools have warmed up.

**Multi-key `DEL` (`Router::del_keys`) uses the same bucket-and-fan-out shape but the
older, unpooled channel mechanism**: one fresh `flume::bounded(1)` per remote shard
touched, all sent before any are awaited, then awaited sequentially. This is a real,
narrower version of the "still allocates a channel per call" pattern in §4.2 — it fans
out to at most `num_shards` channels per call (not per key), which is a much smaller
allocation rate than the pre-fan-out `MGET`/`MSET` design ever had, but has not been
converted to the `mailbox.rs` scatter-gather descriptors that `MGET`/`MSET` now use.

#### 4.4 Redirection for live cluster slot migration

`Router` has real infrastructure for Redis Cluster-style live slot migration:
`slot_owners: Rc<RefCell<Vec<usize>>>` (a per-slot ownership override, seeded from the
static `slot_to_shard` mapping but mutable via `set_slot_owner`), `slot_states` (a sparse
`HashMap<u16, SlotState>` tracking `Migrating`/`Importing`/`Moved` per slot, mutated via
`set_slot_state`), and a helper method with the same shape as the logic actually used:

```rust
pub fn check_slot_redirection(&self, slot: u16, key_exists: bool, asking: bool) -> Result<(), String> {
    match self.get_slot_state(slot) {
        SlotState::Migrating(target) => if !key_exists { return Err(format!("-ASK {} {}\r\n", slot, target)); },
        SlotState::Importing(source) => if !asking { return Err(format!("-MOVED {} {}\r\n", slot, source)); },
        SlotState::Moved(target) => return Err(format!("-MOVED {} {}\r\n", slot, target)),
        SlotState::Stable => {}
    }
    Ok(())
}
```

Grepping the whole codebase confirms this specific method, `check_slot_redirection`, is
**never called anywhere** in production code — only defined. `connection.rs`'s
`execute_command` instead inlines the identical `SlotState` match directly (not via this
helper), gated on `cmd_primary_key(&cmd)`:

```rust
if let Some(key) = cmd_primary_key(&cmd) {
    let slot = key_slot(key);
    match router.get_slot_state(slot) {
        SlotState::Moved(target) => { /* -MOVED slot target */ return false; }
        SlotState::Importing(source) => if !is_asking { /* -MOVED slot source */ return false; },
        SlotState::Migrating(target) => {
            if !router.exists(key.clone()).await { /* -ASK slot target */ return false; }
        }
        SlotState::Stable => {
            // if router.cluster_enabled and slot_owners disagrees with this shard,
            // or the cluster-bus gossip table (Component 11's ClusterHub::my_slots)
            // says a different node owns it, emit -MOVED to that owner instead
        }
    }
}
```

So real `-MOVED`/`-ASK` redirects genuinely are sent from the normal single-command path.
The gap that does hold: this check lives only in `execute_command`, the
single-command/non-squashed-fallback path (Component 02) — the pipelined
`execute_commands_squashed` fast path gates *whether a pipeline is eligible for
squashing* on slot state (a non-`Stable` or foreign-owned slot defeats squashing and
falls back to the unsquashed path above), but **`MGET`/`MSET`'s own dispatch
(`begin_mget_resp`/`begin_mset`, §4.3) never consults `slot_states` at all** — multi-key
commands have no single "primary key" for `cmd_primary_key` to key off of, so they are
not covered by the redirect check that protects every single-key command.

**Two genuinely different routing entry points coexist in `router.rs`, verified by
call site**: the free functions `target_shard`/`target_shard_and_hash` (§4.1 — *static*,
compute `slot_to_shard` directly in cluster mode and never consult `slot_owners`), and the
instance method `Router::target_shard`, which internally calls `target_shard_for_slot`
(*dynamic* — indexes `self.slot_owners`, reflecting any live `set_slot_owner` override).
Grepping every call site today:

- **Static** (free function, ignores live migration overrides): `get`, `set`, `expire`,
  `persist`, `ttl`, `incr_by`, `exists` — and, notably, the pipelined dispatch phase
  `begin_mget_resp`.
- **Dynamic** (instance method, honors `slot_owners`): `expiretime`, `del_keys`, `mget`
  (the whole-call synchronous variant used outside the pipeline), `begin_mset`/`mset`,
  `json_mget`.

The practical consequence: during an active live slot migration, **the same `MGET`
command can route differently depending on which of the two `Router` entry points
handles it** — `Router::mget` (dynamic, honors `set_slot_owner`) versus
`begin_mget_resp`/`finish_mget_resp` (static, does not) — and single-key commands are
inconsistent with most multi-key commands more generally. This is a real, currently
unresolved routing inconsistency, not merely a theoretical one; see §7.

#### 4.5 Cross-shard `SCAN` cursor encoding

`Router::scan` packs which shard to resume from into the cursor itself so a stateless
multi-shard `SCAN` can be resumed correctly between calls:

```rust
pub async fn scan(&self, cursor: u64, pattern: Option<&[u8]>, count: usize) -> (u64, Vec<Bytes>) {
    let shard_id = (cursor >> 32) as usize;
    let slot_idx = (cursor & 0xFFFF_FFFF) as usize;
    ...
    let next_cursor = if next_slot == 0 {
        if shard_id + 1 < self.num_shards { ((shard_id + 1) as u64) << 32 } else { 0 }
    } else {
        ((shard_id as u64) << 32) | (next_slot as u64)
    };
    (next_cursor, keys)
}
```

High 32 bits = which shard the client is currently scanning; low 32 bits = that shard's
own internal cursor. When a shard reports it's exhausted (returns slot `0`), the next
call advances to `shard_id + 1`; reaching the last shard wraps the whole scan back to
cursor `0`.

#### 4.6 Cross-shard transaction locking (`MULTI`/`EXEC` across shards)

```rust
pub async fn acquire_tx_locks(&self, shard_ids: &[usize], tx_id: u64) {
    for &sid in shard_ids {
        if sid == self.shard_id {
            let rx = {
                let mut lock = self.tx_lock.borrow_mut();
                if lock.is_none() { *lock = Some(tx_id); None }
                else {
                    let (tx, rx) = flume::bounded(1);
                    self.tx_waiters.borrow_mut().push_back((tx_id, tx));
                    Some(rx)
                }
            };
            if let Some(rx) = rx { let _ = rx.recv_async().await; }
        } else {
            // ...send ShardMessage::AcquireTxLock to shard `sid` and await it
        }
    }
}
```

A single advisory lock per shard (`tx_lock: Option<u64>`) with a FIFO wait queue
(`tx_waiters`), acquired across every shard a transaction's keys touch — in ascending
shard-ID order — before the transaction executes, released in reverse order afterward
(`release_tx_locks`). This is Rudis's mechanism for atomic multi-shard transactions: a
simple mutual-exclusion lock per shard, not a full multi-version scheduler.

---

### 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): calls `target_shard_of_cmd`/
  `target_shard_and_hash_of_cmd`/`Router` methods to decide local vs. remote dispatch for
  every command; the pipelined hot path (`execute_commands_squashed`) builds
  `ShardMessage::Batch` directly, addressed per shard via a pooled
  `Vec<Arc<BatchResponder>>` (one per shard, reused for the connection's lifetime — §3),
  rather than going through `Router`'s per-op methods. `Command::Mget`/`Mset` call
  `router.begin_mget_resp(...)`/`await`/`router.begin_mset(...)`/`await` (§4.3) — the
  split dispatch/finish pair, not a single `router.mget`/`router.mset` call, specifically
  so a pipeline containing several `MGET`/`MSET`s can have them all in flight before the
  first is awaited.
- **`src/server.rs`** (Component 01): owns the receive side of every `ShardMessage`
  variant, matched in the per-shard event loop (`ShardReceiver::recv_async`, §3). The
  `ScatterMget`/`ScatterMset` handlers branch on whether the shard has a `tier_manager`
  at all, taking a fully synchronous `db.get()`-per-key loop when tiering is disabled and
  spawning an async cold-read path per tiered key when it's enabled; `Batch` similarly
  branches into a synchronous fast path or a `monoio::spawn`-ed async path depending on
  whether any item in the batch needs a tiered read.
- **`src/aof.rs`**: `Router` holds an optional `AofWriter` and calls
  `crate::aof::command_to_resp` to append write commands.
- **`src/tiering.rs`**: `Router::spill_local`/`load_local`/`cool_local`/`decommit_local`/
  `check_auto_tier` orchestrate NVMe tiering, dispatched cross-shard via `TierSpill`/
  `TierLoad`/`TierCool`/`TierDecommit`/`TierGc`/`TierSnapshot` messages.
- **`src/pubsub.rs`**: `Router::publish`/`pubsub_channels`/`pubsub_numsub`/`pubsub_numpat`
  publish locally then fan out to every other shard and merge results.
- **`src/cluster.rs`**: every `cluster_*` method on `Router` is a thin passthrough to a
  singleton `crate::cluster::get_cluster_hub(self.port)`.
- **`src/replication.rs`**: `Router::execute_replica_command` applies a command received
  from a replication stream against the correct shard (with `FLUSHALL`/`MSET`/`DEL`
  special-cased since they don't have one single target shard).
- **`src/table.rs`** (Component 05): `ShardDb.table: RudisTable` is the actual storage
  engine; everything else on `ShardDb` is a separate, later-added subsystem
  (`tier_manager`, `vector_indexes`, `crdt_store`, `json_store`,
  `probabilistic_store`) living alongside it, not folded into `RudisValue`.

---

### 7. Future Improvements

- **High — unify the two routing entry points (§4.1/§4.4).** `get`/`set`/`expire`/
  `persist`/`ttl`/`incr_by`/`exists`/`begin_mget_resp` route through the static free
  functions (`target_shard`/`target_shard_and_hash`), which never consult
  `slot_owners`; `expiretime`/`del_keys`/`mget`/`begin_mset`/`mset`/`json_mget` route
  through the dynamic instance method `Router::target_shard`, which does. This means the
  *same* `MGET` command can route differently depending on whether it goes through
  `Router::mget` or `begin_mget_resp`/`finish_mget_resp`, and single-key commands can
  disagree with most multi-key commands, during an active live slot migration. Pick one
  routing entry point (most likely the dynamic one, since it is the one that can actually
  reflect `set_slot_owner`) and route every command — single-key, multi-key, pipelined and
  not — through it.
- **High — `MGET`/`MSET` never check `slot_states` for `-MOVED`/`-ASK` redirection at
  all (§4.4).** `execute_command`'s slot-migration check is gated on
  `cmd_primary_key(&cmd)`, which has no arm for `Mget`/`Mset` (multi-key commands don't
  have one primary key) — so unlike every single-key command, a live `MGET`/`MSET`
  against a migrating/moved slot never redirects, squashed or not. Fixing this needs a
  per-key (not per-command) redirect check inside `Router::mget`/`begin_mget_resp`/
  `mset`/`begin_mset` themselves, likely alongside the routing-entry-point unification
  above.
- **Medium — delete `Router::check_slot_redirection` or make it the single source of
  truth (§4.4).** The real redirect logic lives duplicated inline in
  `connection.rs::execute_command` while this near-identical helper method sits unused in
  `router.rs`. Either delete the dead helper (simplest — removes a maintenance trap where
  someone "fixes" the wrong copy) or refactor `connection.rs` to call it, eliminating the
  duplication risk either way.
- **Medium — retire the unused `ShardMessage::Get`/`Set`/`Mget`/`Mset` variants, or
  document why they're kept (§3).** They are constructed only in `#[cfg(test)]` code
  today; production traffic exclusively uses `FastGet`/`FastSet`/`ScatterMget`/
  `ScatterMset`. Keeping dead-in-production variants and their `server.rs` match arms
  around is a small but real maintenance and enum-size cost, and a source of confusion for
  a contributor grepping for how `GET`/`SET` actually cross the mesh.
- **Medium — convert `Router::del_keys`'s remote fan-out to the `mailbox.rs`
  scatter-gather pattern (§4.3).** It already buckets keys by shard and dispatches
  concurrently, but still allocates one fresh `flume::bounded(1)` channel per remote
  shard touched, unlike `MGET`/`MSET`'s pooled `ScatterMget`/`ScatterMsetDescriptor`
  path.
- **Low — reduce the duplicated local/remote-fork method bodies (§4.2).** The
  per-operation methods (`get`/`set`/`del`/`exists`/...) are intentionally monomorphic
  rather than generic, which is a reasonable trade for straight-line, easy-to-profile
  code — but a thin macro generating the boilerplate (target-shard computation,
  local-vs-remote branch, channel/descriptor acquire-and-release) from a
  one-line-per-command table would keep the monomorphic-dispatch benefit while cutting
  the repeated structure down to one place to get right.

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: `Router::slot_states` is a sparse `HashMap<u16, SlotState>`, not a dense
  16,384-entry `Vec`, specifically to avoid paying that memory cost on every shard of
  every deployment that never enables cluster mode. A slot absent from the map is
  `Stable` by convention (`get_slot_state` returns `SlotState::Stable` on a miss) — don't
  assume every slot has an explicit entry when reading or extending this code.
* **Gotcha 2**: Each cross-shard `SpscQueue` ring has capacity 256 (rounded to the next
  power of two — it already is one), backed by a mutex-guarded overflow `VecDeque` so a
  traffic burst that temporarily fills the ring is buffered rather than dropped or
  blocked. The overflow path is rare in a healthy system; hitting it consistently under
  load between two specific shards is a sign the ring capacity or the consumer's drain
  rate deserves attention.
* **Gotcha 3**: `MGET`/`MSET` genuinely execute a parallel scatter-gather across every
  shard the key/pair set touches (`ScatterMget`/`ScatterMsetDescriptor`), with results
  written directly into shared memory rather than serialized back over a channel — but
  multi-key `DEL` (`Router::del_keys`) still uses the older per-shard `flume::bounded(1)`
  fan-out (§4.3/§7). Don't assume all three multi-key command families share one
  implementation.
* **Gotcha 4**: Standalone (non-cluster) deployments — the default — route keys with
  `fxhash::hash64(hash_tag) % num_shards`, not CRC16/16384-slot hashing. The CRC16 slot
  scheme only activates once cluster mode is active (`cluster-enabled yes`, or the
  cluster bus reports one via `crate::cluster::HAS_ACTIVE_CLUSTER`). Don't assume
  `CLUSTER KEYSLOT`-style slot math applies to a non-cluster Rudis instance's internal
  shard placement.
* **Gotcha 5**: `Arc<BatchResponder>`/`Arc<ScatterMgetDescriptor>`/etc. pooling is only
  safe to reuse once a strong-count check confirms no remote shard still holds a clone —
  see `acquire_mget_descriptor`'s `Arc::strong_count(&desc) == 1` spin and
  `connection.rs::recycle_conn_scratch`'s equivalent check on `BatchResponder`. Handing a
  "reused" descriptor to a new request while a stale remote write is still in flight would
  let a late reply silently corrupt a different request's results — treat that
  strong-count check as load-bearing if you touch this pooling code.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
