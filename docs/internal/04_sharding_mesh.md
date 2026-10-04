# Component 04: Sharding Architecture & Cross-Core Mesh (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/router.rs` (4,483 lines), `src/shard.rs` (4,137 lines),
> `src/mailbox.rs` (931 lines, cross-shard IPC primitives)
> **High-Level Design Spec**: [`docs/design/04_sharding_mesh.md`](../design/04_sharding_mesh.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

> **Correction notice (this revision)**: all three files have grown substantially since the
> previous pass (`router.rs` +286 lines, `shard.rs` +1,469 lines, `mailbox.rs` +203 lines),
> and the growth is not cosmetic. Three genuinely new lock-free mechanisms now sit on the
> hot cross-shard path and did not exist in the prior revision of this document:
> 1. **A 3-state "Parker" handshake** (`DESC_RUNNING`/`DESC_COMPLETED`/`DESC_SLEEPING`) on
>    `ScatterMgetDescriptor`/`ScatterMsetDescriptor`, replacing a plain `AtomicBool done` +
>    unconditional notify-on-every-shard-finish with a scheme where the *waiter* only pays
>    for a `flume` wake-up if it actually parks (§3.5).
> 2. **A 4-state "Parker" handshake** (`BATCH_IDLE`/`BATCH_RUNNING`/`BATCH_SLEEPING`/
>    `BATCH_COMPLETED`) on `BatchResponder`, combined with **direct-pointer writes into the
>    caller's own response buffer** (`responses_ptr: AtomicPtr<CompactResp>` +
>    `write_slot(idx, resp)`) — a remote shard executing a squashed pipeline batch no
>    longer serializes a `Vec<(usize, CompactResp)>` back to the caller at all; it writes
>    each reply straight into the caller's pre-allocated `Vec<CompactResp>` through a raw
>    pointer (§3.5, §4.3).
> 3. **A "sleeping flag" fast path** on the `ShardSender`/`ShardReceiver` mesh itself: a
>    producer now skips the `flume` notification channel (and the mutex inside it) entirely
>    whenever the consumer is not actually parked, which is the common case under load
>    (§3.4).
>
> The previously-documented finding that two different key-routing entry points (a static
> free function and a dynamic `Router` method) could disagree during live cluster slot
> migration has **moved, not disappeared**: `Router`'s own per-operation methods were
> consolidated to route dynamically almost everywhere (only `Router::del` still uses the
> static free function). The live disagreement that remains is one layer up, between
> `Router`'s dynamic routing and a pair of **static** helper functions in
> `src/connection.rs` (`target_shard_of_cmd`/`target_shard_and_hash_of_cmd`) that the
> squashed-pipeline hot path uses to decide local-vs-remote for every command — see §4.2.
>
> `flume` channels are still used throughout for lower-traffic per-operation methods
> (`DEL`, `EXISTS`, `EXPIRE`, …), which still allocate a fresh `flume::bounded(1)`
> request/reply channel per remote call — see §4.3 for exactly which operations fall into
> which category, re-verified against the current source.

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/router.rs` | Key routing, the `Router` struct, one method per Redis command family, descriptor/channel pooling | `Router`, `target_shard`, `target_shard_and_hash`, `key_slot`, `slot_to_shard`, `pattern_hash_slot`, `extract_hash_tag`, `get`/`set`/`del`/…, `mget`/`mset`/`begin_mget_resp`/`finish_mget_resp`, `begin_mset`/`finish_mset`, `acquire_tx_locks`/`release_tx_locks`, `scan` |
| `src/shard.rs` | Per-shard state container and the cross-shard wire format | `ShardDb` (238 public methods), `ShardMessage` (66 variants), `CompactResp` (5 variants), `SlotState` |
| `src/mailbox.rs` | Lock-free cross-shard transport: SPSC ring buffers, the shard mesh, and every shared-memory reply descriptor | `SpscQueue<T>`, `ShardSender`, `ShardReceiver`, `create_shard_mesh`, `CachePadded<T>`, `FastGetDescriptor`, `FastSetDescriptor`, `BatchResponder`, `ScatterMgetDescriptor`, `ScatterMsetDescriptor` |

`src/connection.rs` (Component 02) is not part of this subsystem's source-file list, but two
free functions defined there — `target_shard_of_cmd`/`target_shard_and_hash_of_cmd` — are
load-bearing for how the squashed-pipeline fast path routes every command, and are
discussed in depth in §4.2 because they are the crux of this revision's central finding.

---

## 2. High-Level Flow

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
                    │                          or target_shard_for_slot(slot)
                    │                           (dynamic, honors live migration
                    │                              via Router::slot_owners)
                    └────────────────┬────────────────────────┘
                                     ▼
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
              Local Shard?                      Remote Shard?
                    │                                 │
       Direct ShardDb mutation             ShardMessage pushed onto a
     (+ AOF append, + tiering hooks)       dedicated mailbox::SpscQueue,
                                       target shard notified via flume IFF
                                       it is actually parked (sleeping flag,
                                                §3.4)
                                                        │
                                                        ▼
                                          Peer shard's event loop pops the
                                          message from its incoming ring
                                          (ShardReceiver::recv_async),
                                          executes it against its own
                                          ShardDb, and replies either via
                                          the message's own
                                          `flume::Sender<T>`, by writing
                                          into a shared mailbox.rs
                                          descriptor and flipping its
                                          completion state, or — for
                                          squashed batches and single-shot
                                          `execute_remote` calls — by
                                          writing the RESP reply *directly*
                                          into the caller's own buffer
                                          through a raw pointer
                                          (`BatchResponder::write_slot`)
```

---

## 3. Component Architecture & Data Structures

### 3.1 `Router` (`src/router.rs:156-183`)

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

**Unchanged field-for-field from the previous revision of this document.** The ~300-line
growth in `router.rs` is entirely in the *method* surface (new search/vector/agent
passthroughs, the `pattern_hash_slot` helper, `MgetInFlight`/`MsetInFlight` handle types,
and the Parker-handshake plumbing in the MGET/MSET paths), not in `Router`'s own state.

Grouped by purpose (unchanged from before):

- **Routing/topology**: `base_port` (port of shard 0, used to compute `-MOVED` targets as
  `base_port + target_shard`), `cluster_enabled` (per-instance flag, checked alongside the
  process-wide `crate::cluster::HAS_ACTIVE_CLUSTER` atomic), `slot_states` (sparse
  `HashMap<u16, SlotState>` — absent slot ⇒ `Stable`), `slot_owners` (dense
  `Vec<usize>` of 16,384 entries, seeded from `slot_to_shard` in `Router::new`, mutated by
  `set_slot_owner` during live migration).
- **`senders: Vec<ShardSender>`**: one send handle per shard (including this one, though a
  shard never sends to itself).
- **Persistence/tiering**: `aof`, `is_saving`, `last_save_time`, `db_dir`,
  `is_auto_tiering`, `tier_stats`.
- **Pub/sub**: `pubsub`, `presence_table`.
- **Cross-shard transactions**: `tx_lock`, `tx_waiters` (§4.6, unchanged).
- **Hot-path pools** (all `Rc<RefCell<...>>`, safe without interior sync since a shard is
  single-threaded): `notify_channel_pool`, `remote_responder_pool`, `mget_batch_pool`/
  `mset_batch_pool`, `mget_desc_pool`/`mset_desc_pool`, `pubsub_responder_pool`.

#### `MgetInFlight` / `MsetInFlight` (`src/router.rs:136-150`)

New named handle types (the prior revision described the begin/finish split only
informally). These are what `Router::begin_mget_resp`/`begin_mset` hand back to the caller
and `Router::finish_mget_resp`/`finish_mset` consume:

```rust
pub struct MgetInFlight {
    descriptor: Arc<crate::mailbox::ScatterMgetDescriptor>,
    notify_tx: flume::Sender<()>,
    notify_rx: flume::Receiver<()>,
    total_keys: usize,
}

pub struct MsetInFlight {
    descriptor: Arc<crate::mailbox::ScatterMsetDescriptor>,
    notify_tx: flume::Sender<()>,
    notify_rx: flume::Receiver<()>,
}
```

Their fields are private to `router.rs`; a connection-handling caller (`connection.rs`)
only ever holds the opaque handle between `begin_*` and `finish_*` — see §4.4.

### 3.2 `ShardMessage` (`src/shard.rs:313-593`) — the wire format, 66 variants (re-counted)

Grouped by reply mechanism, verified variant-by-variant against the current enum
definition:

```rust
pub enum ShardMessage {
    // Connection load-balancing (SO_REUSEPORT is uneven; see src/conn_balance.rs)
    AdoptConnection { fd: std::os::unix::io::RawFd, peer: std::net::SocketAddr },

    // Legacy/test-only per-key ops — still defined and matched in src/server.rs's
    // receive loop, but grepping the whole codebase shows the *sending* side
    // (`self.senders[target].send(...)`) constructs them only inside `#[cfg(test)] mod
    // tests` in router.rs (tests start at router.rs:3357). Production code exclusively
    // uses FastGet/FastSet/ScatterMget/ScatterMset instead. Unchanged finding.
    Get { key: Bytes, responder: flume::Sender<Option<Bytes>> },
    Set { key: Bytes, value: Bytes, expire_in: Option<Duration>, responder: flume::Sender<()> },
    Mget { keys: Vec<(usize, Option<Bytes>)>, responder: flume::Sender<Vec<(usize, Option<Bytes>)>> },
    Mset { pairs: Vec<(Bytes, Bytes)>, responder: flume::Sender<Vec<(Bytes, Bytes)>> },

    // Other fresh-flume-channel-per-call single-key ops (§4.3): Del, DelKeys,
    // ActiveDefrag, Exists, IncrBy, Expire, Persist, Ttl, CountKeysInSlot,
    // GetKeysInSlot, ClientList, JsonMget, DelIfUnchanged, RandomKey, ExpireTime, Delex,
    // IsSticky, Stick, Unstick, FlushSlots, TierSpill/TierLoad/TierSpillAll/TierCool/
    // TierDecommit/TierGc/TierSnapshot, GetUsedMemory, StreamColdRead,
    // AcquireTxLock/ReleaseTxLock, RestoreRdbChunk, ExecuteReplicaCmd, SyncAof,
    // FlushCommandStats, ResetCommandStats, SaveRdbChunk, RewriteAof,
    // Publish/Spublish/Ssubscribe/Sunsubscribe/PubsubChannels/PubsubShardchannels/
    // PubsubNumsub/PubsubShardnumsub/PubsubNumpat/RemoveClientPubSub,
    // InitSearchIndex/DropSearchIndex/SearchQuery, Keys, Scan, SetSlotState,
    // SetSlotOwner, NotifyList (fire-and-forget, no responder at all).

    // Shared-memory descriptor variants — the production hot paths (§3.5, §4.3-§4.4)
    FastGet { descriptor: std::sync::Arc<crate::mailbox::FastGetDescriptor> },
    FastSet { descriptor: std::sync::Arc<crate::mailbox::FastSetDescriptor> },
    ScatterMget { shard_id: usize, keys: Vec<(usize, Bytes)>, descriptor: std::sync::Arc<crate::mailbox::ScatterMgetDescriptor> },
    ScatterMset { shard_id: usize, pairs: Vec<(Bytes, Bytes)>, descriptor: std::sync::Arc<crate::mailbox::ScatterMsetDescriptor> },
    Batch {
        items: Vec<(usize, u64, Command)>,
        responder: std::sync::Arc<crate::mailbox::BatchResponder>,
        is_resp3: bool,
    },
}
```

**Changed since the previous revision**: `Batch` **no longer carries a `results:
Vec<(usize, CompactResp)>` field at all.** Replies are now written directly through
`responder.write_slot(idx, resp)` into the caller's own buffer (§3.5, §4.3) — there is
nothing left to serialize back over the message itself. `DelKeys` and `ActiveDefrag` are
two genuinely new variants versus the oldest baseline of this doc, both still using the
fresh-`flume::bounded(1)`-per-call category.

The 66-variant count is unchanged numerically from the prior revision despite the file's
growth — the churn was additions (`DelKeys`, `ActiveDefrag`, plus AI-native/search
variants already present) balanced against the removal of `Batch`'s `results` field (a
field removal, not a variant removal) and no net change in variant count. Do not assume the
identical number means the enum is unchanged — it is a coincidence of this revision.

### 3.3 `CompactResp` (`src/shard.rs:9-15`) — now **five** variants (grew from four)

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactResp {
    Small { len: u8, data: [u8; 30] },   // inline: :123\r\n, +OK\r\n, $-1\r\n, ...
    Big(Vec<u8>),                        // fallback for anything that doesn't fit inline
    Bulk(Bytes),                         // a bulk string backed by a refcounted Bytes
    Array1Bulk(Bytes),                   // a one-element RESP array wrapping one bulk string
    RawBytes(Bytes),                     // pre-formatted RESP bytes, written out verbatim
}
```

`Small` covers the overwhelming majority of squashed-batch replies (`OK`, small integers,
`NULL`, short bulk strings ≤ 30 bytes after RESP framing) with associated `const` values
(`CompactResp::OK`, `INT_0`, `INT_1`, `NULL`/`NULL_RESP3`, `EMPTY_ARRAY`) built at compile
time. `Array1Bulk` is actively constructed today (`src/table.rs:7282`, `:9762`, `:9773` —
e.g. single-element `ZPOPMIN`/`LPOP` replies). **`RawBytes` is new versus the prior
revision, fully implemented in every match arm (`estimated_len`/`write_to`/`as_slice`/
`into_vec`), but is never constructed anywhere in the codebase** — grepping for
`RawBytes(` outside its own definition and match arms returns zero constructor call
sites. It is dead code today, the same pattern as the legacy `ShardMessage::Get`/`Set`/
`Mget`/`Mset` variants in §3.2: compiled, matched, reachable in principle, never actually
produced by production or test code.

### 3.4 `SlotState` (`src/shard.rs:596-601`) — unchanged

```rust
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotState {
    Stable,
    Migrating(String),
    Importing(String),
    Moved(String),
}
```

Stored sparsely in `Router::slot_states: HashMap<u16, SlotState>`; absent ⇒ `Stable`
(`Router::get_slot_state` returns `SlotState::Stable` on a miss).

### 3.5 `ShardDb` (`src/shard.rs:606-621`) — the per-core state container, five new fields

```rust
pub struct ShardDb {
    pub table: crate::table::RudisTable,
    pub port: u16,
    pub shard_id: usize,
    pub tier_manager: Option<std::rc::Rc<crate::tiering::ShardTierManager>>,
    pub vector_indexes: std::collections::HashMap<String, crate::vector::HnswIndex>,
    pub semantic_caches: hashbrown::HashMap<Bytes, crate::vector::SemanticCache>,
    pub agent_memories: hashbrown::HashMap<Bytes, crate::agent::AgentMemorySession>,
    pub llm_quotas: hashbrown::HashMap<Bytes, crate::agent::LlmQuotaBucket>,
    pub agent_checkpoints: hashbrown::HashMap<Bytes, crate::agent::AgentCheckpointThread>,
    pub agent_tools: hashbrown::HashMap<Bytes, crate::agent::AgentToolRegistry>,
    pub crdt_store: crate::crdt::CrdtStore,
    pub json_store: crate::json::JsonStore,
    pub probabilistic_store: crate::probabilistic::ProbabilisticStore,
    pub sticky_keys: hashbrown::HashSet<Bytes>,
    pub search_indices: std::collections::HashMap<String, crate::search::InvertedIndex>,
}
```

Five fields beyond the previously documented set — `semantic_caches`, `agent_memories`,
`llm_quotas`, `agent_checkpoints`, `agent_tools` — all backing Component 20 (the AI-native
agent runtime / semantic cache: `src/agent.rs`, `src/semcache.rs`, and the `SemanticCache`
type in `src/vector.rs`). This is the single largest contributor to `shard.rs`'s +1,469
line growth: **`ShardDb` now exposes 238 `pub fn`/`pub async fn` methods** (re-counted via
`rg -c '^    pub (async )?fn ' src/shard.rs` over the `impl ShardDb` block), up from "well
over 100" in the prior revision — the bulk of the new methods are thin per-command
delegates for vector sets, semantic cache, agent memory/checkpoint/tool-lease, and
RediSearch commands, following the exact same thin-delegate pattern as the original
`hset`/`lpush`/`zadd` methods.

`ShardDb::set`/`set_extended`/`del` are still the exceptions to "thin delegate": they check
`self.table.is_tiered(&key)`/`is_cooled(&key)` first, call `tier_manager.op_manager
.cancel_pending_stash(&key)` to cancel any in-flight async stash, and update
`tier_manager`'s stats (`on_key_deleted`, `tiered_keys`/`cooled_keys`/`total_deletes`
counters) before delegating to `self.table.set`/`set_extended`/`del` — the storage engine
and the tiering engine have to stay in sync on every mutation, and this is where that
happens (`src/shard.rs:818-962`).

### 3.6 The mesh transport (`src/mailbox.rs`)

#### `CachePadded<T>` and `SpscQueue<T>` (`src/mailbox.rs:6-22, 423-525`)

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
```

Unit test `test_cache_padded_alignment` (`mailbox.rs:689-695`) asserts
`align_of::<CachePadded<UnsafeCell<Option<Bytes>>>>() == 64` and `size_of(...) >= 64` —
these are the load-bearing, currently-CI-verified size/alignment facts for this type; no
other struct size in this subsystem is asserted by a test, so this document does not
state unverified byte counts for the larger structs.

`push`/`pop` are lock-free in the common case (relaxed/acquire/release atomics on the
head/tail cursors); when the ring is momentarily full, `push` falls back to a
`std::sync::Mutex`-guarded `VecDeque` **overflow queue**, with `has_overflow: AtomicBool`
as a fast-path check so `pop` only pays the mutex cost while overflow is actually active.
`mailbox::create_shard_mesh` requests ring capacity **256** per ring (`SpscQueue::new(256)`
at `mailbox.rs:641`; `next_power_of_two(256) == 256`, so no rounding occurs in practice).

#### `ShardSender`/`ShardReceiver` and the sleeping-flag fast path (`src/mailbox.rs:533-624`)

```rust
#[derive(Clone)]
pub struct ShardSender {
    pub target_shard: usize,
    pub ring: std::sync::Arc<SpscQueue<crate::shard::ShardMessage>>,
    pub target_notify: flume::Sender<()>,
    pub target_sleeping: std::sync::Arc<CachePadded<AtomicBool>>,
}

impl ShardSender {
    pub fn send(&self, msg: crate::shard::ShardMessage) -> Result<(), SendError> {
        self.ring.push(msg);
        if self.target_sleeping.0.load(Ordering::SeqCst) {
            let _ = self.target_notify.try_send(());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ShardReceiver {
    pub shard_id: usize,
    pub incoming_rings: Vec<std::sync::Arc<SpscQueue<crate::shard::ShardMessage>>>,
    pub notify_rx: flume::Receiver<()>,
    pub sleeping: std::sync::Arc<CachePadded<AtomicBool>>,
}
```

**New versus the prior revision** (`target_sleeping`/`sleeping`, introduced in commit
`a6b1ac8`, "eliminate Flume mutex contention with sleeping-flag fast path"). Before this
change, `ShardSender::send` called `target_notify.try_send(())` on *every* send,
unconditionally — even though `flume`'s bounded channel has an internal mutex, so every
cross-shard message paid that lock regardless of whether the consumer was even looking at
the channel. Now:

- `ShardReceiver::recv`/`recv_async` (`mailbox.rs:575-623`) only set `sleeping = true`
  (`Ordering::SeqCst`) immediately before checking the rings one more time and then
  actually parking on `notify_rx.recv_async().await`; `sleeping` is cleared back to `false`
  the instant the receiver wakes (found a message via the pre-check, or the notify fired).
- `ShardSender::send` only calls `target_notify.try_send(())` if it observes
  `target_sleeping == true`. If the consumer is actively spinning through its
  `try_recv` loop (the common case under sustained load — a busy shard rarely fully
  drains and parks), every send from every producer skips the `flume` channel — and its
  internal mutex — entirely, paying only the lock-free `SpscQueue::push`.
- This is provably safe against the classic "notify sent before receiver parks, wakeup
  lost" race because `recv`/`recv_async` always re-checks every incoming ring
  (`try_recv`) *after* setting `sleeping = true` and *before* actually awaiting
  `notify_rx` — if a message arrived in the window between the first `try_recv` failing
  and `sleeping` being set, the second `try_recv` catches it, so a receiver "about to
  sleep" can never miss a decision by a sender to skip the notify.
- Verified by `test_mesh_sleeping_flag_bypass` (`mailbox.rs:896-931`): sending while the
  receiver is not marked sleeping leaves `notify_rx` empty (`try_recv().is_err()`) even
  though the message itself is retrievable; sending while `sleeping == true` does push a
  notification.

`create_shard_mesh(num_shards)` builds a full `num_shards × num_shards` matrix of
`SpscQueue`s (`rings[i][j]` = producer shard `i` → consumer shard `j`), one
`flume::bounded::<()>(1)` notify channel and one `Arc<CachePadded<AtomicBool>>` sleeping
flag per **consumer** shard (shared by every producer targeting it), and wires each
`ShardSender`'s `target_sleeping` to the matching `ShardReceiver`'s `sleeping`.

### 3.7 Shared-memory reply descriptors

Every descriptor type below is `unsafe impl Send + Sync` by construction: wrapped in an
`Arc`, handed to one or more remote shards, each of which writes only to disjoint
fields/slots that the waiting shard does not touch until it observes the corresponding
completion state.

#### `FastGetDescriptor` / `FastSetDescriptor` (`src/mailbox.rs:259-324`) — unchanged, simple `AtomicBool`

```rust
pub struct FastGetDescriptor {
    pub key: Bytes,
    pub val: CachePadded<UnsafeCell<Option<Bytes>>>,
    pub done: AtomicBool,
    pub notify: flume::Sender<()>,
}
impl FastGetDescriptor {
    pub fn finish(&self, val: Option<Bytes>) {
        unsafe { *self.val.get() = val; }
        self.done.store(true, Ordering::Release);
        let _ = self.notify.try_send(());
    }
}
```

`FastSetDescriptor` is the same shape without a `val` cell. These two did **not** receive
the Parker-handshake treatment — they still unconditionally `try_send` on every `finish()`
regardless of whether the waiter has parked. This is a real, minor asymmetry: `GET`/`SET`
(the highest-volume single-key ops) pay one unconditional `flume::try_send` per remote
call, while `MGET`/`MSET`/`Batch` do not, on their respective completion paths (see §7).

#### `ScatterMgetDescriptor` / `ScatterMsetDescriptor` — the 3-state Parker handshake (`src/mailbox.rs:24-257`)

```rust
pub const DESC_RUNNING: u8 = 0;
pub const DESC_COMPLETED: u8 = 1;
pub const DESC_SLEEPING: u8 = 2;

pub struct ScatterMgetDescriptor {
    pub results: Box<[CachePadded<UnsafeCell<Option<Bytes>>>]>,
    pub pending: AtomicUsize,
    pub state: std::sync::atomic::AtomicU8,
    pub notify: CachePadded<UnsafeCell<flume::Sender<()>>>,
    pub recycled_keys: Box<[CachePadded<UnsafeCell<Vec<(usize, Bytes)>>>]>,
}

impl ScatterMgetDescriptor {
    pub fn write_result(&self, idx: usize, val: Option<Bytes>) {
        unsafe { *self.results[idx].get() = val; }
    }

    pub fn finish_shard(&self) {
        if self.pending.fetch_sub(1, Ordering::AcqRel) == 1
            && self.state.swap(DESC_COMPLETED, Ordering::AcqRel) == DESC_SLEEPING
        {
            let tx = unsafe { &*self.notify.get() };
            let _ = tx.try_send(());
        }
    }

    pub async fn wait_completed(&self, spin_iters: usize, notify_rx: &flume::Receiver<()>) {
        if self.state.load(Ordering::Acquire) != DESC_COMPLETED {
            for _ in 0..spin_iters {
                std::hint::spin_loop();
                if self.state.load(Ordering::Acquire) == DESC_COMPLETED { return; }
            }
            if self.state.compare_exchange(
                DESC_RUNNING, DESC_SLEEPING, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                let _ = notify_rx.recv_async().await;
            }
        }
    }
}
```

This is **new versus the prior revision** (introduced in commit `b3d3362`, with a race
fixed one commit later in `5d3fb1e`, "synchronize ScatterMgetDescriptor and
ScatterMsetDescriptor completion after notify_tx send"). `ScatterMsetDescriptor` has the
identical `state`/`finish_shard`/`wait_completed` shape (no `results` array — an `MSET`
reply is just "done").

**The state machine, precisely**:
- Freshly constructed/reset: `DESC_RUNNING` (or `DESC_COMPLETED` immediately if
  `pending_shards == 0`, i.e. every key/pair in the operation was local).
- Each targeted remote shard calls `write_result`/nothing, then `finish_shard()`, which
  decrements `pending: AtomicUsize`. **Only the shard that observes the decremented count
  hit zero** (`fetch_sub(...) == 1`) proceeds to swap the descriptor's `state` to
  `DESC_COMPLETED` — every other finishing shard's `finish_shard()` call is a pure atomic
  decrement with no further action.
- That last-to-finish shard's `swap` returns whatever state was there *before* the swap.
  If it was `DESC_SLEEPING`, the waiter genuinely parked on `notify_rx` and needs waking,
  so a `try_send(())` fires. **If it was still `DESC_RUNNING`**, the waiter never parked —
  it is still spinning in `wait_completed`'s loop or hasn't even reached it yet — so
  **no notify is sent at all**, because the waiter's own spin-loop will observe
  `state == DESC_COMPLETED` on its own without needing a wakeup.
- `wait_completed(spin_iters, notify_rx)`: spins up to `spin_iters` times
  (`std::hint::spin_loop()`) checking for `DESC_COMPLETED`; if it doesn't see completion
  within the spin budget, it attempts a `compare_exchange(DESC_RUNNING → DESC_SLEEPING)`.
  If that CAS succeeds, it awaits `notify_rx.recv_async()`. If the CAS fails, that can only
  mean the state is already `DESC_COMPLETED` (a shard finished and swapped it during the
  spin window), so the function simply returns without awaiting.
- This eliminates a `flume` round trip (and the mutex inside it) in the very common case
  where the spin budget is enough to observe completion before the waiter ever commits to
  sleeping — which is the expected case for small MGET/MSET fan-outs across a handful of
  co-located shards. Call sites pass different spin budgets depending on how much latency
  they're willing to burn spinning: `mget`/`begin_mget_resp` use 64 spins on the dispatch
  side and `finish_mget_resp` uses a larger 256-spin budget on the gather side (§4.4);
  `mset`/`begin_mset`/`finish_mset` all use 64.

#### `BatchResponder` — the 4-state Parker handshake + direct-pointer writes (`src/mailbox.rs:326-421`)

```rust
pub const BATCH_IDLE: u8 = 0;
pub const BATCH_RUNNING: u8 = 1;
pub const BATCH_SLEEPING: u8 = 2;
pub const BATCH_COMPLETED: u8 = 3;

/// Direct shared-memory slot for cross-shard squashed batch responses.
/// Remote shards write responses directly into the caller's `responses` slice via `responses_ptr`,
/// and signal completion via the 3-state Parker handshake without Flume mutex contention.
pub struct BatchResponder {
    pub state: CachePadded<std::sync::atomic::AtomicU8>,
    pub responses_ptr: std::sync::atomic::AtomicPtr<crate::shard::CompactResp>,
    pub recycled_items: CachePadded<UnsafeCell<Option<Vec<(usize, u64, crate::resp::Command)>>>>,
    pub notify_tx: flume::Sender<()>,
    pub notify_rx: flume::Receiver<()>,
}

impl BatchResponder {
    pub fn prepare(&self, responses_ptr: *mut crate::shard::CompactResp) {
        self.responses_ptr.store(responses_ptr, Ordering::Relaxed);
        self.state.0.store(BATCH_RUNNING, Ordering::Release);
    }

    pub fn write_slot(&self, idx: usize, resp: crate::shard::CompactResp) {
        unsafe {
            let ptr = self.responses_ptr.load(Ordering::Relaxed);
            *ptr.add(idx) = resp;
        }
    }

    pub fn finish(&self, items: Vec<(usize, u64, crate::resp::Command)>) {
        unsafe { *self.recycled_items.get() = Some(items); }
        if self.state.0.swap(BATCH_COMPLETED, Ordering::AcqRel) == BATCH_SLEEPING {
            let _ = self.notify_tx.try_send(());
        }
    }

    pub fn try_take(&self) -> Option<Vec<(usize, u64, crate::resp::Command)>> {
        if self.state.0.load(Ordering::Acquire) == BATCH_COMPLETED {
            self.state.0.store(BATCH_IDLE, Ordering::Relaxed);
            unsafe { (*self.recycled_items.get()).take() }
        } else { None }
    }

    pub async fn wait_take(&self) -> Option<Vec<(usize, u64, crate::resp::Command)>> {
        if self.state.0.load(Ordering::Acquire) != BATCH_COMPLETED
            && self.state.0.compare_exchange(
                BATCH_RUNNING, BATCH_SLEEPING, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok()
        {
            let _ = self.notify_rx.recv_async().await;
        }
        self.state.0.store(BATCH_IDLE, Ordering::Relaxed);
        unsafe { (*self.recycled_items.get()).take() }
    }
}
```

This is **the single biggest architectural change in this revision** (commit `b01312d`,
"write remote batch responses directly into caller slots with 4-state Parker handshake").
Before this change, `BatchResponder` carried a `payload: UnsafeCell<Option<(Vec<(usize, u64,
Command)>, Vec<(usize, CompactResp)>)>>` — the remote shard built its own
`Vec<(usize, CompactResp)>` of results locally and handed the whole thing back through the
responder; the caller then had to drain that vec and copy each `(idx, resp)` into its own
`responses[idx]` slot. Now:

1. The caller (`execute_commands_squashed` in `connection.rs`, or `Router::execute_remote`)
   owns a `responses: Vec<CompactResp>` buffer up front and calls
   `responder.prepare(responses.as_mut_ptr())` **before** sending the `ShardMessage::Batch`.
   This stores the raw pointer and transitions `state` to `BATCH_RUNNING`.
2. The message crosses the SPSC ring to the remote shard. The ring's own `push`
   (`Ordering::Release`) / `pop` (`Ordering::Acquire`) pair establishes the happens-before
   relationship that makes the caller's `prepare()` writes (including the `Relaxed`
   `responses_ptr` store) visible to the remote shard once it pops the message — the
   correctness of the `Relaxed` ordering on `responses_ptr` itself depends on this ring
   synchronization, not on any ordering internal to `BatchResponder`.
3. The remote shard executes each command in the batch and calls
   `responder.write_slot(idx, resp)` for each one — an `unsafe { *ptr.add(idx) = resp }`
   directly into the *caller's* buffer, from a different OS thread, with no lock. This is
   sound only because every `idx` written by a given remote shard is disjoint from every
   index any other shard (or the caller's own local-execution loop) writes, and because the
   caller does not read `responses` until every dispatched responder reports completion.
4. When the remote shard is done with the whole batch, `responder.finish(items)` stashes
   the recycled `items` vec (for pool reuse — **not** the results, which are already
   written) and swaps `state` to `BATCH_COMPLETED`, notifying only if a waiter had
   transitioned to `BATCH_SLEEPING` (same "only the sleeper pays" pattern as §3.7's
   Scatter descriptors).
5. The caller harvests via `try_take()` (non-blocking poll, used in a spin-harvest loop
   across every dispatched shard) or `wait_take()` (CAS to `BATCH_SLEEPING` then park) —
   see §4.3 for the exact harvest loop in `connection.rs`.

**Correctness caveat worth flagging** (found via reading, not from a commit message): the
soundness of `responses_ptr` depends entirely on the pointed-to `Vec<CompactResp>`
outliving every in-flight remote write. `execute_commands_squashed`'s `responses` buffer
lives in the per-connection `ConnScratch`, which `recycle_conn_scratch`
(`connection.rs:2140`) only returns to the pool after confirming every responder is both
`Arc::strong_count(r) == 1` *and* `r.is_idle()` — i.e. provably unreferenced by any
straggling remote shard — so that path is guarded. `Router::execute_remote`
(`router.rs:2209`), however, points `responses_ptr` at **`&mut slot as *mut _`, a bare
local variable on that `async fn`'s own stack/future** (§4.3), with no equivalent
pool-reuse guard, because there is no pool for it to be returned to — the function simply
awaits `responder.wait_take()` to completion before returning. This is sound as long as
that future is always polled to completion and never dropped early (e.g. by task
cancellation or a panic unwinding through the awaiting frame) between `prepare()` and the
remote shard's `write_slot` call; nothing in the current code makes that invariant
explicit or defends against it the way `recycle_conn_scratch` does for the pipeline path.
No evidence was found of actual task cancellation reaching this await point in the current
codebase, but the asymmetry with the guarded `ConnScratch` path is worth a contributor's
attention if cancellation-safety work is ever done on the connection layer.

---

## 4. Execution Algorithms & Code Logic

### 4.1 Key Routing — free functions (`src/router.rs:11-127`)

```rust
pub fn extract_hash_tag(key: &[u8]) -> &[u8] { /* "{user:1}:profile" -> "user:1" */ }

pub fn key_slot(key: &[u8]) -> u16 {
    crc16::State::<crc16::XMODEM>::calculate(extract_hash_tag(key)) % 16384
}

/// New: used to pre-compute a KEYS/SCAN-style glob pattern's slot when the pattern
/// contains a literal (non-wildcard) hash-tag segment, e.g. "{user:1}:*".
/// Returns None if the pattern contains any glob metacharacter inside/around the tag.
pub fn pattern_hash_slot(pattern: &[u8]) -> Option<u16> { ... }

pub fn slot_to_shard(slot: u16, num_shards: usize) -> usize {
    if num_shards <= 1 { 0 } else { ((slot as usize) * num_shards) / 16384 }
}

pub fn target_shard(key: &[u8], num_shards: usize) -> usize {
    if num_shards <= 1 {
        0
    } else if crate::cluster::HAS_ACTIVE_CLUSTER.load(Ordering::Relaxed) {
        slot_to_shard(key_slot(key), num_shards)
    } else {
        (crate::table::hash_key(extract_hash_tag(key)) as usize) % num_shards
    }
}

pub fn target_shard_and_hash(key: &[u8], num_shards: usize) -> (usize, u64) {
    let key_hash = crate::table::hash_key(key);
    if num_shards <= 1 {
        (0, key_hash)
    } else if crate::cluster::HAS_ACTIVE_CLUSTER.load(Ordering::Relaxed) {
        (slot_to_shard(key_slot(key), num_shards), key_hash)
    } else {
        let tag = extract_hash_tag(key);
        // Reuse the already-computed whole-key hash when there's no hash tag (tag == key)
        // instead of hashing the tag a second time.
        let shard_hash = if tag.len() == key.len() { key_hash } else { crate::table::hash_key(tag) };
        ((shard_hash as usize) % num_shards, key_hash)
    }
}
```

- **Standalone mode (default)**: `fxhash::hash64(hash_tag) % num_shards` — no 16,384-slot
  space involved at all.
- **Cluster mode**: real CRC16/XMODEM over the hash tag, reduced mod 16,384 to a slot, then
  `slot_to_shard` maps slot→shard by `(slot * num_shards) / 16384` (**not modulo**, giving
  each shard one contiguous slot range, matching `CLUSTER SLOTS`).
- `target_shard_and_hash` additionally reuses the whole-key hash as the shard-selection
  hash when there is no hash tag present, avoiding a second hash computation — a small but
  real optimization for the (very common) no-hash-tag case, not present in the standalone
  branch of plain `target_shard`.

### 4.2 The routing-entry-point finding, re-verified: fixed in `Router`, relocated to `connection.rs`

The prior revision of this document's central finding was that `Router`'s own methods were
split between a **static** free-function routing path (`get`, `set`, `expire`, `persist`,
`ttl`, `incr_by`, `exists`, `begin_mget_resp` — none of which consult `slot_owners`) and a
**dynamic** instance-method path (`Router::target_shard`, which indexes
`self.slot_owners`, reflecting live `set_slot_owner` overrides). **That specific split no
longer exists.** Re-grepping every `target_shard(...)`/`target_shard_and_hash(...)` call
site inside `router.rs` today shows that essentially every `Router` method now calls
`self.target_shard(&key)` or `self.target_shard_and_hash(key.as_ref())` — the **dynamic**
instance methods — including `get` (`router.rs:825`), `set` (`:964`), `hget` (`:857`),
`exists` (`:1773`), `incr_by` (`:1788`), `expire` (`:1821`), `persist` (`:1852`), `ttl`
(`:1907`), `mget` (`:1200`), `begin_mget_resp` (`:1309`, via `self.target_shard_and_hash`),
`begin_mset` (`:1446`), `del_keys` (`:1687`), `stick`/`unstick`/`is_sticky` (`:2026`,
`:2048`, `:2068`), `expiretime` (`:2941`), and more.

**The sole remaining static call site inside `Router` itself is `Router::del`**
(`router.rs:1619`):

```rust
pub async fn del(&self, key: Bytes) -> bool {
    let target = target_shard(&key, self.num_shards);   // static free function
    ...
}
```

Every other `Router` method was migrated to the dynamic instance method at some point
between the previous revision and now; `del` alone was not. Since `del_keys` (the
multi-key entry point) delegates single-key deletes straight to `self.del(...)`
(`router.rs:1651`), a single-key `DEL` during an active `set_slot_owner`-driven live
migration can genuinely target a different shard than a 2+-key `DEL` touching the exact
same key (which routes through `del_keys`'s own dynamic `self.target_shard` loop instead
of calling `self.del`). This is real, narrow, and easy to fix by changing one line
(§7).

**The disagreement that actually matters today lives one layer higher, in
`src/connection.rs`.** Two free functions there — `target_shard_of_cmd` (`:12338`) and
`target_shard_and_hash_of_cmd` (`:12692`) — independently re-implement per-command routing
for dozens of `Command` variants (`Get`, `Set`, `IncrBy`, `Hset`, `Hget`, `Lpush`, `Lpop`,
`Sadd`, `Sismember`, `Zadd`, single-key `Exists`/`Del`, `Lrange`, `Zrange`, and many more),
and **both are built entirely on the static free functions** `router::target_shard`/
`target_shard_and_hash`, never on `Router::target_shard`:

```rust
// src/connection.rs:12692
pub fn target_shard_and_hash_of_cmd(cmd: &Command, num_shards: usize) -> Option<(usize, u64)> {
    match cmd {
        Command::Get(key) | Command::Set { key, .. } | Command::IncrBy(key, _) | ... => {
            Some(crate::router::target_shard_and_hash(key, num_shards))   // static
        }
        _ => target_shard_of_cmd(cmd, num_shards).map(|s| { ... }),       // also static
    }
}
```

These two functions are what decides local-vs-remote for **every command inside the
squashed-pipeline fast path** (`execute_commands_squashed`, `connection.rs:19266`) — the
hot path that most real pipelined traffic takes — and for a secondary generic
single-command dispatch fallback used for commands without a dedicated `Router` method
(`connection.rs:7397`, covering things like `VADD`/`XINFO`/agent-checkpoint commands).

The squashed path does have a pre-squash eligibility gate (`can_squash`,
`connection.rs:19120-19219`) that inspects every key of every command via
`for_each_cmd_key` and bails out of squashing (falling back to the single-command
`execute_command` path) if `router.get_slot_state(slot) != SlotState::Stable` for any key,
or if the cluster-bus gossip table (`ClusterHub::my_slots`) shows this *node* doesn't own
the slot. **Neither check consults `Router::slot_owners`.** `Router::set_slot_owner`
(`router.rs:302`) explicitly clears any `SlotState` entry for the slot it's reassigning
(`self.slot_states.borrow_mut().remove(&slot)`), leaving the slot's `SlotState` as
`Stable` — by design, since `set_slot_owner` represents a completed, intra-process
reassignment between two local shards, not an in-progress cross-node migration. That means
a slot whose ownership was changed via `set_slot_owner` looks perfectly `Stable` to both
the squash-eligibility gate and to `execute_command`'s own inline redirect check (§4.5) —
**except** that `execute_command`'s check, when `router.cluster_enabled`, explicitly
re-derives the true owner via `router.target_shard_for_slot(slot)` (dynamic) and redirects
with `-MOVED` if it disagrees with `router.shard_id`, whereas the squash-eligibility gate
does not perform this comparison for squashed commands at all — it only checks `SlotState`
and node-level cluster-bus ownership, never shard-level `slot_owners`.

**Net effect, precisely**: after a live `Router::set_slot_owner` reassignment during an
active cluster deployment, a squashed pipeline containing `GET`/`SET`/`INCRBY`/etc. against
a key in the reassigned slot will pass the eligibility gate (nothing there disagrees), then
`target_shard_and_hash_of_cmd` computes the **stale, pre-migration** shard via the static
formula. If that stale target happens to equal `router.shard_id`, the command silently
executes locally against a slot the router no longer (per `slot_owners`) believes this
shard owns; if not, it's sent to the stale peer shard instead of the new owner — in neither
case does the client receive a `-MOVED` redirect the way it would on the non-squashed path.
This is the modern, more precisely located descendant of the original "two routing entry
points can disagree" finding: it is no longer spread across half of `Router`'s own methods,
it is now concentrated in `connection.rs`'s squashed-pipeline routing helpers versus
`Router`'s (now largely unified) dynamic routing. See §7 for the fix shape.

### 4.3 The Local/Remote Fork — reply mechanisms by operation category

**`get`/`set` use pooled shared-memory descriptors (`FastGetDescriptor`/
`FastSetDescriptor`), not a channel per call** (`router.rs:824-931`, unchanged shape from
the prior revision other than now routing dynamically, §4.2):

```rust
pub async fn get(&self, key: Bytes) -> Option<Bytes> {
    let target = self.target_shard(&key);
    if target == self.shard_id {
        self.get_local_direct(&key).await
    } else {
        let (tx, rx) = self.acquire_notify_channel();          // pooled, not fresh
        let desc = Arc::new(crate::mailbox::FastGetDescriptor::new(key, tx.clone()));
        let msg = ShardMessage::FastGet { descriptor: desc.clone() };
        let res = if self.senders[target].send(msg).is_ok() {
            if !desc.done.load(Ordering::Acquire) {
                for _ in 0..32 { std::hint::spin_loop(); if desc.done.load(Ordering::Acquire) { break; } }
                if !desc.done.load(Ordering::Acquire) { let _ = rx.recv_async().await; }
            }
            while rx.try_recv().is_ok() {}
            unsafe { (*desc.val.get()).take() }
        } else { None };
        self.release_notify_channel(tx, rx);
        res
    }
}
```

`get`'s local branch (`get_local_direct`) has a tiering-aware split: fast in-RAM path
first, then — if the key is tiered — `read_cold_key_local` chooses between a streaming
cold read (`stream_cold_read_local`, when `is_memory_constrained()`) or loading the value
back into RAM (`load_local`) before returning it. The same split is reused by the
`ScatterMget` remote handler in `server.rs` for cross-shard `MGET` cold keys (§4.4).

**`del`/`exists`/`incr_by`/`expire`/`persist`/`ttl`/`del_if_unchanged`/`expiretime`/`random_key`/
`scan`/the `Tier*` operations/... still allocate a fresh `flume::bounded(1)` channel per
remote call** (58 distinct `flume::bounded(1)` call sites remain in `router.rs`). Example,
`del` (also the sole remaining static-routing call site, §4.2):

```rust
pub async fn del(&self, key: Bytes) -> bool {
    let target = target_shard(&key, self.num_shards);
    if target == self.shard_id {
        // local delete + AOF append + keyspace notification
    } else {
        let (tx, rx) = flume::bounded(1);        // fresh allocation, unpooled
        let msg = ShardMessage::Del { key, responder: tx };
        if self.senders[target].send(msg).is_ok() { rx.recv_async().await.unwrap_or(false) } else { false }
    }
}
```

**`execute_remote` — the generic single-command fallback** (`router.rs:2209-2244`, used by
call sites without a dedicated `Router` method, e.g. `BZPOPMIN`'s remote retry,
`CRDT.DUMP`/`CRDT.MERGE`, `DBSIZE`, `FLUSHDB`, the `connection.rs:7397` generic dispatch)
— **rewritten in this revision to use the direct-pointer `BatchResponder` mechanism
instead of the old results-vec serialization**:

```rust
pub async fn execute_remote(&self, target: usize, cmd: Command) -> Vec<u8> {
    let responder = self.remote_responder_pool.borrow_mut().pop()
        .unwrap_or_else(|| Arc::new(crate::mailbox::BatchResponder::new()));
    let mut slot = crate::shard::CompactResp::empty();
    responder.prepare(&mut slot as *mut _);                 // direct-write target
    let h = crate::connection::cmd_primary_key(&cmd).map(|k| crate::table::hash_key(k)).unwrap_or(0);
    let msg = ShardMessage::Batch { items: vec![(0, h, cmd)], responder: responder.clone(), is_resp3 };
    let res = if self.senders[target].send(msg).is_ok() {
        let mut completed = false;
        for _spin in 0..48 { if responder.try_take().is_some() { completed = true; break; } std::hint::spin_loop(); }
        if !completed { let _ = responder.wait_take().await; }
        slot.into_vec()
    } else { b"-ERR internal shard routing error\r\n".to_vec() };
    self.remote_responder_pool.borrow_mut().push(responder);
    res
}
```

A single-element `CompactResp::empty()` stack slot stands in for the `Vec<CompactResp>`
that the squashed-pipeline path uses — see §3.7 for the soundness caveat on this pattern.

### 4.4 `MGET`/`MSET` — shared-memory scatter-gather

Two entry points for cross-shard `MGET`: `Router::mget(keys) -> Vec<Option<Bytes>>`
(synchronous dispatch-then-wait, used by e.g. Lua scripting) and the split
`begin_mget_resp`/`finish_mget_resp` pair returning an `MgetInFlight` handle (used by
`execute_commands_squashed` so a pipeline can fire several `MGET`s before stalling on the
first — `connection.rs:19252-19253` holds `inflight_mgets: Vec<(usize, MgetInFlight)>`).
`Router::mset`/`begin_mset`/`finish_mset` are the `MSET` analogs (`begin_mset` is
synchronous, not `async`, since the dispatch phase never needs to await). **Both entry
points now route every key through `self.target_shard`/`self.target_shard_and_hash` —
fully unified, no static/dynamic split remains for MGET/MSET specifically** (§4.2).

```rust
// begin_mget_resp — dispatch phase, does not await remote shards
for (idx, key) in keys.into_iter().enumerate() {
    let (target, key_hash) = self.target_shard_and_hash(key.as_ref());   // dynamic
    if target == self.shard_id { local_keys.push((idx, key, key_hash)); }
    else { has_remote = true; remote_batches[target].push((idx, key)); }
}
// Fast path: every key local -> zero channel/descriptor operations at all.
if !has_remote { /* write RESP directly into `out`, return None */ }

let (notify_tx, notify_rx) = self.acquire_notify_channel();
let descriptor = self.acquire_mget_descriptor(total_keys, num_remote_shards, notify_tx.clone());
for (target_shard, batch) in remote_batches.iter_mut().enumerate() {
    if !batch.is_empty() {
        let msg = ShardMessage::ScatterMget { shard_id: target_shard, keys: std::mem::take(batch), descriptor: descriptor.clone() };
        if self.senders[target_shard].send(msg).is_err() { descriptor.finish_shard(); }
    }
}
// Local keys execute concurrently with the in-flight remote batches, not after them.
for (idx, key, key_hash) in local_keys {
    descriptor.write_result(idx, self.local_db.borrow_mut().get_with_hash(key.as_ref(), key_hash));
}
Some(MgetInFlight { descriptor, notify_tx, notify_rx, total_keys })
```

Each targeted remote shard, on receiving `ScatterMget` (handled in `server.rs:1388-1446`),
writes results directly into its disjoint slice of `descriptor.results` via
`write_result(global_idx, val)`, splits off any tiered/cold keys into a `monoio::spawn`ed
async follow-up (branching on `r.is_memory_constrained()` exactly like `get`'s tiering
split, §4.3), then calls `descriptor.finish_shard()` — the 3-state Parker handshake means
only the last shard to finish pays for a notify, and only if the waiter actually parked
(§3.7).

**`finish_mget_resp` now serializes directly out of the descriptor's shared memory into
the RESP output buffer**, with no intermediate `Vec<Option<Bytes>>` materialization step:

```rust
pub async fn finish_mget_resp(&self, inflight: MgetInFlight, out: &mut Vec<u8>) {
    let MgetInFlight { descriptor, notify_tx, notify_rx, total_keys } = inflight;
    descriptor.wait_completed(256, &notify_rx).await;          // 256-spin budget, larger than dispatch side
    let recycled = descriptor.take_recycled_keys();
    self.mget_batch_pool.borrow_mut().push(recycled);
    self.release_notify_channel(notify_tx, notify_rx);
    out.reserve(total_keys * 32 + 16);
    crate::connection::write_resp_array_header(out, total_keys);
    unsafe {
        for i in 0..total_keys {
            match (*descriptor.results[i].get()).take() {
                Some(ref v) => crate::connection::write_resp_bulk(out, v),
                None => crate::connection::write_resp_null(out),
            }
        }
    }
    self.release_mget_descriptor(descriptor);
}
```

`MSET` follows the identical dispatch/gather shape with `ScatterMsetDescriptor` (no
`results` array — an `MSET` reply is just "done"); `ScatterMset`'s remote handler
(`server.rs:1448-1469`) applies the pairs, optionally appends one combined AOF record for
the whole sub-batch, and calls `finish_shard()`.

**Pooling throughout**: `mget_batch_pool`/`mset_batch_pool` recycle the per-shard bucket
`Vec`s, `mget_desc_pool`/`mset_desc_pool` recycle the descriptors themselves — with a
128-iteration `Arc::strong_count(&desc) == 1` spin-check (`router.rs:1099-1104`,
`:1142-1147`) before reuse, to confirm no straggling remote shard still holds a clone —
and `notify_channel_pool` recycles the wake-up channel.

**`Router::del_keys` still uses the older, unpooled channel mechanism, unchanged from the
prior revision**: one fresh `flume::bounded(1)` per remote shard touched (bucketed, all
sent before any are awaited), not the `mailbox.rs` scatter-gather descriptors that
`MGET`/`MSET` now use. This remains a real, narrower instance of the "still allocates a
channel per call" pattern — bounded by `num_shards` channels per call rather than per key,
but not converted to the zero-allocation steady-state that `MGET`/`MSET` enjoy (§7).

### 4.5 Redirection for live cluster slot migration — re-verified, partially fixed

`check_slot_redirection` (`router.rs:260-284`) still exists with the identical shape as
the prior revision, and is **still never called anywhere in production code** — re-grepping
the whole codebase for `check_slot_redirection` today returns only its own definition.
`connection.rs::execute_command` still inlines the identical `SlotState` match directly
instead (`connection.rs:5052-5107`), unchanged in shape from the prior revision:

```rust
if let Some(key) = cmd_primary_key(&cmd) {
    let slot = key_slot(key);
    match router.get_slot_state(slot) {
        SlotState::Moved(target) => { /* -MOVED slot target */ return false; }
        SlotState::Importing(source) => if !is_asking { /* -MOVED slot source */ return false; },
        SlotState::Migrating(target) => if !router.exists(key.clone()).await { /* -ASK slot target */ return false; },
        SlotState::Stable => {
            if router.cluster_enabled {
                let target_shard = router.target_shard_for_slot(slot);   // dynamic
                if target_shard != router.shard_id { /* -MOVED to the new local owner */ return false; }
            } else if crate::cluster::HAS_ACTIVE_CLUSTER.load(...) {
                // consult ClusterHub::my_slots / gossip nodes for cross-node -MOVED
                // (does NOT re-check target_shard_for_slot in this branch)
            }
        }
    }
}
```

**This is a genuine fix versus the prior revision's headline finding for this section**:
`cmd_primary_key` (`connection.rs:2778`) now has explicit arms for `Mget`
(`Command::Touch(keys) | Command::Mget(keys) => keys.first()`, `:2978`) and `Mset`
(`Command::Mset(pairs) | ... => pairs.first().map(|(k, _)| k)`, `:3003`) — previously it
had no arm for either, which was the prior revision's stated reason multi-key commands
were invisible to this redirect check entirely. Combined with the separate CROSSSLOT check
a few lines earlier (`connection.rs:5037-5050`, gated on `router.cluster_enabled`, iterating
every key via `cmd_keys(&cmd)` and rejecting with `-CROSSSLOT` if any two keys hash to
different slots), a cluster-mode `MGET`/`MSET` whose keys all share one slot now does get a
correct redirect check via that shared first key, in `execute_command` (the non-squashed
path).

**Two caveats found while re-verifying this**:
1. The CROSSSLOT check is gated only on `router.cluster_enabled`, while the Stable-state
   redirect branch's *second* arm (`else if HAS_ACTIVE_CLUSTER`) handles the case where
   `HAS_ACTIVE_CLUSTER` is true but this specific `Router` instance's own `cluster_enabled`
   is false — an edge case (gossip-discovered cluster activity without this instance having
   explicitly entered cluster mode) where CROSSSLOT would not fire even though
   `Router::target_shard`'s own routing condition (`self.cluster_enabled ||
   HAS_ACTIVE_CLUSTER`) would already be using slot-based routing. This is a narrow gating
   asymmetry, not a confirmed live bug, but worth a contributor's attention if that
   configuration is ever exercised.
2. As established in §4.2, this whole redirect check lives only in `execute_command`, the
   single-command/non-squashed path. It is not what protects the squashed-pipeline's
   `begin_mget_resp`/`begin_mset` dispatch from a stale `slot_owners` entry — that gap is
   real and is the one documented in §4.2/§7, not the "MGET/MSET invisible to redirection"
   gap the prior revision described (which is fixed).

### 4.6 The `ShardMessage::Batch` handler's dead async branch (`src/server.rs:619-1325`)

Found via careful reading, not flagged by any commit message. The receive-side handler for
`ShardMessage::Batch` in `server.rs` is structured as:

```rust
ShardMessage::Batch { mut items, responder, is_resp3 } => {
    let has_tier_manager = cross_shard_db.borrow().tier_manager.is_some();
    let needs_async = false;                         // hard-coded, never recomputed
    if needs_async {
        // ~340 lines: a full monoio::spawn'd async re-implementation of per-command
        // fast paths (Get/Set/IncrBy/Exists/Del/Hget/Hset/...), ending in
        // responder.finish(items)
    } else {
        // ~360 lines: the synchronous version of the same per-command fast paths,
        // collecting any tiered/cold GET keys into `cold_gets: SmallVec<...>` and,
        // if non-empty, spawning a *much smaller* follow-up task that only handles
        // those cold reads before calling responder.finish(items)
    }
}
```

`needs_async` is a local `let needs_async = false;` that is never written to again inside
the match arm — the entire ~340-line `if needs_async { ... }` branch, including its own
independent implementation of the `Get`/`Set`/`IncrBy`/`Exists`/`Del`/`Hget`/`Hset` fast
paths, is unreachable dead code under every current build configuration. The real,
always-taken path is the `else` branch, which handles tiered/cold `GET`s by collecting
them into `cold_gets` and deferring only that subset to a `monoio::spawn`ed follow-up
(`server.rs:1305-1323`) rather than moving the whole batch onto an async task
unconditionally. Functionally, current behavior is coherent and correct (the `else`
branch is a complete, self-sufficient implementation) — but the dead `if needs_async`
branch is a real maintenance hazard: it's a second, silently-unmaintained implementation of
the same per-command fast paths that will not receive future bug fixes made to the `else`
branch, and is large enough (~340 lines) that a future contributor could plausibly edit the
wrong copy. See §7.

### 4.7 Cross-shard `SCAN` cursor encoding (`src/router.rs:2854-2912`)

```rust
pub async fn scan(&self, cursor: u64, pattern: Option<&[u8]>, count: usize, key_type: Option<&[u8]>) -> (u64, Vec<Bytes>) {
    let mut shard_id = (cursor >> 32) as usize;
    let mut slot_idx = (cursor & 0xFFFF_FFFF) as usize;
    let mut all_keys = Vec::new();
    while shard_id < self.num_shards {
        let needed = count.saturating_sub(all_keys.len()).max(1);
        let (next_slot, keys) = /* local .scan() or remote ShardMessage::Scan round trip */;
        all_keys.extend(keys);
        if next_slot == 0 {
            shard_id += 1; slot_idx = 0;
            if all_keys.len() >= count || shard_id >= self.num_shards { return (...); }
        } else {
            return (((shard_id as u64) << 32) | (next_slot as u64), all_keys);
        }
    }
    (0, all_keys)
}
```

High 32 bits = which shard to resume from; low 32 bits = that shard's own internal cursor
— unchanged encoding from the prior revision. **New in this revision**: the function now
*loops* across consecutive exhausted shards within a single call until either `count` keys
have been accumulated or every shard has been visited, rather than returning immediately at
the first shard boundary — so one `SCAN` call can now cross multiple (fully-exhausted,
low-yield) shards' worth of data in one round trip instead of requiring one client-driven
`SCAN` call per shard boundary.

The per-shard cursor itself (`slot_idx`, opaque to `Router`) is interpreted inside
`RudisTable::scan` (`src/table.rs`, Component 05) via **segment-aware cursor mapping**
(`RudisFlatTable::cursor_bound()`/`cursor_to_global_idx()`, added alongside the
`BatchResponder` rewrite in commit `b01312d`): since the underlying hash table can now be
split across multiple fixed-capacity segments plus an overflow "stash" region rather than
one flat, power-of-two-sized slot array, a linear cursor no longer maps directly onto a
physical slot index. `cursor_to_global_idx` translates a linear `0..cursor_bound()` cursor
into `(segment_id << GLOBAL_IDX_SHIFT) | local_idx` when there is more than one segment,
falling back to an identity mapping for the common single-segment case. `Router::scan`
itself is unaffected by this — it only ever threads the opaque `u32` cursor value through —
but the reason `SCAN` continues to visit every live key exactly once across a table resize
is implemented there; see Component 05 (`docs/internal/05_storage_engine.md`) for the full
mechanism.

### 4.8 Cross-shard transaction locking (`MULTI`/`EXEC` across shards) — unchanged

```rust
pub async fn acquire_tx_locks(&self, shard_ids: &[usize], tx_id: u64) {
    for &sid in shard_ids {
        if sid == self.shard_id {
            // local: FIFO-queue on tx_waiters if tx_lock is already held, else take it
        } else {
            // remote: ShardMessage::AcquireTxLock, await its single-shot responder
        }
    }
}
```

A single advisory lock per shard (`tx_lock: Option<u64>`) with a FIFO wait queue
(`tx_waiters`), acquired across every shard a transaction's keys touch in ascending
shard-ID order, released in reverse order (`release_tx_locks`) — unchanged mutual-exclusion
design, re-verified against current source at `router.rs:2959-3005`.

---

## 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): `target_shard_of_cmd`/
  `target_shard_and_hash_of_cmd` decide local-vs-remote for the squashed-pipeline fast path
  (§4.2 — the static-routing finding lives here now); `execute_commands_squashed` builds
  `ShardMessage::Batch` directly, addressed per shard via a pooled `Vec<Arc<BatchResponder>>`
  living in `ConnScratch` (one per shard, reused for the connection's lifetime, recycled only
  after an `is_idle()` + strong-count check — §3.7). `Command::Mget`/`Mset` call
  `router.begin_mget_resp(...)`/`.await`/`router.begin_mset(...)`/`.await` — the split
  dispatch/finish pair, not a single `router.mget`/`router.mset` call — specifically so a
  pipeline containing several `MGET`/`MSET`s can have them all in flight before the first is
  awaited (§4.4).
- **`src/server.rs`** (Component 01): owns the receive side of every `ShardMessage`
  variant in the per-shard event loop (`ShardReceiver::recv_async`). `ScatterMget`/
  `ScatterMset` branch on whether the shard has a `tier_manager`, taking a synchronous
  `db.get()`-per-key loop when tiering is disabled and spawning an async cold-read path per
  tiered key when enabled. `Batch`'s handler contains a dead async branch gated by a
  hard-coded `false` (§4.6) — the always-taken sync branch collects cold `GET`s into a
  `cold_gets` vec and defers only those via a smaller follow-up spawn.
- **`src/aof.rs`**: `Router` holds an optional `AofWriter`; write-path methods append their
  own AOF record inline via `crate::aof::command_to_resp` on the local-execution branch.
- **`src/tiering.rs`**: `Router::spill_local`/`load_local`/`cool_local`/`decommit_local`/
  `check_auto_tier`/`read_cold_key_local` orchestrate NVMe tiering, dispatched cross-shard
  via `TierSpill`/`TierLoad`/`TierCool`/`TierDecommit`/`TierGc`/`TierSnapshot` messages;
  `ShardDb::set`/`set_extended`/`del` keep tiering stats in sync on every mutation (§3.5).
- **`src/pubsub.rs`**: `Router::publish`/`pubsub_channels`/`pubsub_numsub`/`pubsub_numpat`
  publish locally then fan out to every other shard and merge results.
- **`src/cluster.rs`**: every `cluster_*` method on `Router` is a thin passthrough to a
  singleton `crate::cluster::get_cluster_hub(self.port)`; `HAS_ACTIVE_CLUSTER` and
  `ClusterHub::my_slots` are consulted by both the squash-eligibility gate and
  `execute_command`'s redirect check, but are a *different* data source than `Router::
  slot_owners` (§4.2) — a contributor extending live migration needs to keep both in mind.
- **`src/replication.rs`**: `Router::execute_replica_command` applies a command from a
  replication stream against the correct shard, with `FLUSHALL`/`MSET`/`DEL`
  special-cased since they don't have one single target shard.
- **`src/table.rs`** (Component 05): `ShardDb.table: RudisTable` is the actual storage
  engine; `RudisTable`'s segment-aware cursor mapping backs `Router::scan`'s opaque
  per-shard cursor (§4.7). Everything else on `ShardDb` — `tier_manager`, `vector_indexes`,
  `semantic_caches`, `agent_memories`, `llm_quotas`, `agent_checkpoints`, `agent_tools`,
  `crdt_store`, `json_store`, `probabilistic_store`, `search_indices` — is a separate,
  later-added subsystem living alongside the storage engine, not folded into `RudisValue`.
- **`src/agent.rs`, `src/semcache.rs`, `src/vector.rs`** (Component 20): the five new
  `ShardDb` fields documented in §3.5 back `AGENT.*`/`LLM.QUOTA.*`/semantic-cache commands;
  each is routed cross-shard through the same `Router` key-hashing scheme as every other
  keyed command, with per-command `ShardDb` delegate methods contributing the bulk of the
  238-method count.

---

## 6. Concrete Numbers (Summary)

| Quantity | Value | Where verified |
| :--- | :--- | :--- |
| `ShardMessage` variants | 66 | `src/shard.rs:313-593`, counted directly |
| `CompactResp` variants | 5 (`Small`, `Big`, `Bulk`, `Array1Bulk`, `RawBytes`) | `src/shard.rs:9-15` |
| `ShardDb` public methods | 238 | `rg -c '^    pub (async )?fn ' src/shard.rs` over the `impl ShardDb` block |
| Cross-shard `SpscQueue` ring capacity | 256 per ring (`next_power_of_two(256) == 256`) | `src/mailbox.rs:641` |
| Mesh matrix size | `num_shards × num_shards` `SpscQueue`s + 1 notify channel + 1 sleeping flag per consumer shard | `src/mailbox.rs:627-680` |
| `CachePadded<T>` alignment | 64 bytes (asserted by CI test) | `src/mailbox.rs:689-695` (`test_cache_padded_alignment`) |
| Descriptor pool reuse spin budget | 128 iterations (`Arc::strong_count == 1` check) | `src/router.rs:1099-1104`, `:1142-1147` |
| `get`/`set` local spin budget before parking | 32 iterations | `src/router.rs:836-841`, `:1013-1018` |
| `mget`/`begin_mget_resp`/`mset`/`begin_mset` dispatch-side spin budget | 64 iterations | `src/router.rs:1254`, `:1385` uses 256 (gather side), `:1529` |
| `finish_mget_resp` gather-side spin budget | 256 iterations | `src/router.rs:1385` |
| `execute_remote` spin budget | 48 iterations | `src/router.rs:2228` |
| `flume::bounded(1)`-per-call sites remaining in `router.rs` | 58 | grep count, re-verified |
| `router.rs` / `shard.rs` / `mailbox.rs` line counts | 4,483 / 4,137 / 931 | `wc -l` |

---

## 7. Known Bugs, Edge Cases & Future Improvements

- **High — unify `connection.rs`'s static command-routing helpers with `Router`'s now
  largely-dynamic routing (§4.2).** `target_shard_of_cmd`/`target_shard_and_hash_of_cmd`
  (used by the squashed-pipeline fast path and the generic single-command fallback) are
  built on the static free functions and never consult `Router::slot_owners`. Since almost
  every `Router` method was already migrated to dynamic routing, the cleanest fix is
  threading a `&Router` (or just `&[usize]` slot-owners slice) into these two helpers so
  they can call `router.target_shard_for_slot(slot)` in cluster mode, exactly like
  `Router::target_shard` already does — closing the one remaining place where live
  `set_slot_owner` migration can be silently ignored.
- **High — finish unifying `Router::del` onto dynamic routing (§4.2).** A one-line change
  (`target_shard(&key, self.num_shards)` → `self.target_shard(&key)` at `router.rs:1619`)
  would remove the last static-routing straggler inside `Router` itself and make
  single-key `DEL` consistent with every sibling method, including the multi-key
  `del_keys` path that already routes dynamically.
- **Medium — delete `Router::check_slot_redirection` or make it the single source of
  truth (§4.5).** Still defined, still never called; the real redirect logic remains
  duplicated inline in `connection.rs::execute_command`.
- **Medium — remove or actually wire up the dead `if needs_async` branch in
  `server.rs`'s `ShardMessage::Batch` handler (§4.6).** `needs_async` is hard-coded
  `false`; the ~340-line async branch is unreachable and will silently rot out of sync
  with the real (synchronous) implementation.
- **Medium — convert `Router::del_keys`'s remote fan-out to the `mailbox.rs`
  scatter-gather pattern (§4.4).** Still allocates one fresh `flume::bounded(1)` per
  remote shard touched, unlike `MGET`/`MSET`'s pooled `ScatterMget`/`ScatterMsetDescriptor`
  path.
- **Low — give `FastGetDescriptor`/`FastSetDescriptor` the same sleeping-aware notify
  skip that `ScatterMget`/`ScatterMset`/`BatchResponder` now have (§3.7).** They still
  unconditionally `try_send` on every `finish()`; `GET`/`SET` are the highest-volume
  single-key operations, so this is likely the highest-value remaining Parker-handshake
  conversion.
- **Low — retire the unused `ShardMessage::Get`/`Set`/`Mget`/`Mset` variants and
  `CompactResp::RawBytes`, or document why they're kept (§3.2, §3.3).** All are compiled,
  matched, and reachable in principle, but never constructed by production code (the
  `ShardMessage` ones only by `#[cfg(test)]` code; `RawBytes` not even by tests).
- **Low — audit `Router::execute_remote`'s stack-local `responses_ptr` target for
  cancellation-safety (§3.7).** Unlike the pooled, strong-count-guarded `ConnScratch`
  path, `execute_remote`'s single-slot `CompactResp` lives on its own `async fn`'s
  stack/future with no equivalent guard; this is sound today because nothing observed in
  the codebase drops that future early, but it's worth an explicit invariant comment or a
  guard if task cancellation is ever introduced on this code path.

---

## 8. Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: `Router::slot_states` is a sparse `HashMap<u16, SlotState>` (absent ⇒
  `Stable`); `Router::slot_owners` is a **dense** `Vec<usize>` of 16,384 entries. These are
  two different data structures answering two different questions ("is this slot
  mid-migration" vs. "which local shard currently owns this slot") — `set_slot_owner`
  clears any `slot_states` entry for the slot it reassigns, so a shard-level ownership
  change via `set_slot_owner` is, by design, invisible to any code that only checks
  `SlotState` (§4.2).
* **Gotcha 2**: Each cross-shard `SpscQueue` ring has capacity 256, backed by a
  mutex-guarded overflow `VecDeque`. Hitting the overflow path consistently under load
  between two specific shards is a sign the ring capacity or the consumer's drain rate
  deserves attention.
* **Gotcha 3**: Three different "who pays for the wakeup" handshakes coexist in this
  subsystem, and they are *not* the same state machine: `FastGetDescriptor`/
  `FastSetDescriptor` use a plain `AtomicBool done` with an unconditional notify;
  `ScatterMgetDescriptor`/`ScatterMsetDescriptor` use the 3-state `DESC_RUNNING`/
  `DESC_COMPLETED`/`DESC_SLEEPING` Parker handshake; `BatchResponder` uses the 4-state
  `BATCH_IDLE`/`BATCH_RUNNING`/`BATCH_SLEEPING`/`BATCH_COMPLETED` Parker handshake (the
  extra `IDLE` state exists purely to mark a responder safe for pool reuse). Don't assume
  a pattern from one applies to another when modifying this code.
* **Gotcha 4**: `BatchResponder::write_slot` writes through a raw `AtomicPtr<CompactResp>`
  into memory the *caller* owns, from the *remote* shard's thread. The caller must not
  move, reallocate, or drop the pointed-to buffer until every dispatched responder reports
  completion — `connection.rs`'s `ConnScratch` recycling enforces this with a
  strong-count-and-`is_idle()` check; anywhere else this pattern is reused needs the same
  discipline (§3.7).
* **Gotcha 5**: `MGET`/`MSET` genuinely execute a parallel scatter-gather across every
  shard the key/pair set touches, with results written directly into shared memory — but
  multi-key `DEL` (`Router::del_keys`) still uses the older per-shard `flume::bounded(1)`
  fan-out. Don't assume all three multi-key command families share one implementation.
* **Gotcha 6**: Standalone (non-cluster) deployments — the default — route keys with
  `fxhash::hash64(hash_tag) % num_shards`, not CRC16/16384-slot hashing. The CRC16 scheme
  only activates once cluster mode is active (`cluster-enabled yes`, or
  `crate::cluster::HAS_ACTIVE_CLUSTER`).
* **Gotcha 7**: `Arc<BatchResponder>`/`Arc<ScatterMgetDescriptor>`/etc. pooling is only
  safe to reuse once a strong-count check confirms no remote shard still holds a clone —
  see `acquire_mget_descriptor`'s 128-iteration `Arc::strong_count(&desc) == 1` spin and
  `connection.rs::recycle_conn_scratch`'s equivalent check. Handing a "reused" descriptor
  to a new request while a stale remote write is still in flight would let a late reply
  silently corrupt a different request's results.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
