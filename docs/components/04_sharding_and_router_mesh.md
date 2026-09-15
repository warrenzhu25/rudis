# Component 04: Sharding Architecture & Cross-Core Mesh (`src/router.rs`, `src/shard.rs`)

## 1. Architectural Purpose & Scope

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

## 2. Key Invariants & Concurrency Constraints

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

## 3. Component Architecture & Data Structures

```
                        Client Request (Any Thread)
                                     │
                                     ▼
                     extract_hash_tag + CRC16 → slot (0..16383)
                                     │
                          slot_to_shard(slot, num_shards)
                             (contiguous range, NOT modulo)
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                 ▼
              Local Shard?                      Remote Shard?
                    │                                 │
       Direct ShardDb mutation             ShardMessage over flume::Sender
     (+ AOF append, + tiering hooks)                   │
                                                        ▼
                                          Peer shard's ShardMessage receive
                                          loop executes it against its own
                                          ShardDb, replies via the message's
                                          own `responder: flume::Sender<T>`
```

### `Router` (`src/router.rs`)

```rust
#[derive(Clone)]
pub struct Router {
    pub shard_id: usize,
    pub num_shards: usize,
    pub port: u16,
    pub local_db: Rc<RefCell<ShardDb>>,
    pub senders: Vec<flume::Sender<ShardMessage>>,
    pub slot_states: Rc<RefCell<Vec<crate::shard::SlotState>>>,
    pub slot_owners: Rc<RefCell<Vec<usize>>>,
    pub aof: Option<Rc<RefCell<crate::aof::AofWriter>>>,
    pub pubsub: Rc<RefCell<crate::pubsub::PubSubHub>>,
    pub tx_lock: Rc<RefCell<Option<u64>>>,
    pub tx_waiters: Rc<RefCell<std::collections::VecDeque<(u64, flume::Sender<()>)>>>,
    pub is_saving: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub last_save_time: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub db_dir: std::path::PathBuf,
    pub is_auto_tiering: Rc<Cell<bool>>,
}
```

Every field beyond `shard_id`/`num_shards`/`port`/`local_db`/`senders` (the original
five) was added for a later feature: `slot_states`/`slot_owners` for live cluster slot
migration (§4.4), `aof` for write-ahead logging, `pubsub` for cross-shard channel
fan-out, `tx_lock`/`tx_waiters` for cross-shard `MULTI`/`EXEC` mutual exclusion,
`is_saving`/`last_save_time`/`db_dir` for RDB snapshotting, `is_auto_tiering` to prevent
re-entrant auto-tiering triggers.

### `ShardMessage` (`src/shard.rs`) — the real wire format

The enum has grown to roughly 40 variants (up from the original 12). Representative
slice, showing the shape that matters most — `Batch`:

```rust
pub enum ShardMessage {
    Get { key: Bytes, responder: flume::Sender<Option<Bytes>> },
    Set { key: Bytes, value: Bytes, expire_in: Option<Duration>, responder: flume::Sender<()> },
    Batch {
        items: Vec<(usize, Command)>,
        responder: flume::Sender<Vec<(usize, CompactResp)>>,
        is_resp3: bool,
    },
    NotifyList { keys: Vec<Bytes> },
    SetSlotState { slot: u16, state: SlotState },
    SetSlotOwner { slot: u16, owner: usize },
    TierSpill { key: Bytes, responder: flume::Sender<bool> },
    TierLoad { key: Bytes, responder: flume::Sender<bool> },
    TierCool { key: Bytes, responder: flume::Sender<bool> },
    TierDecommit { key: Option<Bytes>, responder: flume::Sender<usize> },
    SyncAof { responder: flume::Sender<()> },
    SaveRdbChunk { responder: flume::Sender<Vec<u8>> },
    RestoreRdbChunk { data: Bytes, responder: flume::Sender<()> },
    Publish { channel: Bytes, message: Bytes, responder: flume::Sender<usize> },
    AcquireTxLock { tx_id: u64, responder: flume::Sender<()> },
    ReleaseTxLock { tx_id: u64 },
    ExecuteReplicaCmd { cmd: Command, responder: flume::Sender<()> },
    Stick { keys: Vec<Bytes>, responder: flume::Sender<usize> },
    Delex { key: Bytes, condition: Option<(String, Bytes)>, responder: flume::Sender<bool> },
    // ...plus Del/Exists/IncrBy/Expire/Persist/Ttl/CountKeysInSlot/GetKeysInSlot/
    // ClientList/DumpKey/PubsubChannels/PubsubNumsub/PubsubNumpat/Keys/Scan/RandomKey/
    // ExpireTime/TierSpillAll/TierGc/TierSnapshot/FlushSlots/Unstick/IsSticky/
    // GetUsedMemory/StreamColdRead — one variant per cross-shard operation.
}
```

**`CompactResp` replaced plain `Vec<u8>` as the `Batch` responder payload** — a
small-buffer-optimized type:

```rust
pub enum CompactResp {
    Small { len: u8, data: [u8; 30] },
    Big(Vec<u8>),
}
```

Most RESP replies (`:123\r\n`, `+OK\r\n`, a short bulk string) fit in 30 bytes, so this
avoids a heap allocation for the overwhelmingly common case of a batched cross-shard
reply, falling back to a real `Vec<u8>` only for longer payloads.

**`is_resp3: bool` on `Batch`**: since a remote shard executing a batched command has no
direct knowledge of which protocol the originating client negotiated (RESP2 vs RESP3
differ in null/boolean/double encoding), the flag is read from a thread-local,
`crate::connection::CURRENT_CLIENT_RESP3`, at the point the message is sent and carried
across the channel so the executing shard serializes the reply correctly.

### `SlotState` — live cluster migration states (see §4.4 for whether this is wired up)

```rust
pub enum SlotState {
    Stable,
    Migrating(String),
    Importing(String),
    Moved(String),
}
```

### `ShardDb` (`src/shard.rs`) — the real per-core state container

```rust
pub struct ShardDb {
    pub table: crate::table::RudisTable,
    pub port: u16,
    pub tier_manager: Option<Rc<crate::tiering::ShardTierManager>>,
    pub vector_indexes: std::collections::HashMap<String, crate::vector::HnswIndex>,
    pub crdt_store: crate::crdt::CrdtStore,
    pub json_store: crate::json::JsonStore,
    pub probabilistic_store: crate::probabilistic::ProbabilisticStore,
    pub sticky_keys: hashbrown::HashSet<Bytes>,
}
```

`ShardDb` is mostly a thin delegate layer — well over 100 `#[inline] pub fn` methods
(`hset`, `lpush`, `zadd`, `xadd`, `pfadd`, `bitcount`, ...) that just forward to
`self.table.*`. The exceptions are `set`/`set_extended`/`del`, which additionally check
`self.table.is_tiered(&key)` / `is_cooled(&key)` first and update `tier_manager`'s stats
(and cancel any in-flight async stash operation via `op_manager.cancel_pending_stash`)
when overwriting or deleting a key that currently lives (partially) on NVMe — the
storage engine (`RudisTable`, documented in Component 05) and the tiering engine have to
stay in sync on every mutation, and `ShardDb::set`/`del` is where that happens.

---

## 4. Execution Algorithms & Code Logic

### 4.1 Key Routing (unchanged from the original design)

```rust
pub fn key_slot(key: &[u8]) -> u16 {
    let tag = extract_hash_tag(key);
    (crc16::State::<crc16::XMODEM>::calculate(tag) % 16384) as u16
}

pub fn slot_to_shard(slot: u16, num_shards: usize) -> usize {
    if num_shards <= 1 { 0 } else { ((slot as usize) * num_shards) / 16384 }
}

pub fn target_shard(key: &[u8], num_shards: usize) -> usize {
    slot_to_shard(key_slot(key), num_shards)
}
```

Still real CRC16/XMODEM over a contiguous 16384-slot space, still multiply-then-divide
(not modulo) so each shard owns one contiguous slot range — matching what Redis Cluster
clients expect from `CLUSTER SLOTS`.

### 4.2 The Local/Remote Fork (still one-shot channels on the per-op path)

Every simple accessor (`get`, `set`, `del`, `exists`, `incr_by`, `expire`, `persist`,
`ttl`, `stick`, `unstick`, `is_sticky`, `delex`, the `Tier*` ops, ...) follows the same
shape:

```rust
pub async fn del(&self, key: Bytes) -> bool {
    let target = target_shard(&key, self.num_shards);
    if target == self.shard_id {
        let deleted = self.local_db.borrow_mut().del(&key);
        if deleted {
            if let Some(aof) = &self.aof {
                if let Some(bytes) = crate::aof::command_to_resp(&Command::Del(vec![key])) {
                    aof.borrow_mut().append(&bytes);
                }
            }
        }
        deleted
    } else {
        let (tx, rx) = flume::bounded(1);
        let msg = ShardMessage::Del { key, responder: tx };
        if self.senders[target].send(msg).is_ok() {
            rx.recv_async().await.unwrap_or(false)
        } else {
            false
        }
    }
}
```

This confirms the finding from the earlier accurate draft of this document still holds:
**every one of these per-operation methods allocates a fresh `flume::bounded(1)` channel
per remote call.** They are not the hot pipelined-batch path (that's
`ShardMessage::Batch` via `connection.rs`, using a connection-scoped pre-allocated
channel pool — see Component 02) — these are the fallback used for lone commands. The
write-path methods (`set`, `del`, `incr_by`, `expire`, `persist`) also each append their
own AOF record inline on the local-execution branch, via `crate::aof::command_to_resp`.

`get` additionally has a tiering-aware fast/slow split on the local branch — it checks
the fast in-RAM path first, then (if the key is tiered) whether the shard is under enough
memory pressure to warrant a zero-copy streaming cold read (`stream_cold_read_local`)
versus loading the value back into RAM (`load_local`) before returning it.

### 4.3 `MGET`/`MSET` — still sequential, not fan-out (a real, still-unfixed gap)

Contrary to what an earlier draft of this document claimed (`execute_mget` bucketing keys
by shard and awaiting them with `futures::future::join_all`), no such function exists.
The actual handling, in `src/connection.rs`:

```rust
Command::Mget(keys) => {
    ...
    for key in keys {
        match router.get(key).await {   // one full round-trip at a time
            Some(v) => { ... }
            None => { ... }
        }
    }
}
Command::Mset(pairs) => {
    for (key, val) in pairs {
        router.set(key, val, None).await;   // one full round-trip at a time
    }
}
```

Each key is still routed and awaited one at a time. A multi-key `MGET`/`MSET` spanning
several remote shards pays for that many serialized round-trips instead of a parallel
fan-out. This is exactly the gap the original, accurate `docs/designs/components.md`
Part 4 §6 identified — it has not been fixed.

### 4.4 Live slot migration machinery exists but is not wired into routing (verified gap)

`Router` has real infrastructure for Redis Cluster-style live slot migration:
`slot_owners: Rc<RefCell<Vec<usize>>>` (a per-slot ownership override, seeded from the
static `slot_to_shard` mapping but mutable via `set_slot_owner`), `slot_states` (tracking
`Migrating`/`Importing`/`Moved` per slot), and:

```rust
pub fn check_slot_redirection(&self, slot: u16, key_exists: bool, asking: bool) -> Result<(), String> {
    match self.slot_states.borrow()[slot as usize].clone() {
        SlotState::Migrating(target) => if !key_exists { return Err(format!("-ASK {} {}\r\n", slot, target)); },
        SlotState::Importing(source) => if !asking { return Err(format!("-MOVED {} {}\r\n", slot, source)); },
        SlotState::Moved(target) => return Err(format!("-MOVED {} {}\r\n", slot, target)),
        SlotState::Stable => {}
    }
    Ok(())
}
```

However, grepping the whole codebase for call sites shows `check_slot_redirection` is
**never called anywhere** (not in `connection.rs`, not elsewhere in `router.rs` itself),
and of the 17 call sites that compute a target shard for a key, only **one**
(`expiretime`, via `self.target_shard(&key)` which does consult `slot_owners`) uses the
dynamic, migration-aware path — the other 16 (`get`, `set`, `del`, `exists`, `incr_by`,
`expire`, `persist`, `ttl`, `stick`, `unstick`, `is_sticky`, `delex`, the tiering ops,
`dump_key`, ...) all call the static free function `target_shard(key, num_shards)`,
which has no knowledge of `slot_owners` at all. In other words: **the live-migration
state machine (`SlotState`, `set_slot_owner`, `check_slot_redirection`) is built, and
`cluster_addslots`/`cluster_addslotsrange` do call `set_slot_state`, but ordinary command
routing does not consult it and no `-MOVED`/`-ASK` redirect is ever actually sent from
the normal command path.** Treat live slot migration as unfinished/dead code until this
is wired up, not as a working feature.

### 4.5 Cross-shard `SCAN` cursor encoding

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

### 4.6 Cross-shard transaction locking (`MULTI`/`EXEC` across shards)

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

## 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): calls `target_shard_of_cmd`/`Router` methods to
  decide local vs. remote dispatch for every command; the pipelined hot path builds
  `ShardMessage::Batch` directly rather than going through `Router`'s per-op methods.
- **`src/server.rs`** (Component 01): owns the receive side of every `ShardMessage`
  variant, matched in the per-shard event loop.
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

## 6. Performance Characteristics

- **Lock-Free Communication**: unchanged — `flume` channels, no mutexes, no atomics on
  the per-key data path itself.
- **Per-op remote calls still allocate**: as documented in §4.2, the non-batched router
  methods allocate a fresh one-shot channel per remote call; only the pipelined
  `ShardMessage::Batch` path (Component 02) uses a pre-allocated pool.
- **`CompactResp`'s 30-byte inline buffer** removes a heap allocation from the
  overwhelmingly common case (short RESP replies) of the cross-shard batch response path.
- **`MGET`/`MSET` do not parallelize across shards** (§4.3) — throughput on multi-shard
  keysets for these two commands is bounded by round-trip latency × number of remote
  shards touched, not by the mesh's actual concurrency capacity.
