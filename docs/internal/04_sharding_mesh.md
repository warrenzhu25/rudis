# Component 04: Sharding Architecture & Cross-Core Mesh (Implementation)

## Component 04: Sharding Architecture & Cross-Core Mesh — Code Reference & Implementation

> **Source Files**: ``src/router.rs`, `src/shard.rs``


---

### 3. Component Architecture & Data Structures

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

#### `Router` (`src/router.rs`)

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

#### `ShardMessage` (`src/shard.rs`) — the real wire format

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

#### `SlotState` — live cluster migration states (see §4.4 for whether this is wired up)

```rust
pub enum SlotState {
    Stable,
    Migrating(String),
    Importing(String),
    Moved(String),
}
```

#### `ShardDb` (`src/shard.rs`) — the real per-core state container

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

---

### 4. Execution Algorithms & Code Logic

#### 4.1 Key Routing (unchanged from the original design)

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

#### 4.2 The Local/Remote Fork (still one-shot channels on the per-op path)

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

#### 4.3 `MGET`/`MSET` — now genuinely fan out, via dedicated `Router::mget`/`mset` methods (previously a real, unfixed gap — now resolved)

`src/connection.rs`'s `Mget`/`Mset` arms no longer loop calling `router.get`/`router.set`
per key. They call two new dedicated methods instead:

```rust
Command::Mget(keys) => {
    let values = router.mget(keys).await;
    ...
}
Command::Mset(pairs) => {
    router.mset(pairs).await;
    ...
}
```

`Router::mget`/`mset` bucket keys by target shard, execute every local key inline
(`get_local_direct`, a factored-out version of `get`'s fast/slow DRAM-vs-tiered logic),
and dispatch **one `ShardMessage::Mget`/`Mset` per remote shard that owns at least one
of the keys/pairs** — the same bucket-once-per-shard shape `execute_commands_squashed`
(Component 02) already used for single-key pipelines, now applied to a single multi-key
command's own key list:

```rust
{
    let owners = self.slot_owners.borrow();
    for (idx, key) in keys.into_iter().enumerate() {
        let slot = key_slot(&key);
        let target = owners[slot as usize];
        if target == self.shard_id { local_keys.push((idx, key)); }
        else { has_remote = true; remote_batches[target].push((idx, key)); }
    }
}
```

**Partitioning here uses the *dynamic* `slot_owners` array, not the static `target_shard`
free function** that every single-key method (`get`/`set`/`del`/...) still uses — see the
new inconsistency this creates, noted in §4.4 and §7.

Three things beyond "just bucket and fan out" are worth understanding:

1. **Pooled channel sets, not fresh allocations.** `mget_channel_pool`/`mset_channel_pool:
   Rc<RefCell<Vec<Vec<MgetChannel/MsetChannel>>>>` hold spare `Vec<flume::bounded(1)>` sets
   (one channel per shard) that `acquire_mget_channels`/`release_mget_channels` check out
   and return, so a connection issuing repeated `MGET`s reuses the same channel vector
   instead of calling `flume::bounded` fresh every time — closing the "one-shot channel
   per call" gap that used to apply here (it still applies to the single-key per-op
   methods in §4.2, unchanged).
2. **A local-only fast path with zero channel operations**: if every key/pair in the call
   happens to be local (`!has_remote`), `mget`/`mset` return immediately after the local
   loop — no channel acquire, no send, no receive at all.
3. **User-space "fast harvest" via non-blocking `try_recv`, falling back to a real
   `.await` only if needed:**

```rust
let mut remaining_mask = sent_mask;
for _ in 0..128 {
    for (target_shard, (_, rx)) in channel_set.iter().enumerate() {
        if (remaining_mask & (1 << target_shard)) != 0
            && let Ok(shard_results) = rx.try_recv()
        {
            remaining_mask &= !(1 << target_shard);
            for (idx, val) in shard_results { results[idx] = val; }
        }
    }
    if remaining_mask == 0 { break; }
    std::hint::spin_loop();
}
if remaining_mask != 0 {
    // any shard that hasn't replied within ~128 spin iterations falls back
    // to a real rx.recv_async().await here
}
```

Because remote shards on other cores often finish a trivial `Mget`/`Mset` batch in well
under a microsecond, this spins with `std::hint::spin_loop()` (a CPU hint, not a real
sleep) polling every still-pending shard's channel with a non-blocking `try_recv` up to
128 times before paying the cost of a real async suspend-and-wake. Correctness is
unaffected either way (the slow path below still awaits properly), but this does mean
the calling task **does not yield to other tasks on the same core** during the spin
window — a deliberate latency-vs-fairness trade, bounded to a small fixed iteration
count specifically so it can't spin forever if a remote shard is genuinely slow or stuck.
`shard_id` values `>= 64` are silently excluded from the bitmask fan-out/harvest
entirely (`target_shard < 64` guards throughout) — see §7 for why that's a real, if
currently theoretical, limit.

#### 4.4 `Router::check_slot_redirection` is dead, but the redirect feature itself is live — via a separate, duplicate implementation in `connection.rs` (correction to an earlier draft of this section)

`Router` has real infrastructure for Redis Cluster-style live slot migration:
`slot_owners: Rc<RefCell<Vec<usize>>>` (a per-slot ownership override, seeded from the
static `slot_to_shard` mapping but mutable via `set_slot_owner`), `slot_states` (tracking
`Migrating`/`Importing`/`Moved` per slot), and a helper method with the same shape as the
logic below:

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

Grepping the whole codebase confirms this specific method, `check_slot_redirection`, is
**never called anywhere** — that part of an earlier draft of this section was right. But
an earlier draft went one step further and concluded the whole *feature* was dead, which
is wrong: `connection.rs::execute_command` inlines the identical `SlotState` match
directly (not via this helper) at the top of its per-command dispatch, gated on
`cmd_primary_key(&cmd)`:

```rust
if let Some(key) = cmd_primary_key(&cmd) {
    let slot = key_slot(key);
    let state = router.slot_states.borrow()[slot as usize].clone();
    match state {
        SlotState::Moved(target) => { out.extend_from_slice(format!("-MOVED {} {}\r\n", slot, target).as_bytes()); return false; }
        SlotState::Importing(source) => if !is_asking {
            out.extend_from_slice(format!("-MOVED {} {}\r\n", slot, source).as_bytes()); return false;
        },
        SlotState::Migrating(target) => {
            let key_exists = router.exists(key.clone()).await;
            if !key_exists { out.extend_from_slice(format!("-ASK {} {}\r\n", slot, target).as_bytes()); return false; }
        }
        SlotState::Stable => {
            // additionally cross-checks live cluster-bus gossip ownership (Component 11's
            // ClusterHub::my_slots) and emits -MOVED to the gossiped owner if this shard's
            // own slot_states says Stable but the gossip table disagrees
        }
    }
}
```

So real `-MOVED`/`-ASK` redirects genuinely are sent from the normal command path — for
commands that go through `execute_command`. The caveat that *does* still hold: this check
lives only in `execute_command`, the single-command/non-squashed-fallback path (see
Component 02 §4/§10) — grepping `slot_states` usage confirms the pipelined
`execute_commands_squashed` fast path never checks it. **A pipelined batch of commands
hitting a migrating/moved slot silently executes against the wrong data instead of
redirecting**, while the same commands sent unpipelined (or as part of a
squash-defeating pipeline) redirect correctly. This is the real, narrower gap — not "live
migration is entirely unwired," which was the earlier draft's overstatement.

**Update since the original finding**: `slot_owners` (a separate field from
`slot_states`) is no longer consulted by only one call site. `Router::mget`/`mset` (§4.3,
newly added) now also partition keys via `self.slot_owners.borrow()[slot]` rather than
the static `target_shard()` free function — so `slot_owners` now has three real
consultation sites (the dynamic `Router::target_shard` method used by `expiretime`, plus
`mget`, plus `mset`) instead of one. **This creates a new, sharper inconsistency rather
than resolving the old one**: every single-key command (`get`, `set`, `del`, `exists`,
...) still routes via the *static* `target_shard()` free function, which has no idea
`slot_owners` exists — meaning during a live slot migration, `MGET foo` and `GET foo`
could now legitimately disagree about which shard owns `foo`, if `set_slot_owner` has
been called for that slot but the static formula would still point elsewhere. Before this
change, at least every read/write path agreed with each other (all wrong in the same way,
consistently); now there are two different, disagreeing notions of ownership active in
the same file. See §7.

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

---

### 5. Cross-Component Interactions

- **`src/connection.rs`** (Component 02): calls `target_shard_of_cmd`/`Router` methods to
  decide local vs. remote dispatch for every command; the pipelined hot path builds
  `ShardMessage::Batch` directly rather than going through `Router`'s per-op methods.
  `Mget`/`Mset` now call `router.mget(keys).await`/`router.mset(pairs).await` directly
  (§4.3) instead of looping over `router.get`/`router.set`.
- **`src/server.rs`** (Component 01): owns the receive side of every `ShardMessage`
  variant, matched in the per-shard event loop, including the two new `Mget`/`Mset`
  variants (§4.3) — the `Mget` handler itself branches on whether the shard has a
  `tier_manager` at all, taking a fully synchronous `db.get()`-per-key loop when tiering
  is disabled and a more careful (tiering-aware) path when it's enabled.
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

---

### 7. Future Improvements

- **High — unify `slot_owners` and `slot_states`/`ClusterHub.my_slots` into one slot-authority mechanism (§4.4). Now more urgent, not less.** The `MGET`/`MSET` fan-out fix (§4.3) made this worse in one specific way: `mget`/`mset` now route via `slot_owners` while every single-key command still routes via the static `target_shard()` free function, so the two families of commands can now genuinely disagree about slot ownership during a live migration, not just theoretically. Pick one source of truth (most likely `slot_states`/`ClusterHub`, since that's the one wired into `-MOVED`/`-ASK` redirection) and route every command — single-key and multi-key alike — through it.
- ~~**High — fix `MGET`/`MSET` fan-out (§4.3)**~~ **Resolved.** `Router::mget`/`mset` now bucket by shard, dispatch one `ShardMessage::Mget`/`Mset` per remote shard via pooled channels, and harvest replies with a non-blocking `try_recv` sweep before falling back to `.await` (§4.3). Two real follow-ups from the fix itself: (1) the slot-authority split noted above, and (2) neither method's bitmask-based dispatch/harvest tracks shards with `target_shard >= 64` (the code guards every bitmask operation with `target_shard < 64`) — harmless at the default `num_shards.min(8)`, but if `--threads` is ever set above 64, keys landing on shard 64+ would be sent a message that's never waited on, silently leaving those result slots as `None`/unset. Worth an explicit assertion or a `Vec<bool>`-based tracking scheme instead of a `u64` bitmask if very high shard counts are ever supported.
- **High — `MGET`/`MSET` never check `slot_states` for redirection at all, even on top of §4.4's gap (newly found).** `execute_command`'s slot-migration check (§4.4) is gated on `cmd_primary_key(&cmd)`, which has no arm for `Mget`/`Mset` (multi-key commands don't have one primary key) — so unlike every single-key command, a live `MGET`/`MSET` against a migrating/moved slot never redirects at all, squashed or not. Fixing this needs a per-key (not per-command) redirect check inside `Router::mget`/`mset` itself, likely alongside the `slot_owners` unification above.
- **Medium — delete `Router::check_slot_redirection` or make it the single source of truth (§4.4).** Right now the real redirect logic lives duplicated inline in `connection.rs` while this near-identical helper method sits unused in `router.rs`. Either delete the dead helper (simplest — removes a maintenance trap where someone "fixes" the wrong copy) or refactor `connection.rs` to call it, eliminating the duplication risk either way.
- ~~**Medium — extend the live slot-migration redirect check to the pipelined squash path.**~~ **Turned out already handled, per Component 02 §7's correction.** The squash-eligibility gate's `SlotState::Stable`-vs-not check predates this round of changes — `Migrating`/`Importing`/`Moved` already correctly defeated squashing before. What this round of changes actually added on top: the `Stable`-but-a-different-node-owns-it-per-cluster-gossip case now also correctly defeats squashing, confirmed by the new `test_cluster_pipelined_squashed_moved_redirect_e2e` E2E test — closing the one piece of this that genuinely was missing.
- **Low — reduce the ~10x duplicated local/remote-fork method bodies (§4).** The per-operation methods (`get`/`set`/`del`/`exists`/...) are intentionally monomorphic rather than generic (§4's stated rationale), which is a reasonable trade — but a thin macro that generates the boilerplate (target-shard computation, local-vs-remote branch, `flume::bounded(1)` fallback) from a one-line-per-command table would keep the monomorphic-dispatch benefit while cutting the ~10x copy-pasted structure down to one place to get right.

---
---
