# Component 02: Connection Lifecycle & Command Execution (`src/connection.rs`)

## 1. Architectural Purpose & Scope

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

## 2. Key Invariants & Concurrency Constraints

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

## 3. Component Architecture & Data Structures

```
                 Client TCP Stream
                        │
                        ▼
              handle_connection() loop
                        │
        ┌───────────────┼────────────────┬───────────────────┐
        ▼               ▼                ▼                   ▼
  SUBSCRIBE/       PSYNC seen?      MULTI/WATCH/       Blocking cmd
  PSUBSCRIBE?      → replica         EXEC active?       (BLPOP/…)?
  → run_pubsub_loop  stream mode     → transaction      → flush out_buf
    (mode switch,      (mode switch,   branch              first, then
    never returns)      never returns)  (§4.2)              execute
        │                                  │                   │
        └──────────────────┬───────────────┴───────────────────┘
                            ▼
              1 command? execute_command()
              N commands? execute_commands_squashed()
                            │
                ┌───────────┴────────────┐
                ▼                        ▼
         Local shard key           Remote shard key
     execute_local_command()   ShardMessage::Batch over
       direct on ShardDb        pre-allocated responder,
                                 awaited in parallel
                            │
                            ▼
                 out_buf (RESP2/RESP3/Memcached)
                            │
                            ▼
              one coalesced io_uring write_all
```

### Real Per-Connection & Per-Shard-Port State

```rust
#[derive(Clone, Debug)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub name: Option<String>,
    pub connected_at: Instant,
    pub last_active: Instant,
    pub last_cmd: String,
    pub is_resp3: bool,
    pub track_tx: Option<flume::Sender<Vec<u8>>>,
    pub raw_fd: std::os::unix::io::RawFd,
}

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

thread_local! {
    pub static CURRENT_CLIENT_RESP3: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub static IN_TX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
```

There is no `ClientContext`/`ClientTxState`/`ClientProtocol` struct anywhere in the file —
per-connection transaction state (`in_multi`, `tx_queue`, `tx_has_error`) lives as plain locals
inside `handle_connection` instead of being grouped into a struct, and RESP3-vs-RESP2 mode is
tracked via the `CURRENT_CLIENT_RESP3` thread-local plus `ClientInfo.is_resp3`, not an enum.

Two `RwLock`-guarded static maps implement optimistic-locking `WATCH` and RESP3 client-side
caching invalidation, both keyed by `(port, client_id)` so state stays scoped to one shard's
listening port even though the maps are process-wide statics:

```rust
static WATCHED_KEYS: LazyLock<RwLock<HashMap<u16, HashMap<Bytes, HashSet<u64>>>>> = ...;
static CLIENT_WATCH_TAINTED: LazyLock<RwLock<HashMap<(u16, u64), bool>>> = ...;
static TRACKING_CLIENTS: LazyLock<RwLock<HashMap<(u16, u64), ClientTracker>>> = ...;
```

`ResponderChannel` is the pre-allocated, per-shard channel pool that
`execute_commands_squashed` reuses across every pipeline flush of a connection's lifetime:

```rust
pub type ResponderChannel = (
    flume::Sender<Vec<(usize, CompactResp)>>,
    flume::Receiver<Vec<(usize, CompactResp)>>,
);
```

(`CompactResp`, defined in `src/shard.rs`, has replaced the plain `Vec<u8>` response payload
used previously — a memory-compacted reply representation, not documented here since it
belongs to `src/shard.rs`.)

---

## 4. Execution Algorithms & Code Logic

### 4.1 `handle_connection`: a mode-switching read loop, not just read-parse-execute

The loop parses every complete command out of the buffer per read, then branches on **what
kind of pipeline it just parsed** before executing anything:

```rust
// 2. Transition to Pub/Sub mode if SUBSCRIBE or PSUBSCRIBE is received
if let Some(sub_idx) = commands.iter().position(|c| matches!(c, Command::Subscribe(_) | Command::Psubscribe(_))) {
    for c in commands.drain(..sub_idx) { execute_command(c, ...).await; }
    // flush, then...
    run_pubsub_loop(stream, client_id, client_registry, router, initial_sub, commands, buf).await;
    return; // this connection never returns to the normal loop
}

// 2.5 Transition to Replica Stream mode if PSYNC is received
if let Some(psync_idx) = commands.iter().position(|c| matches!(c, Command::Psync { .. })) {
    for c in commands.drain(..psync_idx) { execute_command(c, ...).await; }
    // flush, then...
    run_master_replica_stream(stream, client_id, client_registry, router, psync_cmd).await;
    return;
}
```

Once a connection issues `SUBSCRIBE` or `PSYNC`, `handle_connection` hands the socket off to a
dedicated loop (`run_pubsub_loop` or `run_master_replica_stream`) and never returns — those are
one-way mode switches for the lifetime of that TCP connection.

For ordinary traffic, the remaining branch picks one of four execution strategies per read,
in this priority order: **(a)** a transaction is open or this batch contains
`MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` → the transaction branch (§4.2); **(b)** the batch
contains a blocking command → execute sequentially, flushing `out_buf` immediately before each
blocking one (§4.3); **(c)** exactly one command → `execute_command` directly, no batch
bookkeeping; **(d)** more than one command, none blocking, no transaction control → attempt
`execute_commands_squashed` (§4.4).

### 4.2 Transactions: `MULTI`/`EXEC`/`WATCH`, with real cross-shard locking

Queued commands are buffered per-connection (`tx_queue: Vec<Command>`); `WATCH` registers keys
in the `WATCHED_KEYS` static, and any write to a watched key anywhere flips
`CLIENT_WATCH_TAINTED` for that client (checked at `EXEC` time to abort optimistically, exactly
like Redis `WATCH` semantics). When `EXEC` actually runs the queue, if it spans more than one
shard it acquires an explicit cross-shard lock before executing any of the queued commands:

```rust
let mut shards = hashbrown::HashSet::new();
for cmd in &tx_queue {
    for k in cmd_keys(cmd) { shards.insert(router.target_shard(k)); }
}
let mut sorted_shards: Vec<usize> = shards.into_iter().collect();
sorted_shards.sort_unstable();

let use_vll = sorted_shards.len() > 1;
let tx_id = if use_vll {
    static NEXT_TX: AtomicU64 = AtomicU64::new(1);
    let id = NEXT_TX.fetch_add(1, Ordering::Relaxed);
    router.acquire_tx_locks(&sorted_shards, id).await;
    id
} else { 0 };

let hub_arc = crate::block::get_block_hub_for_port(router.port);
hub_arc.lock().unwrap().pause();       // suppress blocking-op wakeups mid-transaction
IN_TX.set(true);
for q_cmd in queued { execute_command(q_cmd, ...).await; }
IN_TX.set(false);
if use_vll { router.release_tx_locks(&sorted_shards, tx_id).await; }
let pending = hub_arc.lock().unwrap().resume();  // replay any notifications that arrived mid-tx
```

Sorting `sorted_shards` before acquiring locks is a lock-ordering discipline to avoid
deadlocking against a concurrent transaction that touches an overlapping, differently-ordered
shard set. The `BlockHub` is explicitly paused for the transaction's duration and any
list/zset-push notifications that arrive while paused are queued and replayed afterward,
rather than waking a blocked client mid-transaction.

### 4.3 Blocking commands: flush before you block

```rust
} else if commands.iter().any(|c| matches!(c, Command::Blpop { .. } | Command::Brpop { .. }
    | Command::Blmove { .. } | Command::Blmpop { .. } | Command::Bzpopmin { .. }
    | Command::Bzpopmax { .. } | Command::Bzmpop { .. }
    | Command::Xread { block_ms: Some(_), .. } | Command::Xreadgroup { block_ms: Some(_), .. })) {
    for cmd in commands {
        if matches!(cmd, /* same blocking set */) {
            if !out_buf.is_empty() {
                // flush everything buffered so far before this command can suspend the task
                let (res, returned_buf) = stream.write_all(write_chunk).await;
                ...
            }
        }
        execute_command(cmd, ...).await;
    }
}
```

Blocking commands are detected up front for the whole batch and executed strictly
sequentially (never squashed), each preceded by a flush of anything already queued in
`out_buf` — necessary because a blocking command can legitimately suspend the connection's
task for seconds while waiting on `BlockHub`, and a client shouldn't see its earlier pipelined
replies delayed by that wait.

### 4.4 `execute_commands_squashed`: the squash gate now enforces ACL and real cluster ownership, and `MGET`/`MSET` are squashable again

The core squash-or-fallback mechanism is unchanged in shape (bucket local commands inline,
bucket remote commands per target shard, dispatch one `ShardMessage::Batch` per shard,
await all in parallel, reassemble in original order), but the eligibility check has grown
two real security/correctness gates beyond the original authentication-and-migration check:

```rust
let mut can_squash = *authenticated;
if can_squash {
    for cmd in &commands {
        if matches!(cmd, Command::Blpop { .. } | ... | Command::Watch(_) | Command::Unwatch) {
            can_squash = false;
            break;
        }
        {
            let acl = crate::acl::get_acl_for_port(router.port);
            let acl_guard = acl.read().unwrap();
            if let Some(user) = acl_guard.get_user(auth_user) {
                let cmd_name = get_cmd_name(cmd);
                if !user.can_execute_command(cmd_name) { can_squash = false; break; }
                if let Some(k) = cmd_primary_key(cmd)
                    && !user.can_access_key(k.as_ref())
                { can_squash = false; break; }
            }
        }
        if let Some(k) = cmd_primary_key(cmd) {
            let slot = key_slot(k);
            if router.slot_states.borrow()[slot as usize] != crate::shard::SlotState::Stable {
                can_squash = false;   // don't squash while this key's slot is migrating
                break;
            }
            let hub = crate::cluster::get_cluster_hub(router.port);
            let my_slots = hub.my_slots.read().unwrap();
            let owns_slot = my_slots.iter().any(|&(s, e)| slot >= s && slot <= e);
            let nodes = hub.nodes.read().unwrap();
            if !owns_slot && !nodes.is_empty() {
                can_squash = false;   // Stable locally, but cluster gossip says a peer node owns it
                break;
            }
        } else if !matches!(cmd, Command::Ping(_) | Command::CommandDocs | Command::Quit
            | Command::Time | Command::Echo(_) | Command::Mget(_) | Command::Mset(_)) {
            can_squash = false;
            break;
        }
    }
}
```

The `SlotState::Stable`-vs-not check alone was already present before this round of changes
and already correctly deferred squashing during `Migrating`/`Importing`/`Moved` (an earlier
version of this doc's Future Improvements section incorrectly flagged this as unfixed — it
wasn't; see §7). What's genuinely new is the **ACL check** (a squashed batch containing a
command the authenticated user can't run, or whose key they can't access, now correctly
falls back to sequential execution so `execute_command`'s real `-NOPERM` rejection applies
per-command — see §4.5) and the **cluster-gossip ownership check**: a slot can be locally
`Stable` (no migration in progress) while a multi-node cluster's gossip table says a *different
node* now owns it (e.g. after `CLUSTER SETSLOT ... NODE` on another node propagated via gossip)
— that case now also defeats squashing so the sequential fallback's real `-MOVED` check
(§4.5) fires, confirmed by `test_cluster_pipelined_squashed_moved_redirect_e2e` in
`tests/test_server_e2e.rs`.

**One narrow gap in this gate, verified**: for multi-key commands, `cmd_primary_key` returns
only the *first* key (`keys.first()`/`pairs.first()`) — so a squash-eligibility check for an
`MGET`/`MSET`/`SINTER`/etc. spanning several keys validates ACL/slot-state/ownership against
only that first key, not every key in the command. A command whose first key is permitted and
stable but whose second key is ACL-forbidden or mid-migration could still be judged
squash-eligible on this check alone (see §4.6 for how that plays out for `MGET`/`MSET`
specifically once inside the squash path).

`MGET`/`MSET` are now explicitly allowed through the eligibility gate (added to the
`Ping`/`CommandDocs`/... safe list above, since `target_shard_of_cmd` still returns `None`
for them — they have no single target shard). The all-or-nothing squash-defeat behavior for
everything *not* on this expanding safe/eligible list is otherwise unchanged: one ineligible
command in a pipeline still forces the whole pipeline to the sequential fallback.

Inside the squash path, `GET` gets a special inline case for transparent NVMe-tiered reads
(the auto-tiering system can move cold values to disk):

```rust
if let Command::Get(ref key) = cmd {
    let val = router.local_db.borrow_mut().get(key);
    if let Some(v) = val {
        write_resp_bulk(&mut local_buf, &v);
    } else if router.local_db.borrow_mut().table.is_tiered(key).is_some() {
        if let Some(v) = router.stream_cold_read_local(key).await {
            write_resp_bulk(&mut local_buf, &v);
        } else { local_buf.extend_from_slice(b"$-1\r\n"); }
    } else { local_buf.extend_from_slice(b"$-1\r\n"); }
}
```

and any batch containing a local `SET`/`DEL`/`IncrBy` spawns a fire-and-forget background
task afterward to check whether auto-tiering should kick in under memory pressure:

```rust
if has_local_writes {
    let r = router.clone();
    monoio::spawn(async move { r.check_auto_tier().await; });
}
```

### 4.5 `execute_command`: auth, `ASKING`/`MOVED`/migration redirection, then dispatch

Every single-command execution (squashed or not) runs through the same gate sequence before
reaching its command-specific match arm:

```rust
if !*authenticated && !matches!(cmd, Command::Auth { .. } | Command::Hello { .. } | Command::Quit) {
    out.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
    return false;
}

if *authenticated {
    let acl = crate::acl::get_acl_for_port(router.port);
    let acl_guard = acl.read().unwrap();
    if let Some(user) = acl_guard.get_user(auth_user) {
        if !user.can_execute_command(cmd_name) {
            out.extend_from_slice(format!(
                "-NOPERM this user has no permissions to run the '{}' command\r\n",
                cmd_name.to_lowercase()).as_bytes());
            return false;
        }
        if let Some(key) = cmd_primary_key(&cmd) && !user.can_access_key(key.as_ref()) {
            out.extend_from_slice(b"-NOPERM this user has no permissions to access one of the keys used as arguments\r\n");
            return false;
        }
    }
}

if let Command::Asking = cmd { *asking = true; out.extend_from_slice(b"+OK\r\n"); return false; }
let is_asking = *asking;
*asking = false;

if let Some(key) = cmd_primary_key(&cmd) {
    let slot = key_slot(key);
    match router.slot_states.borrow()[slot as usize].clone() {
        SlotState::Moved(target) => { /* -MOVED slot target */ return false; }
        SlotState::Importing(source) => { if !is_asking { /* -MOVED slot source */ return false; } }
        SlotState::Migrating(target) => {
            if !router.exists(key.clone()).await { /* -ASK slot target */ return false; }
        }
        SlotState::Stable => { /* check this node still owns the slot per cluster gossip, else -MOVED */ }
    }
}
if crate::replication::get_replication_hub(router.port).is_slave()
    && crate::aof::command_to_resp(&cmd).is_some() {
    out.extend_from_slice(b"-READONLY You can't write against a read only replica.\r\n");
    return false;
}
```

This is a genuine, working implementation of Redis ACL authorization (`-NOPERM` for both
disallowed commands and disallowed keys, backed by real per-user command/key rules — see
Component 15 for `AclUser`'s own real permission model), the Redis Cluster live-migration
protocol (`MOVED`/`ASK`/`ASKING`, four-state `SlotState`), and replica-side write rejection —
not placeholders. `execute_command`'s command-specific match arm that follows is roughly 3,900
lines covering the full command surface; `execute_local_command` (the shard-local,
synchronous counterpart called both for genuinely-local keys and for remote batches arriving
via `ShardMessage::Batch`) is a similarly-sized ~3,750-line match. Both are too large to
usefully excerpt command-by-command here — the mechanism worth understanding is that they
are two independent match statements over the same `Command` enum kept in sync by hand (see
`docs/designs/components.md` Part 3 §7 for why that duplication exists rather than a single
shared function — that rationale still holds structurally, though the functions themselves
have grown far beyond what that doc shows).

### 4.6 `MGET`/`MSET` now genuinely fan out in parallel — real bucketing, a pooled channel set, and a spin-then-block harvest

**This closes the gap this doc previously flagged.** `execute_command`'s `Mget`/`Mset` arms
now delegate to real `Router::mget`/`Router::mset` methods in `src/router.rs` (Component 04)
instead of looping `router.get`/`router.set` per key:

```rust
Command::Mget(keys) => {
    for key in &keys { record_client_read(router.port, client_id, key.as_ref()); }
    let values = router.mget(keys).await;
    write_resp_array_header(out, values.len());
    for val in values { /* write bulk or null per value, in original key order */ }
    false
}
```

`Router::mget`/`mset` (verified in `src/router.rs`, summarized here since the mechanism is
architecturally significant to how this file's commands behave — full detail belongs to
Component 04):
1. **Fast path**: if `num_shards <= 1`, every key is local — no channel involved at all.
2. **Partition by slot using `slot_owners`** (the *dynamic*, migration-aware per-slot
   ownership override — not the static `target_shard` function most other commands use),
   without holding the `RefCell` borrow across an `.await` point.
3. **Local keys execute immediately**, no I/O. If every key turned out to be local, return
   immediately — zero channel operations for an all-local `MGET`/`MSET`.
4. **Remote keys are bucketed one `Vec` per shard** and dispatched as single
   `ShardMessage::Mget`/`Mset` messages (new variants added to `src/shard.rs`'s
   `ShardMessage` enum in this change) over a **pooled, reusable channel set**
   (`acquire_mget_channels`/`release_mget_channels`, backed by `Rc<RefCell<Vec<Vec<MgetChannel>>>>`)
   — not a fresh one-shot channel per call, closing the "Router's per-op methods allocate a
   fresh channel" gap this doc's Future Improvements once raised for the single-command path
   specifically for these two commands.
5. **Harvesting is spin-then-block, not an immediate `.await`**: after sending, up to 128
   iterations of a tight loop non-blockingly `try_recv()`s every still-pending shard's
   channel (with `std::hint::spin_loop()` between sweeps), only falling back to a real
   `rx.recv_async().await` per still-outstanding shard if 128 spins weren't enough. On a
   busy multi-core machine, a cross-shard hop often completes within microseconds — faster
   than a scheduler round-trip would take — so this trades a bounded amount of CPU spinning
   for avoiding that round-trip in the common case.

**Two real, verified gaps in this new mechanism, not present in the old sequential code
(which had no such edge cases because it never batched):**
- **A `> 64` shard correctness bug.** `sent_mask`/`remaining_mask` are `u64` bitmasks, and
  both the send loop and both harvest loops (spin and fallback) gate every bit operation on
  `target_shard < 64`. The *send* to a shard `>= 64` still happens
  (`self.senders[target_shard].send(msg)` is unconditional), but that shard's bit is never
  set in `sent_mask`, so **its response is never collected in either harvest loop** — the
  channel holding that reply is returned to the pool with an unread message still in it, and
  every key that was routed to shard 64+ silently comes back as `None`/not-set in the
  `MGET`/`MSET` reply, even though the key exists. `acquire_mget_channels` itself allocates
  `self.num_shards` channels (not capped at 64), so this isn't an intentional scale limit —
  it's a latent bug that only manifests on a deployment with more than 64 shards (`--threads`
  above 64), which the default `num_cores.min(8)` and typical benchmark configurations never
  exercise.
- **Only the first key of a multi-key `MGET`/`MSET` is checked for squash-eligibility**
  (§4.4's noted gap) — since `router.mget`/`mset` themselves are called either via the
  sequential fallback (`execute_command`, which does its own ACL/slot check but — per the
  code shown above — only against `cmd_primary_key`, i.e. the *first* key, before calling
  `router.mget`) or via the squash path's special case (§4.4), no per-key ACL/slot-state
  check happens inside `Router::mget`/`mset` itself for keys beyond the first. A client with
  access to an `MGET`'s first key but not its second key would not be rejected by anything
  observed in this file.

### 4.7 `format_score`: exact `%.17g` compatibility, verified accurate

```rust
#[inline]
pub fn format_score(val: f64) -> String {
    if val.is_nan() { "nan".to_string() }
    else if val.is_infinite() { if val.is_sign_positive() { "inf".to_string() } else { "-inf".to_string() } }
    else if val == 0.0 { "0".to_string() } // Prevents "-0" for negative zero
    else {
        let mut buf = [0u8; 64];
        let len = unsafe {
            libc::snprintf(buf.as_mut_ptr() as *mut libc::c_char, buf.len(),
                b"%.17g\0".as_ptr() as *const libc::c_char, val)
        };
        if len > 0 && (len as usize) < buf.len() {
            unsafe { std::str::from_utf8_unchecked(&buf[..len as usize]) }.to_string()
        } else { val.to_string() }
    }
}
```

This one is genuinely as previously documented: sorted-set scores are formatted via a direct
`libc::snprintf(..., "%.17g", ...)` FFI call specifically to match the exact string Redis's C
implementation would produce (double-to-string conversion is not guaranteed bit-identical
across languages' default formatters), which matters for passing the vendored official Redis
test suite (`tests/`) byte-for-byte. `write_resp_score` (also in this file) picks between this
and RESP3's native `,<double>\r\n` double type depending on `CURRENT_CLIENT_RESP3`.

---

## 5. Cross-Component Interactions

- **`src/resp.rs`**: Supplies `parse_command`, decoding buffered bytes into `Command` values.
- **`src/router.rs`**: `Router` provides `target_shard`/`key_slot`-based local/remote
  decisions, the `senders` mesh, `slot_states` (cluster migration state), `slot_owners`
  (the dynamic ownership override `mget`/`mset` route by — see §4.6), the pooled
  `mget`/`mset` channel sets, and `stream_cold_read_local`/`check_auto_tier`
  (tiered-storage integration).
- **`src/table.rs`** / **`src/shard.rs`**: `execute_local_command` mutates `ShardDb`/`RudisTable`
  directly; `CompactResp` (defined in `shard.rs`) is the reply payload type carried through
  `ShardMessage::Batch`.
- **`src/block.rs`**: `BlockHub` (`get_block_hub_for_port`) — registration/wakeup for
  `BLPOP`/`BZPOPMIN`/blocking `XREAD`, and the pause/resume mechanism transactions use.
- **`src/pubsub.rs`**: `PubSubHub`, entered via `run_pubsub_loop` on `SUBSCRIBE`/`PSUBSCRIBE`.
- **`src/replication.rs`**: replica-stream mode (`run_master_replica_stream`) and the
  `is_slave()` write-rejection check.
- **`src/cluster.rs`**: `get_cluster_hub` supplies the gossiped slot-ownership table consulted
  during `MOVED` redirection.
- **`src/acl.rs`** (Component 15): `authenticated`/`auth_user` are checked against
  `get_acl_for_port` at connection start and on every `AUTH`; `execute_command` and the
  squash-eligibility gate (§4.4/§4.5) both now also enforce real per-command/per-key
  authorization (`-NOPERM`) via `AclUser::can_execute_command`/`can_access_key`, not just
  the initial authentication check.
- **`src/aof.rs`**: `execute_local_command` takes an `Option<&RefCell<AofWriter>>` to append
  write commands for persistence; `command_to_resp` is also reused to detect "is this command
  a write" for the replica read-only guard.

---

## 6. Performance Characteristics

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

## 7. Future Improvements

- ~~High — fix `MGET`/`MSET` to bucket-and-fan-out instead of one round-trip per key.~~ **Resolved.** `Router::mget`/`mset` (§4.6) now bucket by shard via `slot_owners`, dispatch via pooled channels, and harvest with a spin-then-block loop. Replaced by two new findings below, both verified in the shipped fan-out code itself.
- **High — fix the `> 64`-shard `MGET`/`MSET` response-collection bug (§4.6).** `sent_mask`/`remaining_mask` are `u64` bitmasks that silently exclude any `target_shard >= 64` from both harvest loops, even though the request is still sent and `acquire_mget_channels` allocates a full `num_shards`-sized channel set. On a deployment with more than 64 shards, every key routed to shard 64+ comes back as a false negative (`None`/missing) in the `MGET`/`MSET` reply. Fix: use a `Vec<bool>`/bitset sized to `num_shards` (or chunk the bitmask across multiple `u64`s) instead of a single fixed-width `u64`.
- **Medium — check every key of a multi-key `MGET`/`MSET`/`SINTER`-style command for ACL/slot-state eligibility, not just the first (§4.4/§4.6).** `cmd_primary_key` deliberately returns one key for routing purposes, but reusing it for squash-eligibility and for `execute_command`'s ACL/slot gate means only that first key is actually checked. A command spanning a permitted-and-stable first key plus a forbidden-or-migrating second key currently isn't rejected by anything in this file. Fix: for known multi-key commands, iterate all their keys in the eligibility/ACL/slot checks rather than delegating to `cmd_primary_key` alone.
- ~~High — close the correctness gap where the squashed fast path skips slot-migration redirection.~~ **Turned out already handled** for the `Migrating`/`Importing`/`Moved` states — that `SlotState::Stable`-vs-not check predates this round of changes. What genuinely *was* missing and has now been added: the `Stable`-but-a-different-node-owns-it-per-gossip case (§4.4), confirmed fixed by `test_cluster_pipelined_squashed_moved_redirect_e2e`.
- **Medium — de-risk `execute_command`/`execute_local_command`'s ~3,900/~3,750-line hand-synced duplication (§4.5).** Two independent `match` statements over the same `Command` enum, kept in sync by hand, is exactly the kind of surface where a new command variant gets full local semantics but is forgotten in the remote-batch arm (or vice versa). A macro that generates both arms from one command-behavior definition, or at minimum a `cargo test` that asserts both matches are exhaustive over the same variant set, would catch that class of bug before it reaches production.
- **Medium — replace the two-fresh-`Lua::new()`-per-`FCALL` pattern's blast radius on this file's dispatch cost.** Not this file's bug directly (Component 13 owns it), but every `Fcall`/`Eval`/`Evalsha` arm here pays for it; consider whether `connection.rs`'s command-dispatch layer should expose a lightweight "is this a scripting command" fast-path hint so future caching work in `scripting.rs` doesn't require touching this file's dispatch tables.
- **Low — give `CMD_STATS`/`record_cmd_stat` a per-shard-then-aggregate design instead of one global `RwLock`ed map (§6).** Currently modest overhead, but as command volume grows this is one more process-wide lock on the per-command hot path alongside `BlockHub`/ACL/search/scripting — consolidating or sharding it would keep the "how many process-wide locks exist" count from growing unnoticed.
