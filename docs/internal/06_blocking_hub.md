# Component 06: Blocking Operations & The Reactive Event Hub (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/block.rs` (765 lines), plus the call sites that drive it in `src/connection.rs`, `src/server.rs`, `src/router.rs`, `src/shard.rs`, `src/mailbox.rs`.
> **High-Level Design Spec**: [`docs/design/06_blocking_hub.md`](../design/06_blocking_hub.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

This document is a line-level reference. Every claim below cites the current file/line it was verified against (checked 2026-09-30, HEAD `5657bb6`). `src/block.rs` itself only defines the waiter registry and the pop/notify algorithms — it has **no async code and no command parsing**. All blocking/timeout/polling/IPC logic lives in the callers, chiefly `src/connection.rs`.

---

## 1. Source Module Map

| File | Role |
| :--- | :--- |
| `src/block.rs` | `BlockHub`: waiter queues keyed by `Bytes` (key name), synchronous pop-and-deliver algorithms, pause/resume for `MULTI`/`EXEC`. |
| `src/connection.rs` | Command handlers for every blocking command; `wait_for_blocked_result`/`wait_for_stream_result` (the actual async wait loop + fd-disconnect poll); `notify_list_or_defer`/`notify_zset_or_defer`/`notify_stream_or_defer` (the write-command hooks that drive the hub); `CLIENT UNBLOCK`/`CLIENT LIST`/`INFO` integration. |
| `src/server.rs` | Handles the cross-shard `ShardMessage::NotifyList` IPC message (the one case where a *different* OS thread must run the notify algorithm); contains the `has_blocked_waiters` guards on the `LPUSH`/`ZADD` "fast paths". |
| `src/router.rs` | `rpush`/`lpush`/etc. convenience wrappers that call `notify_list_or_defer` after a local write; `CLIENT LIST` "b" flag rendering via `hub.is_blocked(id)`. |
| `src/shard.rs` | Defines `ShardMessage::NotifyList { keys: Vec<Bytes> }` (shard.rs:423-424), the mailbox variant used for cross-shard notification during `EXEC`. |
| `src/replication.rs` | `wait_replicas` — the **separate**, non-`BlockHub` polling loop that backs `WAIT`/`WAITAOF` (see §6.10). |

---

## 2. Core Data Structures (`src/block.rs:1-94`)

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ListPopType { Left, Right }                              // block.rs:7

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientUnblockType { Timeout, Error, WrongType }          // block.rs:13

#[derive(Clone, Debug)]
pub enum BlockedListResult {                                      // block.rs:20
    Popped(Bytes, Vec<Bytes>),
    Unblocked(ClientUnblockType),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZSetPopType { Min, Max }                                  // block.rs:26

#[derive(Clone, Debug)]
pub enum BlockedZSetResult {                                       // block.rs:32
    Popped { key: Bytes, items: Vec<(Bytes, f64)>, is_zmpop: bool },
    Unblocked(ClientUnblockType),
}

pub struct ZSetWaiter {                                            // block.rs:41
    pub client_id: u64,
    pub key: Bytes,
    pub pop_type: ZSetPopType,
    pub count: usize,
    pub is_zmpop: bool,
    pub sender: Sender<BlockedZSetResult>,                         // flume::Sender
}

#[derive(Clone, Debug)]
pub enum WaiterOp {                                                 // block.rs:51
    Pop   { pop_type: ListPopType, count: usize },
    Move  { where_from: ListPopType, where_to: ListPopType, destination: Bytes },
    Movem { where_from: ListPopType, where_to: ListPopType, destination: Bytes,
            mode: crate::resp::LmovemMode, count: usize, ordering: crate::resp::LmovemOrdering },
}

pub struct ListWaiter {                                            // block.rs:71
    pub client_id: u64,
    pub key: Bytes,
    pub op: WaiterOp,
    pub sender: Sender<BlockedListResult>,
}

pub struct StreamWaiter {                                          // block.rs:78
    pub client_id: u64,
    pub key: Bytes,
    pub sender: Sender<()>,
}
```

`WaiterOp::Movem` is new (it did not exist in the pre-growth version of this file): it backs `BLMOVEM`, the blocking form of the new multi-element `LMOVEM` command (`git log`: `17e55ef feat(list): implement LMOVEM and BLMOVEM`). `ZSetWaiter.is_zmpop` and `ZSetWaiter.count` generalize the old single-element `BZPOPMIN`/`BZPOPMAX` waiter into also covering `BZMPOP` (count ≥ 1, `is_zmpop: true`).

### 2.1 `BlockHub` struct (`block.rs:84-94`)

```rust
pub struct BlockHub {
    pub port: u16,
    list_waiters: HashMap<Bytes, VecDeque<ListWaiter>>,            // FIFO per key
    zset_waiters: HashMap<Bytes, VecDeque<ZSetWaiter>>,             // FIFO per key
    stream_waiters: HashMap<Bytes, Vec<StreamWaiter>>,              // NOT FIFO-ordered, see §7
    blocked_clients: HashMap<u64, Sender<BlockedListResult>>,       // client_id -> list/move/movem sender
    blocked_zset_clients: HashMap<u64, Sender<BlockedZSetResult>>,  // client_id -> zset sender
    blocked_stream_clients: HashMap<u64, Sender<()>>,               // client_id -> stream sender
    paused_count: usize,                                            // nesting depth for MULTI/EXEC pause
    pending_notifies: Vec<Bytes>,                                   // keys deferred while paused
}
```

Nine fields total (was smaller pre-growth; `pending_notifies`/`paused_count` are the `MULTI`/`EXEC` deferred-notify machinery, §8). Every waiter queue is keyed by the **raw key `Bytes`**, not by shard — see §4 for why that is safe.

### 2.2 Global registry (`block.rs:96-111`)

```rust
pub static PORT_BLOCK_HUBS: LazyLock<Mutex<HashMap<u16, Arc<Mutex<BlockHub>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static TOTAL_BLOCKED_WAITERS: std::sync::atomic::AtomicUsize = AtomicUsize::new(0);

#[inline(always)]
pub fn has_blocked_waiters(_port: u16) -> bool {
    TOTAL_BLOCKED_WAITERS.load(Ordering::Relaxed) > 0
}

pub fn get_block_hub_for_port(port: u16) -> Arc<Mutex<BlockHub>> {
    let mut map = PORT_BLOCK_HUBS.lock().unwrap();
    map.entry(port).or_insert_with(|| Arc::new(Mutex::new(BlockHub::new(port)))).clone()
}
```

**There is exactly one `BlockHub` per listening port**, not one per shard, lazily created in a process-wide `Mutex<HashMap<u16, Arc<Mutex<BlockHub>>>>`. This is the cross-shard mechanism — see §4.

**Verified bug/surprise**: `has_blocked_waiters`'s `port` parameter is named `_port` and is **unused**. `TOTAL_BLOCKED_WAITERS` is one process-wide `AtomicUsize`, not per-port, even though `PORT_BLOCK_HUBS` is keyed by port. `BlockHub::sync_atomic_waiters_count` (block.rs:134-144) computes a count from `self` alone and does `TOTAL_BLOCKED_WAITERS.store(count, ...)` — an unconditional overwrite, not an add/sub against a per-hub baseline. If a single process ever hosts two `BlockHub`s for two different ports (e.g. a test binary that spins up two servers, or `cfg(test)` tests in `block.rs` itself running in parallel threads with ports 12345/12346/12347), `sync_atomic_waiters_count` calls on hub A will clobber whatever hub B last stored. In the worst case this can make `has_blocked_waiters(portB)` return `false` while port B genuinely has blocked waiters, which causes `notify_list_or_defer`/`notify_zset_or_defer`/`notify_stream_or_defer` (connection.rs:211-253) to return immediately without ever taking the hub lock or delivering the notification — a real missed-wakeup, not just a missed optimization. In the common single-port-per-process deployment this is inert, since the shared counter degenerates to a correct single-hub counter.

---

## 3. `BlockHub` method inventory (`impl BlockHub`, `block.rs:119-619`)

| Method | Lines | Purpose |
| :--- | :--- | :--- |
| `new(port)` | 120-132 | Zero-initializes all six maps/counters. |
| `sync_atomic_waiters_count(&self)` | 135-144 | Recomputes `paused_count + Σlen(list/zset/stream waiters) + blocked_clients.len()*3-ish` and stores into the (global, see §2.2) atomic. Called after every mutating op. |
| `is_paused` / `pause` / `add_pending_notify` / `resume` / `clear_pending_notifies` | 146-179 | `MULTI`/`EXEC` deferred-notify machinery, §8. |
| `blocked_clients_count` | 181-185 | Sum of the three `blocked_*_clients` map lengths — used by `INFO`'s `blocked_clients`. |
| `blocking_keys_count` | 187-205 | Distinct-key union across list/zset/stream waiter maps (non-empty queues only) — `INFO`'s `total_blocking_keys`. |
| `blocking_keys_on_nokey_count` | 207-209 | `stream_waiters` non-empty-queue count only — `INFO`'s `total_blocking_keys_on_nokey`. Note: this does **not** distinguish "NOKEY" semantics the way real Redis does; it is simply "how many keys have at least one stream waiter," since only stream blocking (`XREAD`/`XREADGROUP`) in this codebase is driven by key-doesn't-exist-yet waits. |
| `register_blocked_client` / `register_blocked_zset_client` | 211-223 | Insert into `blocked_clients`/`blocked_zset_clients` keyed by `client_id` (used by `CLIENT UNBLOCK`/disconnect cleanup — list/move/movem waiters all share one sender type so one map suffices). |
| `is_blocked(client_id)` | 225-229 | OR of membership in all three `blocked_*` maps — backs the `CLIENT LIST`/`CLIENT INFO` `flags=b` bit. |
| `remove_waiters_for_client` | 231-248 | Scrubs a client's entries out of **all** list/zset/stream waiter queues (not just one key) plus the three `blocked_*` maps, then prunes empty queue entries and re-syncs the atomic. This is what guarantees a client blocked on `BLPOP k1 k2` doesn't leave a dangling `k2` waiter after being satisfied via `k1`. |
| `unregister_blocked_client` | 250-252 | Thin wrapper over `remove_waiters_for_client`; called from disconnect/timeout paths. |
| `unblock_client(id, type)` | 254-281 | `CLIENT UNBLOCK` implementation — looks the id up in each `blocked_*` map, sends `Unblocked(type)` (or, for streams, just `try_send(())`, see §9.3 bug), purges that client's waiter entries, returns whether anything was unblocked. |
| `register_list_waiter` | 283-301 | Push a `ListWaiter{ op: Pop }` onto `list_waiters[key]` (back of the `VecDeque` — FIFO). Backs `BLPOP`/`BRPOP`/`BLMPOP`. |
| `register_move_waiter` | 303-326 | Push `ListWaiter{ op: Move }` — backs `BLMOVE`. |
| `register_movem_waiter` | 328-357 | Push `ListWaiter{ op: Movem }` — backs `BLMOVEM` (new). |
| `notify_list(table, key)` | 359-516 | The pop-and-deliver algorithm for all three `WaiterOp` variants, run under the hub lock using the **caller's local `RudisTable`**. See §3.1. |
| `register_zset_waiter` | 518-539 | Push `ZSetWaiter` — backs `BZPOPMIN`/`BZPOPMAX` (count=1, is_zmpop=false) and `BZMPOP` (count=N, is_zmpop=true). |
| `notify_zset(table, key)` | 542-584 | Pop-and-deliver for sorted sets. See §3.1. |
| `register_stream_waiter` | 586-593 | Insert into `blocked_stream_clients` **and** push into `stream_waiters[key]`, de-duplicated by `client_id` within that key's `Vec`. Backs `XREAD BLOCK`/`XREADGROUP BLOCK`. |
| `notify_stream(key)` | 596-612 | Broadcast wakeup to every waiter on that key — see §3.2 for the `thread::sleep` finding. |
| `notify_all_streams` | 614-619 | Iterates every key with stream waiters and calls `notify_stream` on each — used by `MULTI`/`EXEC` resume (`connection.rs:1458`) and `FLUSHALL`/`FLUSHDB` (`connection.rs:14787`) to wake everyone unconditionally. |

### 3.1 `notify_list` / `notify_zset`: the actual pop algorithm

Both run entirely **under the `Mutex<BlockHub>` lock**, taking `&mut RudisTable` from the caller so the pop is atomic with the notification (no other thread can observe the pushed-then-immediately-repopped state): the push already happened (caller already ran `LPUSH`/`ZADD` against its own local table before calling `notify_*_or_defer`), and `notify_list`/`notify_zset` re-enter the table to pop the element(s) back out on the blocked waiter's behalf, then ship the popped value(s) through the waiter's `flume::Sender`.

`notify_list` (`block.rs:361-516`) walks `list_waiters[key]` front-to-back with `pop_front()`:
- `WaiterOp::Pop{ pop_type, count }`: pops `count` elements via `table.lpop`/`rpop`. If elements were available, sends `Popped`, marks the client satisfied, and **keeps looping** (so one `LPUSH` with N elements can satisfy multiple FIFO-queued `BLPOP`/`BLMPOP` waiters in a single notify call) — unless the key no longer exists, which stops the loop. If nothing was available, it `push_front`s the waiter back and stops (nothing later in the queue can be satisfied either, since count-based pops are monotonic in availability).
- `WaiterOp::Move{ where_from, where_to, destination }`: checks `destination`'s type is `none`/`list` (else sends `WrongType` and continues to the next waiter), pops exactly one element, pushes it onto `destination`, sends `Popped`, and **unconditionally `break`s** — only one `BLMOVE` waiter is served per `notify_list` call, even if more elements remain in the source list and more `BLMOVE` waiters are queued behind it. See §9.1 for why this is a real limitation.
- `WaiterOp::Movem{..}`: same shape as `Move` but delegates to `table.lmovem(...)`; also `break`s after one successful satisfaction.
- After the loop, `notify_list` recurses into itself for the `destination` key if a `Move`/`Movem` just pushed into it (`dest_to_notify`), so a chain of `BLMOVE src1->dst1`, `BLMOVE dst1->dst2` wakes transitively in one call.
- Finally, `remove_waiters_for_client` is called once per satisfied client id, scrubbing that client out of every other key's queue too (so a multi-key `BLPOP k1 k2` doesn't leave a stale `k2` entry after being satisfied on `k1`).

`notify_zset` (`block.rs:542-584`) is the same FIFO-pop-while-available shape for `ZSetWaiter`, with one extra guard: `satisfied_clients.contains(&waiter.client_id)` to avoid re-serving the same client twice within one call (possible only if a client passed the same key twice to `BZPOPMIN`/`BZMPOP`).

### 3.2 `notify_stream`: broadcast, not FIFO — and a blocking-sleep finding

```rust
pub fn notify_stream(&mut self, key: &Bytes) {
    if let Some(waiters) = self.stream_waiters.remove(key) {
        for (idx, waiter) in waiters.iter().enumerate() {
            self.blocked_stream_clients.remove(&waiter.client_id);
            if idx > 0 {
                std::thread::sleep(std::time::Duration::from_millis(1));   // block.rs:601
            }
            let _ = waiter.sender.try_send(());
        }
        ...
    }
}
```

Unlike lists/sorted sets (single winner per push, FIFO), `notify_stream` wakes **every** waiter registered on that key (a plain `Vec`, no FIFO ordering is implied or needed since all of them are told to go re-read the stream themselves). The signal itself carries no data — `Sender<()>` — each woken client re-runs its own `XREAD`/`XREADGROUP` to see what's new.

**Verified bug**: `std::thread::sleep(1ms)` is inserted between each waiter's wakeup, apparently to stagger a thundering herd of clients re-reading the same stream. Rudis is a thread-per-core, single-threaded-per-shard async reactor (built on `monoio`); `notify_stream` is a **synchronous, non-async** function called directly from the reactor's command-handling path (via `notify_stream_or_defer`, the `MULTI`/`EXEC` resume path, `ShardMessage::NotifyList`, and `FLUSHALL`). A genuine `std::thread::sleep` here blocks the **entire shard's OS thread** — not just the notifying client, but every other connection pinned to that core — for `(N-1)` milliseconds when N clients are blocked on the same stream key. This directly violates the non-blocking-reactor invariant the rest of the codebase depends on and is the single most actionable finding in this file.

---

## 4. Cross-shard wakeup mechanics

Rudis shards the keyspace across cores (`src/router.rs::target_shard`/`key_slot`); each shard owns its own `RudisTable` and runs on its own OS thread. The question: if a client's `BLPOP k` command is being served by shard A (because its *connection* was accepted there) but key `k` is owned by shard B, how does a `LPUSH k` executed on shard B wake the waiter?

**Answer: there is no per-shard hub to wake across — there is only one.** `get_block_hub_for_port(port)` (`block.rs:106-111`) returns the *same* `Arc<Mutex<BlockHub>>` regardless of which shard thread calls it, because `PORT_BLOCK_HUBS` is keyed only by `port`, not by `(port, shard_id)`. Concretely:

1. A blocking command handler (e.g. `Command::Blpop`, `connection.rs:7909`) first tries a **local, non-blocking fast path**: for each key, if `router.target_shard(k) == router.shard_id` it pops directly from `router.local_db`; otherwise it issues `router.execute_remote(target, Command::Lpop{..})` — a synchronous cross-shard RPC through the mailbox — to try popping on the *other* shard right away. Only if every key comes back empty does it fall through to registering waiters.
2. It then locks the single global hub and calls `hub.register_list_waiter(client_id, k, ..., tx)` **once per key**, regardless of which shard actually owns `k`. The waiter — including its `flume::Sender` — lives in the one shared `BlockHub`.
3. Later, when some shard (whichever one physically owns `k`) executes an `LPUSH k` and the write succeeds, it calls `notify_list_or_defer(&mut local_db, &k)` (`connection.rs:211-222`) using **its own local `db.table`**. This locks the same global hub, finds the waiter (registered possibly by a different shard thread), and runs `notify_list`, which pops from the *owning* shard's local table (correct — the data genuinely lives there) and does `waiter.sender.send(...)`.
4. `flume::Sender`/`Receiver` is a cross-thread MPMC channel, so the result crosses from shard B's thread to shard A's thread without any additional shard IPC — the `Mutex<BlockHub>` is the only cross-shard synchronization primitive involved in the common case.

So the "cross-shard signal" for the ordinary (non-transactional) write path is: **a single process-wide `Mutex<BlockHub>` shared by all shard threads for a port, plus `flume` channels to ferry the result back to whichever thread owns the blocked connection.** There is no extra mailbox/IPC round-trip needed because registration and notification both go through the same lock, and the lock is uncontended in the vast majority of calls thanks to the `has_blocked_waiters` fast-reject check.

### 4.1 The one case that *does* need shard IPC: `MULTI`/`EXEC`

During `EXEC`, the hub is paused (`hub_arc.lock().unwrap().pause()`, `connection.rs:1418`) so that writes inside the transaction don't notify waiters mid-transaction (`notify_*_or_defer` sees `hub.is_paused()==true` and calls `add_pending_notify(key)` instead of notifying — `connection.rs:218-220` etc.). After the transaction body runs, `hub_arc.lock().unwrap().resume()` (`connection.rs:1452`) drains `pending_notifies` and, **for each pending key**, decides where to run the actual notify:

```rust
let pending = hub_arc.lock().unwrap().resume();
for k in pending {
    let shard_id = router.target_shard(&k);
    if shard_id == router.shard_id {
        // local: run notify_* using this thread's own table
        let mut local_db = router.local_db.borrow_mut();
        let mut hub = hub_arc.lock().unwrap();
        hub.notify_stream(&k);
        hub.notify_list(&mut local_db.table, &k);
        hub.notify_zset(&mut local_db.table, &k);
    } else {
        // remote: ask the owning shard to do it with ITS table
        let _ = router.senders[shard_id].send(ShardMessage::NotifyList { keys: vec![k] });
    }
}
```
(`connection.rs:1452-1464`)

If the key is owned by a *different* shard than the one running `EXEC`, there is no local table to pop from, so it sends `ShardMessage::NotifyList { keys }` (`shard.rs:423-424`) through that shard's mailbox. The target shard's reactor loop picks it up in its `ShardMessage` match arm (`server.rs:1725-1733`):

```rust
ShardMessage::NotifyList { keys } => {
    let mut db = cross_shard_db.borrow_mut();
    let hub_arc = crate::block::get_block_hub_for_port(db.port);
    let mut hub = hub_arc.lock().unwrap();
    for k in keys {
        hub.notify_stream(&k);
        hub.notify_list(&mut db.table, &k);
        hub.notify_zset(&mut db.table, &k);
    }
}
```

This re-locks the same global hub from the *owning* shard's thread and runs the exact same `notify_list`/`notify_zset`/`notify_stream` using that shard's own local table — i.e. it is the same algorithm as the ordinary path, just dispatched through a mailbox message instead of being called inline, because `EXEC`'s deferred-notify design only knows "key K needs notifying" after the fact, potentially from the wrong shard thread.

**`CLIENT PAUSE`/`CLIENT UNPAUSE` do not touch `BlockHub` at all** — `pause()`/`is_paused()`/`resume()` are called *only* from the `MULTI`/`EXEC`/`DISCARD`/`RESET` paths (`connection.rs:1335-1465`; grep confirms no other call sites). `ClientSubcommand::Pause`/`Unpause`/`NoTouch` are unconditional `+OK` no-ops (`connection.rs:7189-7192`) — naming collision only, not shared state.

---

## 5. Timeout handling & disconnect detection

### 5.1 `wait_for_blocked_result` (`connection.rs:407-443`) — list/zset blocking commands

```rust
pub async fn wait_for_blocked_result<T>(
    rx: &flume::Receiver<T>, timeout_secs: f64, raw_fd: Option<RawFd>,
) -> (Option<T>, bool) {
    let deadline = (timeout_secs > 0.0).then(|| Instant::now() + Duration::from_secs_f64(timeout_secs));
    loop {
        let check_dur = deadline.map_or(Duration::from_millis(20), |dl| {
            (dl.saturating_duration_since(Instant::now())).min(Duration::from_millis(20))
        });
        match monoio::time::timeout(check_dur, rx.recv_async()).await {
            Ok(Ok(res)) => return (Some(res), false),       // woken by notify_list/notify_zset
            Ok(Err(_))  => return (None, false),             // sender dropped
            Err(_) => {                                      // 20ms tick elapsed, nobody woke us
                if let Some(fd) = raw_fd && is_fd_closed(fd) { return (None, true); }
                if let Some(dl) = deadline && Instant::now() >= dl { return (None, false); }
                // else: loop again, wait up to another 20ms
            }
        }
    }
}
```

Key facts:
- A timeout of `0.0` (the Redis convention for "block forever") means `deadline = None`, so the loop only ever exits via a received value or a detected disconnect — never a synthetic timeout.
- **The 20ms figure is a disconnect-detection poll interval, not a channel-polling interval.** The actual wakeup is a real async wait on `rx.recv_async()` (no busy-polling of the channel); it's wrapped in `monoio::time::timeout(..)` capped at 20ms purely so the loop can periodically check whether the client's socket died.
- `is_fd_closed` (`connection.rs:377-401`) does a zero-timeout `libc::poll` for `POLLRDHUP|POLLHUP|POLLERR`, and if `POLLIN` is set, an `MSG_PEEK|MSG_DONTWAIT` `recv` to distinguish "readable data" from "peer closed" (`recv` returning 0). This exists because a `flume::Receiver` alone cannot observe TCP-level disconnects — a client that vanishes without TCP FIN/RST on a half-duplex connection with no outstanding reads would otherwise block forever past any already-elapsed timeout. Max added latency to detect a dead client: ~20ms.
- On return, the caller always has a `BlockedClientGuard` (`connection.rs:363-374`, RAII `Drop` impl) that calls `hub.unregister_blocked_client(client_id)` no matter which branch was taken, so disconnect, timeout, and success all converge on the same cleanup path.

### 5.2 `wait_for_stream_result` (`connection.rs:445-492`) — `XREAD`/`XREADGROUP BLOCK`

Structurally identical to §5.1 (same 20ms disconnect-poll cap, same `is_fd_closed` check), but takes `timeout_ms: u64` directly (stream blocking is specified in milliseconds, not the float seconds used by list/zset `BLPOP`-family timeouts) and a bare `Receiver<()>` (no payload — see §3.2).

### 5.3 `XREADGROUP ... CLAIM`: event-driven *and* polled

`XREADGROUP` in this codebase accepts an extension not in upstream Redis: `claim: Option<min_idle>` (visible in `Command::Xreadgroup`), implementing `XNACK`/auto-claim semantics (`git log`: `5657bb6 feat(stream): implement XNACK, XREADGROUP CLAIM, and stream PEL persistence`). Its blocking loop (`connection.rs:7598-7672`) **re-registers the stream waiter on every iteration** (safe because `register_stream_waiter` de-dupes by `client_id` within a key, `block.rs:588-591`) and computes its own wait budget instead of trusting a pure event wakeup:

```rust
if let Command::Xreadgroup { ref group, claim: Some(min_idle), .. } = cmd {
    let mut earliest_pel_wait: Option<u64> = None;
    for k in keys {
        if let Some(w) = db.earliest_claim_wait_ms(k, group, min_idle) {
            earliest_pel_wait = Some(earliest_pel_wait.map_or(w, |m| m.min(w)));
        }
    }
    let target_wait = earliest_pel_wait.map_or(50, |w| w.clamp(1, 50));
    wait_timeout_ms = remaining_ms.min(target_wait);   // or just target_wait if no overall deadline
}
```
(`connection.rs:7624-7636`, `earliest_claim_wait_ms` at `table.rs:13870-13891`)

`earliest_claim_wait_ms` scans the consumer group's PEL (pending entries list) and returns the smallest `(delivery_time_ms + min_idle) - now` across all pending entries, i.e. "how long until the next entry becomes eligible to auto-claim." The loop then waits for `min(remaining_deadline, clamp(that, 1, 50))` milliseconds. **This is a genuine poll**, clamped to a 1-50ms interval, layered on top of the push-based `notify_stream` wakeup — `XREADGROUP ... CLAIM` is the one blocking command in this file that is not purely event-driven. Plain `XREAD`/`XREADGROUP` without `claim` never takes this branch and is purely push-driven.

The same loop shape (register → compute wait → `wait_for_stream_result` → re-read → loop) is duplicated verbatim for the cross-shard multi-stream-key case (`connection.rs:7690-7766`, driven by `query_cross_shard_streams`, `connection.rs:4780-4852`, which fans a multi-key `XREAD`/`XREADGROUP` out to each owning shard via `router.execute_remote` and merges the per-shard reply blocks).

---

## 6. Every blocking command and its exact wakeup trigger

All nine blocking command *forms* are enumerated together in two places that must be kept in sync by hand: the pipeline-flush-before-blocking-command list (`connection.rs:1868-1911`, used so a blocking command never gets stuck behind unflushed pipelined output) and the slowlog/stat-name table (`connection.rs:3969-4195`). Current canonical set: `BLPOP`, `BRPOP`, `BLMOVE`, `BLMOVEM`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, `XREAD` (with `block_ms: Some`), `XREADGROUP` (with `block_ms: Some`). `WAIT`/`WAITAOF` are **not** in this set (§6.10).

| Command | Handler (connection.rs) | Registration call | Wakeup trigger |
| :--- | :--- | :--- | :--- |
| `BLPOP keys... timeout` | 7909-8043 | `register_list_waiter(..., Pop{Left,1})` per key | Any `LPUSH`/`RPUSH`/etc. on one of the keys that leaves ≥1 element, via `notify_list` |
| `BRPOP keys... timeout` | 8044-8178 | `register_list_waiter(..., Pop{Right,1})` per key | Same as above |
| `BLMOVE src dst from to timeout` | 8448-8572 | `register_move_waiter` (single key = `src`) | A push onto `src`; requires `src`/`dst` on the **same shard** or returns `CROSSSLOT` before ever blocking |
| `BLMOVEM src dst from to mode count ordering timeout` | 8573-8811 | `register_movem_waiter` (single key = `src`) | A push onto `src`; same same-shard `CROSSSLOT` requirement as `BLMOVE` |
| `BLMPOP timeout numkeys keys... <LEFT\|RIGHT> [COUNT n]` | 8909-9075 | `register_list_waiter(..., Pop{dir,count})` per key | Any push on any of the keys |
| `BZPOPMIN keys... timeout` | 9080 → `handle_bzpop(..., is_min=true)` (4385-4531) | `register_zset_waiter(..., Min, 1, is_zmpop=false)` per key | `ZADD` (incl. fast path, see §10) on one of the keys |
| `BZPOPMAX keys... timeout` | 9083 → `handle_bzpop(..., is_min=false)` | `register_zset_waiter(..., Max, 1, is_zmpop=false)` per key | Same |
| `BZMPOP timeout numkeys keys... <MIN\|MAX> [COUNT n]` | 9178-9317 | `register_zset_waiter(..., count, is_zmpop=true)` per key | Same |
| `XREAD BLOCK ms [...] STREAMS keys... ids...` | 7519-7674 (same-shard), 7690-7850ish (cross-shard) | `register_stream_waiter` per key, **re-registered every poll iteration** | Any `XADD` on one of the keys, via `notify_stream` (broadcast) |
| `XREADGROUP GROUP g c BLOCK ms [CLAIM min-idle] STREAMS keys... ids...` | same handler as `XREAD` (shared match arm, 7519-7850ish) | same, plus the 1-50ms CLAIM poll (§5.3) | `XADD`, **or** CLAIM-eligibility poll elapsing |

Only `ids == ">"` for every key (new-message mode) is eligible to block for `XREADGROUP`; `can_block` is explicitly computed as `ids.iter().all(|id| id == ">")` (`connection.rs:7554-7555`) — reading from an explicit/historical ID never blocks, matching real Redis semantics.

### 6.1-6.9 Per-command behavioral notes

- **Non-blocking fast path first, always.** Every blocking command handler is structured as: try to satisfy immediately via local/remote non-blocking pop → only if truly empty, register a waiter and await. None of them register-then-check; they check-then-register. This avoids a tiny race where data pushed microseconds before the command executes would otherwise be invisible to a naive register-first design — because registration happens strictly after the empty-check, and the empty-check is on the authoritative table, no push can be "lost" between the two steps (any push either lands before the check, in which case the fast path sees it, or after the check, in which case the subsequent push's `notify_*_or_defer` call will find the just-registered waiter).
- **`IN_TX.get()` short-circuits every blocking path** to a non-blocking nil/null reply — `BLPOP` etc. inside `MULTI`/`EXEC` never actually block (matches real Redis: blocking commands inside a transaction return immediately if they would've blocked).
- **`BLMOVE`/`BLMOVEM` require `source`/`destination` to resolve to the same shard** or immediately return `-CROSSSLOT Keys in request don't hash to the same slot` (`connection.rs:8450-8456`, `8575-8581`ish) **before even attempting to block** — this is checked even when cluster mode is off, since both keys must be popped/pushed against one `RudisTable` instance by the single-threaded `notify_list` call.
- **AOF/replication propagation is rewritten on unblock, not replayed verbatim.** E.g. a satisfied `BLPOP` propagates as `LPOP key 1` (`connection.rs:7956-7961`), a satisfied `BLMOVE` propagates as the equivalent `LMOVE` (`connection.rs:8527-8535`), etc. — consistent with how real Redis rewrites blocking commands to their deterministic non-blocking equivalents for replication.

### 6.10 `WAIT`/`WAITAOF` — NOT part of `BlockHub`

```rust
Command::Wait { numreplicas, timeout } => {
    let count = crate::replication::wait_replicas(router.port, numreplicas, timeout).await;
    write_resp_integer(out, count as i64);
}
Command::WaitAof { numlocal: _, numreplicas, timeout } => {
    let count = crate::replication::wait_replicas(router.port, numreplicas, timeout).await;
    out.extend_from_slice(&format!("*2\r\n:1\r\n:{}\r\n", count));
}
```
(`connection.rs:12024-12037`)

`wait_replicas` (`replication.rs:687-712`) is a **pure poll loop**, entirely separate from `BlockHub` — no waiter registration, no `flume` channel, no `notify_*`:

```rust
loop {
    let count = /* replicas whose ack_offset >= target_offset */;
    if count >= numreplicas || timeout_ms == 0 || start.elapsed() >= timeout { return count; }
    monoio::time::sleep(Duration::from_millis(5)).await;
}
```

So `WAIT`/`WAITAOF` block the calling task (asynchronously — `monoio::time::sleep` yields the reactor, it does not stall the thread) by re-checking replica ACK offsets every **5ms** until satisfied or timed out. `WaitAof`'s `numlocal` parameter is accepted but ignored (`numlocal: _`) — it's always reported as `1` (local AOF persisted) regardless of actual local durability state. Grep confirms no reference to `block::` anywhere in `wait_replicas` or its call sites — this is a deliberate separate mechanism, not an oversight to "fix" by wiring it into `BlockHub`, but worth knowing so a reader doesn't go looking for `WAIT` inside `block.rs`.

---

## 7. Fairness / ordering guarantees

- **Lists and sorted sets: FIFO per key**, via `VecDeque::push_back` (register) / `pop_front` (notify). Two clients blocked on the same key in registration order `A` then `B` are satisfied in that same order, as long as each notify call has enough data to satisfy both (§3.1 loop-while-available). **No cross-key fairness**: a client blocked on `BLPOP k1 k2` is simply registered on both queues independently; if `k2` is pushed to first, that client is served from `k2`'s queue regardless of how long other clients have waited on `k1`.
- **`BLMOVE`/`BLMOVEM`: single-winner per push event**, not "while available" — see §3.1's `break` note. If three clients are queued on `BLMOVE src dst` and a single `RPUSH src v1 v2 v3` pushes three elements, only the **first-queued** client is served by that `notify_list` call; the second and third remain blocked until another independent push event occurs (even though data they could consume already sits in `src`). This is a real behavioral gap versus `BLPOP`/`BLMPOP`, which drain as much of a single push as there are waiters.
- **Streams: broadcast, not ordered.** Every waiter on a key is woken on every `XADD`, in `stream_waiters[key]`'s insertion order only for the purpose of the artificial inter-wakeup `sleep(1ms)` stagger (§3.2) — there is no "first blocked, first served" concept here since all waiters get the same signal and independently re-read the stream.
- **`CLIENT UNBLOCK`** bypasses ordering entirely — it targets a specific `client_id` regardless of queue position.
- **`MULTI`/`EXEC`**: within one transaction, multiple writes to the same key only produce one effective notification per key (`pending_notifies` dedups via `add_pending_notify`'s `if !self.pending_notifies.iter().any(|k| k == &key)` check, `block.rs:155-159`) — so three `LPUSH k` calls inside one `EXEC` wake blocked `k` waiters once after the transaction, not three times, and in particular do not give multiple elements to 3 separate single-element `BLPOP` waiters three separate times; the final `notify_list` call after `resume()` sees the fully-updated table and drains as many waiters as the final list length allows.

---

## 8. `MULTI`/`EXEC` deferred notification — full walkthrough

1. `EXEC` begins (`connection.rs:1397` onward): locks resolved, `IN_TX.set(true)`, then `hub_arc.lock().unwrap().pause()` — increments `paused_count`.
2. Each queued command runs through the normal `execute_command` path. Any write that would call `notify_list_or_defer`/`notify_zset_or_defer`/`notify_stream_or_defer` instead hits `hub.is_paused() == true` and calls `hub.add_pending_notify(key)` (deduped), **never acquiring the table to pop early** — waiters stay registered and blocked throughout the transaction body.
3. `IN_TX.set(false)`; `let pending = hub_arc.lock().unwrap().resume()` — decrements `paused_count`; if it reaches 0, drains and returns `pending_notifies` (a `Vec<Bytes>`), else returns `Vec::new()` (nested-pause case, though nothing in this codebase actually nests pause calls beyond one `EXEC` at a time — `paused_count` as a counter rather than a bool is defensive).
4. For each pending key, dispatch locally or via `ShardMessage::NotifyList` depending on shard ownership (§4.1).
5. `DISCARD`, `RESET`, an aborted `EXEC` (previous queuing error), a cross-slot `EXEC` in cluster mode, and a `WATCH`-tainted `EXEC` all call `clear_pending_notifies()` instead of `resume()` (`connection.rs:1337,1350,1364,1376,1388`) — this still decrements `paused_count` and re-syncs the atomic, but **drops `pending_notifies` without ever notifying anyone**. This is correct: those paths mean no writes actually happened (or won't be visible), so there's nothing to wake blocked clients for — but it does mean `pause()`/`clear_pending_notifies()` must be called in matched pairs by every transaction-exit branch, and a future code path that forgets the `clear_pending_notifies()` call on some new early-exit branch would leave `paused_count` permanently incremented, silently disabling the fast notify path hub-wide (since `is_paused()` would stay true and all future notifies degrade to being queued, never actually drained, until some future `resume()` finally reaches 0 — effectively starving every blocked client on that port). There is no `unwrap_or` / panic-safety around this invariant; it relies entirely on each call site remembering to balance `pause()`.

---

## 9. `CLIENT UNBLOCK`, `CLIENT LIST`/`INFO`, and `CLIENT PAUSE`/`KILL`

### 9.1 `CLIENT UNBLOCK <id> [TIMEOUT|ERROR]`

`connection.rs:7178-7186`: directly calls `hub.unblock_client(target_id, unblock_type)` and returns `:1`/`:0`. `unblock_client` (`block.rs:254-281`) handles list/move/movem and zset waiters correctly — it sends `BlockedListResult::Unblocked(type)`/`BlockedZSetResult::Unblocked(type)`, which the waiting command's `match recv_res` arms translate into either a `WRONGTYPE`-style reply (never for `unblock_client`, only `Timeout`/`Error` are reachable here), a `-UNBLOCKED client unblocked via CLIENT UNBLOCK` error (`ClientUnblockType::Error`), or — since `Timeout` has no explicit match arm anywhere in the blocking-command handlers and falls through to each handler's `_ => write_resp_null_array(out)` catch-all — a plain nil reply, exactly matching real Redis's `CLIENT UNBLOCK ... TIMEOUT` semantics.

**Verified bug: `CLIENT UNBLOCK` does not actually unblock `XREAD`/`XREADGROUP BLOCK` clients.** For stream waiters, `unblock_client` does `sender.try_send(())` (`block.rs:271`) — the exact same zero-payload signal `notify_stream` sends for "new data arrived." The `XREAD`/`XREADGROUP` blocking loop (`connection.rs:7598` onward) has no way to distinguish "woken because of real new data" from "woken because of `CLIENT UNBLOCK`" — both just cause it to re-run the read, find nothing new, and **loop back to `register_stream_waiter` + wait again**, ignoring the requested `TIMEOUT`/`ERROR` semantics entirely. A `CLIENT UNBLOCK <id> ERROR` issued against a client blocked in `XREAD BLOCK` will cause one spurious extra poll and then the client silently resumes blocking until real data arrives or its original deadline elapses — it never receives the `-UNBLOCKED` error the caller asked for.

### 9.2 `CLIENT LIST` / `CLIENT INFO` / `INFO`

- `flags=b` is rendered via `hub.is_blocked(client.id)` both in the per-connection `CLIENT INFO` path (`connection.rs:7084-7089`) and the full-registry `CLIENT LIST` path (`router.rs:2142-2146`).
- `INFO`'s `# Clients` section reports `blocked_clients`, `total_blocking_keys`, `total_blocking_keys_on_nokey` by locking the hub once and calling `blocked_clients_count()`/`blocking_keys_count()`/`blocking_keys_on_nokey_count()` (`connection.rs:5954-5970`). Since the hub is global-per-port (not per-shard), these numbers are already correct process-wide totals without needing to aggregate across shards.

### 9.3 `CLIENT PAUSE` / `CLIENT UNPAUSE` / `CLIENT KILL`: no-ops

`ClientSubcommand::Kill(_) => out.extend_from_slice(b"+OK\r\n")` (`connection.rs:7172-7174`) and `ClientSubcommand::Pause(_) | Unpause | NoTouch(_) => out.extend_from_slice(b"+OK\r\n")` (`connection.rs:7189-7192`) are **unconditional acknowledgements that do nothing**. `CLIENT KILL` in particular does not look up the target connection, does not close its socket, and — relevant to this doc — does not call `hub.unblock_client`/`unregister_blocked_client` for the victim, so a `CLIENT KILL` issued against a client that is blocked in `BLPOP`/`XREAD BLOCK`/etc. has **zero effect**; that client keeps blocking until its own timeout or a real data event. (Real disconnects, i.e. the client actually closing its TCP connection, are still caught correctly via the `is_fd_closed` poll in §5.1/5.2 and the `BlockedClientGuard`/`TlsClientCleanup` `Drop` impls — only the *command-driven* `CLIENT KILL` is inert.)

---

## 10. Fast-path bypass hazard: `LPUSH`/`ZADD` "arena" fast paths

Commit `23a1b24` ("perf(arena): implement small collection slab arena, slice zero-alloc dispatch, and lockless block/search bypass") added zero-allocation fast paths for `ZADD` and `LPUSH` inside the pipeline-squashing executors (`execute_commands_squashed` in connection.rs, and the analogous `ShardMessage::Batch` handling in server.rs). These fast paths call `table.zadd_slice_with_hash`/an equivalent `lpush` directly and **skip `notify_zset_or_defer`/`notify_list_or_defer` entirely** — they only handle `WATCH`/client-side-caching invalidation, not blocking wakeups. Each of the four call sites is therefore gated with an explicit `!crate::block::has_blocked_waiters(port)` guard so the fast path is only taken when nothing could possibly be blocked on that key:

- `server.rs:809` (`Lpush`, `ShardMessage::Batch` path 1)
- `server.rs:923` (`Zadd`, `ShardMessage::Batch` path 1)
- `server.rs:1149` (`Lpush`, `ShardMessage::Batch` path 2 / cross-shard router)
- `server.rs:1263` (`Zadd`, `ShardMessage::Batch` path 2)
- `connection.rs:19485` (`Zadd`, `execute_commands_squashed`)
- `connection.rs:19528` (`Lpush`, `execute_commands_squashed`)

The `ZADD` guard was added later than the others, in a dedicated fix: commit `3607df7` ("fix(block): ensure Zadd fast path notifies block hub when waiters are present, passing Redis TCL unit/type/zset suite") — before that fix, a pipelined `ZADD` could silently starve a `BZPOPMIN`/`BZPOPMAX`/`BZMPOP` waiter whenever the squashed-execution fast path was taken, since the fast path never touched `BlockHub` at all. The current code is correct for the common single-port-per-process case; see §2.2 for how the shared, non-per-port `TOTAL_BLOCKED_WAITERS` atomic backing `has_blocked_waiters` could reintroduce exactly this class of bug if multiple `BlockHub`s ever coexist in one process. **There is no equivalent fast path for `RPUSH`** — only `LPUSH` and `ZADD` got the zero-alloc treatment, so `RPUSH` always goes through the normal `notify_list_or_defer`-calling path and cannot regress this way.

---

## 11. Known bugs / limitations (summary)

1. **`has_blocked_waiters`'s `port` argument is unused; `TOTAL_BLOCKED_WAITERS` is one process-wide atomic, not per-port**, and `sync_atomic_waiters_count` does an absolute `store` from a single hub's count rather than an incremental update (`block.rs:98-104, 134-144`). Harmless with one port per process; a real missed-wakeup hazard if a process ever hosts multiple `BlockHub`s concurrently (also makes the `cfg(test)` unit tests in this file theoretically flaky if run concurrently with each other, since they use different ports 12345-12347 and all write through the same atomic).
2. **`notify_stream` calls `std::thread::sleep(1ms)` synchronously between each of N woken waiters** (`block.rs:601`), stalling the entire owning shard's reactor thread for up to `(N-1)` ms on a single `XADD` to a hot stream with many blocked readers — a direct violation of the thread-per-core non-blocking design used everywhere else in this codebase.
3. **`BLMOVE`/`BLMOVEM` satisfy only one waiter per push event**, even when the push delivers enough elements for several queued waiters (`block.rs:439-443, 492-497`, the `break` after one success) — unlike `BLPOP`/`BLMPOP`/`BZPOPMIN`/`BZMPOP`, which drain as many waiters as a single push allows.
4. **`CLIENT UNBLOCK` does not work for stream blocking** (`XREAD BLOCK`/`XREADGROUP BLOCK`) — the stream channel carries no payload to distinguish "real data" from "forced unblock," so the blocking loop just spuriously re-polls and keeps blocking (`block.rs:271` vs. `connection.rs:7598`-onward's re-register-and-loop shape).
5. **`CLIENT KILL` is a complete no-op** — it neither closes the target connection nor unblocks it if it's inside a blocking command (`connection.rs:7172-7174`).
6. **`CLIENT PAUSE`/`CLIENT UNPAUSE` are no-ops** unrelated to `BlockHub.pause()`/`resume()`, which are reserved exclusively for internal `MULTI`/`EXEC` bookkeeping — a reader should not expect `CLIENT PAUSE` to interact with blocking commands at all in the current implementation.
7. **`PORT_BLOCK_HUBS` entries are never removed** — `get_block_hub_for_port` only inserts, never evicts, so a process that creates many short-lived `BlockHub`s for many distinct ports (e.g. a test harness cycling through ports) leaks one `Arc<Mutex<BlockHub>>` map entry per port for the lifetime of the process. Not a concern for a single long-running production port.
8. **`XREADGROUP ... CLAIM` polls at a clamped 1-50ms interval** (`connection.rs:7624-7636`) layered on top of the push-based stream notification — this is by design (there's no timer-wheel in `BlockHub` to schedule a wake at a PEL entry's exact eligibility time), but it means CLAIM-eligible messages can be delivered up to ~50ms late even though the entry has technically been eligible since `delivery_time_ms + min_idle`.
9. **`WAITAOF`'s `numlocal` argument is accepted but ignored** — the reply always reports local persistence as `1` (`connection.rs:12032-12037`), regardless of whether the write was actually fsynced locally. Not a `BlockHub` issue (`WAIT`/`WAITAOF` don't use `BlockHub` at all, §6.10) but easy to mistake for one given their adjacency to the other blocking commands.

---

## 12. How to verify these claims yourself

```bash
# Full struct/enum/impl inventory
rg -n '^(pub )?(struct|enum|impl)\s' src/block.rs

# All call sites that touch the hub, outside block.rs itself
rg -n 'crate::block::|BlockHub' src/connection.rs src/server.rs src/router.rs src/shard.rs

# The thread::sleep finding
rg -n 'thread::sleep' src/block.rs

# The unused `_port` parameter
rg -n 'fn has_blocked_waiters' src/block.rs

# Confirm WAIT/WAITAOF never reference block::
rg -n 'block::' src/replication.rs   # expect: no matches

# Run the hub's own unit tests (block.rs:622-764)
cargo test --lib block::tests -- --test-threads=1   # serialize to avoid the shared-atomic flakiness in §11.1
```
