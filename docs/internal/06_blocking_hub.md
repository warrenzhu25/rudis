# Component 06: Blocking Operations & The Reactive Event Hub (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/block.rs`  
> **High-Level Design Spec**: [`docs/design/06_blocking_hub.md`](../design/06_blocking_hub.md)  
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/block.rs` | Core implementation and logic | Primary data structures and algorithms |

---

### 2. Component Architecture & Data Structures

```
   BLPOP k1 k2 0 (both empty)                    LPUSH k1 "v"  (any shard)
            │                                              │
            ▼                                              ▼
  hub.register_blocked_client(cid, tx)          notify_list_or_defer(db, "k1")
  hub.register_list_waiter(cid,"k1",Pop,1,tx)             │
  hub.register_list_waiter(cid,"k2",Pop,1,tx)     paused? ──yes──▶ add_pending_notify("k1")
            │                                              │no
            ▼                                              ▼
  wait_for_blocked_result(&rx, timeout, fd)        hub.notify_list(&mut table, "k1")
   (polls rx every ≤20ms; also polls the fd         │ pop_front waiter for "k1"
    for POLLHUP/POLLRDHUP/EOF to detect a            │ table.lpop("k1", 1)  ◀── pop happens
    client that vanished without closing            │ inside the notify call, under the
    cleanly — a channel recv alone can't see         │ same Mutex-held critical section
    that)                                            ▼ tx.send(Popped(key, vals))
            │                                  remove_waiters_for_client(cid)
            ▼                                  (drops the still-registered "k2" waiter too)
  BlockedClientGuard::drop → unregister
```

#### Real waiter/result types (`src/block.rs`)

```rust
pub enum WaiterOp {
    Pop { pop_type: ListPopType, count: usize },
    Move { where_from: ListPopType, where_to: ListPopType, destination: Bytes },
}

pub struct ListWaiter {
    pub client_id: u64,
    pub key: Bytes,
    pub op: WaiterOp,
    pub sender: Sender<BlockedListResult>,   // flume::Sender, not oneshot
}

pub struct ZSetWaiter {
    pub client_id: u64,
    pub key: Bytes,
    pub pop_type: ZSetPopType,   // Min | Max
    pub count: usize,
    pub is_zmpop: bool,
    pub sender: Sender<BlockedZSetResult>,
}

pub struct StreamWaiter {
    pub key: Bytes,
    pub sender: Sender<()>,   // pure wakeup, no payload — reader re-polls the stream itself
}

pub enum BlockedListResult {
    Popped(Bytes, Vec<Bytes>),
    Unblocked(ClientUnblockType),   // Timeout | Error | WrongType
}

pub struct BlockHub {
    pub port: u16,
    list_waiters: HashMap<Bytes, VecDeque<ListWaiter>>,
    zset_waiters: HashMap<Bytes, VecDeque<ZSetWaiter>>,
    stream_waiters: HashMap<Bytes, Vec<StreamWaiter>>,
    blocked_clients: HashMap<u64, Sender<BlockedListResult>>,      // for CLIENT UNBLOCK / CLIENT LIST's "b" flag
    blocked_zset_clients: HashMap<u64, Sender<BlockedZSetResult>>,
    paused_count: usize,          // MULTI/EXEC deferral, see §3.4 — NOT CLIENT PAUSE
    pending_notifies: Vec<Bytes>,
}
```

There is no unified `Waiter`/`WaiterType`/`BlockedPopResult` type — list, zset, and stream
waiters are three separate types with three separate queues and three separate result enums,
and channels are `flume::Sender`, not `oneshot::Sender`.

---

### 3. Execution Algorithms & Code Logic

#### 3.1 Registering a blocked client (`BLPOP`, in `src/connection.rs`)

```rust
let _guard = BlockedClientGuard { port: router.port, client_id };
let (tx, rx) = flume::bounded(1);
{
    let hub_arc = crate::block::get_block_hub_for_port(router.port);
    let mut hub = hub_arc.lock().unwrap();
    hub.register_blocked_client(client_id, tx.clone());
    for k in &keys {
        hub.register_list_waiter(client_id, k.clone(), crate::block::ListPopType::Left, 1, tx.clone());
    }
}
let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
let (recv_res, client_disconnected) = wait_for_blocked_result(&rx, timeout, raw_fd).await;
```

One waiter is registered per key in the `BLPOP` argument list, all sharing the same `tx` clone
— whichever key is satisfied first sends on that shared channel, and `remove_waiters_for_client`
(called from inside `notify_list`) then strips the *other* now-stale waiters for that client from
every other key's queue. `BlockedClientGuard` is held across the whole `.await`, so if the
future is ever dropped early (client disconnect, etc.) its `Drop` impl still unregisters.

#### 3.2 `wait_for_blocked_result`: polling, not a pure channel await

```rust
pub async fn wait_for_blocked_result<T>(
    rx: &flume::Receiver<T>,
    timeout_secs: f64,
    raw_fd: Option<std::os::unix::io::RawFd>,
) -> (Option<T>, bool) {
    let deadline = ...;
    loop {
        let check_dur = /* min(remaining time, 20ms) */;
        match monoio::time::timeout(check_dur, rx.recv_async()).await {
            Ok(Ok(res)) => return (Some(res), false),
            Ok(Err(_)) => return (None, false),
            Err(_) => {
                if let Some(fd) = raw_fd {
                    if is_fd_closed(fd) { return (None, true); }
                }
                if /* deadline passed */ { return (None, false); }
            }
        }
    }
}
```

This is **not** a single `select! { rx.await, sleep(timeout).await }` as one might assume — it
loops, re-`await`ing the channel with a cap of 20ms per iteration, and on every timeout tick
also calls `is_fd_closed(raw_fd)`:

```rust
pub fn is_fd_closed(fd: RawFd) -> bool {
    let mut pollfd = libc::pollfd { fd, events: POLLIN|POLLRDHUP|POLLHUP|POLLERR, revents: 0 };
    let ret = unsafe { libc::poll(&mut pollfd, 1, 0) };
    // POLLRDHUP/POLLHUP/POLLERR ⇒ closed; POLLIN + a 0-byte MSG_PEEK recv ⇒ closed
    ...
}
```

The reason: while a client is blocked on `BLPOP`, its connection task isn't in its normal
read loop — it's parked on `rx.recv_async()` — so a client that disappears (network drop,
process killed) without a clean FIN would otherwise leave its waiter (and the task itself)
registered forever. Polling the raw fd every ≤20ms via `libc::poll` + a non-consuming
`MSG_PEEK` recv is how Rudis detects that case without a dedicated epoll registration for
blocked sockets.

#### 3.3 Waking waiters: the pop happens *inside* `notify_list`/`notify_zset`, under the lock

```rust
pub fn notify_list(&mut self, table: &mut crate::table::RudisTable, key: &Bytes) {
    crate::connection::touch_watched_key(self.port, key.as_ref());
    if table.is_key_expired(key.as_ref()) { return; }
    if let Some(waiters) = self.list_waiters.get_mut(key) {
        while let Some(waiter) = waiters.pop_front() {
            if waiter.sender.is_disconnected() { continue; }
            match waiter.op {
                WaiterOp::Pop { pop_type, count } => {
                    let popped = match pop_type {
                        ListPopType::Left => table.lpop(key.as_ref(), count).ok(),
                        ListPopType::Right => table.rpop(key.as_ref(), count).ok(),
                    };
                    if let Some(vals) = popped {
                        if !vals.is_empty() {
                            let _ = waiter.sender.send(BlockedListResult::Popped(key.clone(), vals));
                            satisfied_clients.push(waiter.client_id);
                            if !table.exists(key.as_ref()) { break; }
                        } else {
                            waiters.push_front(waiter);   // put it back, nothing to give it
                            break;
                        }
                    } else { waiters.push_front(waiter); break; }
                }
                WaiterOp::Move { where_from, where_to, ref destination } => { /* see below */ }
            }
        }
    }
    for cid in satisfied_clients { self.remove_waiters_for_client(cid); }
}
```

Contrary to a design where the *writer* (e.g. `LPUSH`) hands the pushed value directly to the
waiter, Rudis instead re-derives the popped value by calling `table.lpop`/`table.rpop` **from
inside `notify_list` itself**, while the caller (`notify_list_or_defer`, in `connection.rs`)
still holds the `BlockHub` mutex *and* the caller's own `ShardDb` borrow. This makes "pick a
waiter" and "remove the value from the list" atomic with respect to each other by construction
— there's no window where two different code paths could both believe they popped the same
element. `touch_watched_key` is called unconditionally at the top, meaning a blocking-wakeup
write also correctly invalidates any `WATCH` on that key (ties into the `MULTI`/`WATCH`
machinery documented in Component 02).

`WaiterOp::Move` (backing `BLMOVE`/`BRPOPLPUSH`-style commands) pops from the source list and
pushes onto the destination list in the same critical section, then **recursively calls**
`self.notify_list(table, destination)` — so if some *other* client is separately blocked on the
destination key, a single `LPUSH`-triggered wakeup can cascade into a second wakeup for a
completely different blocked client, still inside one lock acquisition.

`notify_zset` is structurally identical (`zpopmin`/`zpopmax` instead of `lpop`/`rpop`), except it
also explicitly skips a waiter if `satisfied_clients` already contains its `client_id` — a
duplicate-suppression check the list path doesn't need in the same spot because
`remove_waiters_for_client` is applied immediately afterward per satisfied client.

`notify_stream` (backing `XREAD ... BLOCK`) is much simpler — it's a pure wakeup, no data
handoff:
```rust
pub fn notify_stream(&mut self, key: &Bytes) {
    if let Some(waiters) = self.stream_waiters.remove(key) {
        for waiter in waiters { let _ = waiter.sender.send(()); }
    }
}
```
The blocked `XREAD` task is responsible for re-reading the stream itself once woken.

#### 3.4 `pause()`/`resume()`: deferred notification across `MULTI`/`EXEC` — not `CLIENT PAUSE`

```rust
// connection.rs, around EXEC:
let hub_arc = crate::block::get_block_hub_for_port(router.port);
hub_arc.lock().unwrap().pause();
IN_TX.set(true);
for q_cmd in queued { execute_command(q_cmd, ...).await; }
IN_TX.set(false);
if use_vll { router.release_tx_locks(&sorted_shards, tx_id).await; }
let pending = hub_arc.lock().unwrap().resume();
for k in pending { /* re-dispatch a real notify for each deferred key, possibly cross-shard */ }
```

Every list/zset write goes through `notify_list_or_defer`/`notify_zset_or_defer` rather than
calling `notify_list`/`notify_zset` directly:
```rust
pub fn notify_list_or_defer(db: &mut ShardDb, key: &Bytes) {
    touch_watched_key(db.port, key.as_ref());
    let mut hub = get_block_hub_for_port(db.port).lock().unwrap();
    if hub.is_paused() {
        hub.add_pending_notify(key.clone());
    } else {
        hub.notify_list(&mut db.table, key);
    }
}
```
So while an `EXEC` is running, every write inside the transaction just records its key in
`pending_notifies` instead of immediately waking any blocked client; only after the whole
transaction (and, for cross-shard transactions, the VLL lock release) completes does `resume()`
drain `pending_notifies` and fire real wakeups for each touched key. This means a blocked client
can never observe a partially-applied transaction as if it were a completed write.

**This machinery is unrelated to Redis's `CLIENT PAUSE`/`CLIENT UNPAUSE`.** Those are parsed
into real `Command::Client(ClientSubcommand::Pause(timeout))`/`Unpause` variants, but the
handler for both (and for `CLIENT NO-TOUCH`) is:
```rust
ClientSubcommand::Pause(_) | ClientSubcommand::Unpause | ClientSubcommand::NoTouch(_) => {
    out.extend_from_slice(b"+OK\r\n");
}
```
— an unconditional `+OK` with no effect. `CLIENT PAUSE` does not actually pause anything in
Rudis today.

#### 3.5 `CLIENT UNBLOCK` and the `CLIENT LIST`/`INFO` blocked flag

```rust
let unblocked = hub.unblock_client(target_id, unblock_type);   // CLIENT UNBLOCK <id> [TIMEOUT|ERROR]
...
let is_blocked = get_block_hub_for_port(router.port).lock().unwrap().is_blocked(c.id);
let flags = if is_blocked { "b" } else { "N" };   // surfaced in CLIENT INFO/LIST
```
`unblock_client` looks the target client up in `blocked_clients`/`blocked_zset_clients`, sends a
`BlockedListResult::Unblocked(unblock_type)` / `BlockedZSetResult::Unblocked(unblock_type)` on
its channel (waking `wait_for_blocked_result` immediately), and strips its now-dead waiters from
every key queue it was registered under.

---

### 4. Cross-Component Interactions

- **`src/server.rs`**: the cross-shard receiver's `ShardMessage::NotifyList { keys }` handler
  locks the port's hub once and, for every key in the batch, calls **both** `hub.notify_list`
  and `hub.notify_zset` unconditionally (the sender doesn't track the target's value type, so
  it just tries both — a miss on the wrong map is a cheap no-op `HashMap` lookup).
- **`src/connection.rs`**: registers waiters for `BLPOP`/`BRPOP`/`BLMOVE`/`BZPOPMIN`/
  `BZPOPMAX`/`BZMPOP`/`XREAD BLOCK`; drives `wait_for_blocked_result`; owns
  `BlockedClientGuard`, `notify_list_or_defer`/`notify_zset_or_defer`, and the `MULTI`/`EXEC`
  pause/resume sequencing; calls `touch_watched_key` (Component 02's `WATCH` machinery) from
  inside every notify.
- **`src/table.rs`** (Component 05): `notify_list`/`notify_zset` call directly into
  `RudisTable::lpop`/`rpop`/`zpopmin`/`zpopmax`/`is_key_expired`/`exists` — `BlockHub` mutates
  the storage engine itself rather than being handed already-popped values.
- **`src/router.rs`** (Component 04): local writes broadcast `ShardMessage::NotifyList` to every
  *other* shard so a blocked client on shard A can be woken by a write on shard B.

---

### 5. Future Improvements

- **Medium — replace `wait_for_blocked_result`'s active ≤20ms polling with real readiness notification (§3.2).** Polling `libc::poll`/`MSG_PEEK` every tick to detect a vanished client works but costs a syscall per blocked client per tick even when nothing has happened; since `monoio`'s `io_uring` driver already knows how to wait on fd readiness/hangup without polling, registering an explicit disconnect-watch operation on the ring (if `monoio` exposes one) would remove this cost and also lower worst-case disconnect-detection latency below the current 20ms cap.
- **Medium — implement real `CLIENT PAUSE`/`CLIENT UNPAUSE` semantics (§3.4).** They're currently a no-op `+OK` stub, distinct from the real (but differently-purposed) `pause`/`resume` used internally for `MULTI`/`EXEC` deferral. Since that internal mechanism already exists and does almost the right thing (defer notifications, replay after), extending it to also gate new-command acceptance for `CLIENT PAUSE`'s actual contract (pause all commands, or just writes, for a duration) is a smaller lift than building the feature from scratch.
- **Low — bound `pending_notifies`' growth during a very large `MULTI`/`EXEC`.** Every write inside a paused transaction appends to `pending_notifies` (§3.4) with no cap; a transaction touching an unusually large number of distinct keys could accumulate an unbounded `Vec` before `resume()` drains it. Unlikely to matter in practice (transaction size is bounded by client behavior), but worth a sanity cap if very large scripted transactions become common.
- **Low — consider giving `notify_zset`'s duplicate-suppression check (§3.3) and `notify_list`'s equivalent logic a shared helper** rather than two structurally-identical-but-separately-implemented sweeps, purely to reduce the chance the two drift apart if one gets a bugfix the other doesn't.

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: `BlockHub` is one of the few shared-mutex structures in Rudis (Component 06 design doc §2.3); it is reached from any shard via `get_block_hub_for_port(port)`, and every critical section inside it is short and synchronous (never held across an `.await`).
* **Gotcha 2**: Client disconnects do not automatically cancel registered waiters via any disconnect callback — they are caught in one of two ways: `BlockedClientGuard::drop` unregisters the waiter when the connection task's future is dropped, and, while the task is still parked in `wait_for_blocked_result`, `is_fd_closed` polls the raw fd (via `libc::poll`/`MSG_PEEK`) at most every 20ms to detect a peer that vanished without a clean FIN (§3.2).
* **Gotcha 3**: There is no timeout priority queue. Each blocked task tracks its own deadline locally and `wait_for_blocked_result` re-checks it on every ≤20ms polling tick (§3.2) — timeouts are evaluated per-waiter on wakeup, not via a shared min-heap or scheduled timer structure.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
