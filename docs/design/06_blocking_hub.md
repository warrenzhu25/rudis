# Component 06: Blocking Operations & The Reactive Event Hub (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/block.rs`  
> **Implementation Reference**: [`docs/internal/06_blocking_hub.md`](../internal/06_blocking_hub.md)  
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
Traditional in-memory datastores encounter severe scalability barriers on modern multi-core, high-throughput cloud hardware. Single-threaded architectures (such as Redis) saturate a single CPU core while leaving the remaining 95%+ of server cores idle. Multi-threaded mutex architectures (such as Memcached) suffer from heavy spinlock contention, CPU cache line bouncing, and global memory allocator lock bottlenecks.

### 1.2 The Rudis Solution
Rudis implements the **Thread-Per-Core (Shared-Nothing)** architectural paradigm natively on Linux `io_uring` via Monoio. Each physical CPU core owns its own isolated event loop, its own thread-local memory database, and its own kernel `SO_REUSEPORT` listener. Operations on local keys execute in nanoseconds with zero locks, zero atomic operations, and zero cross-core cache invalidations.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model
A thread-safe reactive registry for blocking operations (BLPOP, BRPOP, BZPOPMIN, XREAD BLOCK). When a key receives a push on any shard, BlockHub signals the waiting connection without polling.

### 2.2 Design Rationale (The "Why")
In a pure shared-nothing design, each shard owns a thread-local `Rc<RefCell<ShardDb>>` that no other thread can touch — this is precisely what makes ordinary command execution lock-free. Blocking commands break that isolation by construction: a client parked on `BLPOP` on Core 0 must be woken by an `LPUSH` executed on Core 3, and the two shards share no memory that either can safely mutate from the other's thread. Two designs were available to bridge this gap:

- **Cross-shard message passing only** (matching the pattern used for ordinary cross-shard command forwarding, Component 04): the producing shard would send an async notification to the consuming shard's event loop, which would then attempt the pop locally. This preserves shared-nothing purity but reintroduces a race — between "producer decides to notify" and "consumer's loop processes the notification," two different producers could both believe they are the one waking a given waiter, or a waiter could be woken and find the value already taken by a `LPOP` that ran in between.
- **A narrow, globally shared, short-lived critical section** (the approach Rudis takes): `BlockHub` is reachable from every shard via `get_block_hub_for_port(port)`, and the *pop itself* happens inside the same lock acquisition that selects the waiter (Component `docs/internal/06_blocking_hub.md` §3, `notify_list`). This makes "pick a waiter" and "remove the value from the source key" atomic with respect to every other shard by construction, eliminating the race the message-passing design would have to solve some other way (e.g. with a second round-trip acknowledgment).

Rudis accepts one small, well-contained shared-state exception to an otherwise lock-free architecture in exchange for a coordination model with no separate race to reason about. The producing shard (Core 3) still executes the `LPUSH` entirely on its own thread-local `ShardDb`; only the *notification and pop* step for waiters touches the shared `BlockHub` mutex, and only when at least one client anywhere is actually blocked on that key (§2.3, `has_blocked_waiters`).

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
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

## 3. High-Level Architecture & Workflow Diagram

```
Core 0: BLPOP list 10
   │  hub.register_blocked_client(cid, tx)
   │  hub.register_list_waiter(cid, "list", ...)
   ▼
BlockHub (Mutex<BlockHub>, shared by every shard on this port)
   ▲
   │  hub.notify_list(&mut table, "list")  — pop happens here, under the lock
   │
Core 3: LPUSH list "x"
   │  1. Applies the push to Core 3's own thread-local ShardDb (no cross-shard call needed
   │     for the write itself — the key hashes to Core 3, so the write is always local).
   │  2. notify_list_or_defer(db, "list") locks the shared BlockHub for this port and, if a
   │     waiter for "list" is registered anywhere, pops the value and sends it over that
   │     waiter's flume channel immediately — regardless of which shard registered it.
   │  3. Local-only shortcut: because BlockHub is reachable from any shard by port (not by
   │     shard id), Core 3 does not need to know Core 0 is the one blocked; it notifies the
   │     hub directly and the hub's own bookkeeping (client_id → sender) does the routing.
   ▼
Core 0's blocked task, parked on `rx.recv_async()` inside `wait_for_blocked_result`, receives
the popped value and resumes — no polling, no cross-shard RPC round trip beyond the single
mutex acquisition.
```

For an ordinary (non-transactional) write, this is the entire cross-core path: **no separate
cross-shard message is required**, because the write is always routed to the shard that owns
the key (Component 04's keyed-command dispatch) before `notify_list_or_defer` runs, so the
shard doing the notifying already holds the mutable reference to its own thread-local
`RudisTable` that `notify_list`/`notify_zset` need in order to perform the pop.

The one place a genuine cross-shard *message* (`ShardMessage::NotifyList`, `src/router.rs`/
`src/server.rs`) is still required is `MULTI`/`EXEC`'s deferred-notification path (§4.4 of the
internal document): a transaction can touch keys owned by several different shards, and the
connection task that runs `EXEC` and later calls `resume()` is pinned to whichever shard
accepted the client connection — not necessarily the shard that owns every deferred key. When
`resume()` yields a pending key owned by a *different* shard, that shard's own thread-local
table is unreachable directly, so the owning shard is asked, via a routed `ShardMessage`, to
run `notify_list`/`notify_zset` against its own local state instead.

---

## 4. Performance Guarantees & Theoretical Complexity

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

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/06_blocking_hub.md`**](../internal/06_blocking_hub.md): Low-level implementation and code reference.
* **Source Files**: `src/block.rs`
