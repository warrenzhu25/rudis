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
In shared-nothing architectures, blocking operations require cross-shard coordination because a producer on Core 0 can unblock a consumer waiting on Core 3. BlockHub provides this narrow, locked coordination layer.

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
Consumer on Core 0 (BLPOP list 10)  ──► Registers in BlockHub
                                                     ▲
       Producer on Core 1 (LPUSH list "x") ──► Unblocks waiting Core 0
```

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
