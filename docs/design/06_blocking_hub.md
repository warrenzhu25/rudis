# Component 06: Blocking Operations & The Reactive Event Hub (Design)

## Component 06: Blocking Operations & The Reactive Event Hub

> **Source Files**: ``src/block.rs``


---

### 1. Architectural Purpose & Scope

`src/block.rs` implements Rudis's waiter registration and wakeup engine (**`BlockHub`**). It
powers the blocking list/zset/stream commands — `BLPOP`, `BRPOP`, `BLMOVE`, `BRPOPLPOP`-style
moves, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`, and `XREAD ... BLOCK` — plus `CLIENT UNBLOCK` and the
blocked-flag reported by `CLIENT LIST`/`CLIENT INFO`. Unlike the rest of Rudis, `BlockHub` is
**not** thread-local: it is one process-wide, mutex-guarded structure per listening port, shared
by every shard thread serving that port.

---

### 2. Key Invariants & Concurrency Constraints

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

---

### 6. Performance Characteristics

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
