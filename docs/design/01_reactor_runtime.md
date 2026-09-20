# Component 01: Reactor Runtime & Server Lifecycle (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/main.rs`, `src/server.rs`
> **Implementation Reference**: [`docs/internal/01_reactor_runtime.md`](../internal/01_reactor_runtime.md)
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem

Traditional single-threaded in-memory datastores (Redis's core event loop) saturate one CPU core and leave the rest of a multi-core host idle. Traditional multi-threaded datastores (Memcached) avoid that ceiling but pay for it with mutex contention, cache-line bouncing between cores, and a shared allocator that becomes a serialization point under load.

### 1.2 The Rudis Solution

Rudis runs one independent event loop per physical CPU core (**thread-per-core**, sometimes called shared-nothing). Each shard thread owns its own `monoio` async runtime, its own kernel listening socket (via `SO_REUSEPORT`), and its own in-memory database (`ShardDb`) that is never touched by another thread. A key handled entirely by its owning shard needs no lock, no atomic increment, and no cross-core cache-line transfer. Cross-shard coordination — needed only when a client's key lives on a different shard, or for operations that are inherently global (pub/sub fan-out, blocking commands, replication) — goes through explicit message passing or a small number of deliberately shared, lock-guarded structures. This is the same family of design as Seastar/ScyllaDB and Dragonfly; the implementation here is Rust-native, built on `monoio`'s `io_uring`-backed (with portable fallback) async runtime rather than a custom C++ reactor.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model

Each shard thread is an isolated world: its own `monoio` runtime, its own `ShardDb`, its own accept loop. Nothing is migrated between threads by the OS scheduler once a shard is pinned to a core, and nothing in the per-command hot path is shared, locked, or atomically updated. Client connections are distributed to shards in two stages: first by the kernel (`SO_REUSEPORT` picks a listening socket by a 4-tuple hash of the incoming connection), then, because that hash is not evenly distributed over a small number of long-lived connections, by an explicit userspace rebalancing step (§2.5) that can hand a freshly accepted file descriptor to a less loaded sibling shard before any work is done on it.

### 2.2 Design Rationale (The "Why")

A single-core event loop (classic Redis `ae.c`) cannot exceed the throughput of one core no matter how many cores the host has. Fine-grained or global mutexes (classic Memcached) let all cores participate, but every lock acquisition is a potential cache-line bounce and every contended lock is a latency tax paid by every request that touches it, not just the ones that collide. Thread-per-core sidesteps both problems for the common case (a key access that stays on one shard) by making cross-thread synchronization not merely cheap but, for `ShardDb` itself, *impossible to express* — `Rc<RefCell<ShardDb>>` is not `Send`, so a `ShardDb` handle cannot cross a thread boundary; the Rust compiler enforces the invariant that a naive implementation would otherwise have to enforce by convention and code review.

The trade-off this buys is architectural, not free: any operation that legitimately needs global state (blocking-command wakeups, replication fan-out, pub/sub across shards, cluster slot ownership) must be implemented as either explicit cross-shard message passing over channels, or as one of a small, intentionally enumerated set of shared structures guarded by a lock. Both patterns are catalogued below (§2.4) precisely because they are exceptions to the rule, not because the rule is absolute.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)

1. **Thread-per-core pinning.** Every shard thread is pinned to an exclusive CPU core with `core_affinity::set_for_current` unless `--no-pin` (or `no_pin` in `Args`) is passed. Once pinned, the OS scheduler never moves the thread to another core.
2. **`SO_REUSEPORT` ingress balancing.** Every shard opens its own listening socket bound to the same port with `SO_REUSEPORT` (and `SO_REUSEADDR`). The kernel chooses which socket's accept queue a new connection lands in by hashing the connection's 4-tuple, without any userspace dispatch. In cluster mode this changes: each shard instead binds a *distinct* port (`base_port + shard_id`), because a cluster client deliberately targets the shard that owns the slots it wants, and `SO_REUSEPORT`'s hash-based balancing would fight that.
3. **`ShardDb` is thread-local and lock-free by construction.** It lives in `Rc<RefCell<ShardDb>>`. Because `Rc` and `RefCell` are not `Send`, the compiler refuses to compile any code that would let a `ShardDb` handle escape its owning thread — there is no way to accidentally introduce a data race on the primary key space.
4. **A small number of explicit, documented exceptions share state across threads:**
   - **The blocking-command hub** (`crate::block::get_block_hub_for_port(port)`) returns an `Arc<Mutex<BlockHub>>` from a process-wide map keyed by port. Blocking commands (`BLPOP`, `BZPOPMIN`, ...) need a shard to be able to wake up a client blocked on a different shard, which the shared-nothing model cannot give for free; a small, narrowly-scoped mutex was introduced specifically for that coordination.
   - **Replication state** (`crate::replication::get_replication_hub(port)`) returns an `Arc<ReplicationHub>` guarded internally by `RwLock`s and atomics, shared by every shard serving a given port. Any shard's write must be visible to every connected replica, not just the shard that executed it, so the replication hub is deliberately process-wide rather than per-shard (see Component 14).
   - **Connection-count balancing** (`crate::conn_balance`) uses a fixed array of plain `AtomicUsize` counters, one slot per tracked shard, touched exactly twice per connection (accept and close) — not on the per-command hot path. It exists because `SO_REUSEPORT`'s hash balancing is measurably uneven for a small number of long-lived connections (§4).
5. **A real, if partial, graceful-shutdown path exists.** `main.rs` installs `SIGINT`/`SIGTERM` handlers (`rudis::shutdown::install_signal_handlers`) that set a process-wide atomic flag; `SIGPIPE` is ignored so a client disconnecting mid-write cannot kill the process. Every shard's accept loop(s) poll that flag roughly every 200ms and break out when it is set, after which the shard flushes and `fsync`s its AOF writer (if AOF is enabled) before its `monoio` runtime returns and the OS thread joins. This is not a full connection-draining shutdown: already-accepted connections, the cross-shard receiver task, and the periodic maintenance tasks are not explicitly told to stop — they end only because the shard's runtime is torn down when `block_on` returns. There is no explicit "stop accepting, wait for in-flight requests to finish, then exit" handshake with connected clients. Framing this as *no* graceful shutdown (as earlier revisions of this document did) is no longer accurate; framing it as a complete one would overstate what exists today.
6. **Every accepted connection is panic-isolated.** `handle_connection` (and the TLS and cross-shard-adopted equivalents) is wrapped in `catch_unwind_async` (`src/server.rs`), a small `Future` combinator built on `std::panic::catch_unwind`. A panic while servicing one client is caught, logged, and counted (`crate::connection::inc_isolated_panics`) instead of unwinding into — and killing — the shard's entire `monoio` runtime and every other connection sharing it.

### 2.4 A Second Reading of "Shared-Nothing"

"Shared-nothing" describes the data path, not the whole process. The exceptions in §2.3.4 are deliberate and small in surface area: a blocking-op mutex, a replication `RwLock`, and a handful of atomics for load balancing. None of them sit on the path of a local key access, which is what the performance claims in §4 are about.

---

## 3. High-Level Architecture & Workflow Diagram

```
                         Linux Kernel: SO_REUSEPORT 4-tuple hash
                        (or, in cluster mode: one port per shard)
                                        │
                ┌───────────────────────┼───────────────────────┐
                ▼                       ▼                       ▼
        Core 0 (Shard 0)        Core 1 (Shard 1)         Core N-1 (Shard N-1)
        • monoio reactor        • monoio reactor         • monoio reactor
          (FusionDriver:          (FusionDriver)            (FusionDriver)
           io_uring or epoll)
        • local ShardDb          • local ShardDb           • local ShardDb
          (Rc<RefCell<_>>,         (zero locks)              (zero locks)
           zero locks)
        • periodic tasks:        • periodic tasks          • periodic tasks
          100ms expire,            (same cadence)             (same cadence)
          20ms auto-tier,
          2s tiering GC
        • cluster bus            (cluster bus runs only on shard 0)
```

Cross-shard traffic (a key that hashes to a different shard, pub/sub fan-out, blocking-op wakeups, replication propagation, cluster slot messages) travels over a mesh of `flume` channels — one sender/receiver pair per shard pair — rather than through shared memory.

---

## 4. Performance Guarantees & Theoretical Complexity

- **Zero-syscall-per-connection ingress in the common case.** `SO_REUSEPORT` lets the kernel pick the destination listener without a userspace dispatch step.
- **No cross-core cache traffic for a local key access.** Reading or writing a key that hashes to the accepting shard never touches another core's cache lines; only the cross-shard `ShardMessage` mesh and the small set of shared structures in §2.3.4 cross cores, and those are only exercised for non-local or inherently cross-cutting operations.
- **Bounded, fixed-size periodic work.** The 100ms active-expiration cycle, the 20ms auto-tiering check, and the 2s tiered-storage GC pass are all designed to do a small, roughly constant amount of work per tick rather than scanning an entire shard, so they should not appear as latency spikes.
- **The cross-shard receiver loop amortizes wakeup cost across up to 64 messages per scheduling event**, draining its channel with `try_recv` after each `recv_async().await` wakeup instead of suspending and resuming once per message — a real win under sustained cross-shard traffic (fan-out `MGET`/`MSET`, pipeline squashing from many peers landing on one shard at once).
- **`SO_REUSEPORT`'s connection balancing is measurably poor for a small number of long-lived connections**, which is exactly the profile of a benchmark client or a small connection-pooled application. `src/conn_balance.rs` documents this directly in its module comment: with 64 connections spread by kernel hash over 16 shards, the busiest shard can end up with roughly 8 while others get 1–2, and since end-to-end throughput is gated by the most loaded shard, the documented measurement is 2.74M ops/s at 64 connections versus 5.44M ops/s at 256 connections on an otherwise identical configuration — close to a Monte-Carlo estimate of ~52% of ideal capacity at the lower connection count. This is why explicit connection handoff (§2.3.4) exists: an overloaded shard hands a newly accepted connection's file descriptor to the least-loaded shard (all shards share one process-wide file descriptor table, so this is a plain integer send over a `ShardMessage::AdoptConnection`, not `SCM_RIGHTS`). Cluster mode is exempt from this rebalancing, because in that mode a client's shard choice is meaningful (it determines which slots it can reach without a `MOVED` redirect).
- **Panic isolation has a per-connection cost of one `catch_unwind` boundary**, not a per-command one; it protects shard availability without adding synchronization to the hot path.

These are architectural properties derived from the code's structure, not independently measured benchmark results (with the one exception above, which is a number already recorded in the source tree, not one produced for this document).

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/01_reactor_runtime.md`**](../internal/01_reactor_runtime.md): Low-level implementation and code reference.
* **Source Files**: `src/main.rs`, `src/server.rs`
