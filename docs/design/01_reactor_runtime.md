# Component 01: Reactor Runtime & Server Lifecycle (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/main.rs, src/server.rs`  
> **Implementation Reference**: [`docs/internal/01_reactor_runtime.md`](../internal/01_reactor_runtime.md)  
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
Each worker core runs an entirely isolated world. Worker threads never share data structures, never take global locks, and never migrate between CPU cores. Ingress connections are kernel-balanced via SO_REUSEPORT, driving an independent Linux io_uring instance via Monoio.

### 2.2 Design Rationale (The "Why")
Traditional Redis uses a single event loop (ae.c) which bottlenecks on a single CPU core, wasting 98% of multi-core servers. Multi-threaded stores like Memcached use global or fine-grained mutexes that trigger cache line bouncing across sockets. Rudis uses Thread-Per-Core on Linux io_uring to achieve zero-syscall batching and 100% L1/L2 cache locality.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **Thread-per-Core Pinning**: Every worker thread is pinned to an exclusive CPU core using `core_affinity::set_for_current`, unless `--no-pin` is passed. No worker thread is migrated by the OS scheduler once pinned.
2. **`SO_REUSEPORT` Ingress Balancing**: Every worker thread opens its own listening socket bound to the same port (`socket.set_reuse_port(true)`). The kernel distributes new incoming connections across all bound sockets by a 4-tuple hash, with zero userspace dispatch.
3. **Thread-local database, no locks in the data path**: `ShardDb` lives in a plain `Rc<RefCell<ShardDb>>` — not `Arc<Mutex<_>>`. Because `Rc`/`RefCell` aren't `Send`, the compiler itself refuses to let a `ShardDb` handle cross a thread boundary.
4. **One real exception to "zero locks": the blocking-command hub.** `crate::block::get_block_hub_for_port(port)` returns an `Arc<Mutex<BlockHub>>` from a process-wide `static` map keyed by port (`PORT_BLOCK_HUBS`), so it *is* shared and mutex-guarded across every shard thread serving that port. This is a deliberate, narrow exception: blocking commands (`BLPOP`, `BZPOPMIN`, ...) need cross-shard wakeups, which the shared-nothing model can't give them for free, so a small locked structure was introduced specifically for that coordination rather than for the data path itself.
5. **No graceful shutdown.** There is no signal handler anywhere in `src/server.rs` or `src/main.rs` — no `SIGINT`/`SIGTERM` trap, no drain, no flush-on-exit logic. The process only stops if every thread's infinite loop is killed externally (the accept loop and the cross-shard receiver loop both run forever).

---

## 3. High-Level Architecture & Workflow Diagram

```
Linux Kernel (SO_REUSEPORT 4-Tuple Hash)
                               │
               ┌───────────────┴───────────────┐
               ▼                               ▼
       Core 0 (Shard 0)                Core 1 (Shard 1)
       • Monoio Reactor (io_uring)     • Monoio Reactor (io_uring)
       • Local ShardDb (Zero Locks)    • Local ShardDb (Zero Locks)
       • Periodic Tasks (Expire, Tier) • Periodic Tasks (Expire, Tier)
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **Zero-syscall-per-connection ingress**: `SO_REUSEPORT` means the kernel — not userspace — decides which shard's listener gets each new connection.
- **No cross-core cache traffic in the common case**: local key access never leaves the owning thread; only the `ShardMessage` mesh and the shared `BlockHub` mutex cross cores, and both are only exercised on non-local or blocking operations.
- **Bounded periodic work, not full scans**: the 100ms expiration cycle, 20ms auto-tier check, and 2s GC task are all designed to do fixed, small amounts of work per tick rather than scanning the whole shard, so they never show up as a latency spike on the shared single-threaded runtime.
- **The cross-shard receiver loop now amortizes async overhead across up to 64 messages per wakeup** (§4.2's `try_recv` burst-draining) instead of paying one `recv_async().await` suspend/resume cycle per message — a real, measurable win under sustained cross-shard traffic (e.g. many concurrent `MGET`/`MSET` fan-outs or heavy pipeline squashing hitting one shard from many peers at once).
- **Batch-level tiering checks are now gated on whether tiering is enabled at all** (`has_tier_manager`, §4.2) — a shard running with no tiered storage configured skips the per-`Get`-in-batch `is_tiered` lookup entirely rather than paying a cheap-but-nonzero check on every batched read.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/01_reactor_runtime.md`**](../internal/01_reactor_runtime.md): Low-level implementation and code reference.
* **Source Files**: `src/main.rs, src/server.rs`
