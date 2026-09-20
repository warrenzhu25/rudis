# Component 02: Connection Lifecycle & Command Execution (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/connection.rs`
> **Implementation Reference**: [`docs/internal/02_connection_lifecycle.md`](../internal/02_connection_lifecycle.md)
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem

Traditional in-memory datastores encounter severe scalability barriers on modern multi-core, high-throughput hardware. Single-threaded architectures (such as Redis) saturate one CPU core while the remaining cores sit idle. Multi-threaded, shared-state architectures (such as Memcached) avoid that ceiling but pay for it with lock/spinlock contention, cross-core cache-line bouncing, and a global memory allocator that becomes a bottleneck under concurrent access.

### 1.2 The Rudis Solution

Rudis implements a **thread-per-core, shared-nothing** architecture on Linux `io_uring` via Monoio. Each physical CPU core owns its own event loop, its own thread-local shard of the keyspace, and accepts connections on a `SO_REUSEPORT` listener shared with the other cores. A connection is pinned to the core that accepted it for its entire lifetime. Operations on keys owned by that core execute inline with no locks and no cross-core synchronization; operations on keys owned by another core are dispatched as an explicit, awaited message over a per-core mesh rather than through shared mutable state.

This document describes the connection-handling and command-execution layer (`src/connection.rs`) that implements this model: the per-connection read loop, the transaction subsystem, pipeline squashing (batched cross-shard dispatch), and the authorization/redirection gates every command passes through before it runs.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model

A connection spends its entire lifetime pinned to the worker core that accepted it. Per read, it parses every complete RESP frame currently buffered, classifies the batch (does it contain a transaction-control command? a blocking command? a mode-switching command such as `SUBSCRIBE` or `PSYNC`?), and then executes it via one of four strategies: the transaction branch, the sequential-with-pre-block-flush branch, a direct single-command call, or — for an ordinary multi-command pipeline — the squashing fast path, which buckets commands by destination shard and issues at most one message per remote shard.

### 2.2 Design Rationale (The "Why")

**Why pin a connection to one core for its lifetime, rather than load-balance per request?** Migrating a connection's state between cores mid-lifetime would require either replicating its `WATCH`/`MULTI`/subscription/ACL-session state across cores or serializing it on every migration — both defeat the purpose of a shared-nothing design. Pinning at accept time (via `SO_REUSEPORT`) means all of that state can live as plain, non-atomic stack and thread-local variables for the connection's entire life; the trade-off accepted is that a single very hot connection cannot be rebalanced away from an otherwise-busy core.

**Why batch remote commands into one message per shard instead of dispatching them individually?** In a pipelined workload, sending each remote-shard command as its own message means every key that happens to live on another core costs one full inter-core round-trip. For a pipeline of N commands spread across S shards, per-key dispatch costs O(N) round-trips in the worst case; pipeline squashing buckets commands by target shard first and sends one batched message per shard actually touched, bounding the round-trip count by the number of distinct remote shards in the batch (at most S − 1, typically far fewer) rather than by the number of remote-routed commands.

**Why a lock-free single-slot mailbox (`BatchResponder`) instead of a channel per outstanding batch?** An MPSC or bounded channel still requires the receiving side to take an internal lock (or CAS loop with retry) on every send/receive under contention, which is exactly the cost a shared-nothing design is trying to avoid on its hottest path. `BatchResponder` instead gives each `(connection, target shard)` pair a pre-allocated, reusable single-value slot: the remote shard writes its result through an `UnsafeCell` and flips an `AtomicBool`, and the connection polls that flag directly. A `flume::bounded(1)` channel is layered on top purely as an async wake-up primitive for the case where polling gives up — it never carries the payload itself. The mailboxes are pooled per reactor thread and reused across different connections on that core (not reallocated per pipeline flush), so steady-state cross-shard fan-out allocates nothing beyond the occasional pool-growth event.

**Why does pipeline squashing conditionally skip its own ACL/cluster-ownership checks?** Evaluating per-command ACL rules and cluster slot ownership on every command in every pipeline is wasted work for the common deployment that runs without a custom ACL and without cluster mode. Rudis checks a pair of cheap global flags (`HAS_CUSTOM_ACL`, and whether this node's cluster hub reports any peers) before paying for the per-key checks at all; when either feature is active, the checks run against *every* key of every command (not a single representative key), because the alternative — checking one key and letting the rest through unchecked — reintroduces exactly the class of authorization/redirection bug this gate exists to prevent (see §3, invariant 4).

**Why do some inline write fast paths in the squashed pipeline path require "no AOF and no connected replica"?** The squashed path's inline fast paths exist to skip a second hash computation and the general per-command dispatch overhead for the hottest local read/write commands. That overhead, however, is also where AOF persistence, replication propagation, search-index maintenance, and blocked-client wakeup are triggered in the general path. Rather than duplicating all of that side-effect machinery into every fast path, Rudis gates each fast path on the specific side effects it would otherwise skip being inactive (no AOF writer configured, no connected replica, no active search index, no blocked waiter on that port, as applicable per command) and falls through to the fully general path whenever any of those conditions doesn't hold. This trades a small amount of missed optimization on persistence/replication-enabled deployments for a hard guarantee that no side effect is ever silently dropped by the fast path — with one narrow, currently-real exception in the plain `SET` fast path's `WATCH`/client-side-cache handling; see the implementation reference, §6.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)

1. **Pure single-threaded client state.** Every connection is handled exclusively by the core that accepted it. `handle_connection`'s locals (`in_multi`, `tx_queue`, `asking`, `authenticated`, `auth_user`) are plain stack variables with no `Arc`/`Mutex`. Per-connection scratch buffers (read/write buffers, cross-shard response mailboxes) are pooled and reused *across* connections on the same reactor thread for allocation efficiency, but never shared *concurrently* across threads — a mailbox is only handed to another core's shard task for the duration of one in-flight batch, and is only returned to the pool once provably idle and unreferenced (checked via `Arc::strong_count`).
2. **Blocking commands force an immediate flush first.** Before executing a command such as `BLPOP` that may suspend the task for up to its timeout, the connection flushes any already-buffered responses to the socket — otherwise earlier pipelined replies would sit unsent for the entire blocking duration.
3. **Cross-shard transactions use explicit, order-disciplined locks.** A `MULTI`/`EXEC` block whose queued commands touch more than one shard acquires a distributed lock across every touched shard, in ascending shard-ID order, before running the batch, and releases it after. Sorting the shard set before acquisition is what prevents deadlock against a concurrent transaction that touches an overlapping but differently-ordered shard set — without that discipline, transaction A locking shards [1, 2] while transaction B concurrently locks shards [2, 1] can deadlock.
4. **Squashing is gated on more than routability.** The pipeline-squashing fast path additionally requires the client to already be authenticated and, whenever ACL or cluster mode is active, have real permission for *every* key of *every* command in the batch — not merely a representative key — and, for keyed commands under cluster mode, that this node's cluster-gossip table actually confirms ownership of every key's slot, not merely that the slot's local state is locally believed stable. Any single ineligible command in the batch defeats squashing for the whole batch, falling back to sequential execution so the real per-command `-NOPERM`/`-MOVED`/`-ASK`/`-CROSSSLOT` checks apply.
5. **Non-blocking cross-shard dispatch.** Remote-shard work is always sent as a `ShardMessage::Batch` over the pre-allocated cross-shard mesh and awaited without blocking the reactor thread — other connections on the same core keep making progress. The awaiting connection spends a bounded number of iterations polling its mailboxes directly (no async yield) before falling back to a real `.await` on a wake-up channel, trading a bounded amount of CPU spin for avoiding a scheduler round-trip in the common case where the remote reply arrives within microseconds.
6. **Unbounded output buffering is not acceptable.** A slow or stalled client reading its socket too slowly must not be allowed to grow this connection's output buffer without bound and consume unbounded server memory. Every class of connection (normal request/response, replica stream, pub/sub) is subject to a configurable hard limit (disconnect immediately) and soft limit (disconnect only if sustained past a configured duration), evaluated on every flush.
7. **Write commands are rejected under `maxmemory` with `noeviction`, not silently admitted.** When a `maxmemory` budget is configured, eviction policy is `noeviction`, and NVMe tiering is not active for this shard, a write command that would push this shard's memory usage over its per-shard share of the budget is rejected with `-OOM` before it runs, rather than being admitted and left for a separate reclamation pass to catch later.

---

## 3. High-Level Architecture & Workflow Diagram

```
Client Pipelined Stream: [GET k1, SET k2, GET k3]
                               │
                In-Place Pipeline Parser
                               │
                ┌──────────────┴──────────────┐
                ▼                             ▼
        Local Shard (k1, k3)          Remote Shard (k2)
        Execute Inline (0 hops)       Single Batched Hop via
                │                     Lock-Free Mailbox
                └──────────────┬──────────────┘
                               ▼
          Coalesced Response Buffer, Non-Blocking Send
          (io_uring write_all as fallback on EAGAIN)
```

A connection can also permanently leave this loop for one of three dedicated, non-returning modes upon receiving the triggering command: pub/sub streaming (`SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`), master→replica replication streaming (`PSYNC`), or shard-to-shard replication flow (Dragonfly-protocol `DFLY FLOW`, used for live shard migration/rebalancing). Each hand-off is one-way for the remaining lifetime of that TCP connection.

---

## 4. Performance Guarantees & Theoretical Complexity

- **Pooled, cross-connection-reused response mailboxes, not one-shot channels.** Per-shard response mailboxes are allocated once per reactor thread (sized to the shard count) and pooled across successive connections handled by that thread, not reallocated per pipeline flush or per connection. This is the mechanism behind zero-steady-state-allocation cross-shard fan-out; see the implementation reference for the pool's reuse-safety condition.
- **Small-reply inline storage.** The reply payload type carried through cross-shard batches stores short replies (a few dozen bytes or fewer — covers `+OK`, small integers, and short bulk strings) inline with no heap allocation, falling back to a heap-backed representation only for larger payloads.
- **Command-name/stat bookkeeping is not free, but is batched off the hot path.** Every executed command updates per-client `last_active`/`last_cmd` bookkeeping and a command-frequency counter; the frequency counter accumulates in a thread-local map and is merged into the shared, lock-guarded global map only once per 1024 commands (or on connection teardown) rather than on every command — real but modest fixed overhead, amortized rather than eliminated.
- **`MGET`/`MSET` genuinely parallelize across shards when they must.** When a multi-key `MGET`/`MSET`'s keys don't all happen to land on the same shard, the command is bucketed by shard and dispatched to every touched remote shard concurrently, then gathered — bounded by the slowest touched shard's response time, not by the sum of every touched shard's response time. When every key of the command does happen to hash to the same shard, it takes the ordinary single-target path with none of the cross-shard machinery at all.
- **The spin-then-block cross-shard harvest trades CPU for latency, with a fairness cost worth naming.** A bounded number of non-blocking polling sweeps happen *without yielding to the cooperative scheduler* — on this shared-nothing, thread-per-core design, that means other connections' tasks on the *same core* make no progress while a squashed batch or a cross-shard `MGET`/`MSET` is in its spin phase. This is a good trade when remote shards reply within microseconds (the common case this was tuned for); a burst of concurrent cross-shard operations each waiting on a genuinely slow remote shard could measurably delay unrelated connections sharing that core until the spin budget is exhausted and each request falls back to a real, scheduler-yielding wait.
- **Output flushing avoids the `io_uring` round-trip in the common case.** The response buffer is sent via a direct, non-blocking `send(2)` first; only a partial write or a would-block result falls back to a full `io_uring` write. This is a single coalesced buffer per flush, not scatter-gather/vectored I/O across multiple buffers.
- **Client output-buffer limits bound worst-case per-connection memory, not just steady-state throughput.** The hard/soft limit mechanism (§2.3, invariant 6) is what keeps a client that stops reading its socket from turning into unbounded server-side memory growth; it is evaluated on every flush for request/response and replica connections, and independently by the pub/sub subsystem's dedicated writer task.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/02_connection_lifecycle.md`**](../internal/02_connection_lifecycle.md): Low-level implementation and code reference, including the currently-known correctness gap in the squashed pipeline path's plain `SET` fast path (§6 of that document).
* **Source Files**: `src/connection.rs`
