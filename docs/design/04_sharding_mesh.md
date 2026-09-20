# Component 04: Sharding Architecture & Cross-Core Mesh (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/router.rs`, `src/shard.rs` (cross-shard IPC primitives live in
> `src/mailbox.rs`)  
> **Implementation Reference**: [`docs/internal/04_sharding_mesh.md`](../internal/04_sharding_mesh.md)  
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem

Traditional in-memory datastores hit severe scalability limits on modern multi-core
hardware. Single-threaded architectures (Redis) saturate one CPU core while the rest of
the machine sits idle. Multi-threaded, shared-memory architectures (Memcached and most
"multi-threaded Redis" forks) avoid that ceiling but pay for it with mutex/spinlock
contention on the hash table, cache-line bouncing between cores, and a memory allocator
that itself becomes a global lock bottleneck under concurrent access.

### 1.2 The Rudis Solution

Rudis implements the **thread-per-core, shared-nothing** architectural paradigm on top of
Linux `io_uring`, via the Monoio async runtime. Each logical shard is a single OS thread,
pinned (where possible) to one CPU core, running its own single-threaded Monoio
(`FusionDriver`) event loop. Each shard owns:

- its own `ShardDb` — a thread-local key space with **no `Mutex`, no `RwLock`, and no
  atomic reference counting on the hot data path** (`Rc<RefCell<ShardDb>>`, not
  `Arc<Mutex<ShardDb>>`);
- its own kernel listening socket, bound with `SO_REUSEPORT` so the kernel load-balances
  new TCP connections across shards by 4-tuple hash without any userspace coordination;
- its own AOF writer, tiering manager, vector/JSON/CRDT/search sub-stores, and pub/sub
  presence state.

A request for a key that belongs to the shard servicing the connection executes entirely
in-process, synchronously, with the same complexity as a single-threaded key-value store.
A request for a key owned by a *different* shard is **not** satisfied by locking that
shard's data structure from another thread — Rudis never does that. Instead it is
serialized into a message and handed across a lock-free cross-shard channel (the
"mesh") to the owning shard's own event loop, which executes it locally and publishes the
result back. This is the same architectural family as Seastar/ScyllaDB and modern
DPDK-style shared-nothing servers, and the reason it scales: no core ever waits on a lock
held by another core, so there is nothing for contention to build up on as core count
grows.

The number of shards defaults to `min(available_cores, 8)` and is overridable with the
`threads` (alias `io-threads`) configuration directive / CLI flag; it need not equal the
physical core count, but pinning (`core_affinity`, disabled with `--no-pin`) assumes a
roughly 1:1 mapping for the latency guarantees below to hold.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model

Every key maps to exactly one shard, deterministically, via one of two routing schemes
depending on operating mode (see §3.1):

- **Standalone mode (the default)** — a 64-bit `FxHash` of the key's hash tag, reduced
  modulo `num_shards`. There is no fixed slot space; the mapping is a direct function of
  `num_shards`.
- **Redis Cluster mode** (`cluster-enabled yes`, or once the cluster bus reports an
  active cluster) — the classical Redis Cluster scheme: CRC16/XMODEM of the hash tag,
  reduced modulo 16,384 to produce a **slot**, then a contiguous slot range assigned to
  each shard (`slot_to_shard`). This mode exists specifically so that `CLUSTER
  SLOTS`/`CLUSTER KEYSLOT` and multi-node Redis Cluster clients see the exact slot
  semantics they expect.

This is an important distinction from a "sharded key-value store with a slot table":
non-cluster Rudis deployments never allocate or consult a 16,384-entry slot table for
ordinary routing — the modulo-hash scheme is cheaper and needs no such table. The slot
table exists specifically to support Redis Cluster compatibility and is only consulted
when cluster mode is active for that node.

Cross-shard communication is a fully-connected mesh of **lock-free single-producer
single-consumer ring buffers**, one dedicated ring per `(producer shard, consumer shard)`
pair, backed by a small `flume` channel used purely as a sleep/wake notification signal
(not as the message transport itself — the message itself always travels through the
ring). A shard blocked on "nothing to do" parks on that notification channel instead of
busy-polling every peer's ring forever.

### 2.2 Design Rationale (The "Why")

**Why per-shard rings instead of one shared MPMC queue?** A single multi-producer queue
that every shard could push into would need its own synchronization (a lock or a
lock-free MPMC algorithm with contended CAS loops) — reintroducing exactly the kind of
cross-core contention thread-per-core is meant to eliminate. A dedicated
single-producer/single-consumer ring per ordered pair of shards needs no synchronization
beyond a release/acquire pair of atomic cursors, because exactly one thread ever writes
the head and exactly one thread ever writes the tail.

**Why not just await a channel receive for every remote call?** Two reasons. First,
parking and waking a task has real latency (a context switch through the runtime's
waker), and the overwhelming majority of cross-shard remote calls in a well-sharded
workload complete in well under a microsecond once the message lands in the peer's ring —
faster than the cost of a full async suspend/resume. Several hot call sites therefore
spin briefly on a plain atomic/`Arc` completion flag before ever touching the channel,
falling back to a real `.await` only if the peer hasn't answered within that window (see
`docs/internal/04_sharding_mesh.md` §4 for the exact iteration counts). Second, a fresh
`flume::bounded(1)` channel allocation per call is real allocator traffic on a path that
can be exercised millions of times per second; the hottest call sites (`GET`, `SET`,
pipelined batches, `MGET`, `MSET`) instead reuse a small pool of pre-allocated
notification channels and shared-memory reply descriptors that outlive any single
request (see the internal doc's §3 for the concrete pool types).

**Why can a "channel" reply also be a raw shared-memory slot?** Because every shard is a
single thread and Rust's `Send`/`Sync` model only prevents *unsynchronized* shared
mutable access — it does not forbid shared memory outright. A descriptor allocated on the
heap (`Arc<...>`) and handed to exactly one other shard, where the fields it writes are
disjoint (its own reply slot, its own bit in a bitmask/counter) from what the sender
reads, only needs a single release-store/acquire-load pair to publish "done" safely. This
is what `src/mailbox.rs` provides: purpose-built, cache-line-padded, single-writer
completion cells (`FastGetDescriptor`, `FastSetDescriptor`, `BatchResponder`) and
scatter-gather descriptors (`ScatterMgetDescriptor`, `ScatterMsetDescriptor`) where each
remote shard writes only the array slot(s) it owns. This removes the heap allocation *and*
the flume-internal locking overhead that a full MPMC channel would otherwise impose on
every single cross-shard reply — the `flume` channels that remain in the mesh are used
only as a cheap wake-up signal, never as the actual payload carrier, for these hot paths.

**Why not lock the remote shard's table directly instead of message-passing?** That would
require every shard's `ShardDb` to be wrapped in a `Mutex`/`RwLock`, which reintroduces
exactly the cache-line-bouncing and contention cost thread-per-core exists to avoid —
every cross-shard access would force the cache line(s) backing the lock and the touched
data to migrate between cores. Message-passing keeps each `ShardDb` genuinely
single-owner; only the (small, fixed-size) message and its reply cross cores.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)

1. **Deterministic key ownership.** `Router::target_shard(key)` (and the free functions
   `target_shard`/`target_shard_and_hash` in `src/router.rs`) are pure functions of
   `(key, num_shards, cluster mode)`. Given the same inputs, every shard computes the
   same target for the same key — this is what makes routing decisions correct without
   any shard having to ask another "who owns this key?" for the common case.
2. **Lock-free mesh, no shared mutable state without a happens-before edge.** Every
   cross-shard interaction is either (a) a message pushed into a dedicated SPSC ring and
   drained by exactly one consumer thread, or (b) a write to a disjoint slot of a shared
   descriptor followed by a release-store the consumer acquire-loads before reading. There
   is no `Mutex`, no `RwLock`, and no busy-wait on shared *mutable* state without such a
   synchronization edge anywhere in the mesh.
3. **Hash-tag compatibility.** `extract_hash_tag(key)`: only the substring between the
   first `{` and the following non-empty `}` is hashed (both for the standalone `FxHash`
   scheme and the cluster CRC16 scheme). This lets an application co-locate related keys
   — e.g. `{user:100}:profile` and `{user:100}:orders` always land on the same shard —
   which is required for multi-key operations and transactions spanning those keys to be
   shard-local.
4. **`Router` is `Clone`, not a singleton reference.** Cloning a `Router` only bumps
   `Rc`/`Arc` reference counts on its fields; it does not clone the underlying `ShardDb`
   or spin up new channels. This is necessary because async tasks spawned off a `Router`
   method (e.g. a background RDB save, or a cross-shard batch's remote-execution future)
   need an owned handle to move into `monoio::spawn`, and because every connection on a
   shard holds its own `Router` clone.
5. **A shard never blocks another shard's event loop.** Because there is no shared lock,
   a slow or stuck remote shard can only ever delay the *caller* waiting on its reply
   (bounded by the caller's own await), never prevent other shards from making progress on
   unrelated keys.

---

## 3. High-Level Architecture & Workflow Diagrams

### 3.1 Key Routing

```
                     key, num_shards, cluster-mode flag
                                     │
                     ┌───────────────┴────────────────┐
                     ▼                                 ▼
          Cluster mode active                 Standalone (default)
   CRC16/XMODEM(hash_tag) % 16384 = slot     FxHash64(hash_tag) % num_shards
   slot_to_shard(slot, num_shards)                       │
   (contiguous slot range → shard)                       │
                     └───────────────┬────────────────┘
                                     ▼
                             target shard index
```

Both schemes only ever hash the *hash tag* (the text between `{` and `}` if present,
otherwise the whole key), so multi-key operations whose keys share a hash tag are
guaranteed to be shard-local regardless of which routing mode is active.

### 3.2 Local vs. Remote Execution

```
                        Client Request (any connection, any shard)
                                     │
                                     ▼
                       target_shard(key) computed locally
                                     │
               ┌─────────────────────┴─────────────────────┐
               ▼                                            ▼
       target == this shard                         target == peer shard
               │                                            │
   Execute synchronously against                Serialize a ShardMessage,
   this shard's own ShardDb                      push onto the dedicated
   (RudisTable + AOF append,                     SPSC ring to that shard,
   no lock, no channel)                          signal its wake channel
                                                             │
                                                             ▼
                                              Peer shard's event loop pops
                                              the message from its incoming
                                              ring, executes it against its
                                              own ShardDb, and publishes the
                                              result through the message's
                                              own reply channel/descriptor
                                              (flume one-shot, or a shared
                                              mailbox.rs descriptor)
```

### 3.3 The Mesh Fabric

```
        shard 0                shard 1                shard 2
   ┌───────────────┐      ┌───────────────┐      ┌───────────────┐
   │  ShardDb (Rc)  │      │  ShardDb (Rc)  │      │  ShardDb (Rc)  │
   │  event loop    │      │  event loop    │      │  event loop    │
   └───────┬────────┘      └───────┬────────┘      └───────┬────────┘
           │  SPSC ring 0→1                │  SPSC ring 1→2         │
           ├──────────────────────────────►│──────────────────────►│
           │◄──────────────────────────────┤◄───────────────────────┤
           │        ... one ring per ordered (producer, consumer) pair ...
```

`create_shard_mesh(num_shards)` builds this once at startup: an `num_shards ×
num_shards` matrix of bounded SPSC rings (capacity 256, with a mutex-guarded overflow
queue as a durability fallback so a burst never drops a message), plus one
notification channel per consumer shard shared by all of that shard's producers.

---

## 4. Cross-Shard Command Families & Their Design Trade-offs

### 4.1 Single-key operations (`GET`/`SET`/`DEL`/`EXISTS`/`EXPIRE`/…)

Each has a dedicated `Router` method with the same shape: compute the target shard; if
local, execute synchronously against `local_db` (writes additionally append to the AOF
inline); if remote, dispatch a `ShardMessage` and await the reply. The two highest-volume
operations, `GET` and `SET`, use a dedicated shared-memory single-slot descriptor
(`FastGetDescriptor`/`FastSetDescriptor`) drawn from a per-shard pool of reusable
notification channels, with a short busy-spin before falling back to an async await — this
is the fast path optimized specifically because `GET`/`SET` dominate real traffic.
Lower-traffic single-key operations use a plain one-shot `flume::bounded(1)` channel
allocated per call; this is a deliberate simplicity/throughput trade — see the internal
doc for which operations fall into which category today.

### 4.2 Multi-key fan-out (`MGET`/`MSET`/multi-key `DEL`)

A naive implementation would loop over the keys and issue one remote call per key,
serializing round-trips. Rudis instead **partitions the key set by target shard once**,
dispatches a single batched message per shard that owns at least one key, executes this
shard's own local keys concurrently with those in-flight remote batches, and then
harvests all remote replies. For `MGET`/`MSET` specifically this harvest uses a
shared-memory scatter-gather descriptor (`ScatterMgetDescriptor`/`ScatterMsetDescriptor`)
so each remote shard writes its results directly into its own slice of a shared results
array rather than serializing them back over a channel — turning what would otherwise be
`O(shards)` deserialization work into direct writes. End-to-end latency for a multi-shard
`MGET`/`MSET` is therefore bounded by the *slowest* touched shard's response time, not by
the sum of every key's individual round-trip.

### 4.3 Pipelined batches (command squashing)

When a client pipelines multiple commands in one write, and every command in the pipeline
is eligible (no ACL restrictions, no cluster cross-slot issues, no commands that require
strict per-command ordering against other side effects), the connection-handling layer
(Component 02) partitions the *entire pipeline* by target shard up front, executes local
commands inline, and sends one `ShardMessage::Batch` per remote shard carrying every
command destined for it. The reply path uses `mailbox::BatchResponder` — a single-slot,
`AtomicBool`-gated completion cell reused for the lifetime of the connection — so a
pipeline touching several remote shards pays for at most one message and one reply slot
per shard touched, no matter how many individual commands are batched together. See
`docs/design/02_connection_lifecycle.md` for the full pipeline-squashing design; this
document covers only the mesh-facing half of that mechanism.

### 4.4 Cross-shard transactions (`MULTI`/`EXEC`)

Atomicity across shards is provided by a simple advisory mutual-exclusion lock per shard
(one optional `tx_id` per shard, with a FIFO wait queue for contending transactions),
acquired in ascending shard-ID order across every shard a transaction's keys touch, and
released in reverse order once the transaction completes. This is intentionally a coarse
per-shard mutex analog, not a multi-version concurrency control scheme — Rudis trades
some concurrency (a transaction touching shard N blocks *any other* transaction touching
shard N, even on disjoint keys) for a scheme that is simple to reason about and cannot
deadlock as long as every transaction acquires shard locks in the same global order.

### 4.5 Live slot migration and cluster redirection

Rudis tracks, per slot, whether it is `Stable`, `Migrating` (to another node),
`Importing` (from another node), or `Moved`. This state gates `-MOVED`/`-ASK` redirection
on the single-command execution path so clients following the Redis Cluster protocol are
redirected correctly during a resharding operation. This mechanism is **only meaningful
when cluster mode is active** — see the internal doc for exactly which code paths
consult it today, and for a known gap where the pipelined multi-key fan-out described in
§4.2 does not yet consult slot-migration state.

---

## 5. Performance Characteristics

- **Local execution has no cross-core cost at all.** A key routed to the shard already
  handling the connection touches no lock, no atomic RMW beyond the ones the storage
  engine itself needs, and no channel.
- **Remote execution cost is dominated by scheduling latency, not the mesh itself.** The
  SPSC ring push/pop is a handful of atomic operations; the dominant cost of a remote
  call is whether the calling task has to actually suspend and be rescheduled, which is
  why the hottest paths spin briefly before doing so.
- **Batching amortizes fixed per-message overhead.** Both pipeline squashing (§4.3) and
  multi-key fan-out (§4.2) exist specifically to turn "N round-trips" into "one message
  per shard touched, regardless of N," which is the dominant lever for cross-shard
  throughput on workloads that don't perfectly hash-tag every related key onto one shard.
- This document intentionally does not restate specific throughput/latency numbers;
  see `docs/benchmarks/` for measured results, which should be treated as the source of
  truth over any number that might otherwise be written here.

---

## 6. Implementation References & Contributor Guide

For concrete struct/enum definitions, exact field names, function-by-function algorithm
walkthroughs, and known gaps/technical debt with file/line references:
* [**`docs/internal/04_sharding_mesh.md`**](../internal/04_sharding_mesh.md): Low-level
  implementation and code reference.
* **Source Files**: `src/router.rs` (routing + per-operation dispatch), `src/shard.rs`
  (`ShardDb`, `ShardMessage`, `CompactResp`, `SlotState`), `src/mailbox.rs` (the SPSC
  ring, shared-memory descriptors, and mesh construction).
