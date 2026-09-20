# Component 04: Sharding Architecture & Cross-Core Mesh (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/router.rs, src/shard.rs`  
> **Implementation Reference**: [`docs/internal/04_sharding_mesh.md`](../internal/04_sharding_mesh.md)  
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
Keys are assigned to 16,384 cluster slots using CRC16: slot = crc16(key) % 16384. Every core knows the slot ownership table. Cross-shard communication uses lock-free bounded SPSC rings paired with eventfd wakers.

### 2.2 Design Rationale (The "Why")
Cross-core communication must not introduce lock contention or thread migration. Dedicated bounded SPSC rings guarantee lock-free message passing, and eventfd wakers notify sleeping reactors only when needed.

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **Deterministic Key Ownership (static formula)**: `target_shard(key, num_shards)` maps
   every key to exactly one shard via `crc16(hash_tag(key)) % 16384` → contiguous slot
   range → shard index. This is unchanged from the original design and is what nearly
   every command actually routes through (see §4.4 for the caveat).
2. **Lock-Free Asynchronous Mesh**: cross-shard communication is exclusively `flume`
   channels (unbounded senders held by every shard, one per-shard receiver drained in
   that shard's own event loop). No shared memory, no mutex, no `oneshot` crate — despite
   what an earlier, inaccurate draft of this document claimed.
3. **Hash Tag Compatibility**: `extract_hash_tag` — only the substring between the first
   `{` and the next non-empty `}` is hashed, so `{user:100}:profile` and
   `{user:100}:orders` land on the same shard.
4. **`Router` is `Clone`, not a singleton reference**: cloning just bumps `Rc`/`Arc`
   refcounts on its fields (see §3) — cheap, and necessary because async tasks spawned
   off a `Router` method (e.g. `bgsave`'s background save) need an owned copy to move into
   `monoio::spawn`.

---

## 3. High-Level Architecture & Workflow Diagram

```
Key Routing: slot = CRC16(key) % 16384
                               │
               ┌───────────────┴───────────────┐
               ▼                               ▼
       Local Shard (Inline)            Remote Shard (Mesh)
       Execute in ShardDb              Bounded SPSC Ring + eventfd
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **Lock-Free Communication**: unchanged — `flume` channels, no mutexes, no atomics on
  the per-key data path itself.
- **Per-op remote calls still allocate**: as documented in §4.2, the non-batched router
  methods allocate a fresh one-shot channel per remote call; only the pipelined
  `ShardMessage::Batch` path (Component 02) uses a pre-allocated pool.
- **`CompactResp`'s 30-byte inline buffer** removes a heap allocation from the
  overwhelmingly common case (short RESP replies) of the cross-shard batch response path.
- **`MGET`/`MSET` now parallelize across shards, with pooled channels and a busy-poll
  fast-harvest phase** (§4.3, resolved) — one `ShardMessage::Mget`/`Mset` per remote
  shard touched, dispatched together and harvested via non-blocking `try_recv` before
  falling back to a real `.await`, so throughput on multi-shard keysets is now bounded by
  the slowest remote shard's response, not by the sum of every key's round-trip.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/04_sharding_mesh.md`**](../internal/04_sharding_mesh.md): Low-level implementation and code reference.
* **Source Files**: `src/router.rs, src/shard.rs`
