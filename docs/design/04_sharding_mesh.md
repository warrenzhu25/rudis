# Component 04: Sharding Architecture & Cross-Core Mesh (Design)

## Component 04: Sharding Architecture & Cross-Core Mesh

> **Source Files**: ``src/router.rs`, `src/shard.rs``


---

### 1. Architectural Purpose & Scope

`src/router.rs` and `src/shard.rs` implement Rudis's data partitioning and inter-thread
messaging system. `router.rs` defines the `Router` struct — the per-shard facade every
command goes through to decide "is this key mine, or do I need to hop to a peer core" —
plus CRC16-based key-to-slot-to-shard mapping. `shard.rs` defines the thread-local
`ShardDb` (the actual per-core state container: `RudisTable` plus every other per-shard
subsystem — tiering, vector search, CRDTs, JSON, probabilistic structures, sticky-key
pinning) and the `ShardMessage` enum that is the entire cross-core wire format.

Beyond routing, `Router` has grown into the coordination point for nearly every
multi-shard concern in the codebase: NVMe tiering orchestration, RDB snapshotting,
AOF fsync fan-out, pub/sub broadcast, cross-shard transaction locking, cluster
administration passthroughs, and replication-stream application. Each of these is
documented below because they all live in this file, even though several belong more to
persistence/replication/tiering conceptually.

---

---

### 2. Key Invariants & Concurrency Constraints

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

---

### 6. Performance Characteristics

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
