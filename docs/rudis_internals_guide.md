# Rudis Internals: A Contributor's Tour

This document is a narrative, example-driven walkthrough of Rudis's architecture for
contributors who want one self-contained read before diving into the per-subsystem
documentation. **It intentionally stays at a conceptual level and defers exact struct layouts,
algorithms, and source line references to the linked subsystem docs in
[`docs/design/`](design/) and [`docs/internal/`](internal/), which have been verified against
the current source tree.** Where this document does state a concrete fact (a type name, a
threshold, a file path), that fact has been checked against `src/` directly. Earlier revisions
of this guide contained illustrative pseudocode presented as if it were real source and several
subsystem descriptions (compact string/set/list encodings, a "virtual lock-free sharding"
transaction model, kernel-bypass claims) that did not match the shipped implementation; those
have been corrected or removed here rather than carried forward.

---

## Table of Contents

1. [Architectural Philosophy: Shared-Nothing & Thread-per-Core](#1-architectural-philosophy-shared-nothing--thread-per-core)
2. [Request Lifecycle & the RESP Engine](#2-request-lifecycle--the-resp-engine)
3. [Memory Engine: `RudisTable` and `RudisValue`](#3-memory-engine-rudistable-and-rudisvalue)
4. [Blocking Operations (`BlockHub`)](#4-blocking-operations-blockhub)
5. [Transactions (`MULTI`/`EXEC`)](#5-transactions-multiexec)
6. [NVMe Tiered Storage](#6-nvme-tiered-storage)
7. [Vector Search: HNSW, SQ8 & Product Quantization](#7-vector-search-hnsw-sq8--product-quantization)
8. [RediSearch & Hybrid Reciprocal Rank Fusion](#8-redisearch--hybrid-reciprocal-rank-fusion)
9. [Networking Beyond the Default Path: AF_XDP & Zero-Copy (Status: Experimental)](#9-networking-beyond-the-default-path-af_xdp--zero-copy-status-experimental)
10. [Cluster Topology, Gossip & Multi-Region CRDTs](#10-cluster-topology-gossip--multi-region-crdts)
11. [Contributor Quick Reference](#11-contributor-quick-reference)

---

## 1. Architectural Philosophy: Shared-Nothing & Thread-per-Core

### 1.1 Why Shared-Nothing?

Traditional multi-threaded in-memory databases rely on shared memory protected by fine-grained
mutexes or lock-free concurrent hash maps. This lets any worker thread touch any key, but hits
scalability limits on modern multi-core servers from cache-line bouncing on shared/atomic
state, lock contention on hot keys, and cross-NUMA memory latency.

Redis avoids these issues by staying single-threaded for its execution engine — simple and
contention-free, but only able to saturate one core.

**Rudis instead adopts a shared-nothing, thread-per-core model**, in the tradition of engines
like Seastar/ScyllaDB and Dragonfly: each shard is one OS thread pinned (where possible) to one
CPU core, running its own single-threaded `monoio` (`io_uring`-backed) event loop and owning
its own key space, search/vector/JSON/CRDT sub-stores, AOF writer, and tiering manager
exclusively. See [`docs/architecture.md`](architecture.md) for the full diagram and
[`docs/design/04_sharding_mesh.md`](design/04_sharding_mesh.md) for the detailed rationale.

### 1.2 Core Invariants — and Their Documented Exceptions

1. **No lock on the per-key hot path.** Every shard's key-value table belongs exclusively to
   one thread; there is no `Mutex`/`RwLock`/contended atomic on ordinary `GET`/`SET`-style
   operations.
2. **Core affinity pinning** via `core_affinity::set_for_current`, disabled with `--no-pin`.
3. **`SO_REUSEPORT` kernel load balancing** — every shard binds its own listener on the same
   port; the kernel distributes new connections by 4-tuple hash, with no userspace dispatcher.
4. **Cross-shard communication is lock-free on the common path**, through per-shard-pair SPSC
   rings in `src/mailbox.rs` — **not** through `flume` channels carrying the message payload;
   `flume` is used only as a sleep/wake signal on that path. This does have real, deliberate,
   narrow exceptions: the mailbox's own burst-overflow queue is `Mutex`-guarded, and control-plane
   state that must be visible identically to every shard (`BlockHub`, the cluster-topology
   registry, the global search-index registry) is shared, lock-protected state by design — not
   an oversight. See [`docs/design/04_sharding_mesh.md`](design/04_sharding_mesh.md) §2.3 for
   the precise, current list of exceptions.

---

## 2. Request Lifecycle & the RESP Engine

Rudis runs on `monoio`, reading from and writing to client sockets via `io_uring`
submission/completion queues. A connection's async loop reads available bytes, repeatedly
parses complete RESP commands out of the buffered bytes (`parse_command`, `src/resp.rs`),
dispatches each one for execution, and flushes accumulated output back to the socket. For the
exact loop structure, buffer management, and pipeline-squashing logic, see
[`docs/design/02_connection_lifecycle.md`](design/02_connection_lifecycle.md) and
[`docs/internal/02_connection_lifecycle.md`](internal/02_connection_lifecycle.md) — this guide
does not reproduce that code here, to avoid drifting out of sync with it again.

### 2.1 Zero-Copy RESP2 & RESP3

Rudis parses RESP frames using `bytes::Bytes` (atomically reference-counted byte slices) sliced
directly out of the connection's read buffer, avoiding a heap copy per argument. RESP3 support
is **output-side only** today: a client sends commands as ordinary RESP2 bulk-string arrays
regardless of the negotiated protocol version (there is no RESP3-typed *input* parsing), but a
connection that has sent `HELLO 3` receives RESP3-typed replies (maps, doubles, booleans, etc.)
where applicable. See [`docs/design/03_resp_engine.md`](design/03_resp_engine.md) §5 for this
and other documented parser gaps (for example: no `proto-max-bulk-len`-style cap is enforced on
declared bulk-string/array length).

### 2.2 Key Routing

Routing is **mode-dependent**, not a single universal scheme:
- **Standalone mode (the default)**: a key's hash-tag is hashed with `FxHash` and reduced
  modulo the shard count. There is no 16,384-slot table consulted in this mode.
- **Redis Cluster mode** (`cluster-enabled yes`): the classic Redis Cluster scheme,
  `CRC16(hash_tag) % 16384`, mapped onto a shard-owned slot range, for wire compatibility with
  Redis Cluster client libraries and tooling.

See [`docs/design/04_sharding_mesh.md`](design/04_sharding_mesh.md) §2.1 for why both modes
exist and how hash-tags (`{...}`) are extracted.

---

## 3. Memory Engine: `RudisTable` and `RudisValue`

### 3.1 `RudisTable` (`src/table.rs`)

`RudisTable` is the per-shard associative store. Like Redis's `dict`, it maps keys to values;
unlike Redis (which keeps a separate `expires` dictionary), Rudis inlines the expiration
timestamp directly into each entry (`RudisEntry { key: Bytes, val: RudisValue, expire_at:
Option<Instant> }`, measured at 88 bytes, 8-byte aligned), so a read that must also check
expiry touches one cache line instead of probing two separate tables. See
[`docs/design/05_storage_engine.md`](design/05_storage_engine.md) and
[`docs/design/rudis_table.md`](design/rudis_table.md) for the verified byte-level layout,
probing strategy, and resize behavior.

### 3.2 `RudisValue` — the Real Variant List

The current `RudisValue` enum (`src/table.rs`) is:

```rust
pub enum RudisValue {
    String(Bytes),
    Int(i64),
    SmallHash(Vec<(Bytes, Bytes)>),
    Hash(Box<RudisHashMap>),           // RudisHashMap = HashMap<Bytes, Bytes, FxBuildHasher>
    List(std::collections::VecDeque<Bytes>),
    Set(Box<RudisSet>),                // Small(Vec<SmallSetEntry>) or Full(HashSet<Bytes>)
    ZSet(Box<RudisZSet>),              // Small(Vec<...>) or Full { dict: HashMap<Bytes,f64>, tree: BTreeSet<...> }
    HyperLogLog(Box<[u8; 16384]>),
    Stream(Box<RudisStream>),
    Tiered(TieredPointer),             // value offloaded to NVMe
    Cooled { ptr: TieredPointer, val: Box<RudisValue> },
}
```

Two corrections against older documentation and common assumptions worth calling out
explicitly:
- **JSON documents, Bloom/Cuckoo/Count-Min-Sketch/Top-K probabilistic structures, and bitmaps
  are not `RudisValue` variants.** JSON values, for example, live in a separate `JsonStore`
  field on the per-shard `ShardDb` (`src/shard.rs`), not inline in the main key-value enum.
  Bitmaps are implemented as ordinary `String` values manipulated with bit-level commands, the
  same as Redis. Do not assume every Redis/RedisJSON/RedisBloom data type has a corresponding
  `RudisValue` case — check the owning module (`src/json.rs`, `src/probabilistic.rs`) instead.
- **There is no Redis-style "listpack"/"quicklist"/"intset" binary compact-encoding format.**
  Small sets and small sorted sets use a plain `Vec`-based variant (promoted to a hash table /
  `BTreeSet`-backed structure past a fixed element-count threshold — 64 elements for sets, per
  `SMALL_SET_LIMIT` in `src/table.rs`); small hashes use `RudisValue::SmallHash(Vec<(Bytes,
  Bytes)>)` directly, promoted to `RudisValue::Hash` past a size threshold. Lists are always a
  plain `VecDeque<Bytes>` — there is no separate compact/expanded encoding for lists at all.
  This achieves a similar goal to Redis's listpacks (avoid hash-table overhead for small
  collections) with a simpler mechanism (a size-threshold promotion between two Rust
  collection types), not a bespoke packed binary format. See
  [`docs/design/05_storage_engine.md`](design/05_storage_engine.md) for the verified promotion
  thresholds and rationale for each type.

### 3.3 Sorted Sets: `HashMap` + `BTreeSet`, Not a Custom Skiplist

`RudisZSet::Full` pairs a `HashMap<Bytes, f64>` (O(1) member → score lookup, used by `ZSCORE`,
`ZINCRBY`) with a `BTreeSet<(OrderedScore, Bytes)>` for ordered traversal (`ZRANGE`,
`ZRANGEBYSCORE`). This is a standard Rust `BTreeSet`, not a hand-rolled skip list with
augmented per-level span counters — see
[`docs/design/05_storage_engine.md`](design/05_storage_engine.md) for how rank-style queries
(`ZRANK`) are actually implemented against this structure and their real complexity.

### 3.4 Score Formatting (`%.17g`)

Redis clients expect exact, C-`printf`-compatible float formatting for scores. Rust's `Display`
does not match this, so Rudis formats scores via `libc::snprintf` with `%.17g`
(`format_score`, `src/connection.rs`), special-casing NaN, ±infinity, and signed zero
(`-0.0` → `"0"`) to match Redis's exact reply strings.

---

## 4. Blocking Operations (`BlockHub`)

Commands like `BLPOP`/`BRPOP`/`BZPOPMIN`/`BZPOPMAX` must suspend a client until a key becomes
non-empty or a timeout expires — without ever blocking the shard's own reactor thread, since
that would stall every other client pinned to that core. Rudis solves this with a per-shard
`BlockHub` (`src/block.rs`): a waiting client registers an async waiter keyed by the key(s) it's
watching; a `PUSH`-style command on any shard notifies the relevant `BlockHub`(s), which wakes
the first matching waiter and guards against notifying the same client twice when it was
waiting on multiple keys that resolve concurrently. `BlockHub` is one of the documented,
deliberate exceptions to per-shard-only state (§1.2) because a waiter registered on one shard
must be reachable from a push executing on a different shard. Full data-structure and
notification-protocol detail: [`docs/design/06_blocking_hub.md`](design/06_blocking_hub.md).

---

## 5. Transactions (`MULTI`/`EXEC`)

`MULTI` enters a per-connection queuing state (subsequent commands reply `+QUEUED` instead of
executing); `DISCARD` clears it; `WATCH` registers optimistic-concurrency-control interest in a
set of keys, aborting the transaction at `EXEC` time if a watched key changed; `EXEC` executes
the queued commands. Transaction state lives in `src/connection.rs`. When a queued transaction
touches keys on more than one shard, execution must still produce a single atomic-looking
batched reply without ever taking a cross-shard lock — for the concrete dispatch and ordering
strategy Rudis uses to do this safely, see
[`docs/design/04_sharding_mesh.md`](design/04_sharding_mesh.md) and
[`docs/internal/02_connection_lifecycle.md`](internal/02_connection_lifecycle.md) rather than
relying on a specific named algorithm here — no such algorithm is named in the source.

---

## 6. NVMe Tiered Storage

Rudis includes an embedded NVMe tiering engine (`src/tiering.rs`) that keeps hot keys in DRAM
and offloads cold values to NVMe SSDs, expanding effective capacity beyond physical RAM. Values
move through a **Hot (DRAM) → Cooled → Cold (NVMe, `RudisValue::Tiered`)** lifecycle, and are
promoted back to Hot on access. Small values are packed into aligned 4 KB blocks
(`SmallBinsManager`) before a single `O_DIRECT` write, avoiding the write amplification of many
tiny writes; space reclamation uses `fallocate(FALLOC_FL_PUNCH_HOLE)`. A real, working
`ioctl(FICLONE)` reflink-cloning path exists for point-in-time snapshots of the NVMe tier's
*own* backing file (`TIER.SNAPSHOT`) — this is unrelated to `SAVE`/`BGSAVE`, which never calls
into this subsystem (see [`docs/rdbsave.md`](rdbsave.md)). Full detail, including a documented
gap where `RudisValue::Tiered` values currently serialize to zero bytes inside a `SAVE`/`BGSAVE`
RDB image: [`docs/design/07_nvme_tiering.md`](design/07_nvme_tiering.md) and
[`docs/design/tiered_storage.md`](design/tiered_storage.md).

---

## 7. Vector Search: HNSW, SQ8 & Product Quantization

Rudis's vector engine (`src/vector.rs`) implements Hierarchical Navigable Small World (HNSW)
graph indexing for approximate nearest-neighbor search, with two optional compression schemes:
**scalar quantization (SQ8)**, which projects each float dimension into an 8-bit integer for a
roughly 4x memory reduction at some recall cost, and **product quantization (PQ)**, which
clusters sub-vectors into a fixed codebook and represents each sub-vector as a single codebook
index, with **asymmetric distance computation (ADC)** turning query-time distance evaluation
into table lookups. For the exact graph construction/search parameters and verified recall/
compression figures, see [`docs/design/08_vector_engine.md`](design/08_vector_engine.md).

---

## 8. RediSearch & Hybrid Reciprocal Rank Fusion

Rudis implements an in-memory full-text index (`src/search.rs`, `FT.CREATE`/`FT.SEARCH`) with
tokenization, inverted posting lists, and Okapi BM25 relevance scoring, plus a `RangeTree`
numeric index for `@field:[min max]` range queries and an `FT.AGGREGATE` pipeline
(`GROUPBY`/`REDUCE`/`APPLY`/`SORTBY`). When a query combines full-text and vector similarity,
results are merged with **Reciprocal Rank Fusion (RRF)**, summing `1 / (60 + rank)` across each
ranked list per candidate document. Full detail, including the global search-index registry's
documented exception to the shared-nothing model: [`docs/design/09_redisearch.md`](design/09_redisearch.md).

---

## 9. Networking Beyond the Default Path: AF_XDP & Zero-Copy (Status: Experimental)

`src/xdp.rs` and `src/zerocopy.rs` are real, unit-tested code — but **neither is on Rudis's live
network path today**. This is worth stating plainly rather than leaving to inference:
- `src/xdp.rs` models AF_XDP (UMEM rings, packet classification) entirely as in-process Rust
  data structures. It is reachable only through explicit `XDP.*` admin commands; no code path
  makes a real `AF_XDP` socket syscall, loads an eBPF program, or binds to a NIC.
- `src/zerocopy.rs` implements correct `SO_ZEROCOPY`/registered-buffer primitives, but the
  real connection write path (`src/connection.rs`, on `monoio`) does not call into it.
- Kernel TLS (kTLS) is attempted best-effort after a real `rustls` handshake, but the promotion
  result is currently discarded (`is_ktls_active` is always `false`).

Treat this subsystem as a real foundation for a future kernel-bypass data path, not as active
acceleration today. Full rationale for why this direction is worth pursuing anyway, and exactly
what would be required to make it real: [`docs/design/10_kernel_bypass_xdp.md`](design/10_kernel_bypass_xdp.md)
and [`docs/design/15_security_tls.md`](design/15_security_tls.md).

---

## 10. Cluster Topology, Gossip & Multi-Region CRDTs

### 10.1 Redis Cluster Bus & Gossip (`src/cluster.rs`)

Rudis implements the standard Redis Cluster wire contract: 16,384 hash slots, `-MOVED`/`-ASK`
redirection, and `CLUSTER NODES`/`CLUSTER SLOTS` introspection. Nodes exchange **full**
node-table gossip payloads (not an incremental/sampled gossip) over a dedicated **cluster
bus** on `port + 10000` roughly every **500 ms** — this bus runs on plain, synchronous
`std::net::TcpStream`/`TcpListener` with short timeouts on dedicated OS threads, **not** on
`io_uring`/Monoio, and its wire format is a plain-text, `\r\n`-terminated line protocol, not a
binary framing. Failure detection and failover promotion both require a strict majority of
known masters to independently corroborate the suspicion/vote before acting, to avoid
split-brain. `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD` perform a real DUMP-and-replay key
migration between nodes; the separate `DFLYCLUSTER`/`DFLYMIGRATE` command family remains
state-bookkeeping only. Full detail: [`docs/design/11_cluster_topology.md`](design/11_cluster_topology.md).

### 10.2 Multi-Region CRDTs (`src/crdt.rs`) — Manual Sync, Not Automatic Replication

Rudis provides a small set of Conflict-Free Replicated Data Types — a Hybrid-Logical-Clock
(HLC) ordered LWW-Register, an Observed-Remove Set, and a PN-Counter — exposed through
`CRDT.*` commands, designed so concurrent updates from independent replicas converge to the
same value regardless of arrival order. **This is manual, not automatic, multi-region
replication**: Rudis does not run a background task that discovers peer regions and streams
CRDT updates to them. A caller must explicitly export a node's state (`CRDT.DUMP`), transport
the bytes to another instance by some external mechanism, and apply it there (`CRDT.MERGE`).
The merge functions guarantee this is safe to do at any time, in any order — but the transport
and scheduling of that exchange is entirely outside Rudis today. Full detail:
[`docs/design/12_crdt_types.md`](design/12_crdt_types.md).

---

## 11. Contributor Quick Reference

| Subsystem | Design Doc | Implementation Doc | Primary Source |
| :--- | :--- | :--- | :--- |
| **Reactor & Server Loop** | [01_reactor_runtime](design/01_reactor_runtime.md) | [01_reactor_runtime](internal/01_reactor_runtime.md) | `src/server.rs`, `src/main.rs` |
| **Connection & Execution** | [02_connection_lifecycle](design/02_connection_lifecycle.md) | [02_connection_lifecycle](internal/02_connection_lifecycle.md) | `src/connection.rs` |
| **RESP Engine** | [03_resp_engine](design/03_resp_engine.md) | [03_resp_engine](internal/03_resp_engine.md) | `src/resp.rs` |
| **Sharding & Router Mesh** | [04_sharding_mesh](design/04_sharding_mesh.md) | [04_sharding_mesh](internal/04_sharding_mesh.md) | `src/router.rs`, `src/shard.rs`, `src/mailbox.rs` |
| **Storage Engine & Encodings** | [05_storage_engine](design/05_storage_engine.md) | [05_storage_engine](internal/05_storage_engine.md) | `src/table.rs` |
| **Blocking Hub** | [06_blocking_hub](design/06_blocking_hub.md) | [06_blocking_hub](internal/06_blocking_hub.md) | `src/block.rs` |
| **NVMe Tiered Storage** | [07_nvme_tiering](design/07_nvme_tiering.md) | [07_nvme_tiering](internal/07_nvme_tiering.md) | `src/tiering.rs` |
| **Vector Search (HNSW)** | [08_vector_engine](design/08_vector_engine.md) | [08_vector_engine](internal/08_vector_engine.md) | `src/vector.rs` |
| **RediSearch & RRF** | [09_redisearch](design/09_redisearch.md) | [09_redisearch](internal/09_redisearch.md) | `src/search.rs` |
| **Kernel Bypass & Zero-Copy** | [10_kernel_bypass_xdp](design/10_kernel_bypass_xdp.md) | [10_kernel_bypass_xdp](internal/10_kernel_bypass_xdp.md) | `src/xdp.rs`, `src/zerocopy.rs` |
| **Cluster Bus & Gossip** | [11_cluster_topology](design/11_cluster_topology.md) | [11_cluster_topology](internal/11_cluster_topology.md) | `src/cluster.rs` |
| **CRDT Engine** | [12_crdt_types](design/12_crdt_types.md) | [12_crdt_types](internal/12_crdt_types.md) | `src/crdt.rs` |
| **Lua Scripting & Functions** | [13_scripting_functions](design/13_scripting_functions.md) | [13_scripting_functions](internal/13_scripting_functions.md) | `src/scripting.rs` |
| **Persistence & Replication** | [14_persistence_replication](design/14_persistence_replication.md) | [14_persistence_replication](internal/14_persistence_replication.md) | `src/replication.rs`, `src/aof.rs`, `src/router.rs` |
| **Security, Allocator & TLS** | [15_security_tls](design/15_security_tls.md) | [15_security_tls](internal/15_security_tls.md) | `src/acl.rs`, `src/allocator.rs`, `src/tls.rs` |
| **JSON Document Store** | [16_json_store](design/16_json_store.md) | [16_json_store](internal/16_json_store.md) | `src/json.rs` |
| **Geospatial** | [17_geospatial](design/17_geospatial.md) | [17_geospatial](internal/17_geospatial.md) | `src/geo.rs` |
| **Probabilistic Structures** | [18_probabilistic](design/18_probabilistic.md) | [18_probabilistic](internal/18_probabilistic.md) | `src/probabilistic.rs` |
| **Pub/Sub** | [19_pubsub](design/19_pubsub.md) | [19_pubsub](internal/19_pubsub.md) | `src/pubsub.rs` |

For RDB/AOF persistence specifically (what triggers a save, why it isn't `io_uring`-accelerated,
and the known `save N M` and tiered-value serialization gaps), see [`docs/rdbsave.md`](rdbsave.md)
directly rather than the table above. For the mandatory engineering invariants that apply when
modifying any of these subsystems, see [`agent.md`](../agent.md).

---
*This guide is a companion to, not a replacement for, the per-subsystem design and internal
docs — when the two disagree, the per-subsystem doc under `docs/design/`/`docs/internal/` is
the more rigorously verified source.*
