# Rudis Architecture & Threading Model

Rudis is a shared-nothing, thread-per-core in-memory and NVMe-tiered datastore. It scales
vertically on a single node by running one independent shard per pinned CPU core rather than
sharing a global lock or hash table across threads. For benchmark numbers backing any
performance claim in this document, see
[`docs/benchmarks/comprehensive_performance_guide.md`](benchmarks/comprehensive_performance_guide.md)
and [`docs/benchmark_multicore_results.md`](benchmark_multicore_results.md) — this document
itself makes no unverified throughput or latency claims.

---

## 1. Threading Model

Rudis employs a **Thread-Per-Core (Shared-Nothing)** multi-reactor architecture built natively on Linux `io_uring` via the `monoio` asynchronous runtime.

```
                     Client Requests (RESP2 / RESP3 replies / Memcached text)
                                          │
           ┌──────────────────────────────┴──────────────────────────────┐
           │                Linux Kernel Networking Layer                │
           │   • SO_REUSEPORT Connection Balancing (Zero Userspace Hop)  │
           │   • Standard TCP sockets, driven via io_uring (Monoio)      │
           └──────────────┬──────────────────────────────┬───────────────┘
                          │                              │
         ┌────────────────▼─────────────┐      ┌─────────▼────────────────────┐
         │       Core 0 (Shard 0)       │      │     Core N-1 (Shard N-1)     │
         │  ┌────────────────────────┐  │      │  ┌────────────────────────┐  │
         │  │ Monoio Runtime         │  │      │  │ Monoio Runtime         │  │
         │  │ (Independent io_uring) │  │      │  │ (Independent io_uring) │  │
         │  └───────────┬────────────┘  │      │  └───────────┬────────────┘  │
         │              ▼               │      │              ▼               │
         │  ┌────────────────────────┐  │      │  ┌────────────────────────┐  │
         │  │ Connection & Protocol  │  │      │  │ Connection & Protocol  │  │
         │  │ • Zero-Copy RESP Parser│  │      │  │ • Zero-Copy RESP Parser│  │
         │  │ • Memcached Gateway    │  │      │  │ • Memcached Gateway    │  │
         │  └───────────┬────────────┘  │      │  └───────────┬────────────┘  │
         │              ▼               │      │              ▼               │
         │  ┌────────────────────────┐  │      │  ┌────────────────────────┐  │
         │  │ Local Thread Storage   │  │      │  │ Local Thread Storage   │  │
         │  │ • In-Memory RudisTable │  │      │  │ • In-Memory RudisTable │  │
         │  │ • RangeTree & Search   │  │      │  │ • RangeTree & Search   │  │
         │  │ • HNSW (SQ8/PQ Vectors)│  │      │  │ • HNSW (SQ8/PQ Vectors)│  │
         │  │ • RedisJSON / Streams  │  │      │  │ • RedisJSON / Streams  │  │
         │  └───────────┬────────────┘  │      │  └───────────┬────────────┘  │
         │              ▼               │      │              ▼               │
         │  ┌────────────────────────┐  │      │  ┌────────────────────────┐  │
         │  │ NVMe Tiering SmallBins │  │      │  │ NVMe Tiering SmallBins │  │
         │  │ • Direct I/O (O_DIRECT)│  │      │  │ • Direct I/O (O_DIRECT)│  │
         │  └────────────────────────┘  │      │  └────────────────────────┘  │
         └──────────────┬───────────────┘      └──────────────┬───────────────┘
                        │                                     │
           ┌────────────▼─────────────────────────────────────▼──────────┐
           │            Lock-Free Cross-Shard Mesh (src/mailbox.rs)      │
           │   • Per-shard-pair SPSC rings, `flume` used only as a       │
           │     sleep/wake signal (not the payload carrier)             │
           │   • Striped Shard Presence Bitmask (Redis 7 Sharded Pub/Sub)│
           │   • Parallel Pipeline Squashing (Batched Multi-Key Dispatch)│
           └────────────┬─────────────────────────────────────┬──────────┘
                        │                                     │
                        ▼                                     ▼
         ┌──────────────────────────────┐      ┌──────────────────────────────┐
         │ Replication (master side)    │      │ Fork-less RDB Snapshots      │
         │ • Single-stream PSYNC (rudis)│      │ • Sequential per-shard save  │
         │ • DFLY FLOW for DF clients   │      │ • Blocking file I/O, no      │
         │   only (rudis replicas never │      │   fork() and no io_uring     │
         │   use this path)             │      │   (see docs/rdbsave.md)      │
         └──────────────────────────────┘      └──────────────────────────────┘
```

> The diagram above intentionally omits AF_XDP kernel bypass and kernel TLS (kTLS): both exist
> in the codebase (`src/xdp.rs`, `src/zerocopy.rs`, `src/tls.rs`) but are not on this live
> request path today — see [`docs/design/10_kernel_bypass_xdp.md`](design/10_kernel_bypass_xdp.md)
> and [`docs/design/15_security_tls.md`](design/15_security_tls.md) for their current, honestly-scoped status.

### Key Concurrency Principles:
1. **Physical Core Pinning**: Every worker thread is pinned to an exclusive physical CPU core via `core_affinity` (disabled with `--no-pin`). The OS scheduler cannot migrate worker threads between cores, improving L1/L2 cache locality.
2. **Independent Event Loops**: Each core runs its own dedicated `monoio` event loop driving an isolated Linux `io_uring` instance. No global event loop or multiplexer exists.
3. **Ingress with `SO_REUSEPORT`**: Every shard thread creates its own TCP listener on the same port with `SO_REUSEPORT` enabled. The Linux kernel distributes incoming client connections across the worker threads using a 4-tuple hash (`src_ip, src_port, dst_ip, dst_port`) with zero userspace locking or proxy hops.
4. **Thread-Local Storage**: State is strictly thread-local (`ShardDb`). Local operations execute against a thread-local hash table with **zero mutexes and zero atomic operations on the read/write hot path**.
5. **Shard count**: defaults to `min(available_cores, 8)`, overridable via `--threads`/`threads` (`src/main.rs`); it is not required to equal the physical core count.
6. **Key routing is mode-dependent, not universally CRC16**: standalone mode (the default) hashes the key's hash-tag with `FxHash` modulo the shard count; only cluster mode (`cluster-enabled yes`) uses the classic Redis Cluster `CRC16(hash_tag) % 16384` slot scheme. See [`docs/design/04_sharding_mesh.md`](design/04_sharding_mesh.md) §2.1 for the full rationale.

---

## 2. Primary Data Structures

| Type | Location | Architectural Role |
| :--- | :--- | :--- |
| `ShardDb` | `src/shard.rs` | Encapsulates all thread-local state: `RudisTable`, secondary indices, AOF handle, and tiering cache. |
| `RudisTable` | `src/table.rs` | Custom cache-conscious hash table optimized for 64-byte CPU cache lines with SIMD probing. |
| `Router` | `src/router.rs` | Coordinates key-to-shard routing (`FxHash % num_shards` standalone, `CRC16 % 16384` slot cluster mode), cross-shard mailbox dispatch, and pipeline squashing. |
| `ShardSender` / `ShardReceiver` | `src/mailbox.rs` | Lock-free bounded SPSC ring (capacity 256) per shard pair, with a `flume` channel used only as a sleep/wake signal and a `Mutex`-guarded overflow queue for bursts. |
| `Connection` | `src/connection.rs` | Manages per-connection I/O, pipelining, RESP2/RESP3 framing, and Memcached protocol auto-detection. |
| `RangeTree` | `src/search.rs` | Balanced B-tree numeric indexing structure providing $O(\log N + K)$ search and $O(1)$ term deletion. |
| `SmallBinsManager` | `src/tiering.rs` | Coalesces sub-4KB cold values into aligned 4KB disk blocks for NVMe direct I/O (`O_DIRECT`). |

---

## 3. Memory Layout & Cache Optimization

High-throughput thread-per-core architectures are frequently memory-bandwidth bound. Rudis enforces compact data structure layouts to maximize CPU L1/L2/L3 cache line utilization:

```
                  RudisEntry (88 Bytes Total, 8-byte aligned)
  ┌─────────────────────────┬─────────────────────────┬─────────────────────────┐
  │      key: Bytes         │   val: RudisValue       │  expire_at: Option<Inst>│
  │       (32 Bytes)        │       (40 Bytes)        │       (16 Bytes)        │
  └─────────────────────────┴─────────────────────────┴─────────────────────────┘
```

(Sizes measured against the current build; see
[`docs/design/rudis_table.md`](design/rudis_table.md) for the `size_of::<RudisEntry>()`/
`size_of::<RudisValue>()` verification and field-by-field breakdown.)

1. **Boxed Large Variants in `RudisValue` (40 Bytes)**:
   - In Rust, an enum's size is determined by its largest variant. Collections like `RudisHashMap`, `RudisSet`, `RudisZSet`, and `RudisStream` are boxed (`Box<T>`).
   - This shrinks `RudisValue` from 88 bytes down to **40 bytes** (-54.5%), ensuring string and scalar values incur minimal memory overhead.
2. **`RudisEntry` Cache Line Density (88 Bytes)**:
   - Shrunk from 136 bytes to **88 bytes** (-35.3%), significantly improving CPU cache locality during hash probe traversals.
3. **Sparse Cluster Slot States**:
   - Instead of allocating a dense 16,384-element vector per shard, `Router` uses a sparse `HashMap<u16, SlotState>` (`src/router.rs`), which only pays for slots actually assigned locally.
4. **SPSC Queue Footprint**:
   - Each inter-shard mailbox uses a compact 256-slot ring buffer with a `Mutex`-guarded overflow queue for bursts beyond that capacity, keeping the `num_shards × num_shards` cross-shard channel matrix memory bounded.
5. **THP Disablement**:
   - Boot-time `libc::prctl(PR_SET_THP_DISABLE, 1)` prevents the Linux kernel from promoting 4KB allocations to 2MB Transparent Huge Pages, eliminating COW page duplication penalties.

---

## 4. Life of a Command Request

When a client sends a command to Rudis, it is processed through one of two execution paths depending on key ownership:

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant Kernel as Linux Kernel (SO_REUSEPORT)
    participant Core0 as Core 0 (Ingress / Shard 0)
    participant Core1 as Core 1 (Target / Shard 1)

    Client->>Kernel: TCP SYN / ACK
    Kernel-->>Core0: Distribute to Core 0 Listener
    Client->>Core0: SET key:1 "hello"
    Note over Core0: Route key:1 to a shard (FxHash % num_shards<br/>in standalone mode; CRC16 % 16384 slot in cluster mode)<br/>Resolves to Shard 0 (Local)
    Core0->>Core0: Execute inline in local RudisTable
    Core0-->>Client: +OK (Zero Channel Hops)

    Client->>Core0: SET key:2 "world"
    Note over Core0: Route key:2 -> resolves to Shard 1 (Remote)
    Core0->>Core1: Push to Lock-Free SPSC Ring; flume channel signals wake
    Core1->>Core1: Execute in Shard 1 RudisTable
    Core1-->>Core0: Return Response via Return Channel
    Core0-->>Client: +OK (1 Lock-Free Hop)
```

### Local Fast Path (Zero Channel Hops):
1. Client connection on Core 0 reads incoming RESP payload from socket into registered buffer.
2. In-place zero-copy RESP parser extracts command arguments without heap allocation.
3. The key is routed to a shard using the mode-dependent scheme in §1.6 above.
4. If the target is Shard 0, the command executes **immediately and inline** against the local `ShardDb`.
5. Response frame is written directly into the connection's TCP write buffer.

### Remote Path (Lock-Free Cross-Shard Dispatch):
1. If the key belongs to Shard 1, Core 0 encapsulates the command in a `ShardMessage` envelope.
2. Core 0 pushes the message into the bounded lock-free SPSC ring leading to Shard 1 (`src/mailbox.rs`); if the ring is full, the message spills into that ring's `Mutex`-guarded overflow queue.
3. If Shard 1 is idle, a `flume` channel dedicated to that shard pair is used purely as a sleep/wake signal to resume its reactor — Rudis does not use `eventfd` for this; the message payload itself always travels through the SPSC ring, never through the `flume` channel.
4. Shard 1 dequeues the message, executes against its local `ShardDb`, and returns the result. The hottest call sites (`GET`/`SET`/batched multi-key commands) return the result through a pooled reply descriptor (`src/mailbox.rs`) rather than an ad hoc channel; other call sites still use a per-call `flume::bounded(1)` reply channel today.
5. Core 0 flushes the response to the client socket.

---

## 5. Parallel Cross-Shard Pipeline Squashing

When a client sends pipelined requests or multi-key commands (`MGET`, `MSET`, `DEL`), naive implementations dispatch requests sequentially or one key at a time, incurring $O(K)$ channel round-trips.

Rudis implements **Parallel Pipeline Squashing**:

```
                       Client Pipelined Batch: [GET k1, SET k2, GET k3, GET k4]
                                                 │
                               ┌─────────────────┴─────────────────┐
                               ▼                                   ▼
                     Shard 0 (Local Keys)                Shard 1 (Remote Keys)
                     • GET k1                            • SET k2
                     • GET k3                            • GET k4
                               │                                   │
                     Execute Inline (0 hops)             Single Batched SPSC Hop
                               │                                   │
                               └─────────────────┬─────────────────┘
                                                 ▼
                               Merged Pipeline Response Stream to Client
```

1. **Batch Partitioning**: All commands in a pipeline are parsed in a single sweep and bucketed by destination shard.
2. **Local Inline Processing**: All commands targeted at the local shard execute immediately with zero hops.
3. **One Hop Per Target Core**: For every remote shard involved, commands are bundled into a single `ShardMessage::Batch` hop.
4. **Scatter-Gather Parallelism**: Destination shards process their sub-batches in parallel. Core 0 collects responses and streams them in the original client sequence.
