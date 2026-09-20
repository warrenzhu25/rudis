# Component 11: Redis Cluster Topology & Gossip Protocol (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/cluster.rs`  
> **Implementation Reference**: [`docs/internal/11_cluster_topology.md`](../internal/11_cluster_topology.md)  
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
Decentralized cluster topology with a dedicated cluster bus port (port + 10000). 16,384 hash slots with dynamic slot state machine, gossip failure detection, and live shard migration.

### 2.2 Design Rationale (The "Why")
Provides transparent horizontal scaling across multiple physical nodes with standard Redis cluster client compatibility (-MOVED and -ASK redirects).

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)
1. **One `ClusterHub` per port, shared via a global registry**: `CLUSTER_HUBS:
   LazyLock<RwLock<HashMap<u16, Arc<ClusterHub>>>>`. `get_cluster_hub(port)`
   lazily creates and caches one `Arc<ClusterHub>` per port — this is process-wide
   shared, mutex/rwlock-guarded state, not thread-local (a deliberate, narrow
   exception to the shared-nothing model, same category as `BlockHub` in
   Component 06).
2. **Plain-text wire protocol, not binary framing**: every cluster-bus message
   (`MEET`, `PING`, `FAIL`, `FAILOVER`, `FAILOVER_AUTH_REQUEST`,
   `FAILOVER_ANNOUNCE`) is a `\r\n`-terminated space-separated ASCII line, parsed
   with `split_whitespace()`. There is no binary struct, no magic-byte signature,
   no `#[repr(C, packed)]` header of any kind.
3. **Synchronous blocking I/O on dedicated OS threads, not `io_uring`/`monoio`**:
   the cluster bus listener runs on its own `std::thread`, using plain
   `std::net::TcpStream`/`TcpListener` with short (200-500ms) read/write
   timeouts — completely separate from the rest of Rudis's async, io_uring-based
   networking. Each inbound connection also gets its own `std::thread::spawn`.
4. **Quorum-based failure detection with distributed gossip corroboration**: a peer is
   locally marked `"fail?"` (PFAIL) after 5s of missed PONGs. That opinion is piggybacked
   in gossip payloads to peer nodes, which record it in `pfail_reports: HashMap<String, HashSet<String>>`.
   Escalation to confirmed `"fail"` requires corroborating PFAIL votes from a strict majority
   of masters (`total_votes >= quorum`), at which point a `FAIL <node_id>` broadcast is sent to
   notify all peers. If connectivity recovers, PFAIL opinions are retracted via gossip.
5. **Replica election *is* a real majority vote**: `start_election` does send
   `FAILOVER_AUTH_REQUEST` to every known master and only promotes itself after
   collecting `>= (total_masters / 2) + 1` `FAILOVER_AUTH_ACK` replies, gated by
   `last_vote_epoch` (one vote per epoch per master) — this part matches the
   Architectural Purpose's claim, unlike the failure-detection consensus.

---

## 3. High-Level Architecture & Workflow Diagram

```
Client ──► Node A (Slot 5000) ──► -MOVED 5000 Node-B:6379
                                                │
       Cluster Bus (Gossip PING/PONG) ──────────┘
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **Not zero-allocation, not io_uring-based**: every gossip tick and every
  `CLUSTER MEET`/`FAILOVER` opens a brand-new blocking `TcpStream` per peer
  (connect + write + read, each with its own 200-500ms timeout) on a plain OS
  thread — the opposite of the rest of Rudis's zero-copy/`monoio` design.
  Acceptable for a control-plane path that runs a few times a second, not
  something to model the data-path invariants on.
- **Full-state gossip, not incremental**: `cluster_bus_tick` re-sends the entire
  known node table to every peer on every 500ms tick — bandwidth is O(peers²)
  per tick, not the randomized-sample gossip real Redis Cluster uses. Fine at
  small cluster sizes (a handful of nodes), not validated or designed for
  hundreds of nodes despite what an earlier draft of this doc claimed about
  "100-node cluster convergence."
- **Failure detection is local and synchronous, not a distributed vote** (§2.4)
  — a node can mark a peer `"fail"` purely from its own missed-PONG timer, with
  no corroboration from other nodes, unlike the real replica-election step which
  does require a genuine majority.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/11_cluster_topology.md`**](../internal/11_cluster_topology.md): Low-level implementation and code reference.
* **Source Files**: `src/cluster.rs`
