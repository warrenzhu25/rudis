# Component 11: Redis Cluster Topology & Gossip Protocol (High-Level Design & Architecture Guide)

> **Subsystem Scope**: `src/cluster.rs`
> **Implementation Reference**: [`docs/internal/11_cluster_topology.md`](../internal/11_cluster_topology.md)
> **Consolidated Design Spec**: [`docs/design/components.md`](components.md)

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem

A single Rudis process already scales vertically across every core of one machine via the thread-per-core, shared-nothing architecture described in Component 01. That does not address horizontal scale: a workload whose data or throughput requirements exceed one machine's memory or network capacity needs to be partitioned across multiple physical or virtual hosts, while remaining transparently addressable by unmodified Redis Cluster clients (`redis-cli -c`, `redis-py` in cluster mode, Jedis Cluster, etc.), which expect the standard Redis Cluster wire contract: 16,384 hash slots, `-MOVED`/`-ASK` redirection, `CLUSTER NODES`/`CLUSTER SLOTS` topology introspection, and gossip-based failure detection.

### 1.2 The Rudis Solution

`src/cluster.rs` implements a decentralized, master-slave cluster topology modeled on Redis Cluster's design: each node owns a subset of the 16,384 hash slots, nodes exchange liveness and slot-ownership information over a dedicated **cluster bus** port (`data port + 10000`), and slot ownership changes are propagated by gossip rather than a central coordinator. On top of that control plane, Rudis layers a genuine key-migration data path (`CLUSTER SETSLOT`, `CLUSTER REBALANCE`, `CLUSTER RESHARD`) that serializes and replays keys between nodes, and a quorum-gated automatic failover mechanism for promoting a replica when its master is confirmed down by multiple peers.

Two details distinguish this from the rest of Rudis's networking stack and are worth stating up front, because they shape every design decision below:

- The cluster bus is **not** built on `io_uring`/Monoio. It runs on plain, synchronous `std::net::TcpStream`/`TcpListener` with short (200-500 ms) timeouts, on dedicated OS threads separate from the async reactor. This is a deliberate, narrow exception — control-plane traffic (a few messages per second per peer) does not need the zero-copy, zero-syscall-overhead data path that per-key operations do, and a simple blocking implementation is far easier to reason about for a subsystem whose correctness (failure detection, election) matters more than its latency.
- Cluster topology state (`ClusterHub`) is **process-wide shared state protected by `RwLock`/atomics**, not thread-local per-shard state. This is the same category of deliberate exception to the shared-nothing model as `BlockHub` (Component 06) and the global search-index registry (Component 09): cluster membership, slot ownership, and epoch/vote state must be visible identically to every shard thread handling client traffic, and there is exactly one `ClusterHub` per listening port, cached in a process-wide registry.

---

## 2. Contributor Mental Model & Architectural Principles

### 2.1 The Mental Model

Every node is symmetric: there is no dedicated "coordinator" process. A node learns about the rest of the cluster by being introduced via `CLUSTER MEET`, after which it and its peers exchange full node-table snapshots every 500 ms over the cluster bus. Each node tracks, for every peer it knows about, an identity, address, role (master/slave), the slot ranges it owns, a liveness flag (`connected`/`fail?`/`fail`), and a monotonically increasing *epoch* used to order configuration changes and elections. A client that lands on the wrong node for a key it addresses gets redirected via `-MOVED`/`-ASK`, exactly as with upstream Redis Cluster, and CRC16-based slot hashing (`key_slot`, `src/router.rs`) is bit-for-bit compatible with standard Redis Cluster clients, including hash-tag (`{...}`) support.

### 2.2 Design Rationale (The "Why")

**Why 16,384 slots and CRC16 hashing, not a different partitioning scheme?** Matching the exact slot count and hash function upstream Redis Cluster uses is what makes Rudis a drop-in replacement for existing Redis Cluster client libraries and operational tooling (`redis-cli --cluster`, cluster-aware client drivers) without modification. Slot count is a fixed protocol contract, not a tunable — deviating from it would break every off-the-shelf client.

**Why gossip instead of a central metadata store (e.g. etcd/ZooKeeper)?** A gossip-based design has no single point of failure for cluster membership and requires no additional operational dependency: any node that has been introduced to the cluster can independently discover the full membership by receiving gossip payloads transitively, without ever calling `CLUSTER MEET` on every other node directly. This trades convergence latency (topology changes propagate over several gossip rounds, not instantaneously) for operational simplicity and failure independence — appropriate for a control plane that changes rarely (node joins/leaves, slot migrations) compared to the data plane's request rate.

**Why full-state gossip (resend the entire node table every tick) rather than incremental, randomized-sample gossip as in production Redis Cluster?** This is a genuine, deliberate simplification, not an oversight: it makes convergence behavior trivial to reason about (every node learns the full state of every peer it can reach within one round-trip) at the cost of O(peers²) bandwidth per tick. This is acceptable for the cluster sizes Rudis currently targets (a handful of nodes) and is called out explicitly in §4 as a scalability boundary, not a hidden limitation.

**Why quorum-based failure detection instead of a single node's opinion?** A lone node's belief that a peer is unreachable is often a symptom of *that node's* network partition, not the peer's actual failure — unilaterally declaring another master `"fail"` on that basis risks a split-brain promotion (two masters serving the same slot range simultaneously, causing silent data loss on write conflicts). Rudis therefore requires corroboration: a node first marks a silent peer as tentatively failed (`"fail?"`/PFAIL) locally, then piggybacks that opinion on its own gossip payloads to other peers. Escalation to a confirmed `"fail"` state — the trigger for automatic failover — only happens once a *strict majority of known masters* have independently reported the same suspicion. This mirrors the failure-detection design of production Redis Cluster and is the single most important correctness property of this subsystem.

**Why does replica promotion require a real majority vote rather than self-promotion on PFAIL?** The same split-brain concern applies to failover as to failure detection: if a replica promoted itself purely on its own observation that its master was unreachable, a transient network partition could produce two masters for the same slot range. Requiring every other known master to independently verify (a) that it is itself a master, (b) that it has not already voted in this epoch, and (c) that it agrees the old master is `"fail"` before granting a vote — and requiring the candidate to collect votes from a strict majority of masters before promoting itself — makes an incorrect promotion require the same kind of multi-node agreement failure that a true network partition (not just one flaky link) would represent.

**Why is real, working slot migration a hard requirement rather than "future work"?** A cluster that supports adding/removing nodes but cannot move existing data between them is not useful for the operational scenarios (rebalancing, decommissioning a node) that motivate clustering in the first place. `CLUSTER SETSLOT`, `CLUSTER REBALANCE`, and `CLUSTER RESHARD` therefore perform an actual DUMP-and-replay of every key in a migrating slot to the target node — this is a genuine data-path operation, distinct from (and more fully built out than) the Dragonfly-compatible `DFLYMIGRATE`/`DFLYCLUSTER` command family, which remains state-bookkeeping only (see the internal doc for the precise scope of that gap).

### 2.3 Key Invariants & Concurrency Constraints (Non-Negotiable Rules)

1. **One `ClusterHub` per port, shared via a global registry.** `get_cluster_hub(port)` lazily creates and caches a single `Arc<ClusterHub>` keyed by listening port in a process-wide `LazyLock<RwLock<HashMap<u16, Arc<ClusterHub>>>>`. This is genuinely shared, lock-guarded state — a narrow, deliberate exception to the shared-nothing model, in the same category as `BlockHub` (Component 06).
2. **The cluster bus is a plain-text, line-oriented protocol**, not binary framing. Every message (`MEET`, `PING`, `FAIL`, `FAILOVER`, `FAILOVER_AUTH_REQUEST`, `FAILOVER_AUTH_ACK`, `FAILOVER_ANNOUNCE`) is a `\r\n`-terminated, space-separated ASCII line parsed with `split_whitespace()`. There is no magic-byte signature and no `#[repr(C)]` binary header anywhere in this path.
3. **Cluster bus I/O is synchronous and thread-per-connection, not `io_uring`/Monoio.** The bus listener runs on its own dedicated `std::thread` using blocking `TcpListener`/`TcpStream` with 200-500 ms read/write timeouts, completely outside Rudis's async reactor. Every inbound connection is handled on its own freshly spawned `std::thread`. This is orthogonal to, and does not participate in, the thread-per-core sharding of client traffic.
4. **Failure detection requires quorum corroboration before escalating to `"fail"`.** A peer transitions to local `"fail?"` after 5 seconds of missed PONGs. That suspicion is piggybacked into gossip payloads sent to other peers, who record it. Escalation to a broadcast `FAIL <node_id>` requires the reporting node to have observed corroborating PFAIL opinions from a strict majority of known masters. Successful contact with a peer retracts any locally held PFAIL opinion about it.
5. **Replica promotion requires a genuine majority vote**, gated by monotonic epochs (`current_epoch`/`last_vote_epoch`, one vote per epoch per voting master) to prevent duplicate or stale votes from being counted twice.
6. **Real, synchronous data migration on slot handoff.** `CLUSTER SETSLOT ... MIGRATING/IMPORTING`, `CLUSTER REBALANCE`, and `CLUSTER RESHARD` drive an actual per-key DUMP-and-replay of the moving slot's contents between the two participating nodes over a fresh async connection, gated by the router-level `SlotState` machine (`Migrating`/`Importing`/`Moved`/`Stable`) that governs `-MOVED`/`-ASK` redirection for the duration of the move.

---

## 3. High-Level Architecture & Workflow Diagram

```
Client ──► Node A (owns slot 5000) ──► -MOVED 5000 Node-B:6379
                                                   │
        Cluster Bus (blocking sockets, port+10000)│
        PING/PONG every 500ms, full node table ───┘
        gossip piggybacked in every PING

Replica R, master M silent > 10s
        │
        ▼
   R marks M "fail?" locally ──► gossips PFAIL opinion to peers
        │                              │
        ▼                              ▼
   peers corroborate ──► quorum reached ──► FAIL M broadcast
        │
        ▼
   R starts election: FAILOVER_AUTH_REQUEST to every known master
        │
        ▼
   majority of FAILOVER_AUTH_ACK ──► R becomes master, inherits M's slots,
                                      broadcasts FAILOVER_ANNOUNCE

CLUSTER REBALANCE / RESHARD / SETSLOT MIGRATING
        │
        ▼
   compute_rebalance_plan / compute_reshard_plan (greedy donor→receiver planner)
        │
        ▼
   per slot: mark Migrating ──► DUMP each key ──► ASKING + replay on target
        │                                              │
        ▼                                              ▼
   SETSLOT <slot> NODE <target>  ◄───────── target now owns the slot
```

---

## 4. Performance Guarantees & Theoretical Complexity

- **Not zero-allocation, not `io_uring`-based.** Every gossip tick and every `CLUSTER MEET`/`FAILOVER`/`SETSLOT` handshake opens a brand-new blocking `TcpStream` per peer (connect + write + read, each independently timed out at 200-300 ms) on a plain OS thread — the opposite of the rest of Rudis's zero-copy, Monoio-based data path. This is an acceptable cost for a control-plane path that runs a few times per second per peer, and should not be used as a template for data-path code.
- **Full-state gossip, not incremental.** `cluster_bus_tick` re-sends the entire known node table to every peer on every 500 ms tick, giving O(peers²) cluster-bus bandwidth per tick rather than the randomized-sample gossip production Redis Cluster uses at large scale. This is a deliberate simplification (§2.2) validated for small clusters (a handful of nodes); it is not designed or tested for hundreds of nodes.
- **Failure detection is a two-phase, quorum-gated process**, not a single node's unilateral opinion: local PFAIL suspicion (missed-PONG timer) must be corroborated by a strict majority of known masters via gossip before a peer is marked `"fail"` and a failover can be triggered. This directly protects against a lone node's network partition triggering a spurious promotion elsewhere in the cluster.
- **Slot migration is fully synchronous and sequential**, not pipelined: keys are dumped and replayed to the target node in batches (fetched from the source in batches of 100), one migration operation at a time, over a single connection to the target. There is no in-flight pipelining of the replay commands despite `CLUSTER REBALANCE`'s `PIPELINE` argument being accepted at the protocol layer (see the internal doc's Future Improvements for the precise gap). Slot migration therefore blocks progress on that slot's ownership handoff for the duration of the transfer; concurrent client traffic to already-migrated keys in the slot is served via `-ASK` redirection to the target in the interim.

---

## 5. Implementation References & Contributor Guide

For concrete struct definitions, memory layout diagrams, step-by-step function walkthroughs, and code-level technical debt:
* [**`docs/internal/11_cluster_topology.md`**](../internal/11_cluster_topology.md): Low-level implementation and code reference.
* **Source Files**: `src/cluster.rs`
