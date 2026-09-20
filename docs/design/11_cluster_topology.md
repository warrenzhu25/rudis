# Component 11: Redis Cluster Topology & Gossip Protocol (Design)

## Component 11: Redis Cluster Topology & Gossip Protocol

> **Source Files**: ``src/cluster.rs``


---

### 1. Architectural Purpose & Scope

`src/cluster.rs` implements a simplified Redis Cluster control plane: per-node slot
ownership tracked as `(start, end)` ranges (not a real Redis Cluster deployment's
16,384-bit bitmask), a plain-text line-oriented gossip protocol between nodes on
`port + 10000`, unilateral (non-consensus) failure detection based on ping/pong
staleness, and a real majority-vote replica election for failover. It is a single
process-wide singleton per listening port (`get_cluster_hub(port)`), and only the
shard-0 worker thread ever starts the cluster-bus listener for that port
(`start_cluster_bus`, called from `run_shard_worker` — see Component 01).

---

---

### 2. Key Invariants & Concurrency Constraints

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

---

### 6. Performance Characteristics

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
