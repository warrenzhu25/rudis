# Component 11: Redis Cluster Topology & Gossip Protocol (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/cluster.rs` (2,306 lines)
> **High-Level Design Spec**: [`docs/design/11_cluster_topology.md`](../design/11_cluster_topology.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/cluster.rs` | Cluster bus, gossip, slot-topology bookkeeping, election, slot-migration planning | `ClusterHub`, `ClusterNodeInfo`, `ActiveMigration`, `SlotMigrationPlan`, `RebalanceOptions`, `ClusterCheckReport`, `cluster_bus_tick`, `handle_cluster_bus_conn`, `start_election` |
| `src/connection.rs` | `CLUSTER *`/`DFLYCLUSTER *`/`DFLYMIGRATE *` command dispatch, the actual `-MOVED`/`-ASK` redirect path, key-migration data transfer (`migrate_keys_to_node`, `execute_rebalance_plans`) | `ClusterSubcommand` match arms |
| `src/router.rs` | `key_slot` (CRC16/XMODEM hashing), `Router::slot_states` (per-shard redirect state machine), `Router::set_slot_state` | — |

---

## 2. Component Architecture & Data Structures

```
   CLUSTER MEET ip port          Client Connection ── CLUSTER SETSLOT / GET / SET
          │                              │
          ▼                              ▼
   ClusterHub::cluster_meet    connection.rs: reads router.slot_states[slot]
   (connects to peer's cport,      (Migrating / Importing / Moved / Stable)
    exchanges MEET/PONG)                 │
          │                    Stable ──► also checks ClusterHub.my_slots /
          ▼                              .nodes for a DIFFERENT node owning
   nodes: HashMap<id, ClusterNodeInfo>    this slot ── -MOVED if so
          │
          ▼
   cluster_bus_tick() every 500ms: PING every known peer,
   embed full node table as a ";"-joined gossip blob in the PING line,
   update pong_recv / flags from the PONG reply or its absence
```

### 2.1 Real Data Structures (`src/cluster.rs`)

```rust
#[derive(Clone, Debug)]
pub struct ClusterNodeInfo {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub cport: u16,
    pub flags: String,       // "myself,master", "master", "slave", "fail?", "fail"
    pub master_id: String,   // "-" for masters, or the master's node id for a replica
    pub ping_sent: u64,      // epoch-millis of the last PING sent to this peer
    pub pong_recv: u64,      // epoch-millis of the last PONG received from this peer
    pub config_epoch: u64,
    pub link_state: String,  // "connected" or "disconnected"
    pub slots: Vec<(u16, u16)>,
}

pub struct ClusterHub {
    pub port: u16,
    pub cport: u16,                                   // port + 10000
    pub my_id: RwLock<String>,
    pub current_epoch: AtomicU64,
    pub config_epoch: AtomicU64,
    pub last_vote_epoch: AtomicU64,
    pub election_in_progress: AtomicBool,
    pub role: RwLock<String>,                          // "master" or "slave"
    pub master_id: RwLock<String>,                      // "-" or an id
    pub has_nodes: AtomicBool,
    pub nodes: RwLock<HashMap<String, ClusterNodeInfo>>,
    pub my_slots: RwLock<Vec<(u16, u16)>>,
    pub pfail_reports: RwLock<HashMap<String, HashSet<String>>>, // peer_id -> reporter node ids
    pub bus_running: AtomicBool,
    pub cancel_bus: RwLock<Option<flume::Sender<()>>>,
    pub active_migration: RwLock<Option<ActiveMigration>>,       // DFLYMIGRATE state only, see §4.6
    pub slot_states: RwLock<HashMap<u16, (String, String)>>,     // slot -> ("migrating"|"importing", peer)
    pub cluster_enabled: AtomicBool,
    pub num_shards: AtomicUsize,
}

pub struct ActiveMigration {
    pub state: String,        // "MIGRATING" / "SYNCING"
    pub source_id: String,
    pub num_shards: usize,
    pub slots: Vec<(u16, u16)>,
    pub keys_migrated: u64,
}

pub struct SlotMigrationPlan {
    pub slot: u16,
    pub source_node_id: String,
    pub source_addr: String,
    pub target_node_id: String,
    pub target_addr: String,
}

pub struct RebalanceOptions {
    pub weights: HashMap<String, f64>,
    pub simulate: bool,
    pub threshold: f64,                                // percent slack around perfectly-even allocation
    pub pipeline: usize,                                // accepted, currently unused — see §7
    pub target_host_port: Option<(String, u16, Option<usize>)>,
}

pub struct ClusterCheckReport {
    pub ok: bool,
    pub masters: usize,
    pub replicas: usize,
    pub total_slots_assigned: usize,
    pub open_slots: Vec<u16>,
    pub duplicate_slots: Vec<u16>,
    pub migrating_slots: Vec<(u16, String)>,
    pub importing_slots: Vec<(u16, String)>,
}
```

`nodes`/`my_slots`/`ClusterNodeInfo.slots` all use `Vec<(u16, u16)>` normalized, non-overlapping range lists (kept sorted/merged by `compact_slots`) — there is **no 16,384-bit slot bitmask** anywhere in this file. Node IDs are **not** derived the way real Redis derives them; `generate_node_id` builds a 40-hex-character string from two `fxhash::hash64` calls over the port and the current time-in-nanoseconds (`format!("{:016x}{:016x}{:08x}", h1, h2, port)`) — the same *length* as a real Redis Cluster node ID, but not collision-resistant or cryptographically derived.

`hub.slot_states` is a **second, distinct migrating/importing tracker** from `router.slot_states` (Component 04) — see §4.6 for how the two are populated together and what each one is actually read by.

### 2.2 Virtual-Topology Bootstrap Mode

Before any `CLUSTER MEET` has been performed, `cluster_nodes()` and `cluster_check()` both special-case a single-process, multi-shard deployment: if `cluster_enabled` is set, `num_shards > 1`, and the gossip-learned `nodes` table is still empty, both functions synthesize a deterministic *virtual* peer table — partitioning the full 16,384-slot space evenly across `num_shards` fabricated peers (`peer_id = format!("{:040x}", shard_index + 1)`, address `127.0.0.1:<port + shard_index>`) — rather than reporting a single-node, single-slot-range cluster. This lets `CLUSTER NODES`/`CLUSTER SLOTS`/`CLUSTER CHECK` present a sensible topology for a thread-per-core deployment that has not yet joined a real multi-process cluster, without requiring an explicit bootstrap command.

---

## 3. Execution Algorithms & Code Logic

### 3.1 `CLUSTER MEET`: a synchronous one-shot handshake

```rust
pub fn cluster_meet(&self, ip: &str, port: u16) -> Result<(), String> {
    // ...pre-insert a temporary node entry so it is visible immediately...
    let addr = format!("{}:{}", ip, port + 10000);
    if let Ok(mut stream) = TcpStream::connect_timeout(&addr.parse()?, Duration::from_millis(300)) {
        let meet_frame = format!("MEET 127.0.0.1 {} {} {} {}\r\n", self.port, self.my_id(), my_epoch, slots_repr);
        stream.write_all(meet_frame.as_bytes())?;
        // read the "+PONG <id> <epoch> <role> <slots>" reply, replace the temp entry with the real one
    }
    Ok(())
}
```

This blocks the calling connection task's OS thread for up to ~300 ms (connect) plus another ~300 ms (read) in the worst case — it is a direct synchronous socket call made from inside `CLUSTER MEET`'s command handler in `connection.rs`, not dispatched to a background task.

### 3.2 The gossip tick — `cluster_bus_tick`, called every 500 ms from the bus thread

For every known peer, it opens a fresh `TcpStream` and sends:

```rust
let ping_msg = format!("PING {} {} {} {} GOSSIP {}\r\n",
    hub.my_id(), my_epoch, role, slots_repr, gossip_payload);
```

where `gossip_payload` is the *entire* known node table serialized as `id,ip,port,cport,flags,epoch;id,ip,port,cport,flags,epoch;...` — every tick re-sends full state to every peer (no incremental/random-sample gossip). A successful `+PONG id epoch role slots` reply updates that peer's `pong_recv`/`slots`/`config_epoch`, clears its `"fail?"`/`"fail"` flag back to `"master"`, and retracts this node's own PFAIL opinion about it (removed from `pfail_reports`). A failed connection or an unanswered PING instead re-evaluates that peer's failure state from elapsed silence since the last successful PONG:

- `> 5000 ms` of silence: compute `total_votes = (corroborating peers already reporting PFAIL for this node) + 1` (the local node's own vote). If `total_votes >= quorum` (`quorum = floor(total_masters / 2) + 1`, counting this node as a master if it is one), the peer is marked `"fail"` and a `FAIL <node_id>` message is broadcast to every known peer via `broadcast_to_peers`. Otherwise the peer is marked `"fail?"` only (a local, unescalated suspicion).

After processing every peer, `cluster_bus_tick` checks whether the *local* node is a replica whose recorded master has flags containing `"fail"`, and if so spawns `start_election` on a fresh `std::thread`, gated by `election_in_progress` (an `AtomicBool` compare-and-swap) so at most one election runs concurrently.

### 3.3 Receiving a message — `handle_cluster_bus_conn`, one thread per inbound connection

A `match` on the first whitespace-separated token of each `\r\n`-terminated line handles:

| Message | Format | Handler behavior |
| :--- | :--- | :--- |
| `MEET` | `MEET <ip> <port> <node_id> <epoch> [<slots>]` | Inserts the sender into `nodes`, replies `+PONG <my_id> <epoch> <role> <my_slots>` |
| `PING` | `PING <node_id> <epoch> <flags> <slots> [GOSSIP <payload>]` | Updates the sender's liveness/flags/slots; parses the embedded `GOSSIP` section (§3.4); replies `+PONG ...` |
| `FAIL` | `FAIL <node_id>` | Marks that node `"fail"`/`"disconnected"` locally; replies `+OK` |
| `FAILOVER` | `FAILOVER <master_id> <epoch> <slots>` | Updates the named node to `"master"`, clears its `master_id`, adopts the announced slots; replies `+OK` |
| `FAILOVER_AUTH_REQUEST` | `FAILOVER_AUTH_REQUEST <replica_id> <epoch> <master_id>` | Grants a vote (§3.5) or rejects it |
| `FAILOVER_ANNOUNCE` | `FAILOVER_ANNOUNCE <new_master_id> <epoch> <slots>` | Promotes the named node, strips the announced slots away from every *other* node's recorded slot list (so stale ownership entries do not linger); replies `+OK` |
| anything else | — | `-ERR unknown clusterbus command` |

### 3.4 Gossip payload ingestion (part of the `PING` handler)

Each `id,ip,port,cport,flags,epoch` entry in the `GOSSIP` payload is processed independently:

- If `flags` contains `"fail"`, the local node records the sending peer as a PFAIL *reporter* for that node id (`pfail_reports[gid].insert(sender_id)`) — this is how PFAIL corroboration actually propagates transitively across the cluster without every node needing a direct connection to every other node.
- If `flags` does not contain `"fail"`, any existing PFAIL report from that sender for that node id is removed (a retraction).
- If `flags == "fail"` exactly, the locally cached copy of that node is also flagged `"fail"` directly.
- If the node id was previously unknown and the gossip entry carries a nonzero port, it is inserted into `nodes` — **this is how third-party nodes are learned about transitively**, without the local node ever calling `CLUSTER MEET` on them directly.

### 3.5 Replica election — `start_election`, the one place with a real quorum vote

```rust
pub fn start_election(&self) {
    let req_epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1;
    let masters = /* every known peer whose flags contain "master" and not "fail" */;
    let mut votes = 1; // self
    for (ip, cport) in &masters {
        // connect, send "FAILOVER_AUTH_REQUEST <id> <epoch> <master_id>", count a "+FAILOVER_AUTH_ACK" reply
    }
    let majority = (masters.len() + 1) / 2 + 1;
    if votes >= majority {
        // become master, inherit the old master's slots, call replication::make_master(),
        // broadcast FAILOVER_ANNOUNCE
    }
    self.election_in_progress.store(false, Ordering::SeqCst);
}
```

A voter (the `FAILOVER_AUTH_REQUEST` handler, §3.3) grants a vote only if all three hold: it is itself currently a master, the requested epoch is strictly newer than its own `last_vote_epoch` (persisted via `last_vote_epoch.store`, enforcing one vote per epoch), and it believes the claimed old master's flags already contain `"fail"`. This is the one piece of this file that is a genuine distributed-consensus mechanism, not local bookkeeping. `CLUSTER FAILOVER FORCE` (manual failover) bypasses the vote entirely: it directly claims mastership and broadcasts `FAILOVER` unconditionally, trusting the operator's judgment instead of peer corroboration.

### 3.6 How this actually reaches `-MOVED`/`-ASK` on the command path

The redirect check runs on every command with a primary key, inline in `connection.rs`:

```rust
if let Some(key) = cmd_primary_key(&cmd) {
    let slot = key_slot(key);
    let state = router.slot_states.borrow()[slot as usize].clone();
    match state {
        SlotState::Moved(target) => { /* -MOVED slot target, always */ }
        SlotState::Importing(source) => { if !is_asking { /* -MOVED slot source */ } }
        SlotState::Migrating(target) => {
            if !router.exists(key.clone()).await { /* -ASK slot target */ }
        }
        SlotState::Stable => {
            // even in the common case, check ClusterHub for a DIFFERENT node
            // (not shard) owning this slot, and -MOVED to it:
            let hub = crate::cluster::get_cluster_hub(router.port);
            if !hub.my_slots.read().unwrap().iter().any(|&(s,e)| slot>=s && slot<=e) {
                if let Some(peer) = hub.nodes.read().unwrap().values()
                    .find(|n| n.flags.contains("master") && !n.flags.contains("fail")
                              && n.slots.iter().any(|&(s,e)| slot>=s && slot<=e)) {
                    // -MOVED slot peer.ip:peer.port
                }
            }
        }
    }
}
```

`router.slot_states` (per-shard, set by `CLUSTER SETSLOT` via `Router::set_slot_state`, which broadcasts a `ShardMessage::SetSlotState` to every local shard) governs the migration-in-progress redirects. `ClusterHub.my_slots`/`.nodes` (populated by gossip/`MEET`) governs steady-state cross-node redirection. Both are genuinely consulted and a real `-MOVED`/`-ASK` is genuinely written to the client on every keyed command.

### 3.7 Slot migration data path — `CLUSTER SETSLOT`, `CLUSTER REBALANCE`, `CLUSTER RESHARD`

Unlike the cluster bus (§3.1-3.4), the slot-migration data path runs over Rudis's normal async, Monoio-based client connection machinery, and genuinely transfers data:

1. **Planning.** `ClusterHub::compute_rebalance_plan` (targeted-node or weighted auto-balance modes) and `ClusterHub::compute_reshard_plan` (explicit source→target, N-slots) each produce a `Vec<SlotMigrationPlan>` without touching any state — pure planning functions.
   - Weighted auto-balance computes each master's target slot count proportional to its `RebalanceOptions.weights` entry (default weight `1.0`), allocates by `floor`, then distributes the remainder by largest fractional remainder (a standard apportionment method), skips nodes already within `threshold` percent of their target, and greedily pairs the most over-allocated donor with the most under-allocated receiver until balanced.
   - Targeted-node mode picks the single largest-slot-count donor (if migrating *out of* the caller) or the caller itself (if migrating *into* a named target) and moves the requested slot count.
2. **Execution** (`execute_rebalance_plans` / the `CLUSTER SETSLOT ... MIGRATING` handler in `connection.rs`), per planned slot:
   - Mark the slot `Migrating` in both `router.slot_states` (drives `-ASK` redirection, §3.6) and `hub.slot_states` (drives `CLUSTER NODES`/`CLUSTER CHECK` migrating/importing reporting, §2.1/§2.2 — a second, independent record of the same fact, kept in sync by the same call site).
   - Send `CLUSTER SETSLOT <slot> IMPORTING <my_id>` to the target so it accepts writes for keys not yet migrated.
   - Fetch up to 100 keys in the slot at a time (`router.get_keys_in_slot`) and migrate them via `migrate_keys_to_node`: each key is serialized with `router.dump_key` (a DUMP-equivalent snapshot of value + remaining TTL) and replayed on the target as an `ASKING` command followed by a type-appropriate write (`SET ... PX <ttl>`, `HSET`, etc.) over a fresh `monoio::net::TcpStream`. This repeats in batches until the slot is empty.
   - Send `CLUSTER SETSLOT <slot> NODE myself` to the target to finalize ownership, then mark the slot `Moved` locally.

This is a real, working, sequential (not pipelined) migration path, in contrast to the Dragonfly-compatible `DFLYMIGRATE`/`DFLYCLUSTER` command family described next.

### 3.8 `DFLYMIGRATE`/`DFLYCLUSTER` — protocol compatibility surface, bookkeeping only

`ActiveMigration` backs the Dragonfly-style migration status commands:

```rust
pub fn dfly_migrate_init(&self, source_id: &str, num_shards: usize, slots: &[(u16, u16)]) {
    *self.active_migration.write().unwrap() = Some(ActiveMigration {
        state: "MIGRATING".to_string(), source_id: source_id.to_string(),
        num_shards, slots: slots.to_vec(), keys_migrated: 0,
    });
}
pub fn dfly_migrate_flow(&self, _source_id: &str, flow_id: u64) {
    // sets state = "SYNCING", keys_migrated += flow_id.max(1)  -- a counter increment, not a real transfer
}
pub fn dfly_migrate_ack(&self, _flow_id: u64) {
    // clears active_migration
}
```

These three functions only maintain an in-memory state string and a counter that is incremented by whatever `flow_id` the caller happens to pass — **no keys are read, serialized, or transferred by this code path.** It exists to let clients that speak the Dragonfly cluster-migration protocol (`DFLYMIGRATE INIT/FLOW/ACK`, `DFLYCLUSTER MYID/CONFIG/GETSLOTINFO/FLUSHSLOTS/SLOT-MIGRATION-STATUS`) observe plausible status transitions; real data movement for Rudis-native cluster operations goes through §3.7 instead.

---

## 4. `CLUSTER` Subcommands Supported (verified against `src/resp.rs`/`src/connection.rs`)

`KEYSLOT`, `COUNTKEYSINSLOT`, `GETKEYSINSLOT`, `SETSLOT <slot> MIGRATING|IMPORTING|STABLE|NODE`, `SLOTS`, `SHARDS`, `LINKS`, `ADDSLOTS`, `DELSLOTS`, `ADDSLOTSRANGE`, `DELSLOTSRANGE`, `NODES`, `INFO`, `MYID`, `MEET`, `MIGRATE-SLOT` (internal `ClusterSubcommand::MigrateSlot`, driving the same key-transfer path as §3.7), `REBALANCE [<host> <port>] [SLOTS n] [WEIGHT id=w ...] [SIMULATE] [THRESHOLD pct] [PIPELINE n]`, `CHECK`, `RESHARD <target> FROM <source> SLOTS <n>`, `FAILOVER [FORCE]`, `RESET [HARD]`, `FORGET <node_id>`, `REPLICATE <node_id>`, `SAVECONFIG`.

`DFLYCLUSTER MYID/CONFIG/GETSLOTINFO/FLUSHSLOTS/SLOT-MIGRATION-STATUS` and `DFLYMIGRATE INIT/FLOW/ACK` are supported as a separate, Dragonfly-compatible command family (§3.8).

---

## 5. Cross-Component Interactions

- **`src/connection.rs`**: reads `router.slot_states` and `get_cluster_hub(port)` directly on every keyed command (§3.6) to decide `-MOVED`/`-ASK`; dispatches the entire `CLUSTER *` subcommand family listed in §4 through thin `Router::cluster_*` passthrough methods into this file's `ClusterHub` methods; owns the actual key-transfer logic (`migrate_keys_to_node`, `execute_rebalance_plans`) that `ClusterHub`'s planning functions feed into.
- **`src/router.rs`**: `Router::set_slot_state`/`slot_states` is a *different*, per-shard-local slot-state mechanism from this file's `my_slots`/gossip table and from `hub.slot_states` — all three are real and consulted by different parts of the redirect/migration path (§3.6, §3.7); they are not the same system and do not share storage.
- **`src/replication.rs`**: `cluster_failover`/`start_election` both call `crate::replication::get_replication_hub(self.port).make_master()` when this node wins or executes a failover, so cluster-level role changes propagate into the replication subsystem.
- **`src/server.rs`** (Component 01): calls `start_cluster_bus(port)` exactly once, only from the `shard_id == 0` worker thread.

---

## 6. Future Improvements

- **Medium — replace full-state gossip with incremental/randomized-sample gossip (design doc §4)** if cluster sizes beyond a handful of nodes become a real target — the current O(peers²)-per-tick full node-table resend every 500 ms is adequate at small scale but will not hold up at real Redis Cluster-scale membership counts.
- **Medium — thread `RebalanceOptions.pipeline` (the `CLUSTER REBALANCE ... PIPELINE n` argument) through to `execute_rebalance_plans`.** The field is parsed from the client command and stored on `RebalanceOptions`, but `execute_rebalance_plans`/`migrate_keys_to_node` always fetch and replay keys sequentially in fixed batches of 100 regardless of its value — the argument is currently a no-op.
- **Low — reconcile `router.slot_states`, `ClusterHub.my_slots`/`.nodes`, and `ClusterHub.slot_states` into fewer independent slot-authority records.** All three are genuinely read by live code (§3.6, §3.7) and are kept in sync by the same call sites today, but maintaining three separate representations of overlapping facts is a latent source of future drift.
- **Low — derive node IDs from something closer to Redis's real scheme**, or at minimum keep this documented clearly: `generate_node_id`'s two-`fxhash`-calls-over-port-and-time approach (§2.1) is not cryptographically meaningful — adequate for uniqueness within one running cluster, not intended to be collision-resistant across a long-running fleet the way real Redis Cluster's ID generation is designed to be.
- **Low — `DFLYMIGRATE`/`DFLYCLUSTER`'s migration-status surface (§3.8) reports state transitions without a real underlying transfer.** If Dragonfly-protocol clients are expected to drive actual cross-node data movement (as opposed to just observing status for compatibility testing), this path needs the same DUMP-and-replay treatment `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD` already have (§3.7).

---
---

## Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: PFAIL-to-FAIL escalation requires strict majority quorum corroboration via gossip (§3.2) — a single node's missed-PONG timer alone only ever produces a local, unescalated `"fail?"`, never a broadcast `FAIL`.
* **Gotcha 2**: The cluster bus runs on dedicated blocking OS threads (one listener thread plus one thread per inbound connection), entirely outside the async/Monoio reactor that serves client traffic — do not assume cluster-bus code can use `await` or touch shard-local state directly.
* **Gotcha 3**: `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD` perform real, synchronous, unpipelined per-key data migration; `DFLYMIGRATE`/`DFLYCLUSTER` status commands are bookkeeping-only and transfer no data (§3.7 vs §3.8) — do not conflate the two when reasoning about whether a given migration path actually moves keys.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests
cargo test --lib -- --test-threads=1
```
