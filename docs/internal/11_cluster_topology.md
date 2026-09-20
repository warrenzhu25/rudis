# Component 11: Redis Cluster Topology & Gossip Protocol (Implementation)

## Component 11: Redis Cluster Topology & Gossip Protocol — Code Reference & Implementation

> **Source Files**: ``src/cluster.rs``


---

### 3. Component Architecture & Data Structures

```
   CLUSTER MEET ip port          Client Connection ── CLUSTER SETSLOT / GET / SET
          │                              │
          ▼                              ▼
   ClusterHub::cluster_meet    connection.rs: read router.slot_states[slot]
   (connects to peer's cport,      (Migrating/Importing/Moved/Stable)
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

#### Real data structures (`src/cluster.rs`)

```rust
#[derive(Clone, Debug)]
pub struct ClusterNodeInfo {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub cport: u16,
    pub flags: String, // "myself,master", "master", "slave", "fail?", "fail"
    pub master_id: String,
    pub ping_sent: u64,
    pub pong_recv: u64,
    pub config_epoch: u64,
    pub link_state: String, // "connected", "disconnected"
    pub slots: Vec<(u16, u16)>,
}

pub struct ClusterHub {
    pub port: u16,
    pub cport: u16,
    pub my_id: RwLock<String>,
    pub current_epoch: AtomicU64,
    pub config_epoch: AtomicU64,
    pub last_vote_epoch: AtomicU64,
    pub election_in_progress: AtomicBool,
    pub role: RwLock<String>,          // "master" or "slave"
    pub master_id: RwLock<String>,     // "-" or an id
    pub nodes: RwLock<HashMap<String, ClusterNodeInfo>>,
    pub my_slots: RwLock<Vec<(u16, u16)>>,
    pub pfail_reports: RwLock<HashMap<String, HashSet<String>>>, // dead — see §2.4
    pub bus_running: AtomicBool,
    pub cancel_bus: RwLock<Option<flume::Sender<()>>>,
    pub active_migration: RwLock<Option<ActiveMigration>>,
}
```

`slots`/`my_slots` are `Vec<(u16, u16)>` range lists, kept normalized by
`compact_slots` (sorts, then merges adjacent/overlapping ranges) — there is no
16,384-bit bitmask anywhere in this file. Node IDs are **not** the real Redis
40-hex-char SHA1-derived ID; `generate_node_id` builds a 40-hex-char string from
two `fxhash::hash64` calls over the port and the current time-in-nanoseconds
(`format!("{:016x}{:016x}{:08x}", h1, h2, port)`), which happens to be the same
length but is not cryptographically meaningful.

`ActiveMigration` backs the Dragonfly-style (`DFLYCLUSTER`) migration status
commands (`dfly_migrate_init`/`dfly_migrate_flow`/`dfly_migrate_ack`/
`dfly_slot_migration_status`) — these are simple state bookkeeping (a state
string, a counter incremented by whatever `flow_id` value is passed in) rather
than any real data-transfer protocol; no keys are actually copied between nodes
by this code.

---

---

### 4. Execution Algorithms & Code Logic

#### 4.1 `CLUSTER MEET`: a synchronous one-shot handshake

```rust
pub fn cluster_meet(&self, ip: &str, port: u16) -> Result<(), String> {
    // ...pre-insert a temp node entry so it's visible immediately...
    let addr = format!("{}:{}", ip, port + 10000);
    if let Ok(mut stream) = TcpStream::connect_timeout(&addr.parse()?, Duration::from_millis(300)) {
        let meet_frame = format!("MEET 127.0.0.1 {} {} {} {}\r\n", self.port, self.my_id(), my_epoch, slots_repr);
        stream.write_all(meet_frame.as_bytes())?;
        // read the "+PONG <id> <epoch> <role> <slots>" reply, replace the temp entry with the real one
    }
    Ok(())
}
```

This blocks the calling connection task's thread for up to ~300ms (connect) plus
another ~300ms (read) in the worst case — it's a direct synchronous socket call
made from inside `CLUSTER MEET`'s command handler in `connection.rs`, not
dispatched to a background task.

#### 4.2 The gossip tick — `cluster_bus_tick`, called every 500ms from the bus thread

```rust
if last_tick.elapsed() >= Duration::from_millis(500) {
    last_tick = std::time::Instant::now();
    cluster_bus_tick(&hub_clone);
}
```

For every known peer, it opens a fresh `TcpStream`, sends:

```rust
let ping_msg = format!("PING {} {} {} {} GOSSIP {}\r\n",
    hub.my_id(), my_epoch, role, slots_repr, gossip_payload);
```

where `gossip_payload` is the *entire* known node table serialized as
`id,ip,port,cport,flags,epoch;id,ip,port,cport,flags,epoch;...` — every tick
re-sends full state to every peer (no incremental/random-sample gossip despite
what the old doc claimed). A successful `+PONG id epoch role slots` reply updates
that peer's `pong_recv`/`slots`/`config_epoch`/`flags`; a failed connection or
timeout instead re-evaluates that peer's failure state from elapsed silence
(`> 5000ms` → `"fail?"`, `> 10000ms` → `"fail"`). After processing all peers, it
checks whether the *local* node is a replica whose recorded master is `"fail"`,
and if so spawns `start_election` on a fresh `std::thread` (guarded by
`election_in_progress` so only one election runs at a time).

#### 4.3 Receiving a message — `handle_cluster_bus_conn`, one thread per inbound connection

A single `match` on the first whitespace-separated token handles `MEET`, `PING`
(also parses the embedded `GOSSIP <payload>` section — this is how third-party
nodes are learned about transitively, without ever calling `CLUSTER MEET` on
them directly), `FAIL`, `FAILOVER`, `FAILOVER_AUTH_REQUEST`, and
`FAILOVER_ANNOUNCE`. Every branch replies inline on the same blocking stream
(`+PONG ...`, `+OK\r\n`, or `+FAILOVER_AUTH_ACK id epoch\r\n` /
`-ERR vote rejected\r\n` for the vote request).

#### 4.4 Replica election — the one place with a real quorum

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
        // become master, inherit the old master's slots, broadcast FAILOVER_ANNOUNCE
    }
}
```

A voter (`FAILOVER_AUTH_REQUEST` handler, §4.3) only grants a vote if it is
itself a master, the requested epoch is newer than its `last_vote_epoch`, and it
believes the claimed old master is already `"fail"` — this is the one piece of
this file that is genuinely a distributed-consensus mechanism, not just local
bookkeeping. `cluster_failover FORCE` (manual failover via `CLUSTER FAILOVER`)
skips the vote entirely and just claims mastership + broadcasts `FAILOVER`
unconditionally.

#### 4.5 How this actually reaches `-MOVED`/`-ASK` on the command path — corrects a claim in Component 04

Component 04's doc states cluster slot-migration/redirection is "dead code" because
the standalone helper `Router::check_slot_redirection` has no call sites. That is
true for that specific function, but the underlying mechanism it would have
implemented **is** live — just inlined directly in `connection.rs` instead of
calling out to that helper:

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
            // even in the common case, check this file's ClusterHub for a
            // DIFFERENT node (not shard) owning this slot, and -MOVED to it:
            let hub = crate::cluster::get_cluster_hub(router.port);
            if !hub.my_slots.read().unwrap().iter().any(|&(s,e)| slot>=s && slot<=e) {
                if let Some(peer) = hub.nodes.read().unwrap().values()
                    .find(|n| n.flags.contains("master") && !n.flags.contains("fail")
                              && n.slots.iter().any(|&(s,e)| slot>=s && slot<=e)) {
                    out.extend_from_slice(format!("-MOVED {} {}:{}\r\n", slot, peer.ip, peer.port).as_bytes());
                    return false;
                }
            }
        }
    }
}
```

This runs on **every command that has a primary key**, for every connection —
`router.slot_states` (set via `CLUSTER SETSLOT`, see `router.rs::set_slot_state`,
which broadcasts a `ShardMessage::SetSlotState` to every local shard) and this
file's `ClusterHub.my_slots`/`.nodes` (populated by gossip/`MEET`) are both
genuinely consulted, and a real `-MOVED`/`-ASK` is genuinely written to the
client. `router.slot_owners` (a *separate*, per-shard-local ownership-override
vector — see Component 04) is a different piece of machinery not used by this
path at all; don't conflate the two.

---

---

### 5. Cross-Component Interactions

- **`src/connection.rs`**: reads `router.slot_states` and `get_cluster_hub(port)`
  directly on every keyed command (§4.5) to decide `-MOVED`/`-ASK`; also the
  entire `CLUSTER *` subcommand family (`MEET`, `NODES`, `INFO`, `ADDSLOTS`,
  `ADDSLOTSRANGE`, `DELSLOTS`, `DELSLOTSRANGE`, `SETSLOT`, `FAILOVER`,
  `REPLICATE`, `RESET`, `SLOTS`, `SHARDS`, `LINKS`, `FORGET`) is dispatched from
  `connection.rs` through thin `Router::cluster_*` passthrough methods straight
  into this file's `ClusterHub` methods.
- **`src/router.rs`**: `Router::set_slot_state`/`slot_states` is a *different*
  slot-state mechanism than this file's `my_slots`/gossip table — see §4.5's
  correction. They coexist and are both real, but they're not the same system.
- **`src/replication.rs`**: `cluster_failover`/`start_election` both call
  `crate::replication::get_replication_hub(self.port).make_master()` when this
  node wins/executes a failover, so cluster-level role changes propagate into
  the replication subsystem.
- **`src/server.rs`** (Component 01): calls `start_cluster_bus(port)` exactly
  once, only from the `shard_id == 0` worker thread.

---

---

### 7. Future Improvements

- **RESOLVED — quorum-based PFAIL corroboration and FAIL escalation (§2.4).** `pfail_reports` records peer PFAIL opinions piggybacked via gossip, retracts them upon successful ping, and strictly enforces majority quorum consensus (`total_votes >= quorum`) without unilateral timeout bypass before escalating to `"fail"` and broadcasting `FAIL <node_id>`. Verified by `test_cluster_quorum_pfail_to_fail_escalation` and `test_cluster_quorum_failure_detection_and_gossip_e2e`.
- **High — unify with Component 04's `slot_owners` (see that doc's §7) rather than maintaining two independent slot-authority systems.** This file's `ClusterHub.my_slots`/`.nodes` is the one actually consulted by the real `-MOVED` redirect path (§4.5); `router.rs`'s `slot_owners` is a parallel, mostly-unread mechanism. Consolidating avoids a future bug where the two disagree.
- **Medium — replace full-state gossip with incremental/randomized-sample gossip (§6)** if cluster sizes beyond a handful of nodes become a real target — the current O(peers²)-per-tick full node-table resend every 500ms is fine at small scale but won't hold up at real Redis Cluster-scale membership counts.
- ~~**Medium — extend the slot-state check (§4.5) to the pipelined squashed-command path**~~ **Resolved** — the squash-eligibility gate now also checks whether this node still owns a `Stable` slot per this file's `ClusterHub.my_slots`/`.nodes` gossip table (not just `slot_states`'s `Migrating`/`Importing`/`Moved`), confirmed by a new E2E test (`test_cluster_pipelined_squashed_moved_redirect_e2e`) — see Component 02 §7 and Component 04 §7 for the full correction (the `Migrating`/`Importing`/`Moved` half of this check already existed before; the gossip-ownership half was the genuinely new piece).
- **Low — derive node IDs from something closer to Redis's real scheme**, or at minimum document clearly that `generate_node_id`'s two-`fxhash`-calls-over-port-and-time approach (§3) is not cryptographically meaningful — it's "good enough" for uniqueness within one test cluster but shouldn't be assumed collision-resistant across a long-running fleet the way real Redis's ID generation is designed to be.

---
---
