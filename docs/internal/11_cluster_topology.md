# Component 11: Redis Cluster Topology & Gossip Protocol (Implementation Deep-Dive & Code Reference)

> **Source Files**: `src/cluster.rs` (2,306 lines — unchanged in size since the prior revision
> of this document, and re-verified line-by-line rather than assumed stable)
> **Cross-referenced**: `src/router.rs` (4,483 lines), `src/shard.rs` (4,137 lines),
> `src/mailbox.rs` (931 lines) — all three grew substantially since the last pass on this
> document and changed how `cluster.rs`'s migration output is actually consumed on the
> command-routing path; see §6.
> **High-Level Design Spec**: [`docs/design/11_cluster_topology.md`](../design/11_cluster_topology.md)
> **Consolidated Implementation Spec**: [`docs/internal/components.md`](components.md)

> **Correction notice (this revision)**: `src/cluster.rs` itself is byte-for-byte unchanged in
> line count and every structure/algorithm described in the prior revision was re-verified
> against the current source and found **still accurate** — quorum-based PFAIL/FAIL
> escalation, the weighted-proportional-with-largest-remainder rebalance planner, real
> DUMP-and-replay slot migration, the virtual-topology bootstrap fallback, and the
> `CLUSTER REBALANCE ... PIPELINE n` no-op are all confirmed present exactly as before (see
> §3, §5, §7). What **is** new in this revision: a careful cross-read of `router.rs`'s
> massively-expanded slot-routing machinery surfaced two additional, concrete findings that
> only exist at the *boundary* between this file's migration bookkeeping and `Router`'s
> actual command-dispatch decision — a dead-code redirect helper (`check_slot_redirection`)
> and a genuine decoupling between `CLUSTER ADDSLOTS`/`ADDSLOTSRANGE` and real command
> routing — plus a previously-undocumented data-loss edge case in the DUMP-and-replay path
> when a migrated key is currently spilled to NVMe tier storage under memory pressure. See
> §6 and §7.

---

## 1. Source Module Map & Responsibilities

| File | Subsystem Role | Key Functions / Structs |
| :--- | :--- | :--- |
| `src/cluster.rs` | Cluster bus (gossip), node/slot bookkeeping (`ClusterHub`), election, migration/rebalance **planning** (not execution) | `ClusterHub`, `ClusterNodeInfo`, `ActiveMigration`, `SlotMigrationPlan`, `RebalanceOptions`, `ClusterCheckReport`, `cluster_bus_tick`, `handle_cluster_bus_conn`, `start_election` |
| `src/connection.rs` | `CLUSTER *` / `DFLYCLUSTER *` / `DFLYMIGRATE *` command dispatch, the actual `-MOVED`/`-ASK`/`-CROSSSLOT` redirect check on every command, migration **execution** (`migrate_keys_to_node`, `execute_rebalance_plans`) | `Command::Cluster` match arm (`:6723`), `migrate_keys_to_node` (`:3449`), `execute_rebalance_plans` (`:3735`) |
| `src/router.rs` | `key_slot` (CRC16/XMODEM), per-shard `slot_states`/`slot_owners` (the real routing table consulted by every command), thin `Router::cluster_*` passthroughs into `ClusterHub` | `Router::get_slot_state`/`set_slot_state`/`set_slot_owner`/`target_shard_for_slot` (`:232-310`) |

---

## 2. Component Architecture & Data Structures

```
   CLUSTER MEET ip port          Client Connection ── CLUSTER SETSLOT / GET / SET
          │                              │
          ▼                              ▼
   ClusterHub::cluster_meet    connection.rs execute_command: inline SlotState match
   (connects to peer's cport,   on router.get_slot_state(slot) (Migrating/Importing/
    exchanges MEET/PONG)         Moved/Stable), plus a Stable-branch fork on
          │                      router.cluster_enabled vs HAS_ACTIVE_CLUSTER (§6)
          ▼
   nodes: HashMap<id, ClusterNodeInfo>
          │
          ▼
   cluster_bus_tick() every 500ms: PING every known peer,
   embed full node table as a ";"-joined gossip blob in the PING line,
   update pong_recv / flags from the PONG reply or its absence
```

### 2.1 Real Data Structures (`src/cluster.rs`, verified line-for-line against current source)

```rust
#[derive(Clone, Debug)]                                          // cluster.rs:8-21
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

#[derive(Clone, Debug)]                                          // cluster.rs:23-30
pub struct ActiveMigration {
    pub state: String,        // "MIGRATING" / "SYNCING" — DFLYMIGRATE bookkeeping only, §4.6
    pub source_id: String,
    pub num_shards: usize,
    pub slots: Vec<(u16, u16)>,
    pub keys_migrated: u64,
}

#[derive(Clone, Debug, PartialEq)]                                // cluster.rs:32-39
pub struct SlotMigrationPlan {
    pub slot: u16,
    pub source_node_id: String,
    pub source_addr: String,
    pub target_node_id: String,
    pub target_addr: String,
}

#[derive(Clone, Debug, PartialEq)]                                // cluster.rs:41-48
pub struct RebalanceOptions {
    pub weights: HashMap<String, f64>,
    pub simulate: bool,
    pub threshold: f64,       // percent slack; Default = 1.25 (cluster.rs:55)
    pub pipeline: usize,      // parsed from PIPELINE n, then DISCARDED — see §7 (real bug)
    pub target_host_port: Option<(String, u16, Option<usize>)>,
}
// Default: weights={}, simulate=false, threshold=1.25, pipeline=16, target_host_port=None

#[derive(Clone, Debug, PartialEq)]                                // cluster.rs:62-72
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

pub struct ClusterHub {                                           // cluster.rs:112-132
    pub port: u16,
    pub cport: u16,                                   // = port + 10000, fixed offset
    pub my_id: RwLock<String>,
    pub current_epoch: AtomicU64,
    pub config_epoch: AtomicU64,
    pub last_vote_epoch: AtomicU64,                    // one FAILOVER_AUTH vote per epoch
    pub election_in_progress: AtomicBool,
    pub role: RwLock<String>,                          // "master" or "slave"
    pub master_id: RwLock<String>,                     // "-" or an id
    pub has_nodes: AtomicBool,
    pub nodes: RwLock<HashMap<String, ClusterNodeInfo>>,
    pub my_slots: RwLock<Vec<(u16, u16)>>,             // default: vec![(0, 16383)]
    pub pfail_reports: RwLock<HashMap<String, HashSet<String>>>,  // peer_id -> reporter ids
    pub bus_running: AtomicBool,
    pub cancel_bus: RwLock<Option<flume::Sender<()>>>,
    pub active_migration: RwLock<Option<ActiveMigration>>,  // DFLYMIGRATE state only, §4.6
    pub slot_states: RwLock<HashMap<u16, (String, String)>>, // slot -> ("migrating"|"importing", peer)
    pub cluster_enabled: AtomicBool,                   // set once, at startup, from config (§6)
    pub num_shards: AtomicUsize,                        // default 1; set from --threads at startup
}
```

`nodes`/`my_slots`/`ClusterNodeInfo.slots` all use `Vec<(u16, u16)>` normalized, non-overlapping
range lists (kept sorted/merged by `compact_slots`, `cluster.rs:1488-1505`) — there is **no
16,384-bit slot bitmask** anywhere in this file. Node IDs are **not** derived the way real Redis
derives them: `generate_node_id` (`cluster.rs:155-164`) builds a 40-hex-character string from two
`fxhash::hash64` calls over the port and the current time-in-nanoseconds
(`format!("{:016x}{:016x}{:08x}", h1, h2, port)`) — the same *length* as a real Redis Cluster node
ID, but not collision-resistant or cryptographically derived.

`hub.slot_states` (a `HashMap<u16, (String, String)>`) is a **second, distinct** migrating/importing
tracker from `router.slot_states` (a per-shard `hashbrown::HashMap<u16, SlotState>` in
`Router`, Component 04) — the two are populated together by the same call sites and read by
different consumers; see §6 for exactly which code reads which one.

### 2.2 Virtual-Topology Bootstrap Mode

Before any `CLUSTER MEET` has been performed, three separate `ClusterHub` methods —
`cluster_nodes` (`:198-344`), `cluster_slots` (`:542-665`), `cluster_shards` (`:667-835`), and
`cluster_check` (`:926-1016`) — all special-case a single-process, multi-shard deployment: if
`cluster_enabled` is set, `num_shards > 1`, and the gossip-learned `nodes` table is still empty,
each function independently synthesizes a deterministic *virtual* peer table — partitioning the
full 16,384-slot space evenly across `num_shards` fabricated peers (`peer_id = format!("{:040x}",
shard_index + 1)`, address `127.0.0.1:<port + shard_index>`, computed as
`shard_index * 16384 / num_shards .. ((shard_index+1) * 16384 / num_shards) - 1`) rather than
reporting a single-node, single-slot-range cluster. This lets `CLUSTER NODES`/`CLUSTER
SLOTS`/`CLUSTER SHARDS`/`CLUSTER CHECK` present a sensible topology for a thread-per-core
deployment that has not yet joined a real multi-process cluster, without requiring an explicit
bootstrap command. **This same `s * 16384 / num_shards` formula is also how `Router::new`
initializes the real routing table (`slot_owners`, via `slot_to_shard`)** — the two are
independently computed but happen to agree by construction in the bootstrap case; see §6 for
what happens once they diverge (e.g. after `CLUSTER ADDSLOTS`).

---

## 3. Execution Algorithms & Code Logic

### 3.1 `CLUSTER MEET`: a synchronous one-shot handshake (`cluster_meet`, `:360-471`)

```rust
pub fn cluster_meet(&self, ip: &str, port: u16) -> Result<(), String> {
    // pre-insert a temporary node entry into `nodes` so it is visible immediately (:373-397)
    let addr = format!("{}:{}", ip, port + 10000);
    if let Ok(mut stream) = TcpStream::connect_timeout(&addr.parse()?, Duration::from_millis(300)) {
        let meet_frame = format!("MEET 127.0.0.1 {} {} {} {}\r\n",
            self.port, self.my_id(), my_epoch, slots_repr);
        stream.write_all(meet_frame.as_bytes())?;
        // read "+PONG <id> <epoch> <role> <slots>", replace the temp entry with the real one
    }
    Ok(())
}
```

This blocks the calling connection task's OS thread for up to ~300 ms (connect) plus another
~300 ms (read, `set_read_timeout`) in the worst case — a direct synchronous `std::net::TcpStream`
call made from inside `CLUSTER MEET`'s command handler, not dispatched to a background task.

### 3.2 The gossip tick — `cluster_bus_tick` (`:1901-2092`), called every 500 ms from the bus thread

The bus thread (spawned once by `start_cluster_bus`, `:1525-1593`) alternates `listener.accept()`
(20 ms poll interval on `WouldBlock`) with a 500 ms-gated call to `cluster_bus_tick`
(`last_tick.elapsed() >= Duration::from_millis(500)`, `:1586-1589`). For every known peer it opens
a **fresh** `TcpStream` per tick (200 ms connect + 200 ms read/write timeouts) and sends:

```rust
let ping_msg = format!("PING {} {} {} {} GOSSIP {}\r\n",
    hub.my_id(), my_epoch, role, slots_repr, gossip_payload);
```

where `gossip_payload` is the **entire** known node table serialized as
`id,ip,port,cport,flags,epoch;id,ip,port,cport,flags,epoch;...` — every tick re-sends full state
to every peer (no incremental/random-sample gossip; O(known_peers) TCP connections opened and
torn down every 500 ms). A successful `+PONG id epoch role slots` reply updates that peer's
`pong_recv`/`slots`/`config_epoch`, clears its `"fail?"`/`"fail"` flag back to `"master"`, and
retracts this node's own PFAIL opinion about it (removed from `pfail_reports`, `:2020-2025`).

**PFAIL/FAIL quorum mechanics (concrete numbers, `:2026-2068`)**: a failed connection or an
unanswered PING triggers a failure-state re-evaluation only after `now.saturating_sub(node.pong_recv)
> 5000` (5000 ms silence). At that point:

```
total_votes = (peers already reporting PFAIL for this node via pfail_reports) + 1   // +1 = self
quorum      = (total_masters / 2) + 1                                               // integer floor
```

`total_masters` (computed identically here and in `cluster_nodes`, `:1913-1922` / `:287-296`) counts
every known node whose `master_id == "-"` or whose flags neither contain `"slave"` nor are empty,
plus this node itself if its own role is `"master"`. If `total_votes >= quorum`, the peer is marked
`"fail"`/`"disconnected"` and a `FAIL <node_id>` message is broadcast to every known peer via
`broadcast_to_peers` (`:910-924`, also a fresh TCP connection per peer, 200 ms timeout). Otherwise
the peer is marked `"fail?"` only — a local, unescalated suspicion. `cluster_nodes()`'s own render
path (`:298-341`, called on every `CLUSTER NODES`) re-derives the identical escalation logic on
read, so the flags reported to a client are always freshly computed from `pfail_reports` + silence,
not just a cached write from the last tick.

After processing every peer, `cluster_bus_tick` checks whether the *local* node is a replica whose
recorded master's flags contain `"fail"` (`:2071-2084`), and if so spawns `start_election` on a
fresh `std::thread`, gated by `election_in_progress.swap(true, ...)` (an `AtomicBool`
compare-and-swap) so at most one election runs concurrently per node.

### 3.3 Receiving a message — `handle_cluster_bus_conn` (`:1595-1899`), one thread per inbound connection

A `match` on the first whitespace-separated token of each `\r\n`-terminated line handles:

| Message | Format | Handler behavior |
| :--- | :--- | :--- |
| `MEET` | `MEET <ip> <port> <node_id> <epoch> [<slots>]` | Inserts sender into `nodes` (`flags="master"`), replies `+PONG <my_id> <epoch> <role> <my_slots>` |
| `PING` | `PING <node_id> <epoch> <flags> <slots> [GOSSIP <payload>]` | Updates sender's `pong_recv`/`flags`/`slots`; parses embedded `GOSSIP` section (§3.4); replies `+PONG ...` |
| `FAIL` | `FAIL <node_id>` | Marks that node `"fail"`/`"disconnected"` locally; replies `+OK` |
| `FAILOVER` | `FAILOVER <master_id> <epoch> <slots>` | Updates the named node to `"master"`, clears its `master_id`, adopts announced slots; replies `+OK` |
| `FAILOVER_AUTH_REQUEST` | `FAILOVER_AUTH_REQUEST <replica_id> <epoch> <master_id>` | Grants a vote (§3.5) or `-ERR vote rejected` |
| `FAILOVER_ANNOUNCE` | `FAILOVER_ANNOUNCE <new_master_id> <epoch> <slots>` | Promotes the named node, **strips** the announced slots from every *other* node's recorded slot list (`remove_slots`, `:1877-1889`) so stale ownership does not linger; replies `+OK` |
| anything else | — | `-ERR unknown clusterbus command` |

### 3.4 Gossip payload ingestion (part of the `PING` handler, `:1714-1767`)

Each `id,ip,port,cport,flags,epoch` entry in the `GOSSIP` payload is processed independently:

- If `flags` contains `"fail"`, the local node records the sending peer as a PFAIL *reporter* for
  that node id (`pfail_reports[gid].insert(sender_id)`) — this is how PFAIL corroboration
  propagates transitively across the cluster without every node needing a direct connection to
  every other node.
- If `flags` does not contain `"fail"`, any existing PFAIL report from that sender for that node id
  is removed (a retraction).
- If `flags == "fail"` exactly, the locally cached copy of that node is also flagged `"fail"`
  directly.
- If the node id was previously unknown and the gossip entry carries a nonzero port, it is inserted
  into `nodes` — **this is how third-party nodes are learned about transitively**, without the
  local node ever calling `CLUSTER MEET` on them directly.

### 3.5 Replica election — `start_election` (`:1276-1362`), the one place with a real quorum vote

```rust
pub fn start_election(&self) {
    let req_epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1;
    let masters = /* every known peer whose flags contain "master" and not "fail" (:1285-1292) */;
    let mut votes = 1; // self
    for (ip, cport) in &masters {
        // connect, send "FAILOVER_AUTH_REQUEST <id> <epoch> <master_id>",
        // count a "+FAILOVER_AUTH_ACK" reply (:1297-1321)
    }
    let majority = (masters.len() + 1) / 2 + 1;   // total_masters = masters.len() + 1 (self)
    if votes >= majority {
        // become master, inherit old master's slots (:1332-1342), call
        // crate::replication::get_replication_hub(self.port).make_master(),
        // broadcast FAILOVER_ANNOUNCE
    }
    self.election_in_progress.store(false, Ordering::SeqCst);
}
```

A voter (the `FAILOVER_AUTH_REQUEST` handler, §3.3, `:1828-1852`) grants a vote only if **all
three** hold: it is itself currently a master, the requested epoch is strictly newer than its own
`last_vote_epoch` (persisted via `.store`, enforcing one vote per epoch), and it believes the
claimed old master's flags already contain `"fail"`. This is the one piece of this file that is a
genuine distributed-consensus mechanism, not local bookkeeping. `CLUSTER FAILOVER FORCE` (manual
failover, `cluster_failover`, `:495-540`) bypasses the vote entirely: it directly claims mastership,
increments `current_epoch`, and broadcasts `FAILOVER` unconditionally — trusting the operator's
judgment instead of peer corroboration.

### 3.6 Slot migration planning — pure functions, no side effects

**`compute_rebalance_plan`** (`:1018-1218`) has two modes:

- **Targeted mode** (`RebalanceOptions.target_host_port` is `Some`): picks a donor — the caller
  itself if migrating *out*, or (if the caller is itself the named target) the single
  largest-slot-count other master if migrating *into* a named target (`:1092-1101`) — and pops
  `num_slots` (default 1) slots off the donor's slot list into `SlotMigrationPlan`s.
- **Auto/weighted mode** (no target specified, `:1122-1217`): for `n` masters with weights `w_i`
  (default `1.0`, floored at `0.01`, from `RebalanceOptions.weights`), computes
  `target_i = floor((w_i / Σw) * 16384)`, then distributes the `16384 - Σtarget_i` leftover slots
  one-by-one to the masters with the **largest fractional remainder** (`exact - exact.floor()`,
  sorted descending, `:1141-1156`) — a standard largest-remainder apportionment method, guaranteeing
  all 16,384 slots are accounted for exactly. Nodes within `threshold` percent of their target slot
  count (`threshold_slots = ceil((threshold/100) * (16384/n))`, default `threshold=1.25` → ~1.25% of
  the average) are left alone if **all** deltas are within tolerance (`:1167-1169`, early return of
  an empty plan). Otherwise it greedily pairs the single most over-allocated donor with the single
  most under-allocated receiver, moves `min(donor_surplus, receiver_deficit)` slots, and repeats
  until no donor/receiver pair remains (`:1173-1215`).

**`compute_reshard_plan`** (`:1220-1274`) is simpler: given explicit `target_node_id`,
`source_node_id`, `num_slots`, it just pops `min(num_slots, source.slots.len())` slots off the
source's slot list into plans — no weighting, no threshold.

Both functions **only read** `self.my_slots`/`self.nodes` and return a `Vec<SlotMigrationPlan>`;
neither mutates any `ClusterHub` state or touches `Router` at all.

### 3.7 Slot migration execution — `execute_rebalance_plans` / `CLUSTER SETSLOT ... MIGRATING`, in `connection.rs`

Unlike the cluster bus (§3.1-3.4), the slot-migration **data** path runs over Rudis's normal async,
Monoio-based client connection machinery, and genuinely transfers data. `execute_rebalance_plans`
(`connection.rs:3735-3839`, used by both `CLUSTER REBALANCE` and `CLUSTER RESHARD`) and the
`CLUSTER SETSLOT ... MIGRATING`/`MigrateSlot` handlers (`connection.rs:6802-6959`) follow the same
five-step shape per planned slot, from the **source** node's perspective:

1. **Mark migrating** in both `router.slot_states` (`SlotState::Migrating(target_addr)`, drives live
   `-ASK` redirection on this node, §3.6/§6) and `hub.slot_states`
   (`("migrating", target_node_id)`, drives `CLUSTER NODES`/`CLUSTER CHECK` reporting, §2.1) — two
   independent records of the same fact, updated together at the same call site.
2. Send `CLUSTER SETSLOT <slot> IMPORTING <my_id>` to the target over a fresh
   `monoio::net::TcpStream` so it starts accepting `ASKING`-qualified writes for keys not yet moved.
3. **Migrate keys in batches of exactly 100** (`router.get_keys_in_slot(slot, 100)`, hardcoded,
   looped until empty — `connection.rs:3778-3789` / `:6924-6937`). Each batch is handed to
   `migrate_keys_to_node` (`connection.rs:3449-3733`):
   - `router.dump_key(key)` (`router.rs:944-961`) returns the **raw internal `(RudisValue,
     Option<Duration>)`** for the key — not a real RESP `DUMP` payload/RDB encoding. This is "DUMP"
     in the conceptual sense (a point-in-time snapshot + remaining TTL), not a literal `DUMP`
     command call.
   - Each value is serialized to a type-appropriate **live write command** against the target: a
     plain `SET`/`SET ... PX <ms>` for `String`/`Int`, `HSET` (+ `PEXPIRE`) for both hash encodings,
     `RPUSH` for lists, `SADD` for sets, `ZADD` for sorted sets, a raw 16,384-byte `SET` for
     `HyperLogLog`, and one `XADD` per stream entry — all prefixed with a single `ASKING` on the
     connection (`connection.rs:3482`) so the target (whose slot is `Importing`) accepts them.
   - **`RudisValue::Tiered(_) | RudisValue::Cooled { .. } => {}`** — an **empty match arm**
     (`connection.rs:3711`). See §7 for why this is a genuine data-loss bug, not just a stylistic
     gap.
   - If `copy` is `false` (the normal migration case, as opposed to the Dragonfly-style `MIGRATE
     ... COPY` semantics), **every key returned by `dump_key`** — regardless of whether it actually
     produced any bytes in step above — is deleted locally via `router.del` (`:3726-3730`).
4. Send `CLUSTER SETSLOT <slot> NODE myself` to the target to finalize ownership.
5. Mark the slot `SlotState::Moved(target_addr)` locally, remove it from `hub.my_slots` and
   `hub.slot_states`, and (for `execute_rebalance_plans` only) append it to the target's recorded
   `ClusterNodeInfo.slots` in the local `nodes` table (`connection.rs:3806-3816`).

The **target** node, on receiving the final `CLUSTER SETSLOT <slot> NODE myself` over the wire, runs
the identical `SetSlotSubcommand::Node` handler an operator would invoke manually
(`connection.rs:6851-6889`) — including, when `node == "myself"`, a call to
**`router.set_slot_owner(slot, shard)`** (`:6864`). This is the exact trigger condition for the
routing-disagreement finding documented in Component 04 (§6 below) — live cluster slot migration
and that finding are not independent.

This is a real, working, sequential (not pipelined — see §7 for the `PIPELINE` option that does
nothing) migration path, in contrast to the Dragonfly-compatible `DFLYMIGRATE`/`DFLYCLUSTER`
command family described next.

### 3.8 `DFLYMIGRATE`/`DFLYCLUSTER` — protocol compatibility surface, bookkeeping only

`ActiveMigration` backs the Dragonfly-style migration status commands, dispatched at
`connection.rs:11832-11853`, each forwarding directly into one of:

```rust
pub fn dfly_migrate_init(&self, source_id: &str, num_shards: usize, slots: &[(u16, u16)]) {
    *self.active_migration.write().unwrap() = Some(ActiveMigration {
        state: "MIGRATING".to_string(), source_id: source_id.to_string(),
        num_shards, slots: slots.to_vec(), keys_migrated: 0,
    });
}                                                                   // cluster.rs:1466-1474
pub fn dfly_migrate_flow(&self, _source_id: &str, flow_id: u64) {
    // sets state = "SYNCING", keys_migrated += flow_id.max(1)  -- a counter bump, not a transfer
}                                                                   // cluster.rs:1476-1481
pub fn dfly_migrate_ack(&self, _flow_id: u64) {
    *self.active_migration.write().unwrap() = None;                // cluster.rs:1483-1485
}
```

These three functions only maintain an in-memory state string and a counter incremented by whatever
`flow_id` the caller happens to pass — **no keys are read, serialized, or transferred by this code
path**, confirmed unchanged from the prior revision. It exists to let clients that speak the
Dragonfly cluster-migration protocol (`DFLYMIGRATE INIT/FLOW/ACK`, `DFLYCLUSTER
MYID/CONFIG/GETSLOTINFO/FLUSHSLOTS/SLOT-MIGRATION-STATUS`) observe plausible status transitions;
real data movement for Rudis-native cluster operations goes through §3.7 instead. `DFLYCLUSTER
CONFIG <json>` (`dfly_cluster_config`, `cluster.rs:1379-1442`) is the one exception with a real
effect: it parses a Dragonfly-shaped slot-assignment JSON blob and, if any ranges are addressed to
this node (`master_id`/`id` matching `my_id`, or empty = "me"), **overwrites `hub.my_slots`
wholesale** — bookkeeping only, again; it does not touch `Router::slot_owners` either (same
decoupling as §6).

---

## 4. `CLUSTER CHECK` / Consistency Verification (`cluster_check`, `:926-1016`)

`cluster_check` builds a `HashMap<u16, Vec<String>>` of slot → claiming-node-ids from scratch on
every call (no caching):

1. If this node is a master, every slot in `self.my_slots` is attributed to `my_id`.
2. If `cluster_enabled && nodes.is_empty() && num_shards > 1` (the virtual-topology bootstrap case,
   §2.2), the same synthetic `num_shards`-way partition used by `cluster_nodes`/`cluster_slots` is
   re-derived independently here and attributed to the synthetic `{:040x}` peer ids.
3. Otherwise, every known node with `flags.contains("master")` contributes its recorded `slots`.
4. For all 16,384 slots: no claimant → `open_slots`; more than one claimant → `duplicate_slots`
   (both capped to the first 20 entries in the human-readable `format_report()` output,
   `:89-98`); exactly one claimant → counted in `total_slots_assigned`.
5. `hub.slot_states` is scanned separately and split into `migrating_slots`/`importing_slots`
   (`:991-1002`) — these are reported but do **not** affect the `ok` verdict.
6. `ok = open_slots.is_empty() && duplicate_slots.is_empty()`.

`format_report()` (`:75-109`) renders either `[OK] All 16384 slots covered by N master nodes...` or
a `[WARNING]` line with counts plus up to 20 open/duplicate slots, followed by any migrating/
importing slot lines — this is the literal string returned to a client by `CLUSTER CHECK`.

A unit test (`test_cluster_check_and_auto_rebalance_planner`, `cluster.rs:2241-2305`) exercises this
against a live `compute_rebalance_plan` call: a fresh 1-master hub reports `ok=true`,
`total_slots_assigned=16384`; adding one zero-slot peer master and calling the default-options
planner produces exactly `8192` migration plans (an even 2-way split of 16,384); after manually
rebalancing both nodes to 8,192 slots each, a second planner call on the now-balanced cluster
produces `0` plans.

---

## 5. Concrete Numbers Reference

| Constant | Value | Where |
| :--- | :--- | :--- |
| Cluster bus port offset | `port + 10000` | `ClusterHub::new`, `:169` |
| Gossip tick interval | 500 ms | `start_cluster_bus`, `:1586` |
| Accept-loop poll interval (no connection pending) | 20 ms | `:1578, :1581` |
| `MEET` connect/read timeout | 300 ms / 300 ms | `cluster_meet`, `:403, :405-406` |
| `PING`/gossip/`FAIL`/`FAILOVER_AUTH_REQUEST` connect+RW timeout | 200 ms | `cluster_bus_tick`/`start_election`/`broadcast_to_peers` |
| Inbound cluster-bus connection read/write timeout | 500 ms | `start_cluster_bus`, `:1572-1573` |
| PFAIL silence threshold | 5000 ms of no successful PONG | `:2028`, `:305`, `:310` |
| PFAIL→FAIL quorum | `(total_masters / 2) + 1` (strict majority, integer floor) | `:296`, `:1922` |
| Election majority | `(total_masters / 2) + 1` | `start_election`, `:1323` |
| Total slot space | 16,384 slots (`0..16383`) | throughout |
| Default rebalance threshold | 1.25 (percent) | `RebalanceOptions::default`, `:55` |
| Default rebalance pipeline (parsed, unused) | 16 | `RebalanceOptions::default`, `:56`; re-hardcoded to 16 again at the call site, `connection.rs:6978` |
| Default targeted-rebalance slot count | 1 | `compute_rebalance_plan`, `:1089` |
| Migration key batch size | 100 keys/round-trip, hardcoded | `connection.rs:3779`, `:6925` |
| Node ID length | 40 hex chars (`16+16+8`) | `generate_node_id`, `:163` |
| `CLUSTER NODES` open/duplicate slot list cap in text output | 20 entries | `ClusterCheckReport::format_report`, `:92`, `:98` |

---

## 6. Cross-Component Interactions: where `cluster.rs`'s bookkeeping meets `Router`'s real routing table

This section answers directly whether `cluster.rs`'s own slot-ownership/migration code is affected
by the key-routing-entry-point disagreement documented in Component 04
(`docs/internal/04_sharding_mesh.md` §4.2/§4.5). **Short answer: partially, and in a specific,
now-identified way.**

- **`Router`'s real per-command routing decision never reads `ClusterHub` for the common case.**
  `Router::target_shard`/`target_shard_for_slot` (`router.rs:240-251`) index a 16,384-entry local
  `Vec<usize>` (`slot_owners`), seeded at `Router::new` time with the same `s * 16384 / num_shards`
  formula as the virtual-topology bootstrap (§2.2), and later mutated **only** by
  `Router::set_slot_owner` (`router.rs:302-310`, broadcasts `ShardMessage::SetSlotOwner` to sibling
  shards). This table has no concept of a *different node* — every value it can ever hold is a
  local shard index `0..num_shards`.
- **`cluster.rs`'s cross-node migration path (§3.7) deliberately does not call `set_slot_owner`
  for cross-node moves.** It uses `SlotState::Moved`/`Migrating`/`Importing` (a per-shard
  `hashbrown::HashMap<u16, SlotState>`, `router.rs`'s `slot_states` field) for that, which is a
  genuinely different mechanism checked inline in `connection.rs::execute_command`
  (`:5052-5107`) on every keyed command. In that sense, **the real cross-node migration path is
  independent of the Component-04 routing-entry-point finding** — it never exercises the buggy
  code path.
- **The one place they do intersect**: the final step of every completed slot migration (§3.7 step
  4) sends `CLUSTER SETSLOT <slot> NODE myself` to the target, and that handler — run identically
  whether invoked by an operator or by the wire message — calls `router.set_slot_owner(slot,
  shard)` when `node == "myself"` (`connection.rs:6851-6871`). **This is exactly the trigger
  condition** for Component 04's finding that a stale `slot_owners` entry can cause the
  squashed-pipeline fast path (`target_shard_of_cmd`/`target_shard_and_hash_of_cmd` in
  `connection.rs`, built on **static** free functions, never consulting the now-updated dynamic
  `slot_owners`) to route a command to the wrong local shard or skip a due `-MOVED`, on a node
  running more than one shard (`num_shards > 1`) that just finished importing a slot. **Conclusion:
  cluster.rs's migration algorithm is correct in isolation; its final handshake step is the
  concrete event that arms the separately-documented squashed-pipeline routing bug.**
- **New finding this revision — `check_slot_redirection` is dead code.** `Router::check_slot_redirection`
  (`router.rs:260-284`) implements the identical `SlotState` match (`Migrating`/`Importing`/`Moved`/
  `Stable`) that `connection.rs::execute_command` inlines by hand at `:5052-5107` — but grepping the
  whole codebase shows `check_slot_redirection` is **never called anywhere** outside its own
  definition. It is not wrong, just unused; the inline copy in `connection.rs` is what actually
  runs. (Already flagged in Component 04; re-confirmed here because it lives on the exact code path
  this document also describes in §3.7.)
- **New finding this revision — `CLUSTER ADDSLOTS`/`ADDSLOTSRANGE` update `ClusterHub` bookkeeping
  but never touch `Router::slot_owners`.** `Router::cluster_addslots`/`cluster_addslotsrange`
  (`router.rs:3326-3351`) call `ClusterHub::cluster_addslots` (updates `hub.my_slots`, read by
  `CLUSTER NODES`/`SLOTS`/`CHECK`) and then only `self.set_slot_state(s, SlotState::Stable)` for
  each slot — which, since `Stable` is already the default absent an active migration, is
  effectively a no-op remove-if-present on `router.slot_states`. **Neither call touches
  `router.slot_owners`.** Concretely: on a node with `cluster_enabled = true`, calling `CLUSTER
  ADDSLOTS`/`ADDSLOTSRANGE` with a slot range that does **not** match this node's built-in
  `s * 16384 / num_shards` partition makes `CLUSTER NODES`/`CLUSTER CHECK` report the new
  assignment correctly, but does **not** change which local shard `execute_command`'s own redirect
  check (`router.target_shard_for_slot(slot)` in the `Stable` branch, `connection.rs:5073-5082`,
  gated on `router.cluster_enabled`) thinks owns that slot, and does not cause a `-MOVED` toward any
  *other* node either (the cross-node `hub.my_slots`/`hub.nodes` check at `:5083-5104` only runs in
  the `else if HAS_ACTIVE_CLUSTER` branch, which is **unreachable whenever `router.cluster_enabled`
  is `true`**, since it's an `if / else if` on the same `Stable` arm). In other words: for a
  cluster-enabled node, steady-state cross-node `-MOVED` redirection for a slot this node never
  actually owned is **not derived from `CLUSTER ADDSLOTS`/gossip topology at all** in the `Stable`
  case — only explicit `SlotState::Moved` markers (set by a completed migration, §3.7 step 5) or the
  node's own fixed `num_shards`-based partition determine local-vs-redirect. A manually
  slot-assigned (non-evenly-partitioned) real multi-process cluster relies entirely on migration
  having explicitly walked every slot through `CLUSTER SETSLOT ... NODE` at least once; a slot
  simply declared via `ADDSLOTS` without ever being migrated will be served or misrouted according
  to the startup partition, not the declared assignment.

---

## 7. Known Bugs & Limitations (re-verified + new)

1. **`CLUSTER REBALANCE ... PIPELINE n` is parsed and discarded — confirmed still present.**
   `resp.rs`'s `"REBALANCE"` parser (`:3383-3442`) correctly reads a `PIPELINE n` token into a local
   `pipeline` variable and stores it on `ClusterSubcommand::Rebalance { pipeline, .. }`. The handler
   in `connection.rs` destructures it as `pipeline: _` (`:6967`, explicitly discarded) and builds
   `RebalanceOptions { pipeline: 16, .. }` with a **hardcoded** `16` (`:6978`) instead. Migration
   always proceeds in fixed batches of 100 keys regardless of what the client requested
   (`get_keys_in_slot(slot, 100)`, hardcoded separately — see item 4). **Two separate no-ops
   stacked on the same option.**
2. **Cluster-bus messages have no authentication, integrity check, or even a length-prefixed
   framing** — any TCP client that can reach `cport` can inject `FAIL <node_id>`, `FAILOVER_ANNOUNCE
   ...`, or a crafted `GOSSIP` payload and have it accepted verbatim by
   `handle_cluster_bus_conn`. (Design-level, not new to this revision, but re-confirmed.)
3. **Node IDs are not collision-resistant** — `generate_node_id` uses two 64-bit `fxhash` values
   over `(port, time_ns)`, not a real UUID/random-128-bit scheme (§2.1).
4. **Migration is always fully sequential, unpipelined, and capped at 100 keys per round-trip
   RTT** — `migrate_keys_to_node` `await`s a full write+read round trip per 100-key batch; large
   slots (millions of keys) will migrate key-batch-by-key-batch, one network RTT at a time.
5. **Fixed — migration no longer drops keys the target didn't store.** `migrate_keys_to_node`
   used to re-encode each value as SET/RPUSH/HSET/... commands, emitted nothing for a key still
   tiered or cooled, read a single 1 KB reply without checking it, and then deleted every source
   key regardless. It now sends each key as `RESTORE key ttl <DUMP payload> [REPLACE] [ABSTTL]`
   (DUMP works straight from the tier record, so tiered keys move even over `maxmemory`), reads
   one reply per key with a timeout, and deletes a source key only once the target replied `+OK`
   for it, and only if the key still has the dumped value (`Router::del_if_unchanged`, checked
   and deleted on the owning shard in one step). Any rejected key makes the call return an error
   (`-ERR Target instance replied with error: ...` for MIGRATE), so slot migration stops instead
   of moving on with the key left behind. Slot migration uses REPLACE; MIGRATE honours its own
   `COPY`, `REPLACE` and `timeout` arguments. `CLUSTER SETSLOT ... MIGRATING`, `CLUSTER
   REBALANCE`, `CLUSTER RESHARD` and `MIGRATE` all share this one path.
6. **`Router::check_slot_redirection` is unreachable dead code** — see §6. Not a behavioral bug
   (the inline copy in `execute_command` is correct and is what runs), but a maintenance hazard:
   a future fix applied to one copy and not the other would silently diverge.
7. **`CLUSTER ADDSLOTS`/`ADDSLOTSRANGE` do not affect real command routing** — see §6. They update
   `ClusterHub` (and thus `CLUSTER NODES`/`SLOTS`/`SHARDS`/`CHECK` output) but never touch
   `Router::slot_owners`, so a non-default (non-evenly-partitioned) slot assignment made purely via
   `ADDSLOTS` is invisible to the actual request-routing/redirect logic until each affected slot is
   separately walked through a real `CLUSTER SETSLOT ... NODE` migration.
8. **`DFLYMIGRATE`/`DFLYCLUSTER`'s migration-status surface reports state transitions without a
   real underlying transfer** — confirmed unchanged (§3.8). If Dragonfly-protocol clients are
   expected to drive actual cross-node data movement (as opposed to just observing status for
   compatibility testing), this path needs the same DUMP-and-replay treatment the native
   `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD` path already has (§3.7).
9. **No `CLUSTER BUMPEPOCH` / `CLUSTER SET-CONFIG-EPOCH` support** — `resp.rs`'s `CLUSTER` parser
   (`:3150-3495`) has no match arm for either; unrecognized subcommands fall through to
   `Command::Unknown(format!("CLUSTER {}", sub))` (`:3493`).

---

## 8. Contributor Gotchas, Invariants & Debugging Guide

* **Gotcha 1**: PFAIL-to-FAIL escalation requires strict majority quorum corroboration via gossip
  (§3.2) — a single node's missed-PONG timer alone only ever produces a local, unescalated
  `"fail?"`, never a broadcast `FAIL`. Quorum is `(total_masters / 2) + 1`, recomputed fresh on
  every `cluster_bus_tick` and every `cluster_nodes()` render — there is no cached/stale quorum
  value to worry about.
* **Gotcha 2**: The cluster bus runs on dedicated blocking OS threads (one listener thread plus one
  thread per inbound connection), entirely outside the async/Monoio reactor that serves client
  traffic — do not assume cluster-bus code can use `await` or touch shard-local state directly.
* **Gotcha 3**: `CLUSTER SETSLOT`/`REBALANCE`/`RESHARD` perform real, synchronous, unpipelined
  per-key data migration; `DFLYMIGRATE`/`DFLYCLUSTER` status commands are bookkeeping-only and
  transfer no data (§3.7 vs §3.8) — do not conflate the two when reasoning about whether a given
  migration path actually moves keys.
* **Gotcha 4**: There are **three** independent slot-authority records — `router.slot_states`
  (per-shard, drives live `-MOVED`/`-ASK`), `router.slot_owners` (per-shard, drives local-vs-remote
  dispatch, only ever points to a *local* shard), and `ClusterHub` (`my_slots`/`nodes`/`slot_states`,
  drives `CLUSTER NODES`/`SLOTS`/`CHECK` output and gossip). They are kept in sync by the same call
  sites today (§3.7), but `ADDSLOTS`/`ADDSLOTSRANGE` only update the `ClusterHub` copy (§6/§7#7) —
  do not assume that issuing `CLUSTER ADDSLOTS` alone changes what a node will actually serve versus
  redirect.
* **Gotcha 5**: if testing migration of large tiered datasets under `maxmemory` pressure, check for
  silently-dropped keys (§7#5) — `CLUSTER CHECK`/`total_slots_assigned` will look fine even if data
  was lost, since slot *ownership* bookkeeping and *key* migration completeness are tracked
  separately and neither cross-checks the other's key count.

### How to Verify Changes
```bash
# 1. Format check
cargo fmt --check

# 2. Clippy verification with zero warnings
cargo clippy --all-targets -- -D warnings

# 3. Run unit tests (cluster.rs's own tests are in its #[cfg(test)] mod, cluster.rs:2094-2306)
cargo test --lib -- --test-threads=1
```
