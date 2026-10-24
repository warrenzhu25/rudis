use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

/// What this node believes about a peer's health (Redis's PFAIL/FAIL).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FailState {
    #[default]
    Ok,
    /// This node got no PONG from the peer within the node timeout.
    PFail,
    /// A majority of masters agree the peer is unreachable: its replicas
    /// may take over.
    Fail,
}

/// Default `cluster-node-timeout`: a peer silent this long is PFAIL.
pub const NODE_TIMEOUT_MS: u64 = 5000;

#[derive(Clone, Debug)]
pub struct ClusterNodeInfo {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub cport: u16,
    /// The peer's role, "master" or "slave". Health is kept apart in
    /// `fail`, so a failing node keeps its role (and its slots stay known).
    pub flags: String,
    pub fail: FailState,
    pub master_id: String,
    pub ping_sent: u64,
    pub pong_recv: u64,
    pub config_epoch: u64,
    pub link_state: String, // "connected", "disconnected"
    pub slots: Vec<(u16, u16)>,
}

impl ClusterNodeInfo {
    pub fn is_master(&self) -> bool {
        self.flags != "slave"
    }

    /// Flags as CLUSTER NODES shows them, e.g. `master,fail?`.
    pub fn flags_repr(&self) -> String {
        match self.fail {
            FailState::Ok => self.flags.clone(),
            FailState::PFail => format!("{},fail?", self.flags),
            FailState::Fail => format!("{},fail", self.flags),
        }
    }
}

/// Splits CLUSTER NODES style flags (`myself,slave,fail?`, or `|`
/// separated as gossip carries them) into the role and the health they
/// carry.
pub fn parse_flags(flags: &str) -> (String, FailState) {
    let mut role = "master";
    let mut fail = FailState::Ok;
    for f in flags.split([',', '|']) {
        match f {
            "slave" | "replica" => role = "slave",
            "fail" => fail = FailState::Fail,
            "fail?" if fail == FailState::Ok => fail = FailState::PFail,
            _ => {}
        }
    }
    (role.to_string(), fail)
}

#[derive(Clone, Debug)]
pub struct ActiveMigration {
    pub state: String,
    pub source_id: String,
    pub num_shards: usize,
    pub slots: Vec<(u16, u16)>,
    pub keys_migrated: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SlotMigrationPlan {
    pub slot: u16,
    pub source_node_id: String,
    pub source_addr: String,
    pub target_node_id: String,
    pub target_addr: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RebalanceOptions {
    pub weights: HashMap<String, f64>,
    pub simulate: bool,
    pub threshold: f64,
    pub pipeline: usize,
    pub target_host_port: Option<(String, u16, Option<usize>)>,
}

impl Default for RebalanceOptions {
    fn default() -> Self {
        Self {
            weights: HashMap::new(),
            simulate: false,
            threshold: 1.25,
            pipeline: 16,
            target_host_port: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
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

impl ClusterCheckReport {
    pub fn format_report(&self) -> String {
        let mut out = String::new();
        if self.ok {
            out.push_str(&format!(
                "[OK] All 16384 slots covered by {} master nodes. 0 open slots, 0 duplicate slots.\r\n",
                self.masters
            ));
        } else {
            out.push_str(&format!(
                "[WARNING] Cluster check found issues: {} slots assigned, {} open/unassigned, {} duplicate.\r\n",
                self.total_slots_assigned,
                self.open_slots.len(),
                self.duplicate_slots.len()
            ));
            if !self.open_slots.is_empty() {
                out.push_str(&format!(
                    "Open slots: {:?}\r\n",
                    &self.open_slots[..self.open_slots.len().min(20)]
                ));
            }
            if !self.duplicate_slots.is_empty() {
                out.push_str(&format!(
                    "Duplicate slots: {:?}\r\n",
                    &self.duplicate_slots[..self.duplicate_slots.len().min(20)]
                ));
            }
        }
        if !self.migrating_slots.is_empty() {
            out.push_str(&format!("Migrating slots: {:?}\r\n", self.migrating_slots));
        }
        if !self.importing_slots.is_empty() {
            out.push_str(&format!("Importing slots: {:?}\r\n", self.importing_slots));
        }
        out
    }
}

pub struct ClusterHub {
    pub port: u16,
    pub cport: u16,
    pub my_id: RwLock<String>,
    pub current_epoch: AtomicU64,
    pub config_epoch: AtomicU64,
    pub last_vote_epoch: AtomicU64,
    pub election_in_progress: AtomicBool,
    pub role: RwLock<String>,      // "master" or "slave"
    pub master_id: RwLock<String>, // "-" or ID
    pub has_nodes: AtomicBool,
    pub nodes: RwLock<HashMap<String, ClusterNodeInfo>>,
    pub my_slots: RwLock<Vec<(u16, u16)>>,
    pub pfail_reports: RwLock<HashMap<String, HashSet<String>>>,
    pub bus_running: AtomicBool,
    pub cancel_bus: RwLock<Option<flume::Sender<()>>>,
    pub active_migration: RwLock<Option<ActiveMigration>>,
    pub slot_states: RwLock<HashMap<u16, (String, String)>>,
    pub cluster_enabled: AtomicBool,
    pub num_shards: std::sync::atomic::AtomicUsize,
    pub config_file: RwLock<Option<std::path::PathBuf>>,
    /// Master this node must start replicating, set when it lost all its
    /// slots to a newer claimant; the shards' runtime picks it up (see
    /// `take_follow`).
    pub follow: parking_lot::Mutex<Option<(String, u16)>>,
    /// `cluster-node-timeout` (ms): a peer silent this long is PFAIL, and
    /// failover starts about this long after a master dies.
    pub node_timeout_ms: AtomicU64,
}

pub static HAS_ACTIVE_CLUSTER: AtomicBool = AtomicBool::new(false);

/// Bumped whenever a hub is added to `CLUSTER_HUBS` (hubs are never
/// removed or replaced), so per-thread copies know when to refresh.
static CLUSTER_HUBS_GEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

thread_local! {
    /// Per-thread copy of `CLUSTER_HUBS` and the generation it was taken at.
    /// `has_active_cluster` runs on every keyed command; the global read
    /// lock it used to take is an atomic RMW on a cache line shared by all
    /// shards.
    static CLUSTER_HUBS_TLS: std::cell::RefCell<(u64, Vec<(u16, Arc<ClusterHub>)>)> =
        const { std::cell::RefCell::new((u64::MAX, Vec::new())) };
}

#[inline]
pub fn has_active_cluster(port: u16) -> bool {
    CLUSTER_HUBS_TLS.with(|cache| {
        let gen_now = CLUSTER_HUBS_GEN.load(Ordering::Acquire);
        let mut c = cache.borrow_mut();
        if c.0 != gen_now {
            let hubs = CLUSTER_HUBS.read();
            c.1 = hubs.iter().map(|(p, h)| (*p, h.clone())).collect();
            c.0 = gen_now;
        }
        if let Some((_, hub)) = c.1.iter().find(|(p, _)| *p == port) {
            hub.has_nodes.load(Ordering::Relaxed) || hub.cluster_enabled.load(Ordering::Relaxed)
        } else {
            HAS_ACTIVE_CLUSTER.load(Ordering::Relaxed)
        }
    })
}

static CLUSTER_HUBS: LazyLock<RwLock<HashMap<u16, Arc<ClusterHub>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn get_cluster_hub(port: u16) -> Arc<ClusterHub> {
    {
        let hubs = CLUSTER_HUBS.read();
        if let Some(hub) = hubs.get(&port) {
            return hub.clone();
        }
    }
    let mut hubs = CLUSTER_HUBS.write();
    if let Some(hub) = hubs.get(&port) {
        return hub.clone();
    }
    let hub = Arc::new(ClusterHub::new(port));
    hubs.insert(port, hub.clone());
    CLUSTER_HUBS_GEN.fetch_add(1, Ordering::Release);
    hub
}

pub fn generate_node_id(port: u16) -> String {
    use fxhash::hash64;
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let h1 = hash64(&port.to_le_bytes());
    let h2 = hash64(&t.to_le_bytes());
    format!("{:016x}{:016x}{:08x}", h1, h2, port)
}

impl ClusterHub {
    pub fn new(port: u16) -> Self {
        let my_id = generate_node_id(port);
        let cport = port + 10000;

        Self {
            port,
            cport,
            my_id: RwLock::new(my_id),
            current_epoch: AtomicU64::new(1),
            config_epoch: AtomicU64::new(0),
            last_vote_epoch: AtomicU64::new(0),
            election_in_progress: AtomicBool::new(false),
            role: RwLock::new("master".to_string()),
            master_id: RwLock::new("-".to_string()),
            has_nodes: AtomicBool::new(false),
            nodes: RwLock::new(HashMap::new()),
            my_slots: RwLock::new(vec![(0, 16383)]),
            pfail_reports: RwLock::new(HashMap::new()),
            bus_running: AtomicBool::new(false),
            cancel_bus: RwLock::new(None),
            active_migration: RwLock::new(None),
            slot_states: RwLock::new(HashMap::new()),
            cluster_enabled: AtomicBool::new(false),
            num_shards: std::sync::atomic::AtomicUsize::new(1),
            config_file: RwLock::new(None),
            follow: parking_lot::Mutex::new(None),
            node_timeout_ms: AtomicU64::new(NODE_TIMEOUT_MS),
        }
    }

    /// Whether this node serves `slot`.
    pub fn owns_slot(&self, slot: u16) -> bool {
        self.my_slots
            .read()
            .iter()
            .any(|&(s, e)| slot >= s && slot <= e)
    }

    /// Whether this node is a replica of the master serving `slot`.
    pub fn replicates_slot(&self, slot: u16) -> bool {
        if *self.role.read() != "slave" {
            return false;
        }
        let master_id = self.master_id.read().clone();
        self.nodes
            .read()
            .get(&master_id)
            .is_some_and(|m| m.slots.iter().any(|&(s, e)| slot >= s && slot <= e))
    }

    /// The master this node was demoted to replicate, once (see `follow`).
    pub fn take_follow(&self) -> Option<(String, u16)> {
        self.follow.lock().take()
    }

    /// Records that `id` serves `slots` at config `epoch`, as its PING or
    /// PONG says. Each slot belongs to the claimant with the highest config
    /// epoch (Redis's `clusterUpdateSlotsConfigWith`): older claims by
    /// other nodes give way, as do ours, and a claim older than a slot's
    /// current owner is ignored for that slot. A master that loses all its
    /// slots this way (it was failed over while down or cut off) becomes a
    /// replica of the claimant, so two nodes never serve the same slots.
    pub fn claim_slots(&self, id: &str, epoch: Option<u64>, slots: Vec<(u16, u16)>) {
        // Like Redis's currentEpoch: the highest epoch seen anywhere, so an
        // epoch we pick next (failover, election) outranks every claim.
        if let Some(e) = epoch {
            self.current_epoch.fetch_max(e, Ordering::SeqCst);
        }
        // Our own state first: never take `my_slots` under `nodes`.
        let my_epoch = self.config_epoch.load(Ordering::SeqCst);
        let my_slots = self.my_slots.read().clone();
        let owns =
            |ranges: &[(u16, u16)], slot: u16| ranges.iter().any(|&(s, e)| slot >= s && slot <= e);
        let mut lost: Vec<u16> = Vec::new();
        let master_addr = {
            let mut nodes = self.nodes.write();
            let Some(node) = nodes.get(id) else {
                return;
            };
            let epoch = match epoch {
                Some(e) if e < node.config_epoch => return,
                Some(e) => e,
                None => node.config_epoch,
            };
            // Slots someone else holds at this claim's epoch or newer: on
            // a tie the current holder keeps them, so a slot never has two
            // owners.
            let mut newer = vec![false; 16384];
            let mut mark = |ranges: &[(u16, u16)]| {
                for &(s, e) in ranges {
                    newer[s as usize..=e as usize].fill(true);
                }
            };
            for (other, n) in nodes.iter() {
                if other != id && n.config_epoch >= epoch {
                    mark(&n.slots);
                }
            }
            if my_epoch >= epoch {
                mark(&my_slots);
            }
            let accepted: Vec<u16> = slots
                .iter()
                .flat_map(|&(s, e)| s..=e)
                .filter(|&slot| !newer[slot as usize])
                .collect();
            let my_master = self.master_id.read().clone();
            let master_had_slots = nodes.get(&my_master).is_some_and(|m| !m.slots.is_empty());
            for (other, n) in nodes.iter_mut() {
                if other != id && n.config_epoch < epoch {
                    remove_slots(&mut n.slots, &accepted);
                }
            }
            // Our master's slots all went to `id` (it was failed over, maybe
            // to another of its replicas): like Redis, follow the new owner.
            if *self.role.read() == "slave"
                && my_master != id
                && master_had_slots
                && nodes.get(&my_master).is_some_and(|m| m.slots.is_empty())
                && !accepted.is_empty()
            {
                let new_master = nodes.get(id).map(|n| (n.ip.clone(), n.port));
                if let Some(addr) = new_master {
                    crate::log_warning!(
                        "Cluster: my master {} was failed over to {}: following it",
                        my_master,
                        id
                    );
                    *self.master_id.write() = id.to_string();
                    *self.follow.lock() = Some(addr);
                }
            }
            if my_epoch < epoch {
                lost = accepted
                    .iter()
                    .copied()
                    .filter(|&s| owns(&my_slots, s))
                    .collect();
            }
            let node = nodes.get_mut(id).expect("checked above");
            node.config_epoch = epoch;
            let mut ranges: Vec<(u16, u16)> = accepted.iter().map(|&s| (s, s)).collect();
            compact_slots(&mut ranges);
            node.slots = ranges;
            (node.ip.clone(), node.port)
        };
        if lost.is_empty() {
            return;
        }
        let emptied = {
            let mut mine = self.my_slots.write();
            remove_slots(&mut mine, &lost);
            mine.is_empty()
        };
        crate::log_warning!(
            "Cluster: {} slots now belong to {} (config epoch {:?} > ours {})",
            lost.len(),
            id,
            epoch,
            my_epoch
        );
        if emptied && *self.role.read() == "master" {
            crate::log_warning!("Cluster: lost all my slots to {}: becoming its replica", id);
            *self.role.write() = "slave".to_string();
            *self.master_id.write() = id.to_string();
            *self.follow.lock() = Some(master_addr);
        }
        let _ = self.save_config();
    }

    pub fn set_config_file(&self, path: std::path::PathBuf) {
        *self.config_file.write() = Some(path);
    }

    pub fn config_file_name(&self) -> String {
        self.config_file
            .read()
            .as_ref()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "nodes.conf".to_string())
    }

    pub fn save_config(&self) -> Result<(), String> {
        let path_opt = self.config_file.read().clone();
        let Some(path) = path_opt else {
            return Ok(());
        };
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            let _ = std::fs::create_dir_all(parent);
        }
        let mut content = self.render_nodes(false);
        content.push_str(&format!(
            "vars currentEpoch {} lastVoteEpoch {}\n",
            self.current_epoch.load(Ordering::Relaxed),
            self.last_vote_epoch.load(Ordering::Relaxed)
        ));
        let tmp_path = path.with_extension(format!("tmp.{}", self.port));
        std::fs::write(&tmp_path, content.as_bytes())
            .map_err(|e| format!("Failed writing cluster config file: {}", e))?;
        std::fs::rename(&tmp_path, &path)
            .map_err(|e| format!("Failed renaming cluster config file: {}", e))?;
        Ok(())
    }

    pub fn load_config(&self, path: &std::path::Path) -> Result<bool, String> {
        if !path.exists() {
            return Ok(false);
        }
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed reading cluster config file: {}", e))?;
        let mut parsed_peers = HashMap::new();
        let mut parsed_my_slots = Vec::new();
        let mut parsed_slot_states = HashMap::new();
        let mut saw_myself = false;

        for raw_line in content.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.is_empty() {
                continue;
            }
            if parts[0] == "vars" {
                let mut i = 1;
                while i + 1 < parts.len() {
                    match parts[i] {
                        "currentEpoch" => {
                            if let Ok(v) = parts[i + 1].parse::<u64>() {
                                self.current_epoch.store(v, Ordering::SeqCst);
                            }
                            i += 2;
                        }
                        "lastVoteEpoch" => {
                            if let Ok(v) = parts[i + 1].parse::<u64>() {
                                self.last_vote_epoch.store(v, Ordering::SeqCst);
                            }
                            i += 2;
                        }
                        _ => i += 1,
                    }
                }
                continue;
            }
            if parts.len() < 8 {
                continue;
            }
            let id = parts[0].to_string();
            let addr_part = parts[1];
            let (ip_port, cport_opt) = match addr_part.split_once('@') {
                Some((ip_p, cp)) => (ip_p, cp.parse::<u16>().ok()),
                None => (addr_part, None),
            };
            let (ip, port) = match ip_port.rsplit_once(':') {
                Some((h, p)) => (h.to_string(), p.parse::<u16>().unwrap_or(self.port)),
                None => ("127.0.0.1".to_string(), self.port),
            };
            let cport = cport_opt.unwrap_or_else(|| port.saturating_add(10000));
            let flags = parts[2].to_string();
            let master_id = parts[3].to_string();
            let ping_sent = parts[4].parse::<u64>().unwrap_or(0);
            let pong_recv = parts[5].parse::<u64>().unwrap_or(0);
            let config_epoch = parts[6].parse::<u64>().unwrap_or(0);
            let link_state = parts[7].to_string();

            let mut node_slots = Vec::new();
            for token in &parts[8..] {
                if token.starts_with('[') && token.ends_with(']') {
                    let inner = &token[1..token.len() - 1];
                    if let Some((slot_str, target_id)) = inner.split_once("->-") {
                        if let Ok(slot) = slot_str.parse::<u16>() {
                            parsed_slot_states
                                .insert(slot, ("migrating".to_string(), target_id.to_string()));
                        }
                    } else if let Some((slot_str, source_id)) = inner.split_once("-<-")
                        && let Ok(slot) = slot_str.parse::<u16>()
                    {
                        parsed_slot_states
                            .insert(slot, ("importing".to_string(), source_id.to_string()));
                    }
                } else if let Some((s_str, e_str)) = token.split_once('-') {
                    if let (Ok(s), Ok(e)) = (s_str.parse::<u16>(), e_str.parse::<u16>()) {
                        node_slots.push((s, e));
                    }
                } else if let Ok(s) = token.parse::<u16>() {
                    node_slots.push((s, s));
                }
            }
            compact_slots(&mut node_slots);

            if flags.split(',').any(|f| f == "myself") {
                saw_myself = true;
                *self.my_id.write() = id;
                let role = if flags.split(',').any(|f| f == "slave" || f == "replica") {
                    "slave"
                } else {
                    "master"
                };
                *self.role.write() = role.to_string();
                *self.master_id.write() = master_id;
                self.config_epoch.store(config_epoch, Ordering::SeqCst);
                self.current_epoch.fetch_max(config_epoch, Ordering::SeqCst);
                parsed_my_slots = node_slots;
            } else if is_own_shard_pseudo_node(self.port, &id, port) {
                // Written by older versions (see `render_nodes`): one of our
                // own shards, not a peer.
                continue;
            } else {
                let clean_flags = flags
                    .split(',')
                    .filter(|&f| f != "myself")
                    .collect::<Vec<_>>()
                    .join(",");
                parsed_peers.insert(
                    id.clone(),
                    ClusterNodeInfo {
                        id,
                        ip,
                        port,
                        cport,
                        flags: parse_flags(&clean_flags).0,
                        fail: parse_flags(&clean_flags).1,
                        master_id,
                        ping_sent,
                        pong_recv,
                        config_epoch,
                        link_state,
                        slots: node_slots,
                    },
                );
            }
        }

        if saw_myself {
            *self.my_slots.write() = parsed_my_slots;
            *self.slot_states.write() = parsed_slot_states;
        }
        if !parsed_peers.is_empty() {
            *self.nodes.write() = parsed_peers;
            self.has_nodes.store(true, Ordering::Release);
        }
        Ok(saw_myself)
    }

    pub fn my_id(&self) -> String {
        self.my_id.read().clone()
    }

    /// Gives this node a new config epoch after its own slot set changed.
    ///
    /// Peers only accept a node's slot list if it carries an epoch at least
    /// as new as the last one they applied, so the bump is what makes the
    /// change win over announcements built before it.
    pub fn bump_config_epoch(&self) -> u64 {
        let next = self
            .config_epoch
            .load(Ordering::SeqCst)
            .max(self.current_epoch.load(Ordering::SeqCst))
            + 1;
        self.config_epoch.store(next, Ordering::SeqCst);
        self.current_epoch.fetch_max(next, Ordering::SeqCst);
        let _ = self.save_config();
        next
    }

    /// Sets the node configuration epoch if it is currently 0 and no slots are assigned.
    pub fn set_config_epoch(&self, epoch: u64) -> Result<(), String> {
        if epoch == 0 {
            return Err("ERR Invalid config epoch specified".to_string());
        }
        let current_cfg = self.config_epoch.load(Ordering::SeqCst);
        if current_cfg != 0 {
            return Err(format!(
                "ERR Node config epoch is already set to {}",
                current_cfg
            ));
        }
        let slots = self.my_slots.read();
        let has_explicit_slots = !slots.is_empty() && !(slots.len() == 1 && slots[0] == (0, 16383));
        if has_explicit_slots {
            return Err(
                "ERR Node has slots assigned to it. Setting config epoch is not allowed."
                    .to_string(),
            );
        }
        drop(slots);
        {
            let mut my_slots = self.my_slots.write();
            if my_slots.len() == 1 && my_slots[0] == (0, 16383) {
                my_slots.clear();
            }
        }
        self.config_epoch.store(epoch, Ordering::SeqCst);
        self.current_epoch.fetch_max(epoch, Ordering::SeqCst);
        let _ = self.save_config();
        Ok(())
    }

    /// Records in the local view that `owner_id` (this node or a known peer)
    /// now serves `slot`, and that no other node does.
    pub fn assign_slot(&self, slot: u16, owner_id: &str) {
        let me = owner_id == self.my_id();
        {
            let mut my_slots = self.my_slots.write();
            if me {
                if !my_slots.iter().any(|&(s, e)| slot >= s && slot <= e) {
                    my_slots.push((slot, slot));
                    compact_slots(&mut my_slots);
                }
            } else {
                remove_slots(&mut my_slots, &[slot]);
            }
        }
        {
            let mut nodes = self.nodes.write();
            for (id, node) in nodes.iter_mut() {
                if id == owner_id {
                    if !node.slots.iter().any(|&(s, e)| slot >= s && slot <= e) {
                        node.slots.push((slot, slot));
                        compact_slots(&mut node.slots);
                    }
                } else if node.slots.iter().any(|&(s, e)| slot >= s && slot <= e) {
                    remove_slots(&mut node.slots, &[slot]);
                }
            }
        }
        let _ = self.save_config();
    }

    /// The `PING` this node sends on the cluster bus.
    fn ping_frame(&self, gossip: &str) -> String {
        format!(
            "PING {} {} {} {} GOSSIP {}\r\n",
            self.my_id(),
            self.config_epoch.load(Ordering::Relaxed),
            self.role.read(),
            slots_repr(&self.my_slots.read()),
            gossip
        )
    }

    /// Applies a `+PONG <id> <epoch> <role> <slots>` reply from `node_id`.
    /// Returns whether it was a PONG.
    pub fn apply_pong(&self, node_id: &str, resp: &str) -> bool {
        if !resp.starts_with("+PONG") {
            return false;
        }
        let parts: Vec<&str> = resp.split_whitespace().collect();
        let epoch = parts.get(2).and_then(|v| v.parse::<u64>().ok());
        let slots = parts.get(4).map_or_else(Vec::new, |r| parse_slot_ranges(r));
        let mut key = node_id;
        {
            let mut nodes = self.nodes.write();
            // A MEET whose handshake reply was lost leaves the peer under a
            // placeholder id; its PONG names it, so move the entry to the real id.
            if let Some(&real_id) = parts.get(1)
                && real_id != node_id
            {
                if let Some(mut node) = nodes.remove(node_id)
                    && !nodes.contains_key(real_id)
                {
                    node.id = real_id.to_string();
                    nodes.insert(real_id.to_string(), node);
                }
                key = real_id;
            }
            if let Some(node) = nodes.get_mut(key)
                && let Some(role) = parts.get(3)
            {
                node.flags = parse_flags(role).0;
            }
        }
        self.claim_slots(key, epoch, slots);
        true
    }

    /// Exchanges a fresh PING/PONG with `node_id`, so both sides see each
    /// other's current slots and epoch right away instead of on the next tick.
    pub async fn refresh_peer(&self, node_id: &str) {
        use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
        let addr = match self.nodes.read().get(node_id) {
            Some(n) => format!("{}:{}", n.ip, n.cport),
            None => return,
        };
        let Ok(addr) = addr.parse::<SocketAddr>() else {
            return;
        };
        let exchange = async {
            let mut stream = monoio::net::TcpStream::connect(addr).await.ok()?;
            let (res, _) = stream.write_all(self.ping_frame("").into_bytes()).await;
            res.ok()?;
            let (res, buf) = stream.read(Vec::with_capacity(512)).await;
            res.ok()?;
            Some(String::from_utf8_lossy(&buf).into_owned())
        };
        if let Ok(Some(resp)) = monoio::time::timeout(Duration::from_millis(500), exchange).await {
            self.apply_pong(node_id, &resp);
        }
    }

    pub fn cluster_nodes(&self) -> String {
        self.render_nodes(true)
    }

    /// CLUSTER NODES. With `single_node_view`, a cluster-mode node that
    /// knows no peers shows each of its shards as a node of its own (each
    /// listens on its own port), as clients see it. That view is never
    /// saved to nodes.conf: read back, the pseudo nodes became peers that
    /// never answer, and their slot claims outlived any real owner's.
    fn render_nodes(&self, single_node_view: bool) -> String {
        let mut out = String::new();

        // 1. Myself entry
        let my_id = self.my_id();
        let role = self.role.read().clone();
        let master_id = self.master_id.read().clone();
        let cfg_epoch = self.config_epoch.load(Ordering::Relaxed);
        let is_cluster = self.cluster_enabled.load(Ordering::Relaxed);
        let num_shards = self.num_shards.load(Ordering::Relaxed).max(1);
        let my_slots_snapshot = self.my_slots.read().clone();
        let has_default_all_slots =
            my_slots_snapshot.len() == 1 && my_slots_snapshot[0] == (0, 16383);
        let my_slots_str = if single_node_view
            && is_cluster
            && self.nodes.read().is_empty()
            && has_default_all_slots
        {
            let end_slot = 16384 / num_shards - 1;
            format!(" 0-{}", end_slot)
        } else {
            let mut s_str = String::new();
            for (s, e) in &my_slots_snapshot {
                if s == e {
                    s_str.push_str(&format!(" {}", s));
                } else {
                    s_str.push_str(&format!(" {}-{}", s, e));
                }
            }
            s_str
        };

        let migrating_str = {
            let states = self.slot_states.read();
            let mut s = String::new();
            let mut sorted_slots: Vec<_> = states.keys().copied().collect();
            sorted_slots.sort();
            for slot in sorted_slots {
                if let Some((state, node_id)) = states.get(&slot) {
                    if state == "migrating" {
                        s.push_str(&format!(" [{}->-{}]", slot, node_id));
                    } else if state == "importing" {
                        s.push_str(&format!(" [{}-<-{}]", slot, node_id));
                    }
                }
            }
            s
        };

        let my_flags = format!("myself,{}", role);
        out.push_str(&format!(
            "{} 127.0.0.1:{}@{} {} {} 0 0 {} connected{}{}\n",
            my_id,
            self.port,
            self.cport,
            my_flags,
            master_id,
            cfg_epoch,
            my_slots_str,
            migrating_str
        ));

        // 2. Peer entries
        let nodes = self.nodes.read();
        if single_node_view && is_cluster && nodes.is_empty() {
            for s in 1..num_shards {
                let peer_port = self.port + s as u16;
                let peer_cport = peer_port + 10000;
                let peer_start = s * 16384 / num_shards;
                let peer_end = if s == num_shards - 1 {
                    16383
                } else {
                    (s + 1) * 16384 / num_shards - 1
                };
                let peer_id = format!("{:040x}", s + 1);
                out.push_str(&format!(
                    "{} 127.0.0.1:{}@{} master - 0 0 {} connected {}-{}\n",
                    peer_id,
                    peer_port,
                    peer_cport,
                    s + 1,
                    peer_start,
                    peer_end
                ));
            }
            return out;
        }
        // Health is judged by the bus tick (`cluster_bus_tick`); this only
        // reports it.
        let mut keys: Vec<String> = nodes.keys().cloned().collect();
        keys.sort();

        for id in keys {
            if id == my_id {
                continue;
            }
            if let Some(node) = nodes.get(&id) {
                let mut peer_slots_str = String::new();
                for (s, e) in &node.slots {
                    if s == e {
                        peer_slots_str.push_str(&format!(" {}", s));
                    } else {
                        peer_slots_str.push_str(&format!(" {}-{}", s, e));
                    }
                }

                out.push_str(&format!(
                    "{} {}:{}@{} {} {} {} {} {} {}{}\n",
                    node.id,
                    node.ip,
                    node.port,
                    node.cport,
                    node.flags_repr(),
                    node.master_id,
                    node.ping_sent,
                    node.pong_recv,
                    node.config_epoch,
                    node.link_state,
                    peer_slots_str
                ));
            }
        }

        out
    }

    pub fn cluster_info(&self) -> String {
        let nodes = self.nodes.read();
        let total_nodes = nodes.len() + 1; // peers + self
        let pfail_count = nodes
            .values()
            .filter(|n| n.fail == FailState::PFail)
            .count();
        let fail_count = nodes.values().filter(|n| n.fail == FailState::Fail).count();
        // Down while a failed master still serves slots (no replica took
        // them over yet).
        let uncovered = nodes
            .values()
            .any(|n| n.fail == FailState::Fail && n.is_master() && !n.slots.is_empty());
        let state = if uncovered { "fail" } else { "ok" };
        let cur_epoch = self.current_epoch.load(Ordering::Relaxed);
        let cfg_epoch = self.config_epoch.load(Ordering::Relaxed);
        format!(
            "cluster_state:{}\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:{}\r\ncluster_slots_fail:{}\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\ncluster_current_epoch:{}\r\ncluster_my_epoch:{}\r\ncluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\n",
            state, pfail_count, fail_count, total_nodes, total_nodes, cur_epoch, cfg_epoch
        )
    }

    pub fn cluster_meet(&self, ip: &str, port: u16) -> Result<(), String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        if port == self.port {
            return Ok(());
        }

        let Some(cport) = bus_port(port) else {
            return Err(format!("Invalid node address specified: {ip}:{port}"));
        };
        let temp_id = generate_node_id(port);

        // Pre-insert peer into nodes table so it is known immediately
        {
            let mut nodes = self.nodes.write();
            if !nodes.values().any(|n| n.port == port) {
                nodes.insert(
                    temp_id.clone(),
                    ClusterNodeInfo {
                        id: temp_id.clone(),
                        ip: ip.to_string(),
                        port,
                        cport,
                        flags: "master".to_string(),
                        fail: FailState::Ok,
                        master_id: "-".to_string(),
                        ping_sent: now,
                        pong_recv: now,
                        // Unknown until the peer answers; any real epoch is newer.
                        config_epoch: 0,
                        link_state: "connected".to_string(),
                        slots: Vec::new(),
                    },
                );
                self.has_nodes.store(true, Ordering::Release);
                HAS_ACTIVE_CLUSTER.store(true, Ordering::Release);
            }
        }

        // Connect to remote cluster bus (cport = port + 10000)
        let addr = format!("{}:{}", ip, cport);
        if let Ok(mut stream) = TcpStream::connect_timeout(
            &addr.parse().map_err(|e| format!("{}", e))?,
            Duration::from_millis(300),
        ) {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(300)));
            let _ = stream.set_write_timeout(Some(Duration::from_millis(300)));

            let my_epoch = self.config_epoch.load(Ordering::Relaxed);
            let my_slots = self.my_slots.read().clone();
            let mut slots_repr = String::new();
            for (s, e) in my_slots {
                slots_repr.push_str(&format!("{}-{},", s, e));
            }
            if slots_repr.ends_with(',') {
                slots_repr.pop();
            }
            let meet_frame = format!(
                "MEET 127.0.0.1 {} {} {} {}\r\n",
                self.port,
                self.my_id(),
                my_epoch,
                slots_repr
            );
            if stream.write_all(meet_frame.as_bytes()).is_ok() {
                let mut buf = [0u8; 512];
                if let Ok(n) = stream.read(&mut buf) {
                    let resp = String::from_utf8_lossy(&buf[..n]);
                    // Parse: "+PONG <id> <epoch> <role> <slots>"
                    if resp.starts_with("+PONG") {
                        let parts: Vec<&str> = resp.split_whitespace().collect();
                        if parts.len() >= 4 {
                            let remote_id = parts[1].to_string();
                            let remote_epoch: u64 = parts[2].parse().unwrap_or(1);
                            let remote_role = parts[3].to_string();
                            let remote_slots =
                                parts.get(4).map_or_else(Vec::new, |r| parse_slot_ranges(r));
                            let mut nodes = self.nodes.write();
                            nodes.remove(&temp_id);
                            nodes.insert(
                                remote_id.clone(),
                                ClusterNodeInfo {
                                    id: remote_id,
                                    ip: ip.to_string(),
                                    port,
                                    cport,
                                    flags: parse_flags(&remote_role).0,
                                    fail: FailState::Ok,
                                    master_id: "-".to_string(),
                                    ping_sent: now,
                                    pong_recv: now,
                                    config_epoch: remote_epoch,
                                    link_state: "connected".to_string(),
                                    slots: remote_slots,
                                },
                            );
                        }
                    }
                }
            }
        }

        let _ = self.save_config();
        Ok(())
    }

    pub fn cluster_forget(&self, node_id: &str) -> Result<(), String> {
        {
            let mut nodes = self.nodes.write();
            nodes.remove(node_id);
        }
        {
            let mut pfail = self.pfail_reports.write();
            pfail.remove(node_id);
            for reports in pfail.values_mut() {
                reports.remove(node_id);
            }
        }
        let _ = self.save_config();
        Ok(())
    }

    /// Makes this node a replica of `master_id` and returns the master's
    /// client address, which the caller starts replicating from. Like
    /// Redis, a master that still serves slots refuses: its keys would be
    /// replaced by the master's dataset.
    pub fn cluster_replicate(&self, master_id: &str) -> Result<(String, u16), String> {
        if master_id == self.my_id() {
            return Err("Can't replicate myself".to_string());
        }
        let addr = {
            let nodes = self.nodes.read();
            let Some(master) = nodes.get(master_id) else {
                return Err(format!("Unknown node {}", master_id));
            };
            if !master.is_master() {
                return Err("I can only replicate a master, not a replica.".to_string());
            }
            (master.ip.clone(), master.port)
        };
        if *self.role.read() == "master" && !self.my_slots.read().is_empty() {
            return Err(
                "To set a master the node must be empty and without assigned slots.".to_string(),
            );
        }
        *self.role.write() = "slave".to_string();
        *self.master_id.write() = master_id.to_string();
        self.my_slots.write().clear();
        let _ = self.save_config();
        Ok(addr)
    }

    /// Client address of the master this node replicates in the cluster
    /// (from `CLUSTER REPLICATE` or a `nodes.conf` loaded at startup).
    pub fn replication_master_addr(&self) -> Option<(String, u16)> {
        if *self.role.read() != "slave" {
            return None;
        }
        let master_id = self.master_id.read().clone();
        self.nodes
            .read()
            .get(&master_id)
            .map(|m| (m.ip.clone(), m.port))
    }

    /// Promotes this replica in place of its master. The caller stops the
    /// replication link first (see the CLUSTER FAILOVER handler).
    pub fn cluster_failover(&self, _force: bool) -> Result<(), String> {
        if *self.role.read() != "slave" {
            return Err("You should send CLUSTER FAILOVER to a replica".to_string());
        }
        let new_epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.config_epoch.store(new_epoch, Ordering::SeqCst);
        let old_master_id = self.master_id.write().clone();
        let my_id = self.my_id();
        *self.role.write() = "master".to_string();
        *self.master_id.write() = "-".to_string();

        // Inherit slots from old master if known
        let mut inherited_slots = Vec::new();
        {
            let mut nodes = self.nodes.write();
            if let Some(m) = nodes.get_mut(&old_master_id) {
                inherited_slots = std::mem::take(&mut m.slots);
                m.flags = "slave".to_string();
                m.master_id = my_id.clone();
            }
        }
        if !inherited_slots.is_empty() {
            *self.my_slots.write() = inherited_slots.clone();
        }

        // Notify replication hub if active
        crate::replication::get_replication_hub(self.port).make_master();

        // Broadcast FAILOVER to all peers
        {
            let nodes = self.nodes.read();
            for peer in nodes.values() {
                let addr = format!("{}:{}", peer.ip, peer.cport);
                if let Ok(mut stream) =
                    TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_millis(200))
                {
                    let mut slots_repr = String::new();
                    for (s, e) in &inherited_slots {
                        slots_repr.push_str(&format!("{}-{},", s, e));
                    }
                    if slots_repr.ends_with(',') {
                        slots_repr.pop();
                    }
                    let msg = format!("FAILOVER {} {} {}\r\n", my_id, new_epoch, slots_repr);
                    let _ = stream.write_all(msg.as_bytes());
                }
            }
        }

        let _ = self.save_config();
        Ok(())
    }

    pub fn cluster_slots(&self, out: &mut Vec<u8>, num_shards: usize) {
        let nodes = self.nodes.read();
        if nodes.is_empty() {
            // Standalone node fallback: report slots divided across local num_shards
            out.extend_from_slice(format!("*{}\r\n", num_shards).as_bytes());
            for s in 0..num_shards {
                let start_slot = s * 16384 / num_shards;
                let end_slot = if s == num_shards - 1 {
                    16383
                } else {
                    (s + 1) * 16384 / num_shards - 1
                };
                let node_id = format!("{:040x}", s + 1);
                let shard_port = if self.cluster_enabled.load(Ordering::Relaxed) {
                    self.port + s as u16
                } else {
                    self.port
                };
                out.extend_from_slice(b"*3\r\n");
                out.extend_from_slice(format!(":{}\r\n:{}\r\n", start_slot, end_slot).as_bytes());
                out.extend_from_slice(
                    format!(
                        "*3\r\n$9\r\n127.0.0.1\r\n:{}\r\n${}\r\n{}\r\n",
                        shard_port,
                        node_id.len(),
                        node_id
                    )
                    .as_bytes(),
                );
            }
            return;
        }

        // Multi-node cluster: collect slot ranges for all master nodes
        let my_id = self.my_id();
        let my_role = self.role.read().clone();
        let my_slots = self.my_slots.read().clone();

        struct MasterSlotEntry {
            start: u16,
            end: u16,
            master_ip: String,
            master_port: u16,
            master_id: String,
            replicas: Vec<(String, u16, String)>,
        }

        let mut entries = Vec::new();

        // 1. Myself if master
        if my_role == "master" {
            let my_replicas: Vec<(String, u16, String)> = nodes
                .values()
                .filter(|n| n.master_id == my_id)
                .map(|n| (n.ip.clone(), n.port, n.id.clone()))
                .collect();
            for &(s, e) in &my_slots {
                entries.push(MasterSlotEntry {
                    start: s,
                    end: e,
                    master_ip: "127.0.0.1".to_string(),
                    master_port: self.port,
                    master_id: my_id.clone(),
                    replicas: my_replicas.clone(),
                });
            }
        }

        // 2. Peer masters
        for peer in nodes.values() {
            if peer.is_master() {
                let peer_replicas: Vec<(String, u16, String)> = nodes
                    .values()
                    .filter(|n| n.master_id == peer.id)
                    .map(|n| (n.ip.clone(), n.port, n.id.clone()))
                    .collect();
                for &(s, e) in &peer.slots {
                    entries.push(MasterSlotEntry {
                        start: s,
                        end: e,
                        master_ip: peer.ip.clone(),
                        master_port: peer.port,
                        master_id: peer.id.clone(),
                        replicas: peer_replicas.clone(),
                    });
                }
            }
        }

        entries.sort_by_key(|e| e.start);

        out.extend_from_slice(format!("*{}\r\n", entries.len()).as_bytes());
        for entry in entries {
            let total_nodes = 1 + entry.replicas.len();
            out.extend_from_slice(format!("*{}\r\n", 2 + total_nodes).as_bytes());
            out.extend_from_slice(format!(":{}\r\n:{}\r\n", entry.start, entry.end).as_bytes());
            // Master node tuple: [ip, port, id]
            out.extend_from_slice(
                format!(
                    "*3\r\n${}\r\n{}\r\n:{}\r\n${}\r\n{}\r\n",
                    entry.master_ip.len(),
                    entry.master_ip,
                    entry.master_port,
                    entry.master_id.len(),
                    entry.master_id
                )
                .as_bytes(),
            );
            // Replicas tuples
            for (rip, rport, rid) in entry.replicas {
                out.extend_from_slice(
                    format!(
                        "*3\r\n${}\r\n{}\r\n:{}\r\n${}\r\n{}\r\n",
                        rip.len(),
                        rip,
                        rport,
                        rid.len(),
                        rid
                    )
                    .as_bytes(),
                );
            }
        }
    }

    pub fn cluster_shards(&self, out: &mut Vec<u8>) {
        let nodes = self.nodes.read();
        let my_id = self.my_id();
        let my_role = self.role.read().clone();
        let my_slots = self.my_slots.read().clone();

        struct ShardItem {
            slots: Vec<(u16, u16)>,
            nodes: Vec<(String, u16, String, String, String)>, // id, port, ip, role, health
        }

        let mut shards = Vec::new();

        // 1. Shard for myself if master
        if my_role == "master" {
            let mut shard_nodes = Vec::new();
            shard_nodes.push((
                my_id.clone(),
                self.port,
                "127.0.0.1".to_string(),
                "master".to_string(),
                "online".to_string(),
            ));
            for n in nodes.values() {
                if n.master_id == my_id {
                    let health = if n.fail == FailState::Fail {
                        "fail"
                    } else {
                        "online"
                    };
                    shard_nodes.push((
                        n.id.clone(),
                        n.port,
                        n.ip.clone(),
                        "replica".to_string(),
                        health.to_string(),
                    ));
                }
            }
            shards.push(ShardItem {
                slots: my_slots.clone(),
                nodes: shard_nodes,
            });
        }

        // 2. Shards for peer masters
        for peer in nodes.values() {
            if peer.is_master() {
                let mut shard_nodes = Vec::new();
                let master_health = if peer.fail == FailState::Fail {
                    "fail"
                } else {
                    "online"
                };
                shard_nodes.push((
                    peer.id.clone(),
                    peer.port,
                    peer.ip.clone(),
                    "master".to_string(),
                    master_health.to_string(),
                ));
                for n in nodes.values() {
                    if n.master_id == peer.id {
                        let health = if n.fail == FailState::Fail {
                            "fail"
                        } else {
                            "online"
                        };
                        shard_nodes.push((
                            n.id.clone(),
                            n.port,
                            n.ip.clone(),
                            "replica".to_string(),
                            health.to_string(),
                        ));
                    }
                }
                // If myself is replica of this peer
                if *self.master_id.read() == peer.id {
                    shard_nodes.push((
                        my_id.clone(),
                        self.port,
                        "127.0.0.1".to_string(),
                        "replica".to_string(),
                        "online".to_string(),
                    ));
                }
                shards.push(ShardItem {
                    slots: peer.slots.clone(),
                    nodes: shard_nodes,
                });
            }
        }

        if shards.is_empty() || (nodes.is_empty() && self.cluster_enabled.load(Ordering::Relaxed)) {
            if nodes.is_empty() && self.cluster_enabled.load(Ordering::Relaxed) {
                shards.clear();
                let num_shards = self.num_shards.load(Ordering::Relaxed).max(1);
                for s in 0..num_shards {
                    let start_slot = s * 16384 / num_shards;
                    let end_slot = if s == num_shards - 1 {
                        16383
                    } else {
                        (s + 1) * 16384 / num_shards - 1
                    };
                    let shard_port = self.port + s as u16;
                    let node_id = if s == 0 {
                        my_id.clone()
                    } else {
                        format!("{:040x}", s + 1)
                    };
                    let shard_nodes = vec![(
                        node_id,
                        shard_port,
                        "127.0.0.1".to_string(),
                        "master".to_string(),
                        "online".to_string(),
                    )];
                    shards.push(ShardItem {
                        slots: vec![(start_slot as u16, end_slot as u16)],
                        nodes: shard_nodes,
                    });
                }
            } else if shards.is_empty() {
                // Fallback for single node
                let shard_nodes = vec![(
                    my_id,
                    self.port,
                    "127.0.0.1".to_string(),
                    my_role,
                    "online".to_string(),
                )];
                shards.push(ShardItem {
                    slots: my_slots,
                    nodes: shard_nodes,
                });
            }
        }

        out.extend_from_slice(format!("*{}\r\n", shards.len()).as_bytes());
        for shard in shards {
            out.extend_from_slice(b"*4\r\n");
            // Field 1: slots
            out.extend_from_slice(b"$5\r\nslots\r\n");
            out.extend_from_slice(format!("*{}\r\n", shard.slots.len() * 2).as_bytes());
            for (s, e) in shard.slots {
                out.extend_from_slice(format!(":{}\r\n:{}\r\n", s, e).as_bytes());
            }
            // Field 2: nodes
            out.extend_from_slice(b"$5\r\nnodes\r\n");
            out.extend_from_slice(format!("*{}\r\n", shard.nodes.len()).as_bytes());
            for (id, port, ip, role, health) in shard.nodes {
                out.extend_from_slice(b"*14\r\n");
                out.extend_from_slice(b"$2\r\nid\r\n");
                out.extend_from_slice(format!("${}\r\n{}\r\n", id.len(), id).as_bytes());
                out.extend_from_slice(b"$4\r\nport\r\n");
                out.extend_from_slice(format!(":{}\r\n", port).as_bytes());
                out.extend_from_slice(b"$2\r\nip\r\n");
                out.extend_from_slice(format!("${}\r\n{}\r\n", ip.len(), ip).as_bytes());
                out.extend_from_slice(b"$8\r\nendpoint\r\n");
                out.extend_from_slice(format!("${}\r\n{}\r\n", ip.len(), ip).as_bytes());
                out.extend_from_slice(b"$4\r\nrole\r\n");
                out.extend_from_slice(format!("${}\r\n{}\r\n", role.len(), role).as_bytes());
                out.extend_from_slice(b"$18\r\nreplication-offset\r\n:0\r\n");
                out.extend_from_slice(b"$6\r\nhealth\r\n");
                out.extend_from_slice(format!("${}\r\n{}\r\n", health.len(), health).as_bytes());
            }
        }
    }

    pub fn cluster_links(&self, out: &mut Vec<u8>) {
        let nodes = self.nodes.read();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        out.extend_from_slice(format!("*{}\r\n", nodes.len()).as_bytes());
        for node in nodes.values() {
            out.extend_from_slice(b"*12\r\n");
            out.extend_from_slice(b"$9\r\ndirection\r\n$2\r\nto\r\n");
            out.extend_from_slice(b"$4\r\nnode\r\n");
            out.extend_from_slice(format!("${}\r\n{}\r\n", node.id.len(), node.id).as_bytes());
            out.extend_from_slice(b"$11\r\ncreate-time\r\n");
            out.extend_from_slice(format!(":{}\r\n", now).as_bytes());
            out.extend_from_slice(b"$6\r\nevents\r\n$1\r\nr\r\n");
            out.extend_from_slice(b"$21\r\nsend-buffer-allocated\r\n:0\r\n");
            out.extend_from_slice(b"$16\r\nsend-buffer-used\r\n:0\r\n");
        }
    }

    pub fn cluster_addslots(&self, slots: &[u16]) -> Result<(), String> {
        for &s in slots {
            if s >= 16384 {
                return Err(format!("ERR Slot {} out of range", s));
            }
        }
        {
            let mut my_slots = self.my_slots.write();
            if my_slots.len() == 1 && my_slots[0] == (0, 16383) {
                my_slots.clear();
            }
            for &s in slots {
                my_slots.push((s, s));
            }
            compact_slots(&mut my_slots);
        }
        let _ = self.save_config();
        Ok(())
    }

    pub fn cluster_delslots(&self, slots: &[u16]) -> Result<(), String> {
        {
            let mut my_slots = self.my_slots.write();
            remove_slots(&mut my_slots, slots);
        }
        let _ = self.save_config();
        Ok(())
    }

    pub fn cluster_addslotsrange(&self, ranges: &[(u16, u16)]) -> Result<(), String> {
        for &(start, end) in ranges {
            if start > end || end >= 16384 {
                return Err(format!("ERR Invalid slot range {}-{}", start, end));
            }
        }
        {
            let mut my_slots = self.my_slots.write();
            if my_slots.len() == 1 && my_slots[0] == (0, 16383) {
                my_slots.clear();
            }
            for &(start, end) in ranges {
                my_slots.push((start, end));
            }
            compact_slots(&mut my_slots);
        }
        let _ = self.save_config();
        Ok(())
    }

    pub fn cluster_delslotsrange(&self, ranges: &[(u16, u16)]) -> Result<(), String> {
        {
            let mut my_slots = self.my_slots.write();
            let mut to_remove = Vec::new();
            for &(start, end) in ranges {
                for s in start..=end {
                    to_remove.push(s);
                }
            }
            remove_slots(&mut my_slots, &to_remove);
        }
        let _ = self.save_config();
        Ok(())
    }

    pub fn broadcast_to_peers(&self, msg: &str) {
        let peers: Vec<(String, u16)> = {
            let nodes = self.nodes.read();
            nodes.values().map(|n| (n.ip.clone(), n.cport)).collect()
        };
        for (ip, cport) in peers {
            let addr = format!("{}:{}", ip, cport);
            if let Ok(parsed) = addr.parse::<SocketAddr>()
                && let Ok(mut stream) =
                    TcpStream::connect_timeout(&parsed, Duration::from_millis(200))
            {
                let _ = stream.write_all(msg.as_bytes());
            }
        }
    }

    pub fn cluster_check(&self) -> ClusterCheckReport {
        let is_cluster = self.cluster_enabled.load(Ordering::Relaxed);
        let num_shards = self.num_shards.load(Ordering::Relaxed).max(1);
        let mut slot_owners: HashMap<u16, Vec<String>> = HashMap::new();

        let my_id = self.my_id();
        let is_master = self.role.read().contains("master");
        if is_master {
            let my_slots = self.my_slots.read();
            for &(s, e) in my_slots.iter() {
                for slot in s..=e {
                    slot_owners.entry(slot).or_default().push(my_id.clone());
                }
            }
        }

        let nodes = self.nodes.read();
        let mut masters_count = if is_master { 1 } else { 0 };
        let mut replicas_count = if is_master { 0 } else { 1 };

        if is_cluster && nodes.is_empty() && num_shards > 1 {
            masters_count = num_shards;
            for s in 1..num_shards {
                let peer_start = (s * 16384 / num_shards) as u16;
                let peer_end = if s == num_shards - 1 {
                    16383
                } else {
                    ((s + 1) * 16384 / num_shards - 1) as u16
                };
                let peer_id = format!("{:040x}", s + 1);
                for slot in peer_start..=peer_end {
                    slot_owners.entry(slot).or_default().push(peer_id.clone());
                }
            }
        } else {
            for node in nodes.values() {
                if node.is_master() {
                    masters_count += 1;
                    for &(s, e) in &node.slots {
                        for slot in s..=e {
                            slot_owners.entry(slot).or_default().push(node.id.clone());
                        }
                    }
                } else {
                    replicas_count += 1;
                }
            }
        }

        let mut open_slots = Vec::new();
        let mut duplicate_slots = Vec::new();
        let mut total_assigned = 0;

        for slot in 0..16384u16 {
            match slot_owners.get(&slot) {
                None => open_slots.push(slot),
                Some(owners) => {
                    total_assigned += 1;
                    if owners.len() > 1 {
                        duplicate_slots.push(slot);
                    }
                }
            }
        }

        let mut migrating_slots = Vec::new();
        let mut importing_slots = Vec::new();
        {
            let states = self.slot_states.read();
            for (&slot, (state, target)) in states.iter() {
                if state == "migrating" {
                    migrating_slots.push((slot, target.clone()));
                } else if state == "importing" {
                    importing_slots.push((slot, target.clone()));
                }
            }
        }

        let ok = open_slots.is_empty() && duplicate_slots.is_empty();

        ClusterCheckReport {
            ok,
            masters: masters_count,
            replicas: replicas_count,
            total_slots_assigned: total_assigned,
            open_slots,
            duplicate_slots,
            migrating_slots,
            importing_slots,
        }
    }

    pub fn compute_rebalance_plan(
        &self,
        options: &RebalanceOptions,
    ) -> Result<Vec<SlotMigrationPlan>, String> {
        struct Master {
            id: String,
            addr: String,
            port: u16,
            slots: Vec<u16>,
        }

        let mut masters = Vec::new();
        let my_id = self.my_id();
        let my_addr = format!("127.0.0.1:{}", self.port);
        let my_slots: Vec<u16> = {
            let ranges = self.my_slots.read();
            let mut s = Vec::new();
            for &(start, end) in ranges.iter() {
                for slot in start..=end {
                    s.push(slot);
                }
            }
            s
        };

        if self.role.read().contains("master") {
            masters.push(Master {
                id: my_id.clone(),
                addr: my_addr,
                port: self.port,
                slots: my_slots,
            });
        }

        {
            let nodes = self.nodes.read();
            for n in nodes.values() {
                if n.is_master() && n.id != my_id {
                    let mut s = Vec::new();
                    for &(start, end) in &n.slots {
                        for slot in start..=end {
                            s.push(slot);
                        }
                    }
                    masters.push(Master {
                        id: n.id.clone(),
                        addr: format!("{}:{}", n.ip, n.port),
                        port: n.port,
                        slots: s,
                    });
                }
            }
        }

        if masters.is_empty() {
            return Err("No master nodes found in cluster".to_string());
        }

        // Targeted rebalance to a specific node:
        if let Some((ref target_host, target_port, opt_slots)) = options.target_host_port {
            let target_idx = masters.iter().position(|m| {
                m.port == target_port || m.addr == format!("{}:{}", target_host, target_port)
            });
            let (target_node_id, target_addr) = match target_idx {
                Some(idx) => (masters[idx].id.clone(), masters[idx].addr.clone()),
                None => (
                    generate_node_id(target_port),
                    format!("{}:{}", target_host, target_port),
                ),
            };

            let num_slots = opt_slots.unwrap_or(1);
            let mut plans = Vec::new();

            let donor_idx = if target_idx.is_some_and(|idx| masters[idx].id == my_id) {
                masters
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| Some(*i) != target_idx)
                    .max_by_key(|(_, m)| m.slots.len())
                    .map(|(i, _)| i)
            } else {
                masters.iter().position(|m| m.id == my_id).or(Some(0))
            };

            if let Some(d_idx) = donor_idx {
                let donor = &mut masters[d_idx];
                let to_take = num_slots.min(donor.slots.len());

                for _ in 0..to_take {
                    if let Some(slot) = donor.slots.pop() {
                        plans.push(SlotMigrationPlan {
                            slot,
                            source_node_id: donor.id.clone(),
                            source_addr: donor.addr.clone(),
                            target_node_id: target_node_id.clone(),
                            target_addr: target_addr.clone(),
                        });
                    }
                }
            }
            return Ok(plans);
        }

        if masters.len() <= 1 {
            return Ok(Vec::new());
        }

        // Auto / Weighted rebalance across all masters:
        let num_m = masters.len();
        let weights: Vec<f64> = masters
            .iter()
            .map(|m| options.weights.get(&m.id).copied().unwrap_or(1.0).max(0.01))
            .collect();
        let total_weight: f64 = weights.iter().sum();

        let mut target_slots: Vec<usize> = weights
            .iter()
            .map(|&w| ((w / total_weight) * 16384.0).floor() as usize)
            .collect();
        let allocated: usize = target_slots.iter().sum();
        let mut remainder = 16384usize.saturating_sub(allocated);

        let mut remainders: Vec<(usize, f64)> = weights
            .iter()
            .enumerate()
            .map(|(i, &w)| {
                let exact = (w / total_weight) * 16384.0;
                (i, exact - exact.floor())
            })
            .collect();
        remainders.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        for &(i, _) in &remainders {
            if remainder == 0 {
                break;
            }
            target_slots[i] += 1;
            remainder -= 1;
        }

        let avg_slots = 16384.0 / num_m as f64;
        let threshold_slots = ((options.threshold / 100.0) * avg_slots).ceil() as isize;

        let mut deltas: Vec<isize> = masters
            .iter()
            .enumerate()
            .map(|(i, m)| (m.slots.len() as isize) - (target_slots[i] as isize))
            .collect();

        if deltas.iter().all(|&d| d.abs() <= threshold_slots) {
            return Ok(Vec::new());
        }

        let mut plans = Vec::new();

        loop {
            let donor_idx = deltas
                .iter()
                .enumerate()
                .filter(|(_, d)| **d > 0)
                .max_by_key(|(_, d)| **d)
                .map(|(i, _)| i);
            let receiver_idx = deltas
                .iter()
                .enumerate()
                .filter(|(_, d)| **d < 0)
                .min_by_key(|(_, d)| **d)
                .map(|(i, _)| i);

            match (donor_idx, receiver_idx) {
                (Some(d), Some(r)) => {
                    let to_move = (deltas[d] as usize).min((-deltas[r]) as usize);
                    if to_move == 0 {
                        break;
                    }
                    let target_id = masters[r].id.clone();
                    let target_addr = masters[r].addr.clone();
                    let source_id = masters[d].id.clone();
                    let source_addr = masters[d].addr.clone();

                    for _ in 0..to_move {
                        if let Some(slot) = masters[d].slots.pop() {
                            plans.push(SlotMigrationPlan {
                                slot,
                                source_node_id: source_id.clone(),
                                source_addr: source_addr.clone(),
                                target_node_id: target_id.clone(),
                                target_addr: target_addr.clone(),
                            });
                        }
                    }

                    deltas[d] -= to_move as isize;
                    deltas[r] += to_move as isize;
                }
                _ => break,
            }
        }

        Ok(plans)
    }

    pub fn compute_reshard_plan(
        &self,
        target_node_id: &str,
        source_node_id: &str,
        num_slots: usize,
    ) -> Result<Vec<SlotMigrationPlan>, String> {
        let my_id = self.my_id();
        let (source_addr, mut source_slots) = if source_node_id == my_id {
            let ranges = self.my_slots.read();
            let mut s = Vec::new();
            for &(start, end) in ranges.iter() {
                for slot in start..=end {
                    s.push(slot);
                }
            }
            (format!("127.0.0.1:{}", self.port), s)
        } else {
            let nodes = self.nodes.read();
            let node = nodes
                .get(source_node_id)
                .ok_or_else(|| format!("Source node {} not found", source_node_id))?;
            let mut s = Vec::new();
            for &(start, end) in &node.slots {
                for slot in start..=end {
                    s.push(slot);
                }
            }
            (format!("{}:{}", node.ip, node.port), s)
        };

        let target_addr = if target_node_id == my_id {
            format!("127.0.0.1:{}", self.port)
        } else {
            let nodes = self.nodes.read();
            let node = nodes
                .get(target_node_id)
                .ok_or_else(|| format!("Target node {} not found", target_node_id))?;
            format!("{}:{}", node.ip, node.port)
        };

        let to_take = num_slots.min(source_slots.len());
        let mut plans = Vec::with_capacity(to_take);
        for _ in 0..to_take {
            if let Some(slot) = source_slots.pop() {
                plans.push(SlotMigrationPlan {
                    slot,
                    source_node_id: source_node_id.to_string(),
                    source_addr: source_addr.clone(),
                    target_node_id: target_node_id.to_string(),
                    target_addr: target_addr.clone(),
                });
            }
        }
        Ok(plans)
    }

    pub fn start_election(&self) {
        let master_id = self.master_id.read().clone();
        if master_id == "-" || master_id.is_empty() {
            self.election_in_progress.store(false, Ordering::SeqCst);
            return;
        }

        // A replica that never got its master's data must not take over:
        // it would serve the slots empty.
        if !crate::replication::get_replication_hub(self.port).holds_master_data() {
            crate::log_warning!(
                "Cluster: master {} failed but I hold none of its data: not taking over",
                master_id
            );
            self.election_in_progress.store(false, Ordering::SeqCst);
            return;
        }
        // Like Redis, wait a little (500 ms plus up to 500 ms of jitter), so
        // the FAIL verdict spreads and replicas of the same master don't
        // split the votes of one epoch.
        std::thread::sleep(Duration::from_millis(500 + random_below(500)));
        let still_failed = self
            .nodes
            .read()
            .get(&master_id)
            .is_some_and(|m| m.fail == FailState::Fail);
        if !still_failed || *self.master_id.read() != master_id {
            self.election_in_progress.store(false, Ordering::SeqCst);
            return;
        }

        let req_epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1;

        // The masters serving slots vote, the failed one included in the
        // count: winning takes a majority of them. A replica gets no vote
        // of its own (it used to count itself, so with 3 masters one ACK
        // was enough).
        let (masters, total_masters): (Vec<(String, u16)>, usize) = {
            let nodes = self.nodes.read();
            let serving = || {
                nodes
                    .values()
                    .filter(|n| n.is_master() && (!n.slots.is_empty() || n.id == master_id))
            };
            let alive = serving()
                .filter(|n| n.fail != FailState::Fail && n.id != master_id)
                .map(|n| (n.ip.clone(), n.cport))
                .collect();
            (alive, serving().count())
        };

        let mut votes = 0;

        for (ip, cport) in &masters {
            let addr = format!("{}:{}", ip, cport);
            if let Ok(parsed) = addr.parse::<SocketAddr>()
                && let Ok(mut stream) =
                    TcpStream::connect_timeout(&parsed, Duration::from_millis(500))
            {
                let _ = stream.set_read_timeout(Some(Duration::from_millis(1000)));
                let _ = stream.set_write_timeout(Some(Duration::from_millis(1000)));
                let req = format!(
                    "FAILOVER_AUTH_REQUEST {} {} {}\r\n",
                    self.my_id(),
                    req_epoch,
                    master_id
                );
                if stream.write_all(req.as_bytes()).is_ok() {
                    let mut buf = [0u8; 128];
                    if let Ok(n) = stream.read(&mut buf) {
                        let resp = String::from_utf8_lossy(&buf[..n]);
                        if resp.starts_with("+FAILOVER_AUTH_ACK") {
                            votes += 1;
                        }
                    }
                }
            }
        }

        let majority = (total_masters / 2) + 1;
        if votes >= majority {
            // Won the election!
            let my_id = self.my_id();
            *self.role.write() = "master".to_string();
            *self.master_id.write() = "-".to_string();
            self.config_epoch.store(req_epoch, Ordering::SeqCst);

            // Inherit old master's slots
            let mut inherited = Vec::new();
            {
                let mut nodes = self.nodes.write();
                if let Some(m) = nodes.get_mut(&master_id) {
                    inherited = std::mem::take(&mut m.slots);
                    // The old master is down; when it returns it is our replica.
                    m.flags = "slave".to_string();
                    m.master_id = my_id.clone();
                }
            }
            if !inherited.is_empty() {
                *self.my_slots.write() = inherited.clone();
            }

            // The master is gone: end the link to it before taking over,
            // so nothing it sent lands after the promotion.
            let repl = crate::replication::get_replication_hub(self.port);
            repl.stop_sync_and_wait_blocking(Duration::from_secs(5));
            repl.make_master();

            // Announce failover to all peers
            let mut slots_repr = String::new();
            for (s, e) in &inherited {
                slots_repr.push_str(&format!("{}-{},", s, e));
            }
            if slots_repr.ends_with(',') {
                slots_repr.pop();
            }
            let announce = format!(
                "FAILOVER_ANNOUNCE {} {} {}\r\n",
                my_id, req_epoch, slots_repr
            );
            self.broadcast_to_peers(&announce);
        } else {
            // Lost (another replica of the same master got the votes of
            // this epoch): back off a random while before running again,
            // so two replicas don't keep splitting every epoch.
            std::thread::sleep(Duration::from_millis(1000 + random_below(1000)));
        }

        self.election_in_progress.store(false, Ordering::SeqCst);
    }

    pub fn cluster_reset(&self, hard: bool) -> Result<(), String> {
        self.nodes.write().clear();
        self.pfail_reports.write().clear();
        if hard {
            let new_id = generate_node_id(self.port);
            *self.my_id.write() = new_id;
            self.current_epoch.store(1, Ordering::SeqCst);
            self.config_epoch.store(0, Ordering::SeqCst);
            *self.role.write() = "master".to_string();
            *self.master_id.write() = "-".to_string();
            self.my_slots.write().clear();
        }
        let _ = self.save_config();
        Ok(())
    }

    pub fn dfly_cluster_config(&self, config_json: &str) -> Result<(), String> {
        let val: serde_json::Value = serde_json::from_str(config_json)
            .map_err(|e| format!("ERR Invalid JSON configuration: {}", e))?;

        let mut new_slots = Vec::new();
        let my_id = self.my_id();

        if let Some(arr) = val.as_array() {
            for item in arr {
                let node_id = item
                    .get("master_id")
                    .or_else(|| item.get("id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let is_me = node_id == my_id || node_id.is_empty();
                if let Some(ranges) = item.get("slot_ranges").and_then(|r| r.as_array()) {
                    for r in ranges {
                        if let Some(pair) = r.as_array() {
                            if pair.len() == 2
                                && let (Some(s), Some(e)) = (pair[0].as_u64(), pair[1].as_u64())
                                && is_me
                            {
                                new_slots.push((s as u16, e as u16));
                            }
                        } else if let Some(obj) = r.as_object()
                            && let (Some(s), Some(e)) = (
                                obj.get("start").and_then(|v| v.as_u64()),
                                obj.get("end").and_then(|v| v.as_u64()),
                            )
                            && is_me
                        {
                            new_slots.push((s as u16, e as u16));
                        }
                    }
                }
            }
        } else if let Some(obj) = val.as_object()
            && let Some(ranges) = obj.get("slot_ranges").and_then(|r| r.as_array())
        {
            for r in ranges {
                if let Some(pair) = r.as_array() {
                    if pair.len() == 2
                        && let (Some(s), Some(e)) = (pair[0].as_u64(), pair[1].as_u64())
                    {
                        new_slots.push((s as u16, e as u16));
                    }
                } else if let Some(obj) = r.as_object()
                    && let (Some(s), Some(e)) = (
                        obj.get("start").and_then(|v| v.as_u64()),
                        obj.get("end").and_then(|v| v.as_u64()),
                    )
                {
                    new_slots.push((s as u16, e as u16));
                }
            }
        }

        if !new_slots.is_empty() {
            compact_slots(&mut new_slots);
            *self.my_slots.write() = new_slots;
        }

        Ok(())
    }

    pub fn dfly_slot_migration_status(&self, out: &mut Vec<u8>) {
        let mig = self.active_migration.read();
        match &*mig {
            None => {
                out.extend_from_slice(
                    b"*4\r\n$5\r\nstate\r\n$4\r\nIDLE\r\n$10\r\nmigrations\r\n*0\r\n",
                );
            }
            Some(m) => {
                out.extend_from_slice(b"*6\r\n");
                out.extend_from_slice(b"$5\r\nstate\r\n");
                out.extend_from_slice(format!("${}\r\n{}\r\n", m.state.len(), m.state).as_bytes());
                out.extend_from_slice(b"$9\r\nsource_id\r\n");
                out.extend_from_slice(
                    format!("${}\r\n{}\r\n", m.source_id.len(), m.source_id).as_bytes(),
                );
                out.extend_from_slice(b"$13\r\nkeys_migrated\r\n");
                out.extend_from_slice(format!(":{}\r\n", m.keys_migrated).as_bytes());
            }
        }
    }

    pub fn dfly_migrate_init(&self, source_id: &str, num_shards: usize, slots: &[(u16, u16)]) {
        *self.active_migration.write() = Some(ActiveMigration {
            state: "MIGRATING".to_string(),
            source_id: source_id.to_string(),
            num_shards,
            slots: slots.to_vec(),
            keys_migrated: 0,
        });
    }

    pub fn dfly_migrate_flow(&self, _source_id: &str, flow_id: u64) {
        if let Some(ref mut m) = *self.active_migration.write() {
            m.state = "SYNCING".to_string();
            m.keys_migrated += flow_id.max(1);
        }
    }

    pub fn dfly_migrate_ack(&self, _flow_id: u64) {
        *self.active_migration.write() = None;
    }
}

/// Encodes slot ranges as the bus does: `0-100,200-300`.
/// Whether a nodes.conf entry is the single-node view of one of our own
/// shards: `{:040x}` of the shard number + 1, at our port + shard.
fn is_own_shard_pseudo_node(my_port: u16, id: &str, port: u16) -> bool {
    port > my_port && port - my_port < 1024 && id == format!("{:040x}", port - my_port + 1)
}

fn slots_repr(slots: &[(u16, u16)]) -> String {
    slots
        .iter()
        .map(|(s, e)| format!("{}-{}", s, e))
        .collect::<Vec<_>>()
        .join(",")
}

pub fn compact_slots(slots: &mut Vec<(u16, u16)>) {
    if slots.is_empty() {
        return;
    }
    slots.sort_by_key(|&(s, _)| s);
    let mut merged = Vec::new();
    let mut curr = slots[0];
    for &(s, e) in &slots[1..] {
        if s <= curr.1.saturating_add(1) {
            curr.1 = curr.1.max(e);
        } else {
            merged.push(curr);
            curr = (s, e);
        }
    }
    merged.push(curr);
    *slots = merged;
}

pub fn remove_slots(slots: &mut Vec<(u16, u16)>, to_remove: &[u16]) {
    let remove_set: std::collections::HashSet<u16> = to_remove.iter().copied().collect();
    let mut new_individual = Vec::new();
    for &(s, e) in slots.iter() {
        for slot in s..=e {
            if !remove_set.contains(&slot) {
                new_individual.push(slot);
            }
        }
    }
    let mut new_ranges = Vec::new();
    for slot in new_individual {
        new_ranges.push((slot, slot));
    }
    compact_slots(&mut new_ranges);
    *slots = new_ranges;
}

pub fn start_cluster_bus(port: u16) {
    let hub = get_cluster_hub(port);
    if hub.bus_running.swap(true, Ordering::SeqCst) {
        return;
    }

    let cport = port + 10000;
    let (cancel_tx, cancel_rx) = flume::bounded(1);
    *hub.cancel_bus.write() = Some(cancel_tx);

    let hub_clone = hub.clone();
    std::thread::Builder::new()
        .name(format!("cluster-bus-{}", cport))
        .spawn(move || {
            // Same interfaces as the client port (`bind`), never a blanket 0.0.0.0.
            let listeners: Vec<TcpListener> =
                match crate::netsec::bind_all(&crate::netsec::bind_addrs(port), cport, 128) {
                    Ok(v) => v.into_iter().map(|(_, l)| l).collect(),
                    Err(e) => {
                        crate::log_warning!("[ClusterBus {}] {}", cport, e);
                        return;
                    }
                };

            // Heartbeats on a thread of their own: a tick waits on peers, and
            // accepting must never wait behind it. Queued PINGs used to be
            // read seconds late, so a dead node kept "talking" and live ones
            // looked silent.
            let tick_hub = hub_clone.clone();
            let tick_cancel = cancel_rx.clone();
            std::thread::Builder::new()
                .name(format!("cluster-tick-{}", cport))
                .spawn(move || {
                    while tick_cancel.is_empty() {
                        let started = std::time::Instant::now();
                        cluster_bus_tick(&tick_hub);
                        std::thread::sleep(
                            Duration::from_millis(500).saturating_sub(started.elapsed()),
                        );
                    }
                })
                .expect("Failed to spawn cluster tick thread");

            let open_conns = Arc::new(std::sync::atomic::AtomicUsize::new(0));

            while cancel_rx.is_empty() {
                // Accept every pending cluster bus connection.
                let mut accepted_any = false;
                for listener in &listeners {
                    while let Ok((stream, _)) = listener.accept() {
                        accepted_any = true;
                        if open_conns.fetch_add(1, Ordering::AcqRel) >= MAX_BUS_CONNS {
                            open_conns.fetch_sub(1, Ordering::AcqRel);
                            continue;
                        }
                        let _ = stream.set_nonblocking(false);
                        let hub_for_conn = hub_clone.clone();
                        let open_conns = open_conns.clone();
                        std::thread::spawn(move || {
                            let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
                            let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
                            handle_cluster_bus_conn(stream, &hub_for_conn);
                            open_conns.fetch_sub(1, Ordering::AcqRel);
                        });
                    }
                }
                if !accepted_any {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        })
        .expect("Failed to spawn cluster bus thread");
}

/// Longest cluster-bus frame (one line) accepted. A gossip section listing
/// `MAX_CLUSTER_NODES` peers fits; a longer line drops the connection.
const MAX_BUS_LINE: usize = 1 << 20;

/// Peers learned through MEET or gossip beyond this many are ignored (Redis
/// Cluster is designed for up to 1000 nodes), so a peer cannot grow the node
/// table, or the per-tick PING fan-out over it, without bound.
pub const MAX_CLUSTER_NODES: usize = 1000;

/// Concurrent inbound cluster-bus connections; each one holds a thread.
const MAX_BUS_CONNS: usize = 256;

/// Parses a peer's `start-end,start-end` slot list. Ranges that are reversed
/// or reach past slot 16383 are dropped and the rest are merged, so a node
/// never holds more than the 16384 real slots however long the list is.
pub fn parse_slot_ranges(repr: &str) -> Vec<(u16, u16)> {
    let mut ranges: Vec<(u16, u16)> = repr
        .split(',')
        .filter_map(|r| {
            let (s, e) = r.split_once('-')?;
            let (s, e) = (s.parse::<u16>().ok()?, e.parse::<u16>().ok()?);
            (s <= e && e < 16384).then_some((s, e))
        })
        .collect();
    compact_slots(&mut ranges);
    ranges
}

/// The cluster-bus port of a node whose client port is `port`, if it has one.
fn bus_port(port: u16) -> Option<u16> {
    port.checked_add(10000).filter(|_| port != 0)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn pong_reply(hub: &ClusterHub) -> String {
    let my_epoch = hub.config_epoch.load(Ordering::Relaxed);
    let role = hub.role.read().clone();
    let my_slots = hub.my_slots.read().clone();
    let mut slots_repr = String::new();
    for (s, e) in my_slots {
        slots_repr.push_str(&format!("{}-{},", s, e));
    }
    if slots_repr.ends_with(',') {
        slots_repr.pop();
    }
    format!(
        "+PONG {} {} {} {}\r\n",
        hub.my_id(),
        my_epoch,
        role,
        slots_repr
    )
}

fn handle_cluster_bus_conn(stream: TcpStream, hub: &Arc<ClusterHub>) {
    let mut reader = std::io::BufReader::new(stream);
    let mut line = Vec::new();
    loop {
        line.clear();
        match (&mut reader)
            .take(MAX_BUS_LINE as u64 + 1)
            .read_until(b'\n', &mut line)
        {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        if line.len() > MAX_BUS_LINE {
            break;
        }
        if let Some(reply) = handle_cluster_bus_line(&String::from_utf8_lossy(&line), hub)
            && reader.get_mut().write_all(reply.as_bytes()).is_err()
        {
            break;
        }
    }
}

/// Applies one cluster-bus frame from a peer and returns the reply to send, if any.
fn handle_cluster_bus_line(line: &str, hub: &ClusterHub) -> Option<String> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    let cmd = *parts.first()?;
    match cmd {
        "MEET" => {
            // MEET <ip> <port> <node_id> <epoch> <slots>
            if parts.len() < 4 {
                return None;
            }
            let peer_ip = parts[1].to_string();
            let Some((peer_port, peer_cport)) = parts[2]
                .parse::<u16>()
                .ok()
                .and_then(|p| Some((p, bus_port(p)?)))
            else {
                return Some("-ERR invalid port\r\n".to_string());
            };
            let peer_id = parts[3].to_string();
            let peer_epoch: u64 = if parts.len() >= 5 {
                parts[4].parse().unwrap_or(1)
            } else {
                1
            };
            let now = now_ms();
            let peer_slots = parts.get(5).map_or_else(Vec::new, |r| parse_slot_ranges(r));

            {
                let mut nodes = hub.nodes.write();
                if !nodes.contains_key(&peer_id) && nodes.len() >= MAX_CLUSTER_NODES {
                    return Some("-ERR too many cluster nodes\r\n".to_string());
                }
                // What we knew of it stays until its claim is weighed below.
                let (known_epoch, known_slots) = nodes
                    .get(&peer_id)
                    .map_or((0, Vec::new()), |n| (n.config_epoch, n.slots.clone()));
                nodes.insert(
                    peer_id.clone(),
                    ClusterNodeInfo {
                        id: peer_id.clone(),
                        ip: peer_ip,
                        port: peer_port,
                        cport: peer_cport,
                        flags: "master".to_string(),
                        fail: FailState::Ok,
                        master_id: "-".to_string(),
                        ping_sent: now,
                        pong_recv: now,
                        config_epoch: known_epoch,
                        link_state: "connected".to_string(),
                        slots: known_slots,
                    },
                );
            }
            hub.claim_slots(&peer_id, Some(peer_epoch), peer_slots);

            // Respond PONG
            Some(pong_reply(hub))
        }
        "PING" => {
            // PING <node_id> <epoch> <flags> <slots> [GOSSIP <peers...>]
            if parts.len() < 2 {
                return None;
            }
            let peer_id = parts[1].to_string();
            let now = now_ms();

            {
                let mut nodes = hub.nodes.write();
                if let Some(node) = nodes.get_mut(&peer_id) {
                    node.pong_recv = now;
                    node.link_state = "connected".to_string();
                    // It is talking to us: reachable, whatever we thought.
                    node.fail = FailState::Ok;
                    if parts.len() >= 4 {
                        node.flags = parse_flags(parts[3]).0;
                    }
                }
            }
            if parts.len() >= 4 {
                // No slot list (the next field is GOSSIP or nothing): none.
                let slots = parts
                    .get(4)
                    .filter(|f| **f != "GOSSIP")
                    .map_or_else(Vec::new, |r| parse_slot_ranges(r));
                hub.claim_slots(&peer_id, parts[2].parse::<u64>().ok(), slots);
            }

            // Parse gossip section if present
            if let Some(gossip_pos) = parts.iter().position(|&p| p == "GOSSIP")
                && gossip_pos + 1 < parts.len()
            {
                let gossip_data = parts[gossip_pos + 1];
                for peer_repr in gossip_data.split(';') {
                    let fields: Vec<&str> = peer_repr.split(',').collect();
                    if fields.len() >= 5 {
                        let gid = fields[0].to_string();
                        let gip = fields[1].to_string();
                        let gport: u16 = fields[2].parse().unwrap_or(0);
                        let gcport: u16 = fields[3].parse().unwrap_or(0);
                        let (grole, gfail) = parse_flags(fields[4]);
                        let gepoch: u64 = fields.get(5).and_then(|e| e.parse().ok()).unwrap_or(0);
                        if gid != hub.my_id() {
                            let mut nodes = hub.nodes.write();
                            // Failure reports only count for nodes we know.
                            if nodes.contains_key(&gid) {
                                if gfail != FailState::Ok {
                                    hub.pfail_reports
                                        .write()
                                        .entry(gid.clone())
                                        .or_default()
                                        .insert(peer_id.clone());
                                } else if let Some(reports) =
                                    hub.pfail_reports.write().get_mut(&gid)
                                {
                                    reports.remove(&peer_id);
                                }
                            }

                            if let Some(n) = nodes.get_mut(&gid) {
                                if gfail == FailState::Fail {
                                    n.fail = FailState::Fail;
                                }
                            } else if gport > 0 && nodes.len() < MAX_CLUSTER_NODES {
                                nodes.insert(
                                    gid.clone(),
                                    ClusterNodeInfo {
                                        id: gid,
                                        ip: gip,
                                        port: gport,
                                        cport: gcport,
                                        flags: grole,
                                        fail: gfail,
                                        master_id: "-".to_string(),
                                        ping_sent: now,
                                        pong_recv: now,
                                        // As the sender knows it: a fresh node runs at epoch 0, and a
                                        // guess above that made its PONGs (and slots) look stale.
                                        config_epoch: gepoch,
                                        link_state: "connected".to_string(),
                                        slots: Vec::new(),
                                    },
                                );
                            }
                        }
                    }
                }
            }

            Some(pong_reply(hub))
        }
        "FAIL" => {
            // FAIL <failed_node_id>
            if parts.len() < 2 {
                return None;
            }
            let failed_id = parts[1];
            let mut nodes = hub.nodes.write();
            if let Some(node) = nodes.get_mut(failed_id) {
                node.fail = FailState::Fail;
                node.link_state = "disconnected".to_string();
            }
            Some("+OK\r\n".to_string())
        }
        "FAILOVER" => {
            // FAILOVER <new_master_id> <epoch> <slots>
            if parts.len() < 3 {
                return None;
            }
            let master_id = parts[1].to_string();
            let epoch: u64 = parts[2].parse().unwrap_or(1);
            let peer_slots = parts.get(3).map_or_else(Vec::new, |r| parse_slot_ranges(r));
            if let Some(node) = hub.nodes.write().get_mut(&master_id) {
                node.flags = "master".to_string();
                node.fail = FailState::Ok;
                node.master_id = "-".to_string();
            }
            hub.claim_slots(&master_id, Some(epoch), peer_slots);
            Some("+OK\r\n".to_string())
        }
        "FAILOVER_AUTH_REQUEST" => {
            // FAILOVER_AUTH_REQUEST <replica_id> <epoch> <master_id>
            if parts.len() < 4 {
                return None;
            }
            let req_epoch: u64 = parts[2].parse().unwrap_or(0);
            let claimed_master = parts[3];
            let is_master = *hub.role.read() == "master";
            let last_vote = hub.last_vote_epoch.load(Ordering::Relaxed);
            // Down, and not failed over yet: once another node took its
            // slots, a later election (another replica) must not take them.
            let master_is_down = {
                let nodes = hub.nodes.read();
                nodes
                    .get(claimed_master)
                    .is_some_and(|n| n.fail == FailState::Fail && !n.slots.is_empty())
            };

            if is_master && req_epoch > last_vote && master_is_down {
                hub.last_vote_epoch.store(req_epoch, Ordering::SeqCst);
                Some(format!(
                    "+FAILOVER_AUTH_ACK {} {}\r\n",
                    hub.my_id(),
                    req_epoch
                ))
            } else {
                Some("-ERR vote rejected\r\n".to_string())
            }
        }
        "FAILOVER_ANNOUNCE" => {
            // FAILOVER_ANNOUNCE <new_master_id> <epoch> <slots>
            if parts.len() < 3 {
                return None;
            }
            let new_master_id = parts[1].to_string();
            let epoch: u64 = parts[2].parse().unwrap_or(1);
            let peer_slots = parts.get(3).map_or_else(Vec::new, |r| parse_slot_ranges(r));
            if let Some(node) = hub.nodes.write().get_mut(&new_master_id) {
                node.flags = "master".to_string();
                node.fail = FailState::Ok;
                node.master_id = "-".to_string();
            }
            // Older claims (the failed master's, everywhere) give way.
            hub.claim_slots(&new_master_id, Some(epoch), peer_slots);
            Some("+OK\r\n".to_string())
        }
        _ => Some("-ERR unknown clusterbus command\r\n".to_string()),
    }
}

/// A random number below `n`, different in every process and call. (The
/// wall clock is no source: replicas reacting to the same FAIL broadcast
/// read nearly the same time, so their "random" delays matched.)
fn random_below(n: u64) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
    );
    h.finish() % n.max(1)
}

/// Masters that must agree before a node is FAIL: a majority of the
/// masters serving slots, `me` included when this node is one. (Read
/// before taking `nodes`: never take `my_slots` under it.)
fn failure_quorum(me_serves: bool, nodes: &HashMap<String, ClusterNodeInfo>) -> usize {
    let me = usize::from(me_serves);
    let masters = nodes
        .values()
        .filter(|n| n.is_master() && !n.slots.is_empty())
        .count()
        + me;
    masters / 2 + 1
}

/// A PING to `id` got no PONG. Past the node timeout the peer is PFAIL;
/// once a quorum of masters reports it (our own view counts if we are a
/// master), it is FAIL and the verdict is broadcast.
fn note_unreachable(hub: &ClusterHub, id: &str, now: u64, quorum: usize) {
    let reports = {
        let nodes = hub.nodes.read();
        let reporters = hub.pfail_reports.read();
        reporters.get(id).map_or(0, |r| {
            r.iter()
                .filter(|from| nodes.get(*from).is_some_and(|n| n.is_master()))
                .count()
        })
    };
    let me = usize::from(*hub.role.read() == "master");
    let mut broadcast = false;
    if let Some(node) = hub.nodes.write().get_mut(id) {
        let timeout = hub.node_timeout_ms.load(Ordering::Relaxed);
        if now.saturating_sub(node.pong_recv) <= timeout || node.fail == FailState::Fail {
            return;
        }
        if reports + me >= quorum {
            node.fail = FailState::Fail;
            node.link_state = "disconnected".to_string();
            broadcast = true;
        } else {
            node.fail = FailState::PFail;
        }
    }
    if broadcast {
        hub.broadcast_to_peers(&format!("FAIL {}\r\n", id));
    }
}

fn cluster_bus_tick(hub: &Arc<ClusterHub>) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let me_serves = *hub.role.read() == "master" && !hub.my_slots.read().is_empty();
    let (peers, quorum): (Vec<(String, String, u16, u16)>, usize) = {
        let nodes = hub.nodes.read();
        let p = nodes
            .values()
            .map(|n| (n.id.clone(), n.ip.clone(), n.port, n.cport))
            .collect();
        (p, failure_quorum(me_serves, &nodes))
    };

    // Prepare gossip payload of all known nodes
    let mut gossip_payload = String::new();
    {
        let nodes = hub.nodes.read();
        for n in nodes.values() {
            gossip_payload.push_str(&format!(
                "{},{},{},{},{},{};",
                n.id,
                n.ip,
                n.port,
                n.cport,
                // Fields are comma-separated: flags travel as `master|fail?`.
                n.flags_repr().replace(',', "|"),
                n.config_epoch
            ));
        }
    }
    if gossip_payload.ends_with(';') {
        gossip_payload.pop();
    }

    // Every peer at once (a bounded batch at a time): one slow or dead
    // peer must not delay the others' heartbeats.
    let exchange = |id: &str, ip: &str, cport: u16| -> bool {
        let Ok(addr) = format!("{}:{}", ip, cport).parse::<SocketAddr>() else {
            return false;
        };
        let Ok(mut stream) = TcpStream::connect_timeout(&addr, Duration::from_millis(500)) else {
            return false;
        };
        let _ = stream.set_read_timeout(Some(Duration::from_millis(1000)));
        let _ = stream.set_write_timeout(Some(Duration::from_millis(1000)));
        // Built per peer, so it carries this node's slots as of sending.
        let ping_msg = hub.ping_frame(&gossip_payload);
        let mut buf = [0u8; 4096];
        // apply_pong records the peer's slots and epoch, ignoring stale ones.
        stream.write_all(ping_msg.as_bytes()).is_ok()
            && stream
                .read(&mut buf)
                .is_ok_and(|n| hub.apply_pong(id, &String::from_utf8_lossy(&buf[..n])))
    };
    for batch in peers.chunks(64) {
        for (id, ..) in batch {
            if let Some(node) = hub.nodes.write().get_mut(id) {
                node.ping_sent = now;
            }
        }
        let results: Vec<bool> = std::thread::scope(|scope| {
            let handles: Vec<_> = batch
                .iter()
                .map(|(id, ip, _port, cport)| scope.spawn(|| exchange(id, ip, *cport)))
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or(false))
                .collect()
        });
        for ((id, ..), success) in batch.iter().zip(results) {
            if success {
                if let Some(node) = hub.nodes.write().get_mut(id) {
                    node.pong_recv = now;
                    node.link_state = "connected".to_string();
                    node.fail = FailState::Ok;
                }
                if let Some(reports) = hub.pfail_reports.write().get_mut(id) {
                    reports.remove(&hub.my_id());
                }
            } else {
                note_unreachable(hub, id, now, quorum);
            }
        }
    }

    // Automated failover trigger for replicas
    let should_elect = {
        let is_slave = *hub.role.read() == "slave";
        let master_id = hub.master_id.read().clone();
        if is_slave && master_id != "-" && !master_id.is_empty() {
            let nodes = hub.nodes.read();
            nodes
                .get(&master_id)
                .is_some_and(|n| n.fail == FailState::Fail && !n.slots.is_empty())
        } else {
            false
        }
    };

    if should_elect && !hub.election_in_progress.swap(true, Ordering::SeqCst) {
        let hub_election = hub.clone();
        std::thread::spawn(move || {
            hub_election.start_election();
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nodes_conf_holds_no_pseudo_shard_nodes() {
        // A cluster-mode node with 2 shards and no peers shows each shard
        // as a node, but saves only itself.
        let hub = ClusterHub::new(7400);
        hub.cluster_enabled.store(true, Ordering::Relaxed);
        hub.num_shards.store(2, Ordering::Relaxed);
        let pseudo = format!("{:040x}", 2);
        assert!(hub.cluster_nodes().contains(&pseudo));
        let path = std::env::temp_dir().join(format!("nodes-pseudo-{}.conf", std::process::id()));
        hub.set_config_file(path.clone());
        hub.save_config().unwrap();
        let saved = std::fs::read_to_string(&path).unwrap();
        assert!(!saved.contains(&pseudo), "{saved}");
        assert!(
            saved.contains("myself,master - 0 0 0 connected 0-16383"),
            "{saved}"
        );

        // A file an older version wrote with the pseudo node loads without it.
        std::fs::write(
            &path,
            format!("{saved}{pseudo} 127.0.0.1:7401@17401 master - 0 0 2 connected 8192-16383\n"),
        )
        .unwrap();
        let restarted = ClusterHub::new(7400);
        assert!(restarted.load_config(&path).unwrap());
        assert!(restarted.nodes.read().is_empty());
        assert_eq!(*restarted.my_slots.read(), vec![(0, 16383)]);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_newer_claim_takes_slots_and_demotes_an_emptied_master() {
        let peer = |id: &str, port: u16, epoch: u64| ClusterNodeInfo {
            id: id.to_string(),
            ip: "127.0.0.1".to_string(),
            port,
            cport: port + 10000,
            flags: "master".to_string(),
            fail: FailState::Ok,
            master_id: "-".to_string(),
            ping_sent: 0,
            pong_recv: 0,
            config_epoch: epoch,
            link_state: "connected".to_string(),
            slots: Vec::new(),
        };
        // We are the old master of 10923-16383 at epoch 0, back after a
        // failover promoted `new` at epoch 2.
        let hub = ClusterHub::new(7300);
        *hub.my_slots.write() = vec![(10923, 16383)];
        let new = "n".repeat(40);
        let other = "o".repeat(40);
        hub.nodes.write().insert(new.clone(), peer(&new, 7301, 0));
        hub.nodes
            .write()
            .insert(other.clone(), peer(&other, 7302, 0));

        // A claim no newer than ours takes nothing from us.
        hub.claim_slots(&other, Some(0), vec![(10923, 16383)]);
        assert_eq!(*hub.my_slots.read(), vec![(10923, 16383)]);
        assert!(hub.nodes.read()[&other].slots.is_empty());

        // The promoted replica's newer claim wins: we lose the slots and
        // follow it, so the slots are never served twice.
        hub.claim_slots(&new, Some(2), vec![(10923, 16383)]);
        assert!(hub.my_slots.read().is_empty());
        assert_eq!(hub.nodes.read()[&new].slots, vec![(10923, 16383)]);
        assert_eq!(*hub.role.read(), "slave");
        assert_eq!(*hub.master_id.read(), new);
        assert_eq!(hub.take_follow(), Some(("127.0.0.1".to_string(), 7301)));
        assert_eq!(hub.take_follow(), None);

        // A stale claim (another node still announcing the old epoch)
        // doesn't take the slots back from the newer owner.
        hub.claim_slots(&other, Some(1), vec![(10923, 16383)]);
        assert!(hub.nodes.read()[&other].slots.is_empty());
        assert_eq!(hub.nodes.read()[&new].slots, vec![(10923, 16383)]);
    }

    #[test]
    fn test_bus_tick_pings_peers_in_parallel() {
        // 16 peers that accept but never answer, and one that answers.
        // Pinged one after another, the tick took one timeout per silent
        // peer (3.2 s here), delaying everyone's heartbeats (and inbound
        // PINGs queued behind it); in parallel it takes about one timeout.
        let hub = Arc::new(ClusterHub::new(7500));
        let mut silent = Vec::new();
        let add = |hub: &ClusterHub, id: String, listener: &TcpListener| {
            let port = listener.local_addr().unwrap().port();
            hub.nodes.write().insert(
                id.clone(),
                ClusterNodeInfo {
                    id,
                    ip: "127.0.0.1".to_string(),
                    port: port - 10000,
                    cport: port,
                    flags: "master".to_string(),
                    fail: FailState::Ok,
                    master_id: "-".to_string(),
                    ping_sent: 0,
                    pong_recv: 0,
                    config_epoch: 0,
                    link_state: "connected".to_string(),
                    slots: Vec::new(),
                },
            );
        };
        let bind = || {
            (0..200)
                .find_map(|i| TcpListener::bind(("127.0.0.1", 47600 + i)).ok())
                .unwrap()
        };
        for i in 0..16 {
            let l = bind();
            add(&hub, format!("{:0>40}", format!("s{i}")), &l);
            silent.push(l); // accepted by the kernel, never answered
        }
        let answering = bind();
        let fast_id = "f".repeat(40);
        add(&hub, fast_id.clone(), &answering);
        let reply = format!("+PONG {fast_id} 0 master\r\n");
        std::thread::spawn(move || {
            for mut c in answering.incoming().flatten() {
                let mut buf = [0u8; 4096];
                let _ = c.read(&mut buf);
                let _ = c.write_all(reply.as_bytes());
            }
        });

        let started = std::time::Instant::now();
        cluster_bus_tick(&hub);
        let took = started.elapsed();
        assert!(took < Duration::from_millis(2000), "tick took {took:?}");
        assert!(hub.nodes.read()[&fast_id].pong_recv > 0);
        drop(silent);
    }

    #[test]
    fn test_failover_epoch_outranks_every_epoch_seen() {
        // A replica of a master promoted at epoch 5 elsewhere: its manual
        // failover must pick an epoch above 5, or the tie leaves the slots
        // with the current owner and the replica a master of nothing.
        let hub = ClusterHub::new(7600);
        hub.my_slots.write().clear();
        let owner = "o".repeat(40);
        handle_cluster_bus_line(&format!("MEET 127.0.0.1 7601 {owner} 0"), &hub);
        handle_cluster_bus_line(&format!("PING {owner} 5 master 0-5460"), &hub);
        assert!(hub.current_epoch.load(Ordering::SeqCst) >= 5);
        hub.cluster_replicate(&owner).unwrap();
        hub.cluster_failover(false).unwrap();
        assert!(hub.config_epoch.load(Ordering::SeqCst) > 5);
        assert_eq!(*hub.my_slots.read(), vec![(0, 5460)]);
    }

    #[test]
    fn test_election_needs_data_and_a_majority_of_masters() {
        // We replicate M (0-5460), which failed; A and B serve the other
        // slots and vote. A always grants; B grants only when told to.
        let hub = ClusterHub::new(7700);
        hub.my_slots.write().clear();
        let m = "m".repeat(40);
        let b_grants = Arc::new(AtomicBool::new(false));
        let voter = |grants: Option<Arc<AtomicBool>>| {
            let l = (0..200)
                .find_map(|i| TcpListener::bind(("127.0.0.1", 47900 + i)).ok())
                .unwrap();
            let cport = l.local_addr().unwrap().port();
            std::thread::spawn(move || {
                for mut c in l.incoming().flatten() {
                    let mut buf = [0u8; 512];
                    let _ = c.read(&mut buf);
                    let yes = grants.as_ref().is_none_or(|g| g.load(Ordering::SeqCst));
                    let reply: &[u8] = if yes {
                        b"+FAILOVER_AUTH_ACK x 1\r\n"
                    } else {
                        b"-ERR vote rejected\r\n"
                    };
                    let _ = c.write_all(reply);
                }
            });
            cport
        };
        let node =
            |id: &str, cport: u16, slots: Vec<(u16, u16)>, fail: FailState| ClusterNodeInfo {
                id: id.to_string(),
                ip: "127.0.0.1".to_string(),
                port: cport - 10000,
                cport,
                flags: "master".to_string(),
                fail,
                master_id: "-".to_string(),
                ping_sent: 0,
                pong_recv: 0,
                config_epoch: 0,
                link_state: "connected".to_string(),
                slots,
            };
        let (a_port, b_port) = (voter(None), voter(Some(b_grants.clone())));
        hub.nodes
            .write()
            .insert(m.clone(), node(&m, 17701, vec![(0, 5460)], FailState::Fail));
        let (a, b) = ("a".repeat(40), "b".repeat(40));
        hub.nodes.write().insert(
            a.clone(),
            node(&a, a_port, vec![(5461, 10922)], FailState::Ok),
        );
        hub.nodes.write().insert(
            b.clone(),
            node(&b, b_port, vec![(10923, 16383)], FailState::Ok),
        );
        *hub.role.write() = "slave".to_string();
        *hub.master_id.write() = m.clone();
        let won = |hub: &ClusterHub| *hub.role.read() == "master";

        // 1. Without the master's data, no election even if all would vote.
        b_grants.store(true, Ordering::SeqCst);
        hub.start_election();
        assert!(!won(&hub), "an empty replica took over");

        // 2. With data, one vote of three masters is not a majority (the
        // replica used to count a vote of its own).
        crate::replication::get_replication_hub(7700).pretend_synced();
        b_grants.store(false, Ordering::SeqCst);
        hub.start_election();
        assert!(!won(&hub), "promoted on one vote of three");

        // 3. Two of three: promoted with M's slots.
        b_grants.store(true, Ordering::SeqCst);
        hub.start_election();
        assert!(won(&hub));
        assert_eq!(*hub.my_slots.read(), vec![(0, 5460)]);
    }

    #[test]
    fn test_bus_tick_never_waits_on_my_slots_holding_nodes() {
        // Client commands check `my_slots`, then read `nodes` (MOVED). If
        // the tick held `nodes` while waiting for `my_slots`, a writer on
        // each (ADDSLOTS, a slot claim) could close a cycle and hang the
        // node. Hold `my_slots` for writing: a tick must block on it
        // without holding `nodes`.
        let hub = Arc::new(ClusterHub::new(7800));
        let guard = hub.my_slots.write();
        let ticking = hub.clone();
        let tick = std::thread::spawn(move || cluster_bus_tick(&ticking));
        std::thread::sleep(Duration::from_millis(100));
        let nodes_free = hub
            .nodes
            .try_write_for(Duration::from_millis(500))
            .is_some();
        drop(guard);
        tick.join().unwrap();
        assert!(
            nodes_free,
            "the tick held `nodes` while waiting for `my_slots`"
        );
    }

    #[test]
    fn test_replicates_slot_only_for_its_masters_slots() {
        let hub = ClusterHub::new(7900);
        hub.my_slots.write().clear();
        let m = "m".repeat(40);
        handle_cluster_bus_line(&format!("MEET 127.0.0.1 7901 {m} 0"), &hub);
        handle_cluster_bus_line(&format!("PING {m} 0 master 0-5460"), &hub);
        assert!(!hub.replicates_slot(100), "a master replicates nothing");
        hub.cluster_replicate(&m).unwrap();
        assert!(hub.replicates_slot(100));
        assert!(!hub.replicates_slot(5461));
    }

    #[test]
    fn test_pfail_follows_the_configured_node_timeout() {
        let hub = ClusterHub::new(7950);
        hub.node_timeout_ms.store(1000, Ordering::Relaxed);
        let now = now_ms();
        for (id, silent) in [("a", 1200u64), ("b", 800)] {
            let id = id.repeat(40);
            handle_cluster_bus_line(&format!("MEET 127.0.0.1 7951 {id} 0"), &hub);
            hub.nodes.write().get_mut(&id).unwrap().pong_recv = now - silent;
            note_unreachable(&hub, &id, now, 99);
        }
        assert_eq!(hub.nodes.read()[&"a".repeat(40)].fail, FailState::PFail);
        assert_eq!(hub.nodes.read()[&"b".repeat(40)].fail, FailState::Ok);
    }

    #[test]
    fn test_gossiped_node_takes_its_slots_from_its_pongs() {
        // A node first heard of through gossip must accept its own PONGs,
        // even at epoch 0 (a fresh node), or its slots stay unknown here.
        let hub = ClusterHub::new(7200);
        hub.my_slots.write().clear();
        let sender = "s".repeat(40);
        let fresh = "f".repeat(40);
        handle_cluster_bus_line(&format!("MEET 127.0.0.1 7201 {sender} 1 0-5460"), &hub);
        handle_cluster_bus_line(
            &format!("PING {sender} 1 master 0-5460 GOSSIP {fresh},127.0.0.1,7202,17202,master,0"),
            &hub,
        );
        assert_eq!(hub.nodes.read()[&fresh].config_epoch, 0);
        assert!(hub.apply_pong(&fresh, &format!("+PONG {fresh} 0 master 10923-16383")));
        assert_eq!(hub.nodes.read()[&fresh].slots, vec![(10923, 16383)]);
    }

    #[test]
    fn test_cluster_replicate_returns_the_master_to_sync_from() {
        let peer = |id: &str, port: u16, flags: &str| ClusterNodeInfo {
            id: id.to_string(),
            ip: "127.0.0.1".to_string(),
            port,
            cport: port + 10000,
            flags: flags.to_string(),
            fail: FailState::Ok,
            master_id: "-".to_string(),
            ping_sent: 0,
            pong_recv: 0,
            config_epoch: 1,
            link_state: "connected".to_string(),
            slots: Vec::new(),
        };
        let hub = ClusterHub::new(7100);
        let master = "m".repeat(40);
        let replica = "r".repeat(40);
        hub.nodes
            .write()
            .insert(master.clone(), peer(&master, 7101, "master"));
        hub.nodes
            .write()
            .insert(replica.clone(), peer(&replica, 7102, "slave"));

        // A master serving slots would lose its keys to the sync.
        assert!(!hub.my_slots.read().is_empty());
        assert!(
            hub.cluster_replicate(&master)
                .unwrap_err()
                .contains("without assigned slots")
        );
        hub.my_slots.write().clear();
        assert_eq!(
            hub.cluster_replicate(&hub.my_id()).unwrap_err(),
            "Can't replicate myself"
        );
        assert!(
            hub.cluster_replicate(&replica)
                .unwrap_err()
                .contains("not a replica")
        );
        assert!(
            hub.cluster_replicate(&"x".repeat(40))
                .unwrap_err()
                .starts_with("Unknown node")
        );
        assert_eq!(hub.replication_master_addr(), None);

        assert_eq!(
            hub.cluster_replicate(&master),
            Ok(("127.0.0.1".to_string(), 7101))
        );
        assert_eq!(
            hub.replication_master_addr(),
            Some(("127.0.0.1".to_string(), 7101))
        );

        // A restart (nodes.conf) knows which master to resume replicating.
        let path = std::env::temp_dir().join(format!("nodes-repl-{}.conf", std::process::id()));
        hub.set_config_file(path.clone());
        hub.save_config().unwrap();
        let restarted = ClusterHub::new(7100);
        assert!(restarted.load_config(&path).unwrap());
        assert_eq!(
            restarted.replication_master_addr(),
            Some(("127.0.0.1".to_string(), 7101))
        );
        let _ = std::fs::remove_file(&path);

        assert!(restarted.cluster_failover(false).is_ok());
        assert_eq!(restarted.replication_master_addr(), None);
        assert!(
            restarted
                .cluster_failover(false)
                .unwrap_err()
                .contains("to a replica")
        );
    }

    #[test]
    fn test_cluster_quorum_pfail_to_fail_escalation() {
        let hub = ClusterHub::new(7000);
        let node2_id = "node2_abcdef0123456789abcdef0123456789abcd".to_string();
        let node3_id = "node3_abcdef0123456789abcdef0123456789abcd".to_string();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Insert Node 2 and Node 3 as masters
        {
            let mut nodes = hub.nodes.write();
            nodes.insert(
                node2_id.clone(),
                ClusterNodeInfo {
                    id: node2_id.clone(),
                    ip: "127.0.0.1".to_string(),
                    port: 7001,
                    cport: 17001,
                    flags: "master".to_string(),
                    fail: FailState::Ok,
                    master_id: "-".to_string(),
                    ping_sent: now,
                    pong_recv: now.saturating_sub(6000), // silent for 6s (>5s PFAIL threshold)
                    config_epoch: 1,
                    link_state: "connected".to_string(),
                    slots: vec![(0, 5460)],
                },
            );
            nodes.insert(
                node3_id.clone(),
                ClusterNodeInfo {
                    id: node3_id.clone(),
                    ip: "127.0.0.1".to_string(),
                    port: 7002,
                    cport: 17002,
                    flags: "master".to_string(),
                    fail: FailState::Ok,
                    master_id: "-".to_string(),
                    ping_sent: now,
                    pong_recv: now,
                    config_epoch: 1,
                    link_state: "connected".to_string(),
                    slots: vec![(5461, 10922)],
                },
            );
        }

        // Masters serving slots: hub(7000) + node2 + node3 = 3, quorum 2.
        let quorum = failure_quorum(true, &hub.nodes.read());
        assert_eq!(quorum, 2);
        let node2_line = |hub: &ClusterHub| {
            hub.cluster_nodes()
                .lines()
                .find(|l| l.starts_with(&node2_id))
                .unwrap()
                .to_string()
        };

        // 1. Silent past the node timeout, our view alone: PFAIL, and it
        // keeps its role (and slots).
        note_unreachable(&hub, &node2_id, now, quorum);
        assert!(
            node2_line(&hub).contains(" master,fail? "),
            "{}",
            node2_line(&hub)
        );
        assert!(
            node2_line(&hub).ends_with(" 0-5460"),
            "{}",
            node2_line(&hub)
        );

        // 2. A replica's report does not count toward the quorum.
        let replica_id = "node4_abcdef0123456789abcdef0123456789abcd".to_string();
        let replica = ClusterNodeInfo {
            flags: "slave".to_string(),
            id: replica_id.clone(),
            port: 7003,
            cport: 17003,
            slots: Vec::new(),
            ..hub.nodes.read()[&node3_id].clone()
        };
        hub.nodes.write().insert(replica_id.clone(), replica);
        hub.pfail_reports
            .write()
            .entry(node2_id.clone())
            .or_default()
            .insert(replica_id.clone());
        note_unreachable(&hub, &node2_id, now, quorum);
        assert_eq!(hub.nodes.read()[&node2_id].fail, FailState::PFail);

        // 3. Node 3 (a master) gossips that it can't reach node 2 either:
        // FAIL.
        handle_cluster_bus_line(
            &format!(
                "PING {node3_id} 1 master 5461-10922 GOSSIP {node2_id},127.0.0.1,7001,17001,master|fail?,1"
            ),
            &hub,
        );
        assert!(hub.pfail_reports.read()[&node2_id].contains(&node3_id));
        note_unreachable(&hub, &node2_id, now, quorum);
        assert!(
            node2_line(&hub).contains(" master,fail "),
            "{}",
            node2_line(&hub)
        );

        // 4. Reading CLUSTER NODES changes nothing; hearing from the node
        // again clears it.
        let _ = hub.cluster_nodes();
        assert_eq!(hub.nodes.read()[&node2_id].fail, FailState::Fail);
        handle_cluster_bus_line(&format!("PING {} 1 master 0-5460", node2_id), &hub);
        assert_eq!(hub.nodes.read()[&node2_id].fail, FailState::Ok);
        assert!(
            node2_line(&hub).contains(" master - "),
            "{}",
            node2_line(&hub)
        );

        // 5. Test cluster_forget cleans up pfail_reports
        hub.pfail_reports
            .write()
            .entry(node2_id.clone())
            .or_default()
            .insert(node3_id.clone());
        assert!(!hub.pfail_reports.read().get(&node2_id).unwrap().is_empty());
        hub.cluster_forget(&node2_id).unwrap();
        assert!(!hub.pfail_reports.read().contains_key(&node2_id));
    }

    #[test]
    fn test_cluster_setslot_migrating_importing_formatting() {
        let hub = Arc::new(ClusterHub::new(7100));
        let target_node_id = "0123456789abcdef0123456789abcdef01234567";

        // Initial state: no migrating slots
        let initial_nodes = hub.cluster_nodes();
        assert!(!initial_nodes.contains("->-"));
        assert!(!initial_nodes.contains("-<-"));

        // Set slot 500 to migrating
        hub.slot_states
            .write()
            .insert(500, ("migrating".to_string(), target_node_id.to_string()));
        let migrating_nodes = hub.cluster_nodes();
        assert!(migrating_nodes.contains(&format!("[500->-{}]", target_node_id)));

        // Set slot 600 to importing
        hub.slot_states
            .write()
            .insert(600, ("importing".to_string(), target_node_id.to_string()));
        let dual_nodes = hub.cluster_nodes();
        assert!(dual_nodes.contains(&format!("[500->-{}]", target_node_id)));
        assert!(dual_nodes.contains(&format!("[600-<-{}]", target_node_id)));

        // Reset to stable
        hub.slot_states.write().remove(&500);
        hub.slot_states.write().remove(&600);
        let stable_nodes = hub.cluster_nodes();
        assert!(!stable_nodes.contains("->-"));
        assert!(!stable_nodes.contains("-<-"));
    }

    #[test]
    fn test_cluster_check_and_auto_rebalance_planner() {
        let hub = Arc::new(ClusterHub::new(7200));

        // 1. Cluster check on standalone initial master
        let report = hub.cluster_check();
        assert!(report.ok);
        assert_eq!(report.masters, 1);
        assert_eq!(report.total_slots_assigned, 16384);
        assert!(report.open_slots.is_empty());
        assert!(report.duplicate_slots.is_empty());
        assert!(
            report
                .format_report()
                .contains("[OK] All 16384 slots covered")
        );

        // 2. Add peer master node with 0 slots initially
        let peer_id = "1111111111111111111111111111111111111111".to_string();
        hub.nodes.write().insert(
            peer_id.clone(),
            ClusterNodeInfo {
                id: peer_id.clone(),
                ip: "127.0.0.1".to_string(),
                port: 7201,
                cport: 17201,
                flags: "master".to_string(),
                fail: FailState::Ok,
                master_id: "-".to_string(),
                ping_sent: 0,
                pong_recv: 0,
                config_epoch: 2,
                link_state: "connected".to_string(),
                slots: Vec::new(),
            },
        );

        // 3. Compute auto-rebalance plan between the 2 masters
        let opts = RebalanceOptions::default();
        let plan = hub.compute_rebalance_plan(&opts).expect("Plan computed");
        // Each master should have 8192 slots, so exactly 8192 slots should be planned to move from hub to peer
        assert_eq!(plan.len(), 8192);
        assert_eq!(plan[0].source_node_id, hub.my_id());
        assert_eq!(plan[0].target_node_id, peer_id);

        // 4. Test programmatic reshard plan: move exactly 50 slots
        let reshard_plan = hub
            .compute_reshard_plan(&peer_id, &hub.my_id(), 50)
            .expect("Reshard plan computed");
        assert_eq!(reshard_plan.len(), 50);

        // 5. Update peer slots to simulate balanced cluster (each has 8192 slots)
        let peer_slots = vec![(8192, 16383)];
        hub.nodes.write().get_mut(&peer_id).unwrap().slots = peer_slots;
        *hub.my_slots.write() = vec![(0, 8191)];

        // Re-check: cluster is fully covered with 2 masters
        let report2 = hub.cluster_check();
        assert!(report2.ok);
        assert_eq!(report2.masters, 2);
        assert_eq!(report2.total_slots_assigned, 16384);
        assert!(report2.open_slots.is_empty());

        // Auto-rebalance on already balanced cluster should generate 0 migration plans
        let plan_balanced = hub.compute_rebalance_plan(&opts).expect("Plan computed");
        assert_eq!(plan_balanced.len(), 0);
    }

    fn assert_sane(hub: &ClusterHub) {
        let nodes = hub.nodes.read();
        assert!(nodes.len() <= MAX_CLUSTER_NODES);
        for n in nodes.values() {
            let mut total = 0usize;
            for &(s, e) in &n.slots {
                assert!(s <= e && e < 16384, "{:?}", n.slots);
                total += (e - s) as usize + 1;
            }
            assert!(total <= 16384, "{total}");
        }
        for id in hub.pfail_reports.read().keys() {
            assert!(
                nodes.contains_key(id),
                "failure report for unknown node {id}"
            );
        }
    }

    #[test]
    fn test_parse_slot_ranges_keeps_only_real_slots() {
        assert_eq!(
            parse_slot_ranges("0-100,50-200,300-300"),
            vec![(0, 200), (300, 300)]
        );
        assert_eq!(
            parse_slot_ranges("0-65535,10-5,16384-16384,x-1,,-,7"),
            vec![]
        );
        assert_eq!(parse_slot_ranges("16383-16383"), vec![(16383, 16383)]);
        let many = vec!["0-16383"; 10_000].join(",");
        assert_eq!(parse_slot_ranges(&many), vec![(0, 16383)]);
    }

    /// Peer frames used to panic while holding the node-table lock (MEET with a
    /// port above 55535 overflowed `port + 10000`), poisoning it for every later
    /// CLUSTER command, or to store slot ranges past 16383 that later loops
    /// walked slot by slot.
    #[test]
    fn test_cluster_bus_rejects_hostile_frames() {
        let hub = ClusterHub::new(7300);
        for port in ["65535", "55536", "0", "-1", "x"] {
            assert_eq!(
                handle_cluster_bus_line(&format!("MEET 1.2.3.4 {port} badnode 1 0-1"), &hub),
                Some("-ERR invalid port\r\n".to_string())
            );
        }
        assert!(hub.nodes.read().is_empty());
        assert!(
            handle_cluster_bus_line("MEET 1.2.3.4 7301 peer 1 0-65535,9-3,10-20", &hub)
                .unwrap()
                .starts_with("+PONG ")
        );
        assert_eq!(hub.nodes.read()["peer"].slots, vec![(10, 20)]);
        assert_eq!(hub.nodes.read()["peer"].cport, 17301);

        // Failure reports about unknown nodes are not kept.
        handle_cluster_bus_line("PING peer 1 master 0-1 GOSSIP ghost,1.1.1.1,0,0,fail", &hub);
        assert!(hub.pfail_reports.read().is_empty());

        // Gossip and MEET stop adding nodes at the cap.
        let gossip: Vec<String> = (0..MAX_CLUSTER_NODES + 50)
            .map(|i| format!("g{i},10.0.0.1,{},{},master", 1000 + i, 11000 + i))
            .collect();
        handle_cluster_bus_line(
            &format!("PING peer 1 master 0-1 GOSSIP {}", gossip.join(";")),
            &hub,
        );
        assert_eq!(hub.nodes.read().len(), MAX_CLUSTER_NODES);
        assert_eq!(
            handle_cluster_bus_line("MEET 1.2.3.4 7302 another 1", &hub),
            Some("-ERR too many cluster nodes\r\n".to_string())
        );
        // A known node may still re-MEET.
        assert!(handle_cluster_bus_line("MEET 1.2.3.4 7301 peer 2", &hub).is_some());

        let ranges = vec!["0-65535"; 5000].join(",");
        handle_cluster_bus_line(&format!("FAILOVER_ANNOUNCE peer 3 {ranges}"), &hub);
        handle_cluster_bus_line(&format!("FAILOVER peer 4 {ranges},1-2"), &hub);
        assert_eq!(hub.nodes.read()["peer"].slots, vec![(1, 2)]);
        assert_sane(&hub);
        assert!(hub.nodes.try_write().is_some(), "node table lock left held");
        assert!(hub.cluster_check().masters > 0);
    }

    /// Cheap deterministic fuzz: random frames built from bus keywords and
    /// hostile numbers never panic and leave the node table within bounds.
    #[test]
    fn test_cluster_bus_frames_fuzz() {
        const WORDS: &[&str] = &[
            "MEET",
            "PING",
            "FAIL",
            "FAILOVER",
            "FAILOVER_AUTH_REQUEST",
            "FAILOVER_ANNOUNCE",
            "GOSSIP",
            "peer",
            "n1",
            "master",
            "slave",
            "fail",
            "fail?",
            "127.0.0.1",
            "0",
            "1",
            "65535",
            "55535",
            "55536",
            "16383",
            "16384",
            "18446744073709551615",
            "-1",
            "0-16383",
            "0-65535",
            "5-1",
            "1-2,3-4",
            "a,b,c,d,e",
            "n1,1.2.3.4,7000,17000,fail",
            "n2,1.2.3.4,65535,0,master,1;n3,::1,1,1,fail",
            ",,,,;;;",
            "\u{fffd}",
            "",
        ];
        let hub = ClusterHub::new(7400);
        *hub.role.write() = "master".to_string();
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        for _ in 0..20_000 {
            let n = (next() % 8) as usize;
            let line: Vec<&str> = (0..n)
                .map(|_| WORDS[(next() % WORDS.len() as u64) as usize])
                .collect();
            let _ = handle_cluster_bus_line(&line.join(" "), &hub);
        }
        assert_sane(&hub);
        let _ = hub.cluster_check();
        let _ = hub.cluster_nodes();
    }

    /// Frames are whole lines: one split across reads is reassembled, and a
    /// line longer than `MAX_BUS_LINE` drops the connection.
    #[test]
    fn test_cluster_bus_framing() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let hub = Arc::new(ClusterHub::new(7500));
        let server_hub = hub.clone();
        let server = std::thread::spawn(move || {
            for _ in 0..2 {
                let (s, _) = listener.accept().unwrap();
                s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
                handle_cluster_bus_conn(s, &server_hub);
            }
        });

        let mut c = TcpStream::connect(addr).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        c.write_all(b"MEET 1.2.3.4 7501 split").unwrap();
        c.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        c.write_all(b" 1 0-5\r\n").unwrap();
        let mut buf = [0u8; 256];
        let n = c.read(&mut buf).unwrap();
        assert!(buf[..n].starts_with(b"+PONG "));
        assert_eq!(hub.nodes.read()["split"].slots, vec![(0, 5)]);
        drop(c);

        let mut c = TcpStream::connect(addr).unwrap();
        c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let huge = vec![b'A'; MAX_BUS_LINE + 10];
        let _ = c.write_all(&huge);
        let mut rest = Vec::new();
        // The server hangs up without replying.
        assert!(matches!(c.read_to_end(&mut rest), Ok(0) | Err(_)));
        server.join().unwrap();
    }

    #[test]
    fn test_cluster_save_and_load_nodes_conf() {
        let dir = std::env::temp_dir().join(format!(
            "rudis_cluster_cfg_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let cfg_path = dir.join("nodes-7600.conf");

        let hub = ClusterHub::new(7600);
        hub.set_config_file(cfg_path.clone());
        assert_eq!(hub.config_file_name(), "nodes-7600.conf");
        *hub.my_id.write() = "1111111111111111111111111111111111111111".to_string();
        hub.current_epoch.store(12, Ordering::SeqCst);
        hub.config_epoch.store(7, Ordering::SeqCst);
        hub.last_vote_epoch.store(5, Ordering::SeqCst);
        *hub.my_slots.write() = vec![(0, 5460), (10000, 10000)];
        hub.slot_states.write().insert(
            5460,
            (
                "migrating".to_string(),
                "2222222222222222222222222222222222222222".to_string(),
            ),
        );
        hub.slot_states.write().insert(
            5461,
            (
                "importing".to_string(),
                "2222222222222222222222222222222222222222".to_string(),
            ),
        );
        hub.nodes.write().insert(
            "2222222222222222222222222222222222222222".to_string(),
            ClusterNodeInfo {
                id: "2222222222222222222222222222222222222222".to_string(),
                ip: "127.0.0.1".to_string(),
                port: 7601,
                cport: 17601,
                flags: "master".to_string(),
                fail: FailState::Ok,
                master_id: "-".to_string(),
                ping_sent: 0,
                pong_recv: 0,
                config_epoch: 9,
                link_state: "connected".to_string(),
                slots: vec![(5461, 9999), (10001, 16383)],
            },
        );
        hub.has_nodes.store(true, Ordering::Release);
        hub.save_config().unwrap();

        let loaded = ClusterHub::new(7600);
        assert!(loaded.load_config(&cfg_path).unwrap());
        assert_eq!(loaded.my_id(), "1111111111111111111111111111111111111111");
        assert_eq!(loaded.current_epoch.load(Ordering::Relaxed), 12);
        assert_eq!(loaded.config_epoch.load(Ordering::Relaxed), 7);
        assert_eq!(loaded.last_vote_epoch.load(Ordering::Relaxed), 5);
        assert_eq!(*loaded.my_slots.read(), vec![(0, 5460), (10000, 10000)]);
        assert_eq!(
            loaded.slot_states.read().get(&5460),
            Some(&(
                "migrating".to_string(),
                "2222222222222222222222222222222222222222".to_string()
            ))
        );
        assert_eq!(
            loaded.slot_states.read().get(&5461),
            Some(&(
                "importing".to_string(),
                "2222222222222222222222222222222222222222".to_string()
            ))
        );
        let peers = loaded.nodes.read();
        let peer = peers
            .get("2222222222222222222222222222222222222222")
            .unwrap();
        assert_eq!(peer.port, 7601);
        assert_eq!(peer.cport, 17601);
        assert_eq!(peer.config_epoch, 9);
        assert_eq!(peer.slots, vec![(5461, 9999), (10001, 16383)]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
