use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

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
        }
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
        let mut nodes = self.nodes.write();
        let mut key = node_id;
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
        if let Some(node) = nodes.get_mut(key) {
            apply_peer_slots(node, epoch, slots);
        }
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut out = String::new();

        // 1. Myself entry
        let my_id = self.my_id();
        let role = self.role.read().clone();
        let master_id = self.master_id.read().clone();
        let cfg_epoch = self.config_epoch.load(Ordering::Relaxed);
        let is_cluster = self.cluster_enabled.load(Ordering::Relaxed);
        let num_shards = self.num_shards.load(Ordering::Relaxed).max(1);
        let my_slots_str = if is_cluster && self.nodes.read().is_empty() {
            let end_slot = 16384 / num_shards - 1;
            format!(" 0-{}", end_slot)
        } else {
            let my_slots = self.my_slots.read().clone();
            let mut s_str = String::new();
            for (s, e) in &my_slots {
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
        let mut nodes = self.nodes.write();
        if is_cluster && nodes.is_empty() {
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
        let pfail_reports = self.pfail_reports.read();
        let mut keys: Vec<String> = nodes.keys().cloned().collect();
        keys.sort();

        let total_masters = nodes
            .values()
            .filter(|n| n.master_id == "-" || (!n.flags.contains("slave") && !n.flags.is_empty()))
            .count()
            + if *self.role.read() == "master" { 1 } else { 0 };
        let quorum = (total_masters / 2) + 1;

        for id in keys {
            if id == my_id {
                continue;
            }
            if let Some(node) = nodes.get_mut(&id) {
                let silence = now.saturating_sub(node.pong_recv);
                let pfail_count = pfail_reports.get(&id).map(|s| s.len()).unwrap_or(0);
                let total_pfail_votes = pfail_count + if silence > 5000 { 1 } else { 0 };

                if node.flags == "fail" || total_pfail_votes >= quorum {
                    node.flags = "fail".to_string();
                    node.link_state = "disconnected".to_string();
                } else if silence > 5000 {
                    node.flags = "fail?".to_string();
                } else if node.flags == "fail?" {
                    node.flags = "master".to_string();
                    node.link_state = "connected".to_string();
                }

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
                    node.flags,
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
        let pfail_count = nodes.values().filter(|n| n.flags.contains("fail?")).count();
        let fail_count = nodes.values().filter(|n| n.flags == "fail").count();
        let state = if fail_count > 0 { "fail" } else { "ok" };
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
                                    flags: remote_role,
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

        Ok(())
    }

    pub fn cluster_forget(&self, node_id: &str) -> Result<(), String> {
        let mut nodes = self.nodes.write();
        nodes.remove(node_id);
        let mut pfail = self.pfail_reports.write();
        pfail.remove(node_id);
        for reports in pfail.values_mut() {
            reports.remove(node_id);
        }
        Ok(())
    }

    pub fn cluster_replicate(&self, master_id: &str) -> Result<(), String> {
        let nodes = self.nodes.read();
        if !nodes.contains_key(master_id) {
            return Err("ERR Unknown node".to_string());
        }
        *self.role.write() = "slave".to_string();
        *self.master_id.write() = master_id.to_string();
        self.my_slots.write().clear();
        Ok(())
    }

    pub fn cluster_failover(&self, _force: bool) -> Result<(), String> {
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
            if peer.flags.contains("master") {
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
                    let health = if n.flags.contains("fail") {
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
            if peer.flags.contains("master") {
                let mut shard_nodes = Vec::new();
                let master_health = if peer.flags.contains("fail") {
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
                        let health = if n.flags.contains("fail") {
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
        let mut my_slots = self.my_slots.write();
        if my_slots.len() == 1 && my_slots[0] == (0, 16383) {
            my_slots.clear();
        }
        for &s in slots {
            my_slots.push((s, s));
        }
        compact_slots(&mut my_slots);
        Ok(())
    }

    pub fn cluster_delslots(&self, slots: &[u16]) -> Result<(), String> {
        let mut my_slots = self.my_slots.write();
        remove_slots(&mut my_slots, slots);
        Ok(())
    }

    pub fn cluster_addslotsrange(&self, ranges: &[(u16, u16)]) -> Result<(), String> {
        for &(start, end) in ranges {
            if start > end || end >= 16384 {
                return Err(format!("ERR Invalid slot range {}-{}", start, end));
            }
        }
        let mut my_slots = self.my_slots.write();
        if my_slots.len() == 1 && my_slots[0] == (0, 16383) {
            my_slots.clear();
        }
        for &(start, end) in ranges {
            my_slots.push((start, end));
        }
        compact_slots(&mut my_slots);
        Ok(())
    }

    pub fn cluster_delslotsrange(&self, ranges: &[(u16, u16)]) -> Result<(), String> {
        let mut my_slots = self.my_slots.write();
        let mut to_remove = Vec::new();
        for &(start, end) in ranges {
            for s in start..=end {
                to_remove.push(s);
            }
        }
        remove_slots(&mut my_slots, &to_remove);
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
                if node.flags.contains("master") {
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
                if n.flags.contains("master") && n.id != my_id {
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

        let req_epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1;

        let masters: Vec<(String, u16)> = {
            let nodes = self.nodes.read();
            nodes
                .values()
                .filter(|n| n.flags.contains("master") && !n.flags.contains("fail"))
                .map(|n| (n.ip.clone(), n.cport))
                .collect()
        };

        let mut votes = 1; // self vote
        let total_masters = masters.len() + 1; // plus the failed master

        for (ip, cport) in &masters {
            let addr = format!("{}:{}", ip, cport);
            if let Ok(parsed) = addr.parse::<SocketAddr>()
                && let Ok(mut stream) =
                    TcpStream::connect_timeout(&parsed, Duration::from_millis(200))
            {
                let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
                let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
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
                    m.flags = "fail".to_string();
                }
            }
            if !inherited.is_empty() {
                *self.my_slots.write() = inherited.clone();
            }

            crate::replication::get_replication_hub(self.port).make_master();

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
fn slots_repr(slots: &[(u16, u16)]) -> String {
    slots
        .iter()
        .map(|(s, e)| format!("{}-{}", s, e))
        .collect::<Vec<_>>()
        .join(",")
}

/// Applies a peer's own announcement of its slots and config epoch.
///
/// An announcement older than the last one applied (lower epoch) is stale:
/// it was built before a migration the peer has since taken part in, so it
/// must not undo the newer assignment.
fn apply_peer_slots(node: &mut ClusterNodeInfo, epoch: Option<u64>, slots: Vec<(u16, u16)>) {
    if let Some(epoch) = epoch {
        if epoch < node.config_epoch {
            return;
        }
        node.config_epoch = epoch;
    }
    if !slots.is_empty() {
        node.slots = slots;
    }
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

            let mut last_tick = std::time::Instant::now();
            let open_conns = Arc::new(std::sync::atomic::AtomicUsize::new(0));

            while cancel_rx.is_empty() {
                // 1. Accept new cluster bus connections
                let mut accepted_any = false;
                for listener in &listeners {
                    if let Ok((stream, _)) = listener.accept() {
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
                    std::thread::sleep(Duration::from_millis(20));
                }

                // 2. Heartbeat tick every 500ms
                if last_tick.elapsed() >= Duration::from_millis(500) {
                    last_tick = std::time::Instant::now();
                    cluster_bus_tick(&hub_clone);
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
                nodes.insert(
                    peer_id.clone(),
                    ClusterNodeInfo {
                        id: peer_id,
                        ip: peer_ip,
                        port: peer_port,
                        cport: peer_cport,
                        flags: "master".to_string(),
                        master_id: "-".to_string(),
                        ping_sent: now,
                        pong_recv: now,
                        config_epoch: peer_epoch,
                        link_state: "connected".to_string(),
                        slots: peer_slots,
                    },
                );
            }

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
                    if parts.len() >= 4 {
                        node.flags = parts[3].to_string();
                    }
                    if parts.len() >= 5 {
                        let epoch = parts[2].parse::<u64>().ok();
                        apply_peer_slots(node, epoch, parse_slot_ranges(parts[4]));
                    }
                }
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
                        let gflags = fields[4].to_string();
                        if gid != hub.my_id() {
                            let mut nodes = hub.nodes.write();
                            // Failure reports only count for nodes we know.
                            if nodes.contains_key(&gid) {
                                if gflags.contains("fail") {
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
                                if gflags == "fail" {
                                    n.flags = "fail".to_string();
                                }
                            } else if gport > 0 && nodes.len() < MAX_CLUSTER_NODES {
                                nodes.insert(
                                    gid.clone(),
                                    ClusterNodeInfo {
                                        id: gid,
                                        ip: gip,
                                        port: gport,
                                        cport: gcport,
                                        flags: gflags,
                                        master_id: "-".to_string(),
                                        ping_sent: now,
                                        pong_recv: now,
                                        config_epoch: 1,
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
                node.flags = "fail".to_string();
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
            let mut nodes = hub.nodes.write();
            if let Some(node) = nodes.get_mut(&master_id) {
                node.flags = "master".to_string();
                node.master_id = "-".to_string();
                node.config_epoch = epoch;
                if !peer_slots.is_empty() {
                    node.slots = peer_slots;
                }
            }
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
            let master_is_down = {
                let nodes = hub.nodes.read();
                nodes
                    .get(claimed_master)
                    .map(|n| n.flags.contains("fail"))
                    .unwrap_or(false)
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
            let mut nodes = hub.nodes.write();
            if let Some(node) = nodes.get_mut(&new_master_id) {
                node.flags = "master".to_string();
                node.master_id = "-".to_string();
                node.config_epoch = epoch;
                if !peer_slots.is_empty() {
                    node.slots = peer_slots.clone();
                }
            }
            if !peer_slots.is_empty() {
                let mut to_remove = Vec::new();
                for &(s, e) in &peer_slots {
                    for slot in s..=e {
                        to_remove.push(slot);
                    }
                }
                for (other_id, other_node) in nodes.iter_mut() {
                    if other_id != &new_master_id {
                        remove_slots(&mut other_node.slots, &to_remove);
                    }
                }
            }
            Some("+OK\r\n".to_string())
        }
        _ => Some("-ERR unknown clusterbus command\r\n".to_string()),
    }
}

fn cluster_bus_tick(hub: &Arc<ClusterHub>) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let (peers, quorum): (Vec<(String, String, u16, u16)>, usize) = {
        let nodes = hub.nodes.read();
        let p = nodes
            .values()
            .map(|n| (n.id.clone(), n.ip.clone(), n.port, n.cport))
            .collect();
        let total_masters = nodes
            .values()
            .filter(|n| n.master_id == "-" || (!n.flags.contains("slave") && !n.flags.is_empty()))
            .count()
            + if *hub.role.read() == "master" { 1 } else { 0 };
        (p, (total_masters / 2) + 1)
    };

    // Prepare gossip payload of all known nodes
    let mut gossip_payload = String::new();
    {
        let nodes = hub.nodes.read();
        for n in nodes.values() {
            gossip_payload.push_str(&format!(
                "{},{},{},{},{},{};",
                n.id, n.ip, n.port, n.cport, n.flags, n.config_epoch
            ));
        }
    }
    if gossip_payload.ends_with(';') {
        gossip_payload.pop();
    }

    for (id, ip, _port, cport) in peers {
        let addr = format!("{}:{}", ip, cport);
        let parsed_addr = match addr.parse::<SocketAddr>() {
            Ok(a) => a,
            Err(_) => continue,
        };

        // Record ping_sent
        if let Some(node) = hub.nodes.write().get_mut(&id) {
            node.ping_sent = now;
        }

        if let Ok(mut stream) = TcpStream::connect_timeout(&parsed_addr, Duration::from_millis(200))
        {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
            let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
            // Built per peer, so it carries this node's slots as of sending.
            let ping_msg = hub.ping_frame(&gossip_payload);
            let mut buf = [0u8; 512];
            // apply_pong records the peer's slots and epoch, ignoring stale ones.
            let success = stream.write_all(ping_msg.as_bytes()).is_ok()
                && stream
                    .read(&mut buf)
                    .is_ok_and(|n| hub.apply_pong(&id, &String::from_utf8_lossy(&buf[..n])));

            if let Some(node) = hub.nodes.write().get_mut(&id) {
                if success {
                    node.pong_recv = now;
                    node.link_state = "connected".to_string();
                    if node.flags == "fail?" || node.flags == "fail" {
                        node.flags = "master".to_string();
                    }
                    if let Some(reports) = hub.pfail_reports.write().get_mut(&id) {
                        reports.remove(&hub.my_id());
                    }
                } else {
                    let elapsed = now.saturating_sub(node.pong_recv);
                    if elapsed > 5000 {
                        let pfail_count = hub
                            .pfail_reports
                            .read()
                            .get(&id)
                            .map(|s| s.len())
                            .unwrap_or(0);
                        let total_votes = pfail_count + 1;
                        if total_votes >= quorum {
                            node.flags = "fail".to_string();
                            node.link_state = "disconnected".to_string();
                            hub.broadcast_to_peers(&format!("FAIL {}\r\n", node.id));
                        } else {
                            node.flags = "fail?".to_string();
                        }
                    }
                }
            }
        } else if let Some(node) = hub.nodes.write().get_mut(&id) {
            let elapsed = now.saturating_sub(node.pong_recv);
            if elapsed > 5000 {
                let pfail_count = hub
                    .pfail_reports
                    .read()
                    .get(&id)
                    .map(|s| s.len())
                    .unwrap_or(0);
                let total_votes = pfail_count + 1;
                if total_votes >= quorum {
                    node.flags = "fail".to_string();
                    node.link_state = "disconnected".to_string();
                    hub.broadcast_to_peers(&format!("FAIL {}\r\n", node.id));
                } else {
                    node.flags = "fail?".to_string();
                }
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
                .map(|n| n.flags.contains("fail"))
                .unwrap_or(false)
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
                    master_id: "-".to_string(),
                    ping_sent: now,
                    pong_recv: now,
                    config_epoch: 1,
                    link_state: "connected".to_string(),
                    slots: vec![(5461, 10922)],
                },
            );
        }

        // Total masters: hub(7000) + node2 + node3 = 3. Quorum = (3/2) + 1 = 2.
        // 1. Without peer corroboration, Node 2 is marked fail? (PFAIL), NOT fail
        let nodes_output = hub.cluster_nodes();
        assert!(nodes_output.contains(&format!("{} 127.0.0.1:7001@17001 fail?", node2_id)));
        assert!(!nodes_output.contains(&format!("{} 127.0.0.1:7001@17001 fail ", node2_id)));

        // 2. Node 3 reports Node 2 as failing
        hub.pfail_reports
            .write()
            .entry(node2_id.clone())
            .or_default()
            .insert(node3_id.clone());

        // Now total votes = 1 (local) + 1 (node 3) = 2 >= quorum(2).
        // Node 2 must be promoted to fail!
        let nodes_output_escalated = hub.cluster_nodes();
        assert!(
            nodes_output_escalated.contains(&format!("{} 127.0.0.1:7001@17001 fail", node2_id))
        );

        // 3. Node 3 retracts report
        hub.pfail_reports
            .write()
            .get_mut(&node2_id)
            .unwrap()
            .remove(&node3_id);
        {
            let mut nodes = hub.nodes.write();
            nodes.get_mut(&node2_id).unwrap().flags = "master".to_string();
        }
        let nodes_output_retracted = hub.cluster_nodes();
        assert!(
            nodes_output_retracted.contains(&format!("{} 127.0.0.1:7001@17001 fail?", node2_id))
        );
        assert!(
            !nodes_output_retracted.contains(&format!("{} 127.0.0.1:7001@17001 fail ", node2_id))
        );

        // 4. Test cluster_forget cleans up pfail_reports
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
}
