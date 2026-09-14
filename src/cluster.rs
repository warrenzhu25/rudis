use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
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

pub struct ClusterHub {
    pub port: u16,
    pub cport: u16,
    pub my_id: RwLock<String>,
    pub current_epoch: AtomicU64,
    pub config_epoch: AtomicU64,
    pub role: RwLock<String>, // "master" or "slave"
    pub master_id: RwLock<String>, // "-" or ID
    pub nodes: RwLock<HashMap<String, ClusterNodeInfo>>,
    pub my_slots: RwLock<Vec<(u16, u16)>>,
    pub pfail_reports: RwLock<HashMap<String, HashSet<String>>>,
    pub bus_running: AtomicBool,
    pub cancel_bus: RwLock<Option<flume::Sender<()>>>,
}

static CLUSTER_HUBS: LazyLock<RwLock<HashMap<u16, Arc<ClusterHub>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn get_cluster_hub(port: u16) -> Arc<ClusterHub> {
    let mut hubs = CLUSTER_HUBS.write().unwrap();
    if let Some(hub) = hubs.get(&port) {
        return hub.clone();
    }
    let hub = Arc::new(ClusterHub::new(port));
    hubs.insert(port, hub.clone());
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
        let hub = Self {
            port,
            cport,
            my_id: RwLock::new(my_id),
            current_epoch: AtomicU64::new(1),
            config_epoch: AtomicU64::new(1),
            role: RwLock::new("master".to_string()),
            master_id: RwLock::new("-".to_string()),
            nodes: RwLock::new(HashMap::new()),
            my_slots: RwLock::new(vec![(0, 16383)]),
            pfail_reports: RwLock::new(HashMap::new()),
            bus_running: AtomicBool::new(false),
            cancel_bus: RwLock::new(None),
        };
        hub
    }

    pub fn my_id(&self) -> String {
        self.my_id.read().unwrap().clone()
    }

    pub fn cluster_nodes(&self) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let mut out = String::new();

        // 1. Myself entry
        let my_id = self.my_id();
        let role = self.role.read().unwrap().clone();
        let master_id = self.master_id.read().unwrap().clone();
        let cfg_epoch = self.config_epoch.load(Ordering::Relaxed);
        let my_slots = self.my_slots.read().unwrap().clone();
        let mut my_slots_str = String::new();
        for (s, e) in &my_slots {
            if s == e {
                my_slots_str.push_str(&format!(" {}", s));
            } else {
                my_slots_str.push_str(&format!(" {}-{}", s, e));
            }
        }
        let my_flags = format!("myself,{}", role);
        out.push_str(&format!(
            "{} 127.0.0.1:{}@{} {} {} 0 0 {} connected{}\n",
            my_id, self.port, self.cport, my_flags, master_id, cfg_epoch, my_slots_str
        ));

        // 2. Peer entries
        let mut nodes = self.nodes.write().unwrap();
        let pfail_reports = self.pfail_reports.read().unwrap();
        let mut keys: Vec<String> = nodes.keys().cloned().collect();
        keys.sort();

        for id in keys {
            if id == my_id {
                continue;
            }
            if let Some(node) = nodes.get_mut(&id) {
                let silence = now.saturating_sub(node.pong_recv);
                if silence > 10000 {
                    node.flags = "fail".to_string();
                    node.link_state = "disconnected".to_string();
                } else if silence > 5000 {
                    let reports = pfail_reports.get(&id).map(|s| s.len()).unwrap_or(0);
                    if reports >= 1 {
                        node.flags = "fail".to_string();
                    } else {
                        node.flags = "fail?".to_string();
                    }
                } else if node.flags == "fail?" || node.flags == "fail" {
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
        let nodes = self.nodes.read().unwrap();
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

        let cport = port + 10000;
        let temp_id = generate_node_id(port);

        // Pre-insert peer into nodes table so it is known immediately
        {
            let mut nodes = self.nodes.write().unwrap();
            if !nodes.values().any(|n| n.port == port) {
                let next_epoch = self.current_epoch.load(Ordering::Relaxed) + 1;
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
                        config_epoch: next_epoch,
                        link_state: "connected".to_string(),
                        slots: Vec::new(),
                    },
                );
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
            let my_slots = self.my_slots.read().unwrap().clone();
            let mut slots_repr = String::new();
            for (s, e) in my_slots {
                slots_repr.push_str(&format!("{}-{},", s, e));
            }
            if slots_repr.ends_with(',') {
                slots_repr.pop();
            }
            let meet_frame = format!(
                "MEET 127.0.0.1 {} {} {} {}\r\n",
                self.port, self.my_id(), my_epoch, slots_repr
            );
            if stream.write_all(meet_frame.as_bytes()).is_ok() {
                let mut buf = [0u8; 512];
                if let Ok(n) = stream.read(&mut buf) {
                    let resp = String::from_utf8_lossy(&buf[..n]);
                    // Parse: "+PONG <id> <epoch> <role> <slots>"
                    if resp.starts_with("+PONG") {
                        let parts: Vec<&str> = resp.trim().split_whitespace().collect();
                        if parts.len() >= 4 {
                            let remote_id = parts[1].to_string();
                            let remote_epoch: u64 = parts[2].parse().unwrap_or(1);
                            let remote_role = parts[3].to_string();
                            let mut remote_slots = Vec::new();
                            if parts.len() >= 5 {
                                for r in parts[4].split(',') {
                                    if let Some((start, end)) = r.split_once('-') {
                                        if let (Ok(s), Ok(e)) = (start.parse::<u16>(), end.parse::<u16>()) {
                                            remote_slots.push((s, e));
                                        }
                                    }
                                }
                            }
                            let mut nodes = self.nodes.write().unwrap();
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
        let mut nodes = self.nodes.write().unwrap();
        nodes.remove(node_id);
        Ok(())
    }

    pub fn cluster_replicate(&self, master_id: &str) -> Result<(), String> {
        let nodes = self.nodes.read().unwrap();
        if !nodes.contains_key(master_id) {
            return Err("ERR Unknown node".to_string());
        }
        *self.role.write().unwrap() = "slave".to_string();
        *self.master_id.write().unwrap() = master_id.to_string();
        self.my_slots.write().unwrap().clear();
        Ok(())
    }

    pub fn cluster_failover(&self, _force: bool) -> Result<(), String> {
        let new_epoch = self.current_epoch.fetch_add(1, Ordering::SeqCst) + 1;
        self.config_epoch.store(new_epoch, Ordering::SeqCst);
        let old_master_id = self.master_id.write().unwrap().clone();
        let my_id = self.my_id();
        *self.role.write().unwrap() = "master".to_string();
        *self.master_id.write().unwrap() = "-".to_string();

        // Inherit slots from old master if known
        let mut inherited_slots = Vec::new();
        {
            let mut nodes = self.nodes.write().unwrap();
            if let Some(m) = nodes.get_mut(&old_master_id) {
                inherited_slots = std::mem::take(&mut m.slots);
                m.flags = "slave".to_string();
                m.master_id = my_id.clone();
            }
        }
        if !inherited_slots.is_empty() {
            *self.my_slots.write().unwrap() = inherited_slots.clone();
        }

        // Notify replication hub if active
        crate::replication::get_replication_hub(self.port).make_master();

        // Broadcast FAILOVER to all peers
        let nodes = self.nodes.read().unwrap();
        for peer in nodes.values() {
            let addr = format!("{}:{}", peer.ip, peer.cport);
            if let Ok(mut stream) = TcpStream::connect_timeout(
                &addr.parse().unwrap(),
                Duration::from_millis(200),
            ) {
                let mut slots_repr = String::new();
                for (s, e) in &inherited_slots {
                    slots_repr.push_str(&format!("{}-{},", s, e));
                }
                if slots_repr.ends_with(',') {
                    slots_repr.pop();
                }
                let msg = format!(
                    "FAILOVER {} {} {}\r\n",
                    my_id, new_epoch, slots_repr
                );
                let _ = stream.write_all(msg.as_bytes());
            }
        }

        Ok(())
    }

    pub fn cluster_reset(&self, hard: bool) -> Result<(), String> {
        self.nodes.write().unwrap().clear();
        self.pfail_reports.write().unwrap().clear();
        if hard {
            let new_id = generate_node_id(self.port);
            *self.my_id.write().unwrap() = new_id;
            self.current_epoch.store(1, Ordering::SeqCst);
            self.config_epoch.store(1, Ordering::SeqCst);
            *self.role.write().unwrap() = "master".to_string();
            *self.master_id.write().unwrap() = "-".to_string();
            *self.my_slots.write().unwrap() = vec![(0, 16383)];
        }
        Ok(())
    }
}

pub fn start_cluster_bus(port: u16) {
    let hub = get_cluster_hub(port);
    if hub.bus_running.swap(true, Ordering::SeqCst) {
        return;
    }

    let cport = port + 10000;
    let (cancel_tx, cancel_rx) = flume::bounded(1);
    *hub.cancel_bus.write().unwrap() = Some(cancel_tx);

    let hub_clone = hub.clone();
    std::thread::Builder::new()
        .name(format!("cluster-bus-{}", cport))
        .spawn(move || {
            let socket = match socket2::Socket::new(
                socket2::Domain::IPV4,
                socket2::Type::STREAM,
                Some(socket2::Protocol::TCP),
            ) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[ClusterBus {}] Socket create error: {}", cport, e);
                    return;
                }
            };
            let _ = socket.set_reuse_address(true);
            let _ = socket.set_reuse_port(true);
            let _ = socket.set_nonblocking(true);
            let bind_addr: SocketAddr = format!("0.0.0.0:{}", cport).parse().unwrap();
            if let Err(e) = socket.bind(&bind_addr.into()) {
                eprintln!("[ClusterBus {}] Socket bind error: {}", cport, e);
                return;
            }
            if let Err(e) = socket.listen(128) {
                eprintln!("[ClusterBus {}] Socket listen error: {}", cport, e);
                return;
            }
            let listener: TcpListener = socket.into();

            let mut last_tick = std::time::Instant::now();

            while cancel_rx.is_empty() {
                // 1. Accept new cluster bus connections
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = stream.set_read_timeout(Some(Duration::from_millis(300)));
                        let _ = stream.set_write_timeout(Some(Duration::from_millis(300)));
                        handle_cluster_bus_conn(stream, &hub_clone);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => {
                        std::thread::sleep(Duration::from_millis(20));
                    }
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

fn handle_cluster_bus_conn(mut stream: TcpStream, hub: &Arc<ClusterHub>) {
    let mut buf = [0u8; 1024];
    let n = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };

    let msg = String::from_utf8_lossy(&buf[..n]);
    for line in msg.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.is_empty() {
            continue;
        }
        match parts[0] {
            "MEET" => {
                // MEET <ip> <port> <node_id> <epoch> <slots>
                if parts.len() >= 4 {
                    let peer_ip = parts[1].to_string();
                    let peer_port: u16 = parts[2].parse().unwrap_or(0);
                    let peer_id = parts[3].to_string();
                    let peer_epoch: u64 = if parts.len() >= 5 {
                        parts[4].parse().unwrap_or(1)
                    } else {
                        1
                    };
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    let mut peer_slots = Vec::new();
                    if parts.len() >= 6 {
                        for r in parts[5].split(',') {
                            if let Some((s, e)) = r.split_once('-') {
                                if let (Ok(s), Ok(e)) = (s.parse::<u16>(), e.parse::<u16>()) {
                                    peer_slots.push((s, e));
                                }
                            }
                        }
                    }

                    {
                        let mut nodes = hub.nodes.write().unwrap();
                        nodes.insert(
                            peer_id.clone(),
                            ClusterNodeInfo {
                                id: peer_id,
                                ip: peer_ip,
                                port: peer_port,
                                cport: peer_port + 10000,
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
                    let my_epoch = hub.config_epoch.load(Ordering::Relaxed);
                    let role = hub.role.read().unwrap().clone();
                    let my_slots = hub.my_slots.read().unwrap().clone();
                    let mut slots_repr = String::new();
                    for (s, e) in my_slots {
                        slots_repr.push_str(&format!("{}-{},", s, e));
                    }
                    if slots_repr.ends_with(',') {
                        slots_repr.pop();
                    }
                    let resp = format!(
                        "+PONG {} {} {} {}\r\n",
                        hub.my_id(), my_epoch, role, slots_repr
                    );
                    let _ = stream.write_all(resp.as_bytes());
                }
            }
            "PING" => {
                // PING <node_id> <epoch> <flags> <slots> [GOSSIP <peers...>]
                if parts.len() >= 2 {
                    let peer_id = parts[1].to_string();
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    {
                        let mut nodes = hub.nodes.write().unwrap();
                        if let Some(node) = nodes.get_mut(&peer_id) {
                            node.pong_recv = now;
                            node.link_state = "connected".to_string();
                            if parts.len() >= 4 {
                                node.flags = parts[3].to_string();
                            }
                        }
                    }

                    // Parse gossip section if present
                    if let Some(gossip_pos) = parts.iter().position(|&p| p == "GOSSIP") {
                        if gossip_pos + 1 < parts.len() {
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
                                        let mut nodes = hub.nodes.write().unwrap();
                                        if let Some(n) = nodes.get_mut(&gid) {
                                            if gflags == "fail" {
                                                n.flags = "fail".to_string();
                                            }
                                        } else if gport > 0 {
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
                    }

                    let my_epoch = hub.config_epoch.load(Ordering::Relaxed);
                    let role = hub.role.read().unwrap().clone();
                    let my_slots = hub.my_slots.read().unwrap().clone();
                    let mut slots_repr = String::new();
                    for (s, e) in my_slots {
                        slots_repr.push_str(&format!("{}-{},", s, e));
                    }
                    if slots_repr.ends_with(',') {
                        slots_repr.pop();
                    }
                    let resp = format!(
                        "+PONG {} {} {} {}\r\n",
                        hub.my_id(), my_epoch, role, slots_repr
                    );
                    let _ = stream.write_all(resp.as_bytes());
                }
            }
            "FAIL" => {
                // FAIL <failed_node_id>
                if parts.len() >= 2 {
                    let failed_id = parts[1];
                    let mut nodes = hub.nodes.write().unwrap();
                    if let Some(node) = nodes.get_mut(failed_id) {
                        node.flags = "fail".to_string();
                        node.link_state = "disconnected".to_string();
                    }
                    let _ = stream.write_all(b"+OK\r\n");
                }
            }
            "FAILOVER" => {
                // FAILOVER <new_master_id> <epoch> <slots>
                if parts.len() >= 3 {
                    let master_id = parts[1].to_string();
                    let epoch: u64 = parts[2].parse().unwrap_or(1);
                    let mut peer_slots = Vec::new();
                    if parts.len() >= 4 {
                        for r in parts[3].split(',') {
                            if let Some((s, e)) = r.split_once('-') {
                                if let (Ok(s), Ok(e)) = (s.parse::<u16>(), e.parse::<u16>()) {
                                    peer_slots.push((s, e));
                                }
                            }
                        }
                    }
                    let mut nodes = hub.nodes.write().unwrap();
                    if let Some(node) = nodes.get_mut(&master_id) {
                        node.flags = "master".to_string();
                        node.master_id = "-".to_string();
                        node.config_epoch = epoch;
                        if !peer_slots.is_empty() {
                            node.slots = peer_slots;
                        }
                    }
                    let _ = stream.write_all(b"+OK\r\n");
                }
            }
            _ => {
                let _ = stream.write_all(b"-ERR unknown clusterbus command\r\n");
            }
        }
    }
}

fn cluster_bus_tick(hub: &Arc<ClusterHub>) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let peers: Vec<(String, String, u16, u16)> = {
        let nodes = hub.nodes.read().unwrap();
        nodes
            .values()
            .map(|n| (n.id.clone(), n.ip.clone(), n.port, n.cport))
            .collect()
    };

    // Prepare gossip payload of all known nodes
    let mut gossip_payload = String::new();
    {
        let nodes = hub.nodes.read().unwrap();
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

    let my_epoch = hub.config_epoch.load(Ordering::Relaxed);
    let role = hub.role.read().unwrap().clone();
    let my_slots = hub.my_slots.read().unwrap().clone();
    let mut slots_repr = String::new();
    for (s, e) in my_slots {
        slots_repr.push_str(&format!("{}-{},", s, e));
    }
    if slots_repr.ends_with(',') {
        slots_repr.pop();
    }

    for (id, ip, _port, cport) in peers {
        let addr = format!("{}:{}", ip, cport);
        let parsed_addr = match addr.parse::<SocketAddr>() {
            Ok(a) => a,
            Err(_) => continue,
        };

        // Record ping_sent
        if let Ok(mut nodes) = hub.nodes.write() {
            if let Some(node) = nodes.get_mut(&id) {
                node.ping_sent = now;
            }
        }

        let success = if let Ok(mut stream) =
            TcpStream::connect_timeout(&parsed_addr, Duration::from_millis(200))
        {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
            let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
            let ping_msg = format!(
                "PING {} {} {} {} GOSSIP {}\r\n",
                hub.my_id(), my_epoch, role, slots_repr, gossip_payload
            );
            if stream.write_all(ping_msg.as_bytes()).is_ok() {
                let mut buf = [0u8; 512];
                if let Ok(n) = stream.read(&mut buf) {
                    let resp = String::from_utf8_lossy(&buf[..n]);
                    resp.starts_with("+PONG")
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        };

        if let Ok(mut nodes) = hub.nodes.write() {
            if let Some(node) = nodes.get_mut(&id) {
                if success {
                    node.pong_recv = now;
                    node.link_state = "connected".to_string();
                    if node.flags == "fail?" || node.flags == "fail" {
                        node.flags = "master".to_string();
                    }
                } else {
                    let elapsed = now.saturating_sub(node.pong_recv);
                    if elapsed > 10000 {
                        node.flags = "fail".to_string();
                        node.link_state = "disconnected".to_string();
                    } else if elapsed > 5000 {
                        node.flags = "fail?".to_string();
                    }
                }
            }
        }
    }
}
