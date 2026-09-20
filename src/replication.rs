use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReplicationRole {
    Master {
        replid: String,
        replid2: String,
        second_offset: i64,
    },
    Slave {
        master_host: String,
        master_port: u16,
        link_status: String,
        master_repl_offset: u64,
        master_replid: String,
        sync_in_progress: bool,
    },
}

pub struct ConnectedReplica {
    pub id: u64,
    pub sender: flume::Sender<Vec<u8>>,
    pub listening_port: AtomicU64,
    pub ack_offset: AtomicU64,
    pub last_ack_time: AtomicU64,
}

pub struct ShardReplicaFlow {
    pub client_id: u64,
    pub shard_id: usize,
    pub sender: flume::Sender<Vec<u8>>,
    pub lsn: AtomicU64,
    pub ack_lsn: AtomicU64,
}

pub struct ReplicationBacklog {
    pub buffer: Vec<u8>,
    pub write_idx: usize,
    pub len: usize,
    pub max_size: usize,
    pub first_byte_offset: u64,
}

impl ReplicationBacklog {
    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: vec![0u8; max_size],
            write_idx: 0,
            len: 0,
            max_size,
            first_byte_offset: 1,
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn append(&mut self, data: &[u8], current_master_offset: u64) {
        let n = data.len();
        if n == 0 {
            return;
        }
        if n >= self.max_size {
            let slice = &data[n - self.max_size..];
            self.buffer[..self.max_size].copy_from_slice(slice);
            self.write_idx = 0;
            self.len = self.max_size;
            self.first_byte_offset = current_master_offset.saturating_sub(self.max_size as u64) + 1;
            return;
        }

        let first_chunk = (self.max_size - self.write_idx).min(n);
        self.buffer[self.write_idx..self.write_idx + first_chunk]
            .copy_from_slice(&data[..first_chunk]);
        let second_chunk = n - first_chunk;
        if second_chunk > 0 {
            self.buffer[..second_chunk].copy_from_slice(&data[first_chunk..]);
        }
        self.write_idx = (self.write_idx + n) % self.max_size;
        self.len = (self.len + n).min(self.max_size);
        self.first_byte_offset = current_master_offset.saturating_sub(self.len as u64) + 1;
    }

    pub fn can_partial_sync(&self, target_offset: u64, current_master_offset: u64) -> bool {
        if target_offset > current_master_offset + 1 {
            return false;
        }
        if self.len == 0 {
            return target_offset == current_master_offset + 1;
        }
        target_offset >= self.first_byte_offset
    }

    pub fn get_diff(&self, target_offset: u64, current_master_offset: u64) -> Option<Vec<u8>> {
        if !self.can_partial_sync(target_offset, current_master_offset) {
            return None;
        }
        if target_offset == current_master_offset + 1 {
            return Some(Vec::new());
        }
        let diff_len = (current_master_offset + 1 - target_offset) as usize;
        if diff_len > self.len {
            return None;
        }
        let mut out = vec![0u8; diff_len];
        let read_start = (self.write_idx + self.max_size - diff_len) % self.max_size;
        let first_chunk = (self.max_size - read_start).min(diff_len);
        out[..first_chunk].copy_from_slice(&self.buffer[read_start..read_start + first_chunk]);
        let second_chunk = diff_len - first_chunk;
        if second_chunk > 0 {
            out[first_chunk..].copy_from_slice(&self.buffer[..second_chunk]);
        }
        Some(out)
    }
}

pub struct ReplicationHub {
    pub port: u16,
    pub role: RwLock<ReplicationRole>,
    pub master_replid: String,
    pub master_repl_offset: AtomicU64,
    pub is_slave_atomic: std::sync::atomic::AtomicBool,
    pub has_replicas: std::sync::atomic::AtomicBool,
    pub backlog_active: std::sync::atomic::AtomicBool,
    pub backlog: RwLock<ReplicationBacklog>,
    pub replicas: RwLock<HashMap<u64, Arc<ConnectedReplica>>>,
    pub cancel_sync: RwLock<Option<flume::Sender<()>>>,
    pub shard_flows: RwLock<HashMap<usize, HashMap<u64, Arc<ShardReplicaFlow>>>>,
    pub has_shard_flows: std::sync::atomic::AtomicBool,
}

impl ReplicationHub {
    pub fn new(port: u16) -> Self {
        use fxhash::hash64;
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let h1 = hash64(&port.to_le_bytes());
        let h2 = hash64(&t.to_le_bytes());
        let replid = format!("{:016x}{:016x}{:08x}", h1, h2, port);

        Self {
            port,
            role: RwLock::new(ReplicationRole::Master {
                replid: replid.clone(),
                replid2: "0000000000000000000000000000000000000000".to_string(),
                second_offset: -1,
            }),
            master_replid: replid,
            master_repl_offset: AtomicU64::new(0),
            is_slave_atomic: std::sync::atomic::AtomicBool::new(false),
            has_replicas: std::sync::atomic::AtomicBool::new(false),
            backlog_active: std::sync::atomic::AtomicBool::new(true),
            backlog: RwLock::new(ReplicationBacklog::new(1024 * 1024)),
            replicas: RwLock::new(HashMap::new()),
            cancel_sync: RwLock::new(None),
            shard_flows: RwLock::new(HashMap::new()),
            has_shard_flows: std::sync::atomic::AtomicBool::new(false),
        }
    }

    #[inline(always)]
    pub fn is_master(&self) -> bool {
        !self.is_slave_atomic.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn is_slave(&self) -> bool {
        self.is_slave_atomic.load(Ordering::Relaxed)
    }

    pub fn make_master(&self) {
        use fxhash::hash64;
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let h1 = hash64(&self.port.to_le_bytes());
        let h2 = hash64(&t.to_le_bytes());
        let new_replid = format!("{:016x}{:016x}{:08x}", h1, h2, self.port);

        let mut role = self.role.write().unwrap();
        let (replid2, second_offset) = match &*role {
            ReplicationRole::Slave {
                master_replid,
                master_repl_offset,
                ..
            } if !master_replid.is_empty() => (master_replid.clone(), *master_repl_offset as i64),
            _ => ("0000000000000000000000000000000000000000".to_string(), -1),
        };

        if second_offset >= 0 {
            self.master_repl_offset
                .store(second_offset as u64, Ordering::SeqCst);
        }

        *role = ReplicationRole::Master {
            replid: new_replid,
            replid2,
            second_offset,
        };
        self.is_slave_atomic.store(false, Ordering::Release);
    }

    pub fn stop_sync(&self) {
        if let Some(cancel) = self.cancel_sync.write().unwrap().take() {
            let _ = cancel.send(());
        }
    }

    pub fn activate_backlog(&self) {
        self.backlog_active.store(true, Ordering::Release);
        HAS_ACTIVE_REPLICATION.store(true, Ordering::Release);
    }

    pub fn register_replica(
        &self,
        id: u64,
        sender: flume::Sender<Vec<u8>>,
    ) -> Arc<ConnectedReplica> {
        let rep = Arc::new(ConnectedReplica {
            id,
            sender,
            listening_port: AtomicU64::new(0),
            ack_offset: AtomicU64::new(0),
            last_ack_time: AtomicU64::new(0),
        });
        self.replicas.write().unwrap().insert(id, rep.clone());
        self.has_replicas.store(true, Ordering::Release);
        self.backlog_active.store(true, Ordering::Release);
        HAS_ACTIVE_REPLICATION.store(true, Ordering::Release);
        rep
    }

    pub fn unregister_replica(&self, id: u64) {
        let mut reps = self.replicas.write().unwrap();
        reps.remove(&id);
        if reps.is_empty() {
            self.has_replicas.store(false, Ordering::Release);
        }
    }

    pub fn update_replica_ack(&self, id: u64, offset: u64) {
        if let Some(rep) = self.replicas.read().unwrap().get(&id) {
            rep.ack_offset.store(offset, Ordering::SeqCst);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            rep.last_ack_time.store(now, Ordering::SeqCst);
        }
    }

    pub fn set_replica_port(&self, id: u64, port: u16) {
        if let Some(rep) = self.replicas.read().unwrap().get(&id) {
            rep.listening_port.store(port as u64, Ordering::SeqCst);
        }
    }

    pub fn register_shard_flow(
        &self,
        shard_id: usize,
        client_id: u64,
        sender: flume::Sender<Vec<u8>>,
    ) -> Arc<ShardReplicaFlow> {
        let flow = Arc::new(ShardReplicaFlow {
            client_id,
            shard_id,
            sender,
            lsn: AtomicU64::new(0),
            ack_lsn: AtomicU64::new(0),
        });
        let mut flows = self.shard_flows.write().unwrap();
        flows
            .entry(shard_id)
            .or_default()
            .insert(client_id, flow.clone());
        self.has_shard_flows.store(true, Ordering::Release);
        HAS_ACTIVE_REPLICATION.store(true, Ordering::Release);
        flow
    }

    pub fn unregister_shard_flow(&self, shard_id: usize, client_id: u64) {
        let mut flows = self.shard_flows.write().unwrap();
        if let Some(map) = flows.get_mut(&shard_id) {
            map.remove(&client_id);
            if map.is_empty() {
                flows.remove(&shard_id);
            }
        }
        let any_left = !flows.is_empty();
        self.has_shard_flows.store(any_left, Ordering::Release);
    }

    pub fn update_shard_flow_ack(&self, shard_id: usize, client_id: u64, ack_lsn: u64) {
        let flows = self.shard_flows.read().unwrap();
        if let Some(map) = flows.get(&shard_id)
            && let Some(flow) = map.get(&client_id)
        {
            flow.ack_lsn.store(ack_lsn, Ordering::Relaxed);
        }
    }

    pub fn try_partial_resync(
        &self,
        client_id: u64,
        sender: flume::Sender<Vec<u8>>,
        req_replid: &str,
        req_offset: i64,
    ) -> Option<(String, Vec<u8>, Arc<ConnectedReplica>)> {
        if req_offset < 0 {
            return None;
        }
        let target_offset = (req_offset as u64) + 1;

        let (current_replid, replid_matches) = {
            let role = self.role.read().unwrap();
            match &*role {
                ReplicationRole::Master {
                    replid,
                    replid2,
                    second_offset,
                } => {
                    let matches = req_replid == replid
                        || req_replid == self.master_replid
                        || (!replid2.is_empty()
                            && req_replid == replid2
                            && *second_offset >= 0
                            && req_offset <= *second_offset);
                    (replid.clone(), matches)
                }
                _ => (String::new(), false),
            }
        };

        if !replid_matches {
            return None;
        }

        let current_offset = self.master_repl_offset.load(Ordering::SeqCst);
        let backlog = self.backlog.read().unwrap();
        if !backlog.can_partial_sync(target_offset, current_offset) {
            return None;
        }

        let diff = backlog.get_diff(target_offset, current_offset)?;
        drop(backlog);

        let rep = self.register_replica(client_id, sender);
        Some((current_replid, diff, rep))
    }

    pub fn can_partial_resync(&self, req_replid: &str, req_offset: i64) -> bool {
        if req_offset < 0 {
            return false;
        }
        let target_offset = (req_offset as u64) + 1;
        let role = self.role.read().unwrap();
        let replid_matches = match &*role {
            ReplicationRole::Master {
                replid,
                replid2,
                second_offset,
            } => {
                if req_replid == replid || req_replid == self.master_replid {
                    true
                } else {
                    !replid2.is_empty()
                        && req_replid == replid2
                        && *second_offset >= 0
                        && req_offset <= *second_offset
                }
            }
            _ => false,
        };
        if !replid_matches {
            return false;
        }
        let current_offset = self.master_repl_offset.load(Ordering::SeqCst);
        let backlog = self.backlog.read().unwrap();
        backlog.can_partial_sync(target_offset, current_offset)
    }

    pub fn propagate(&self, bytes: &[u8]) {
        if !self.is_master() {
            return;
        }
        if !self.backlog_active.load(Ordering::Relaxed)
            && !self.has_replicas.load(Ordering::Relaxed)
        {
            return;
        }
        if self.backlog_active.load(Ordering::Relaxed) {
            let new_offset = self
                .master_repl_offset
                .fetch_add(bytes.len() as u64, Ordering::SeqCst)
                + bytes.len() as u64;
            self.backlog.write().unwrap().append(bytes, new_offset);
        }

        if self.has_replicas.load(Ordering::Relaxed) {
            let dead: Vec<u64> = {
                let reps = self.replicas.read().unwrap();
                let mut to_remove = Vec::new();
                for (&id, rep) in reps.iter() {
                    if rep.sender.send(bytes.to_vec()).is_err() {
                        to_remove.push(id);
                    }
                }
                to_remove
            };
            if !dead.is_empty() {
                let mut reps = self.replicas.write().unwrap();
                for id in dead {
                    reps.remove(&id);
                }
            }
        }

        // Also broadcast to all shard flows if generic propagate was called
        if self.has_shard_flows.load(Ordering::Relaxed) {
            let dead_flows: Vec<(usize, u64)> = {
                let flows = self.shard_flows.read().unwrap();
                let mut dead = Vec::new();
                for (&sid, map) in flows.iter() {
                    for (&cid, flow) in map {
                        flow.lsn.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        if flow.sender.send(bytes.to_vec()).is_err() {
                            dead.push((sid, cid));
                        }
                    }
                }
                dead
            };
            if !dead_flows.is_empty() {
                let mut flows = self.shard_flows.write().unwrap();
                for (sid, cid) in dead_flows {
                    if let Some(map) = flows.get_mut(&sid) {
                        map.remove(&cid);
                    }
                }
            }
        }
    }

    pub fn propagate_shard(&self, shard_id: usize, bytes: &[u8]) {
        if !self.is_master() {
            return;
        }
        if self.backlog_active.load(Ordering::Relaxed) {
            let new_offset = self
                .master_repl_offset
                .fetch_add(bytes.len() as u64, Ordering::SeqCst)
                + bytes.len() as u64;
            self.backlog.write().unwrap().append(bytes, new_offset);
        }
        if self.has_replicas.load(Ordering::Relaxed) {
            let dead: Vec<u64> = {
                let reps = self.replicas.read().unwrap();
                let mut to_remove = Vec::new();
                for (&id, rep) in reps.iter() {
                    if rep.sender.send(bytes.to_vec()).is_err() {
                        to_remove.push(id);
                    }
                }
                to_remove
            };
            if !dead.is_empty() {
                let mut reps = self.replicas.write().unwrap();
                for id in dead {
                    reps.remove(&id);
                }
            }
        }

        if !self.has_shard_flows.load(Ordering::Relaxed) {
            return;
        }

        let dead_flows: Vec<u64> = {
            let flows = self.shard_flows.read().unwrap();
            let mut dead = Vec::new();
            if let Some(map) = flows.get(&shard_id) {
                for (&cid, flow) in map {
                    flow.lsn.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    if flow.sender.send(bytes.to_vec()).is_err() {
                        dead.push(cid);
                    }
                }
            }
            dead
        };

        if !dead_flows.is_empty() {
            let mut flows = self.shard_flows.write().unwrap();
            if let Some(map) = flows.get_mut(&shard_id) {
                for cid in dead_flows {
                    map.remove(&cid);
                }
            }
        }
    }

    pub fn format_role_resp(&self) -> Vec<u8> {
        let role = self.role.read().unwrap().clone();
        match role {
            ReplicationRole::Master { .. } => {
                let offset = self.master_repl_offset.load(Ordering::SeqCst);
                let reps = self.replicas.read().unwrap();
                let mut out = Vec::with_capacity(256);
                out.extend_from_slice(b"*3\r\n$6\r\nmaster\r\n:");
                out.extend_from_slice(offset.to_string().as_bytes());
                out.extend_from_slice(format!("\r\n*{}\r\n", reps.len()).as_bytes());
                for rep in reps.values() {
                    let rport = rep.listening_port.load(Ordering::SeqCst);
                    let rack = rep.ack_offset.load(Ordering::SeqCst);
                    out.extend_from_slice(b"*3\r\n$9\r\n127.0.0.1\r\n$");
                    let rport_s = rport.to_string();
                    out.extend_from_slice(rport_s.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(rport_s.as_bytes());
                    out.extend_from_slice(b"\r\n:");
                    out.extend_from_slice(rack.to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                out
            }
            ReplicationRole::Slave {
                master_host,
                master_port,
                link_status,
                master_repl_offset,
                ..
            } => {
                let mut out = Vec::with_capacity(128);
                out.extend_from_slice(b"*5\r\n$5\r\nslave\r\n$");
                out.extend_from_slice(master_host.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(master_host.as_bytes());
                out.extend_from_slice(format!("\r\n:{}\r\n$", master_port).as_bytes());
                let state_str = if link_status == "up" {
                    "connected"
                } else {
                    "connect"
                };
                out.extend_from_slice(state_str.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(state_str.as_bytes());
                out.extend_from_slice(format!("\r\n:{}\r\n", master_repl_offset).as_bytes());
                out
            }
        }
    }

    pub fn format_info_replication(&self) -> String {
        let role = self.role.read().unwrap().clone();
        match role {
            ReplicationRole::Master {
                replid,
                replid2,
                second_offset,
            } => {
                let reps = self.replicas.read().unwrap();
                let offset = self.master_repl_offset.load(Ordering::SeqCst);
                let backlog = self.backlog.read().unwrap();
                format!(
                    "# Replication\r\n\
                     role:master\r\n\
                     connected_slaves:{}\r\n\
                     master_replid:{}\r\n\
                     master_replid2:{}\r\n\
                     master_repl_offset:{}\r\n\
                     second_repl_offset:{}\r\n\
                     repl_backlog_active:1\r\n\
                     repl_backlog_size:{}\r\n\
                     repl_backlog_first_byte_offset:{}\r\n\
                     repl_backlog_histlen:{}\r\n",
                    reps.len(),
                    replid,
                    replid2,
                    offset,
                    second_offset,
                    backlog.max_size,
                    backlog.first_byte_offset,
                    backlog.len()
                )
            }
            ReplicationRole::Slave {
                master_host,
                master_port,
                link_status,
                master_repl_offset,
                master_replid,
                sync_in_progress,
            } => {
                let displayed_replid = if !master_replid.is_empty() {
                    master_replid.as_str()
                } else {
                    self.master_replid.as_str()
                };
                format!(
                    "# Replication\r\n\
                     role:slave\r\n\
                     master_host:{}\r\n\
                     master_port:{}\r\n\
                     master_link_status:{}\r\n\
                     master_last_io_seconds_ago:0\r\n\
                     master_sync_in_progress:{}\r\n\
                     slave_repl_offset:{}\r\n\
                     slave_priority:100\r\n\
                     slave_read_only:1\r\n\
                     connected_slaves:0\r\n\
                     master_replid:{}\r\n\
                     master_repl_offset:{}\r\n",
                    master_host,
                    master_port,
                    link_status,
                    if sync_in_progress { 1 } else { 0 },
                    master_repl_offset,
                    displayed_replid,
                    master_repl_offset,
                )
            }
        }
    }
}

pub static HAS_ACTIVE_REPLICATION: AtomicBool = AtomicBool::new(false);
pub static HAS_SLAVE_INSTANCE: AtomicBool = AtomicBool::new(false);

static REPLICATION_HUBS: LazyLock<RwLock<HashMap<u16, Arc<ReplicationHub>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn get_replication_hub(port: u16) -> Arc<ReplicationHub> {
    {
        let hubs = REPLICATION_HUBS.read().unwrap();
        if let Some(hub) = hubs.get(&port) {
            return hub.clone();
        }
    }
    let mut hubs = REPLICATION_HUBS.write().unwrap();
    hubs.entry(port)
        .or_insert_with(|| Arc::new(ReplicationHub::new(port)))
        .clone()
}

#[inline(always)]
pub fn has_connected_replicas(port: u16) -> bool {
    if !HAS_ACTIVE_REPLICATION.load(Ordering::Relaxed) {
        return false;
    }
    let hubs = REPLICATION_HUBS.read().unwrap();
    if let Some(hub) = hubs.get(&port) {
        hub.has_replicas.load(Ordering::Relaxed) || hub.backlog_active.load(Ordering::Relaxed)
    } else {
        false
    }
}

#[inline(always)]
pub fn propagate_bytes(port: u16, bytes: &[u8]) {
    if !HAS_ACTIVE_REPLICATION.load(Ordering::Relaxed) {
        return;
    }
    let hub = get_replication_hub(port);
    hub.propagate(bytes);
}

#[inline(always)]
pub fn propagate_shard_bytes(port: u16, shard_id: usize, bytes: &[u8]) {
    if !HAS_ACTIVE_REPLICATION.load(Ordering::Relaxed) {
        return;
    }
    let hub = get_replication_hub(port);
    hub.propagate_shard(shard_id, bytes);
}

pub fn record_mutation(
    port: u16,
    aof: Option<&std::cell::RefCell<crate::aof::AofWriter>>,
    cmd: &crate::resp::Command,
) {
    if let Some(bytes) = crate::aof::command_to_resp(cmd) {
        if let Some(aof) = aof {
            aof.borrow_mut().append(&bytes);
        }
        propagate_bytes(port, &bytes);
    }
}

pub fn start_replica_sync(
    port: u16,
    master_host: String,
    master_port: u16,
    router: crate::router::Router,
) {
    let hub = get_replication_hub(port);
    hub.stop_sync();

    hub.is_slave_atomic.store(true, Ordering::Release);
    HAS_SLAVE_INSTANCE.store(true, Ordering::Release);

    let (cached_replid, cached_offset) = {
        let role = hub.role.read().unwrap();
        if let ReplicationRole::Slave {
            master_host: ref prev_host,
            master_port: prev_port,
            ref master_replid,
            master_repl_offset,
            ..
        } = *role
        {
            if prev_host == &master_host && prev_port == master_port {
                (master_replid.clone(), master_repl_offset)
            } else {
                (String::new(), 0)
            }
        } else {
            (String::new(), 0)
        }
    };

    *hub.role.write().unwrap() = ReplicationRole::Slave {
        master_host: master_host.clone(),
        master_port,
        link_status: "connecting".to_string(),
        master_repl_offset: cached_offset,
        master_replid: cached_replid,
        sync_in_progress: true,
    };

    let (cancel_tx, cancel_rx) = flume::bounded(1);
    *hub.cancel_sync.write().unwrap() = Some(cancel_tx);

    let hub_clone = hub.clone();
    monoio::spawn(async move {
        run_replica_worker(port, master_host, master_port, router, cancel_rx, hub_clone).await;
    });
}

#[inline]
fn is_sync_cancelled(rx: &flume::Receiver<()>) -> bool {
    rx.try_recv().is_ok() || rx.is_disconnected()
}

async fn run_replica_worker(
    my_port: u16,
    master_host: String,
    master_port: u16,
    router: crate::router::Router,
    cancel_rx: flume::Receiver<()>,
    hub: Arc<ReplicationHub>,
) {
    use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
    use monoio::net::TcpStream;

    let addr_str = format!("{}:{}", master_host, master_port);
    let addr: std::net::SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(_) => {
            if master_host == "localhost" {
                format!("127.0.0.1:{}", master_port).parse().unwrap()
            } else {
                return;
            }
        }
    };

    'reconnect_loop: loop {
        if is_sync_cancelled(&cancel_rx) {
            break 'reconnect_loop;
        }

        let mut stream = match TcpStream::connect(&addr).await {
            Ok(s) => s,
            Err(_) => {
                if let ReplicationRole::Slave {
                    ref mut link_status,
                    ..
                } = *hub.role.write().unwrap()
                {
                    *link_status = "down".to_string();
                }
                if is_sync_cancelled(&cancel_rx) {
                    break 'reconnect_loop;
                }
                monoio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue 'reconnect_loop;
            }
        };

        let mut buf = bytes::BytesMut::with_capacity(65536);
        let mut read_buf = vec![0u8; 8192];

        macro_rules! send_and_expect_line {
            ($payload:expr) => {{
                if stream.write_all($payload).await.0.is_err() {
                    if let ReplicationRole::Slave {
                        ref mut link_status,
                        ..
                    } = *hub.role.write().unwrap()
                    {
                        *link_status = "down".to_string();
                    }
                    if is_sync_cancelled(&cancel_rx) {
                        break 'reconnect_loop;
                    }
                    monoio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue 'reconnect_loop;
                }
                let mut line_res = None;
                loop {
                    if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
                        let line = buf.split_to(pos + 2);
                        line_res = Some(line);
                        break;
                    }
                    let (res, returned) = stream.read(read_buf).await;
                    read_buf = returned;
                    match res {
                        Ok(0) | Err(_) => {
                            if let ReplicationRole::Slave {
                                ref mut link_status,
                                ..
                            } = *hub.role.write().unwrap()
                            {
                                *link_status = "down".to_string();
                            }
                            break;
                        }
                        Ok(n) => buf.extend_from_slice(&read_buf[..n]),
                    }
                }
                match line_res {
                    Some(l) => l,
                    None => {
                        if is_sync_cancelled(&cancel_rx) {
                            break 'reconnect_loop;
                        }
                        monoio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue 'reconnect_loop;
                    }
                }
            }};
        }

        // 1. PING
        let line = send_and_expect_line!(b"*1\r\n$4\r\nPING\r\n");
        if !line.starts_with(b"+PONG") {
            if is_sync_cancelled(&cancel_rx) {
                break 'reconnect_loop;
            }
            monoio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue 'reconnect_loop;
        }

        // 2. REPLCONF listening-port
        let my_port_s = my_port.to_string();
        let replconf_port = format!(
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${}\r\n{}\r\n",
            my_port_s.len(),
            my_port_s
        );
        let line = send_and_expect_line!(replconf_port.into_bytes());
        if !line.starts_with(b"+OK") {
            if is_sync_cancelled(&cancel_rx) {
                break 'reconnect_loop;
            }
            monoio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue 'reconnect_loop;
        }

        // 3. REPLCONF capa psync2
        let line = send_and_expect_line!(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
        if !line.starts_with(b"+OK") {
            if is_sync_cancelled(&cancel_rx) {
                break 'reconnect_loop;
            }
            monoio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue 'reconnect_loop;
        }

        // 4. PSYNC
        let (cached_replid, cached_offset) = {
            let role = hub.role.read().unwrap();
            if let ReplicationRole::Slave {
                ref master_replid,
                master_repl_offset,
                ..
            } = *role
            {
                (master_replid.clone(), master_repl_offset)
            } else {
                (String::new(), 0)
            }
        };

        let psync_payload = if !cached_replid.is_empty() {
            format!(
                "*3\r\n$5\r\nPSYNC\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                cached_replid.len(),
                cached_replid,
                cached_offset.to_string().len(),
                cached_offset
            )
            .into_bytes()
        } else {
            b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".to_vec()
        };

        let line = send_and_expect_line!(psync_payload);
        let is_continue = line.starts_with(b"+CONTINUE");
        if !line.starts_with(b"+FULLRESYNC") && !is_continue {
            if let ReplicationRole::Slave {
                ref mut link_status,
                ..
            } = *hub.role.write().unwrap()
            {
                *link_status = "down".to_string();
            }
            if is_sync_cancelled(&cancel_rx) {
                break 'reconnect_loop;
            }
            monoio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue 'reconnect_loop;
        }

        let line_str = String::from_utf8_lossy(&line);
        let parts: Vec<&str> = line_str.split_whitespace().collect();

        let (new_replid, initial_offset) = if is_continue {
            let r_id = if parts.len() >= 2 {
                parts[1].to_string()
            } else {
                cached_replid.clone()
            };
            (r_id, cached_offset)
        } else {
            let r_id = if parts.len() >= 2 {
                parts[1].to_string()
            } else {
                String::new()
            };
            let off: u64 = if parts.len() >= 3 {
                parts[2].parse().unwrap_or(0)
            } else {
                0
            };
            (r_id, off)
        };

        if !is_continue {
            // 5. Read RDB header: $<len>\r\n
            let mut rdb_len: Option<usize> = None;
            loop {
                if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
                    let line = buf.split_to(pos + 2);
                    if line.starts_with(b"$") {
                        let s = std::str::from_utf8(&line[1..line.len() - 2]).unwrap_or("0");
                        rdb_len = Some(s.parse().unwrap_or(0));
                        break;
                    }
                }
                let (res, returned) = stream.read(read_buf).await;
                read_buf = returned;
                match res {
                    Ok(0) | Err(_) => break,
                    Ok(n) => buf.extend_from_slice(&read_buf[..n]),
                }
            }

            let rdb_len = match rdb_len {
                Some(l) => l,
                None => {
                    if let ReplicationRole::Slave {
                        ref mut link_status,
                        ..
                    } = *hub.role.write().unwrap()
                    {
                        *link_status = "down".to_string();
                    }
                    if is_sync_cancelled(&cancel_rx) {
                        break 'reconnect_loop;
                    }
                    monoio::time::sleep(std::time::Duration::from_millis(50)).await;
                    continue 'reconnect_loop;
                }
            };

            // 6. Read rdb_len bytes
            let mut read_failed = false;
            while buf.len() < rdb_len {
                let (res, returned) = stream.read(read_buf).await;
                read_buf = returned;
                match res {
                    Ok(0) | Err(_) => {
                        read_failed = true;
                        break;
                    }
                    Ok(n) => buf.extend_from_slice(&read_buf[..n]),
                }
            }
            if read_failed {
                if let ReplicationRole::Slave {
                    ref mut link_status,
                    ..
                } = *hub.role.write().unwrap()
                {
                    *link_status = "down".to_string();
                }
                if is_sync_cancelled(&cancel_rx) {
                    break 'reconnect_loop;
                }
                monoio::time::sleep(std::time::Duration::from_millis(50)).await;
                continue 'reconnect_loop;
            }

            let rdb_bytes = buf.split_to(rdb_len).freeze();

            // 7. Restore RDB into router
            router.restore_rdb_bytes(rdb_bytes).await;
        }

        // 8. Mark link_status up
        {
            let mut role = hub.role.write().unwrap();
            if let ReplicationRole::Slave {
                ref mut link_status,
                ref mut master_repl_offset,
                ref mut master_replid,
                ref mut sync_in_progress,
                ..
            } = *role
            {
                *link_status = "up".to_string();
                *master_repl_offset = initial_offset;
                *master_replid = new_replid;
                *sync_in_progress = false;
            }
        }

        // 9. Streaming loop: receive and apply mutations
        let mut current_offset = initial_offset;
        loop {
            if is_sync_cancelled(&cancel_rx) {
                break 'reconnect_loop;
            }

            while !buf.is_empty() {
                let initial_buf_len = buf.len();
                match crate::resp::parse_command(&mut buf) {
                    Ok(Some(cmd)) => {
                        let consumed = initial_buf_len - buf.len();
                        current_offset += consumed as u64;

                        match &cmd {
                            crate::resp::Command::Replconf(args) => {
                                if args.len() >= 2 && args[0].eq_ignore_ascii_case(b"getack") {
                                    let off_str = current_offset.to_string();
                                    let ack_reply = format!(
                                        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                                        off_str.len(),
                                        off_str
                                    );
                                    let _ = stream.write_all(ack_reply.into_bytes()).await.0;
                                }
                            }
                            crate::resp::Command::Ping(_) => {}
                            _ => {
                                router.execute_replica_command(cmd).await;
                            }
                        }

                        if let ReplicationRole::Slave {
                            ref mut master_repl_offset,
                            ..
                        } = *hub.role.write().unwrap()
                        {
                            *master_repl_offset = current_offset;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {
                        buf.clear();
                        break;
                    }
                }
            }

            let (res, returned) = stream.read(read_buf).await;
            read_buf = returned;
            match res {
                Ok(0) | Err(_) => break,
                Ok(n) => buf.extend_from_slice(&read_buf[..n]),
            }
        }

        if let ReplicationRole::Slave {
            ref mut link_status,
            ..
        } = *hub.role.write().unwrap()
        {
            *link_status = "down".to_string();
        }

        if is_sync_cancelled(&cancel_rx) {
            break 'reconnect_loop;
        }
        monoio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    if let ReplicationRole::Slave {
        ref mut link_status,
        ..
    } = *hub.role.write().unwrap()
    {
        *link_status = "down".to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backlog_append_and_diff() {
        let mut backlog = ReplicationBacklog::new(20);
        assert_eq!(backlog.first_byte_offset, 1);
        assert!(backlog.can_partial_sync(1, 0));
        assert!(!backlog.can_partial_sync(2, 0));
        assert_eq!(backlog.get_diff(1, 0), Some(Vec::new()));

        // Append 10 bytes: "0123456789"
        backlog.append(b"0123456789", 10);
        assert_eq!(backlog.len(), 10);
        assert_eq!(backlog.first_byte_offset, 1);
        assert!(backlog.can_partial_sync(1, 10));
        assert!(backlog.can_partial_sync(6, 10));
        assert!(backlog.can_partial_sync(11, 10));
        assert!(!backlog.can_partial_sync(12, 10));

        // Diff from offset 1 (target 1, index 0): all 10 bytes
        assert_eq!(backlog.get_diff(1, 10), Some(b"0123456789".to_vec()));
        // Diff from offset 6 (target 6, index 5): "56789"
        assert_eq!(backlog.get_diff(6, 10), Some(b"56789".to_vec()));
        // Diff from offset 11 (target 11, up-to-date): empty
        assert_eq!(backlog.get_diff(11, 10), Some(Vec::new()));

        // Overflow backlog (max_size is 20, append 15 bytes -> total 25 bytes, drain 5)
        backlog.append(b"abcdefghijklmno", 25);
        assert_eq!(backlog.len(), 20);
        // first_byte_offset = 25 - 20 + 1 = 6
        assert_eq!(backlog.first_byte_offset, 6);
        // target < 6 cannot partial sync
        assert!(!backlog.can_partial_sync(5, 25));
        assert_eq!(backlog.get_diff(5, 25), None);
        // target 6 can partial sync (index 0)
        assert!(backlog.can_partial_sync(6, 25));
        assert_eq!(backlog.get_diff(6, 25).unwrap().len(), 20);
    }

    #[test]
    fn test_try_partial_resync() {
        let hub = ReplicationHub::new(19999);
        let (tx, _rx) = flume::unbounded();

        // Initially offset is 0, empty backlog
        let replid = hub.master_replid.clone();

        // Unknown replid fails
        assert!(
            hub.try_partial_resync(1, tx.clone(), "unknown_replid", 0)
                .is_none()
        );

        // Negative offset fails
        assert!(hub.try_partial_resync(1, tx.clone(), &replid, -1).is_none());

        // Offset 0 succeeds with empty diff
        let res = hub.try_partial_resync(1, tx.clone(), &replid, 0);
        assert!(res.is_some());
        let (out_id, diff, rep) = res.unwrap();
        assert_eq!(out_id, replid);
        assert!(diff.is_empty());
        assert_eq!(rep.id, 1);
        hub.unregister_replica(1);

        // Propagate mutation
        hub.propagate(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
        let current_offset = hub.master_repl_offset.load(Ordering::SeqCst);
        assert!(current_offset > 0);

        // Can partial resync from offset 0 (wants diff from byte 1)
        let res2 = hub.try_partial_resync(2, tx.clone(), &replid, 0);
        assert!(res2.is_some());
        let (_, diff2, _) = res2.unwrap();
        assert_eq!(diff2, b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
        hub.unregister_replica(2);

        // Replay from current offset (up to date)
        let res3 = hub.try_partial_resync(3, tx.clone(), &replid, current_offset as i64);
        assert!(res3.is_some());
        let (_, diff3, _) = res3.unwrap();
        assert!(diff3.is_empty());
        hub.unregister_replica(3);

        // Offset beyond master fails
        assert!(
            hub.try_partial_resync(4, tx.clone(), &replid, (current_offset + 10) as i64)
                .is_none()
        );
    }

    #[test]
    fn test_replication_atomic_bypass_and_lock_elimination() {
        let hub = ReplicationHub::new(19998);
        assert!(hub.is_master());
        assert!(!hub.is_slave());
        assert!(!hub.has_replicas.load(Ordering::Relaxed));
        assert!(hub.backlog_active.load(Ordering::Relaxed));

        // When backlog is active, propagate records mutation and increments offset
        hub.propagate(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
        assert!(hub.master_repl_offset.load(Ordering::Relaxed) > 0);

        // Register replica attaches connected replica
        let (tx, _rx) = flume::unbounded();
        let rep = hub.register_replica(10, tx);
        assert!(hub.has_replicas.load(Ordering::Relaxed));
        assert!(hub.backlog_active.load(Ordering::Relaxed));
        assert!(HAS_ACTIVE_REPLICATION.load(Ordering::Relaxed));

        // Now propagate mutates backlog and increments offset
        hub.propagate(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
        assert!(hub.master_repl_offset.load(Ordering::Relaxed) > 0);

        hub.unregister_replica(rep.id);
        assert!(!hub.has_replicas.load(Ordering::Relaxed));

        // Make master clears is_slave
        hub.is_slave_atomic.store(true, Ordering::Release);
        assert!(hub.is_slave());
        hub.make_master();
        assert!(hub.is_master());
        assert!(!hub.is_slave());
    }

    #[test]
    fn test_replica_partial_resync_and_psync2_failover() {
        let hub = ReplicationHub::new(19997);
        let master_replid = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_string();
        *hub.role.write().unwrap() = ReplicationRole::Slave {
            master_host: "127.0.0.1".to_string(),
            master_port: 6379,
            link_status: "up".to_string(),
            master_repl_offset: 120,
            master_replid: master_replid.clone(),
            sync_in_progress: false,
        };
        hub.is_slave_atomic.store(true, Ordering::Release);

        // Verify info replication outputs master_replid and offset
        let info = hub.format_info_replication();
        assert!(info.contains("role:slave"));
        assert!(info.contains(&format!("master_replid:{}", master_replid)));
        assert!(info.contains("slave_repl_offset:120"));

        // Promote slave to master via make_master
        hub.make_master();
        assert!(hub.is_master());
        assert!(!hub.is_slave());
        assert_eq!(hub.master_repl_offset.load(Ordering::SeqCst), 120);

        // Verify replid2 and second_offset inherited
        {
            let role = hub.role.read().unwrap();
            match &*role {
                ReplicationRole::Master {
                    replid,
                    replid2,
                    second_offset,
                } => {
                    assert_ne!(replid, &master_replid);
                    assert_eq!(replid2, &master_replid);
                    assert_eq!(*second_offset, 120);
                }
                _ => panic!("Expected master role"),
            }
        }

        // Test that try_partial_resync succeeds for a client asking for replid2 at offset <= second_offset
        let (tx, _rx) = flume::unbounded();
        assert!(
            hub.try_partial_resync(100, tx.clone(), &master_replid, 120)
                .is_some()
        );
        hub.unregister_replica(100);

        // Asking for replid2 at offset > second_offset fails
        assert!(
            hub.try_partial_resync(101, tx, &master_replid, 121)
                .is_none()
        );
    }

    #[test]
    fn test_per_shard_parallel_replication_flows() {
        let hub = ReplicationHub::new(19996);
        let (tx0, rx0) = flume::bounded(16);
        let (tx1, rx1) = flume::bounded(16);

        // Register flows for Shard 0 and Shard 1
        let flow0 = hub.register_shard_flow(0, 100, tx0);
        let flow1 = hub.register_shard_flow(1, 101, tx1);

        assert!(hub.has_shard_flows.load(Ordering::Relaxed));
        assert_eq!(flow0.shard_id, 0);
        assert_eq!(flow1.shard_id, 1);

        // Mutation on Shard 0
        let cmd0 = b"*3\r\n$3\r\nSET\r\n$4\r\nkey0\r\n$4\r\nval0\r\n";
        hub.propagate_shard(0, cmd0);

        // Shard 0 flow receives mutation, Shard 1 receives nothing
        assert_eq!(rx0.try_recv().unwrap(), cmd0.to_vec());
        assert!(rx1.try_recv().is_err());
        assert_eq!(flow0.lsn.load(Ordering::Relaxed), cmd0.len() as u64);
        assert_eq!(flow1.lsn.load(Ordering::Relaxed), 0);

        // Mutation on Shard 1
        let cmd1 = b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n";
        hub.propagate_shard(1, cmd1);

        // Shard 1 flow receives mutation, Shard 0 receives nothing
        assert_eq!(rx1.try_recv().unwrap(), cmd1.to_vec());
        assert!(rx0.try_recv().is_err());
        assert_eq!(flow1.lsn.load(Ordering::Relaxed), cmd1.len() as u64);

        // Test ACK tracking
        hub.update_shard_flow_ack(0, 100, 42);
        assert_eq!(flow0.ack_lsn.load(Ordering::Relaxed), 42);

        // Unregister flows
        hub.unregister_shard_flow(0, 100);
        hub.unregister_shard_flow(1, 101);
        assert!(!hub.has_shard_flows.load(Ordering::Relaxed));
    }
}
