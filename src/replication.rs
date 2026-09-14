use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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

pub struct ReplicationBacklog {
    pub buffer: Vec<u8>,
    pub max_size: usize,
    pub first_byte_offset: u64,
}

impl ReplicationBacklog {
    pub fn new(max_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(max_size),
            max_size,
            first_byte_offset: 1,
        }
    }

    pub fn append(&mut self, data: &[u8], current_master_offset: u64) {
        self.buffer.extend_from_slice(data);
        if self.buffer.len() > self.max_size {
            let overflow = self.buffer.len() - self.max_size;
            self.buffer.drain(..overflow);
            self.first_byte_offset = current_master_offset.saturating_sub(self.buffer.len() as u64) + 1;
        }
    }
}

pub struct ReplicationHub {
    pub port: u16,
    pub role: RwLock<ReplicationRole>,
    pub master_replid: String,
    pub master_repl_offset: AtomicU64,
    pub has_replicas: std::sync::atomic::AtomicBool,
    pub backlog: RwLock<ReplicationBacklog>,
    pub replicas: RwLock<HashMap<u64, Arc<ConnectedReplica>>>,
    pub cancel_sync: RwLock<Option<flume::Sender<()>>>,
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
            has_replicas: std::sync::atomic::AtomicBool::new(false),
            backlog: RwLock::new(ReplicationBacklog::new(1024 * 1024)),
            replicas: RwLock::new(HashMap::new()),
            cancel_sync: RwLock::new(None),
        }
    }

    #[inline]
    pub fn is_master(&self) -> bool {
        matches!(*self.role.read().unwrap(), ReplicationRole::Master { .. })
    }

    #[inline]
    pub fn is_slave(&self) -> bool {
        matches!(*self.role.read().unwrap(), ReplicationRole::Slave { .. })
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
        *role = ReplicationRole::Master {
            replid: new_replid,
            replid2: "0000000000000000000000000000000000000000".to_string(),
            second_offset: -1,
        };
    }

    pub fn stop_sync(&self) {
        if let Some(cancel) = self.cancel_sync.write().unwrap().take() {
            let _ = cancel.send(());
        }
    }

    pub fn register_replica(&self, id: u64, sender: flume::Sender<Vec<u8>>) -> Arc<ConnectedReplica> {
        let rep = Arc::new(ConnectedReplica {
            id,
            sender,
            listening_port: AtomicU64::new(0),
            ack_offset: AtomicU64::new(0),
            last_ack_time: AtomicU64::new(0),
        });
        self.replicas.write().unwrap().insert(id, rep.clone());
        self.has_replicas.store(true, Ordering::Release);
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

    pub fn propagate(&self, bytes: &[u8]) {
        if !self.has_replicas.load(Ordering::Relaxed) || !self.is_master() {
            return;
        }
        let new_offset = self.master_repl_offset.fetch_add(bytes.len() as u64, Ordering::SeqCst)
            + bytes.len() as u64;
        self.backlog.write().unwrap().append(bytes, new_offset);

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
                let state_str = if link_status == "up" { "connected" } else { "connect" };
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
                    backlog.buffer.len()
                )
            }
            ReplicationRole::Slave {
                master_host,
                master_port,
                link_status,
                master_repl_offset,
                sync_in_progress,
            } => {
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
                    self.master_replid,
                    self.master_repl_offset.load(Ordering::SeqCst)
                )
            }
        }
    }
}

static REPLICATION_HUBS: LazyLock<RwLock<HashMap<u16, Arc<ReplicationHub>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn get_replication_hub(port: u16) -> Arc<ReplicationHub> {
    let mut hubs = REPLICATION_HUBS.write().unwrap();
    hubs.entry(port)
        .or_insert_with(|| Arc::new(ReplicationHub::new(port)))
        .clone()
}

#[inline]
pub fn has_connected_replicas(port: u16) -> bool {
    let hubs = REPLICATION_HUBS.read().unwrap();
    if let Some(hub) = hubs.get(&port) {
        hub.has_replicas.load(Ordering::Relaxed)
    } else {
        false
    }
}

pub fn propagate_bytes(port: u16, bytes: &[u8]) {
    let hub = get_replication_hub(port);
    hub.propagate(bytes);
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

    *hub.role.write().unwrap() = ReplicationRole::Slave {
        master_host: master_host.clone(),
        master_port,
        link_status: "connecting".to_string(),
        master_repl_offset: 0,
        sync_in_progress: true,
    };

    let (cancel_tx, cancel_rx) = flume::bounded(1);
    *hub.cancel_sync.write().unwrap() = Some(cancel_tx);

    let hub_clone = hub.clone();
    monoio::spawn(async move {
        run_replica_worker(port, master_host, master_port, router, cancel_rx, hub_clone).await;
    });
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
            return;
        }
    };

    let mut buf = bytes::BytesMut::with_capacity(65536);
    let mut read_buf = vec![0u8; 8192];

    macro_rules! send_and_expect_line {
        ($payload:expr) => {{
            if stream.write_all($payload).await.0.is_err() {
                return;
            }
            loop {
                if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
                    let line = buf.split_to(pos + 2);
                    break line;
                }
                let (res, returned) = stream.read(read_buf).await;
                read_buf = returned;
                match res {
                    Ok(0) | Err(_) => return,
                    Ok(n) => buf.extend_from_slice(&read_buf[..n]),
                }
            }
        }};
    }

    // 1. PING
    let line = send_and_expect_line!(b"*1\r\n$4\r\nPING\r\n");
    if !line.starts_with(b"+PONG") {
        return;
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
        return;
    }

    // 3. REPLCONF capa psync2
    let line = send_and_expect_line!(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
    if !line.starts_with(b"+OK") {
        return;
    }

    // 4. PSYNC ? -1
    let line = send_and_expect_line!(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
    if !line.starts_with(b"+FULLRESYNC") {
        return;
    }

    let line_str = String::from_utf8_lossy(&line);
    let parts: Vec<&str> = line_str.trim().split_whitespace().collect();
    let initial_offset: u64 = if parts.len() >= 3 {
        parts[2].parse().unwrap_or(0)
    } else {
        0
    };

    // 5. Read RDB header: $<len>\r\n
    let rdb_len: usize = loop {
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            let line = buf.split_to(pos + 2);
            if line.starts_with(b"$") {
                let s = std::str::from_utf8(&line[1..line.len() - 2]).unwrap_or("0");
                break s.parse().unwrap_or(0);
            }
        }
        let (res, returned) = stream.read(read_buf).await;
        read_buf = returned;
        match res {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&read_buf[..n]),
        }
    };

    // 6. Read rdb_len bytes
    while buf.len() < rdb_len {
        let (res, returned) = stream.read(read_buf).await;
        read_buf = returned;
        match res {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&read_buf[..n]),
        }
    }
    let rdb_bytes = buf.split_to(rdb_len).freeze();

    // 7. Restore RDB into router
    router.restore_rdb_bytes(rdb_bytes).await;

    // 8. Mark link_status up
    {
        let mut role = hub.role.write().unwrap();
        if let ReplicationRole::Slave {
            ref mut link_status,
            ref mut master_repl_offset,
            ref mut sync_in_progress,
            ..
        } = *role
        {
            *link_status = "up".to_string();
            *master_repl_offset = initial_offset;
            *sync_in_progress = false;
        }
    }

    // 9. Streaming loop: receive and apply mutations
    let mut current_offset = initial_offset;
    loop {
        if cancel_rx.try_recv().is_ok() {
            break;
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
}
