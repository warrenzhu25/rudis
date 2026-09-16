use bytes::{Bytes, BytesMut};
use monoio::io::{AsyncReadRent, AsyncWriteRentExt, Splitable};
use monoio::net::TcpStream;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::Instant;

use std::os::unix::io::AsRawFd;

use crate::resp::{
    ClientSubcommand, ClusterSubcommand, Command, MemorySubcommand, MsetexCondition, MsetexExpiry,
    SetSlotSubcommand, parse_command,
};
use crate::router::{Router, key_slot, target_shard};
use crate::shard::{CompactResp, ShardDb, ShardMessage};

const READ_BUFFER_SIZE: usize = 65536;

pub type ResponderChannel = (
    flume::Sender<Vec<(usize, CompactResp)>>,
    flume::Receiver<Vec<(usize, CompactResp)>>,
);

#[derive(Clone, Debug)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub name: Option<String>,
    pub connected_at: Instant,
    pub last_active: Instant,
    pub last_cmd: String,
    pub is_resp3: bool,
    pub track_tx: Option<flume::Sender<Vec<u8>>>,
    pub raw_fd: std::os::unix::io::RawFd,
}

#[derive(Clone, Debug)]
pub struct ClientTracker {
    pub port: u16,
    pub client_id: u64,
    pub bcast: bool,
    pub prefixes: Vec<Bytes>,
    pub tracked_keys: hashbrown::HashSet<Vec<u8>>,
    pub sender: flume::Sender<Vec<u8>>,
    pub is_resp3: bool,
}

thread_local! {
    pub static CURRENT_CLIENT_RESP3: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub static IN_TX: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[inline]
pub fn notify_list_or_defer(db: &mut ShardDb, key: &Bytes) {
    touch_watched_key(db.port, key.as_ref());
    let hub_arc = crate::block::get_block_hub_for_port(db.port);
    let mut hub = hub_arc.lock().unwrap();
    if hub.is_paused() {
        hub.add_pending_notify(key.clone());
    } else {
        hub.notify_list(&mut db.table, key);
    }
}

#[inline]
pub fn notify_zset_or_defer(db: &mut ShardDb, key: &Bytes) {
    touch_watched_key(db.port, key.as_ref());
    let hub_arc = crate::block::get_block_hub_for_port(db.port);
    let mut hub = hub_arc.lock().unwrap();
    if hub.is_paused() {
        hub.add_pending_notify(key.clone());
    } else {
        hub.notify_zset(&mut db.table, key);
    }
}

pub static DIRTY_CHANGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[inline]
pub fn write_resp_null(out: &mut Vec<u8>) {
    if CURRENT_CLIENT_RESP3.get() {
        out.extend_from_slice(b"_\r\n");
    } else {
        out.extend_from_slice(b"$-1\r\n");
    }
}

#[inline]
pub fn write_resp_null_array(out: &mut Vec<u8>) {
    if CURRENT_CLIENT_RESP3.get() {
        out.extend_from_slice(b"_\r\n");
    } else {
        out.extend_from_slice(b"*-1\r\n");
    }
}

#[inline]
pub fn format_score(val: f64) -> String {
    if val.is_nan() {
        "nan".to_string()
    } else if val.is_infinite() {
        if val.is_sign_positive() {
            "inf".to_string()
        } else {
            "-inf".to_string()
        }
    } else if val == 0.0 {
        "0".to_string()
    } else {
        let mut buf = [0u8; 64];
        let len = unsafe {
            libc::snprintf(
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
                c"%.17g".as_ptr(),
                val,
            )
        };
        if len > 0 && (len as usize) < buf.len() {
            unsafe { std::str::from_utf8_unchecked(&buf[..len as usize]) }.to_string()
        } else {
            val.to_string()
        }
    }
}

#[inline]
pub fn write_resp_score(out: &mut Vec<u8>, val: f64) {
    if CURRENT_CLIENT_RESP3.get() {
        if val.is_infinite() {
            if val.is_sign_positive() {
                out.extend_from_slice(b",inf\r\n");
            } else {
                out.extend_from_slice(b",-inf\r\n");
            }
        } else if val.is_nan() {
            out.extend_from_slice(b",nan\r\n");
        } else {
            let s = format_score(val);
            out.extend_from_slice(format!(",{}\r\n", s).as_bytes());
        }
    } else {
        let s = format_score(val);
        write_resp_bulk(out, s.as_bytes());
    }
}

#[inline]
pub fn format_zmpop_response(out: &mut Vec<u8>, key: &Bytes, items: &[(Bytes, f64)]) {
    out.extend_from_slice(b"*2\r\n");
    write_resp_bulk(out, key);
    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
    for (m, s) in items {
        out.extend_from_slice(b"*2\r\n");
        write_resp_bulk(out, m);
        write_resp_score(out, *s);
    }
}

#[inline]
pub fn format_bzpop_response(out: &mut Vec<u8>, key: &Bytes, m: &Bytes, s: f64) {
    out.extend_from_slice(b"*3\r\n");
    write_resp_bulk(out, key);
    write_resp_bulk(out, m);
    write_resp_score(out, s);
}

pub struct BlockedClientGuard {
    pub port: u16,
    pub client_id: u64,
}

impl Drop for BlockedClientGuard {
    fn drop(&mut self) {
        let hub_arc = crate::block::get_block_hub_for_port(self.port);
        let mut hub = hub_arc.lock().unwrap();
        hub.unregister_blocked_client(self.client_id);
    }
}

#[inline]
pub fn is_fd_closed(fd: std::os::unix::io::RawFd) -> bool {
    let mut pollfd = libc::pollfd {
        fd,
        events: libc::POLLIN | libc::POLLRDHUP | libc::POLLHUP | libc::POLLERR,
        revents: 0,
    };
    let ret = unsafe { libc::poll(&mut pollfd, 1, 0) };
    if ret > 0 {
        if pollfd.revents & (libc::POLLRDHUP | libc::POLLHUP | libc::POLLERR) != 0 {
            return true;
        }
        if pollfd.revents & libc::POLLIN != 0 {
            let mut buf = [0u8; 1];
            let n = unsafe {
                libc::recv(
                    fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    1,
                    libc::MSG_PEEK | libc::MSG_DONTWAIT,
                )
            };
            if n == 0 {
                return true;
            }
        }
    }
    false
}

pub async fn wait_for_blocked_result<T>(
    rx: &flume::Receiver<T>,
    timeout_secs: f64,
    raw_fd: Option<std::os::unix::io::RawFd>,
) -> (Option<T>, bool) {
    let has_timeout = timeout_secs > 0.0;
    let deadline = if has_timeout {
        Some(Instant::now() + std::time::Duration::from_secs_f64(timeout_secs))
    } else {
        None
    };

    loop {
        let check_dur = match deadline {
            Some(dl) => {
                let now = Instant::now();
                if now >= dl {
                    return (None, false);
                }
                let rem = dl - now;
                rem.min(std::time::Duration::from_millis(20))
            }
            None => std::time::Duration::from_millis(20),
        };

        match monoio::time::timeout(check_dur, rx.recv_async()).await {
            Ok(Ok(res)) => return (Some(res), false),
            Ok(Err(_)) => return (None, false),
            Err(_) => {
                if let Some(fd) = raw_fd
                    && is_fd_closed(fd)
                {
                    return (None, true);
                }
                if let Some(dl) = deadline
                    && Instant::now() >= dl
                {
                    return (None, false);
                }
            }
        }
    }
}

pub async fn wait_for_stream_result(
    rx: &flume::Receiver<()>,
    timeout_ms: u64,
    raw_fd: Option<std::os::unix::io::RawFd>,
) -> (bool, bool) {
    let has_timeout = timeout_ms > 0;
    let deadline = if has_timeout {
        Some(Instant::now() + std::time::Duration::from_millis(timeout_ms))
    } else {
        None
    };

    loop {
        let check_dur = match deadline {
            Some(dl) => {
                let now = Instant::now();
                if now >= dl {
                    return (false, false);
                }
                let rem = dl - now;
                rem.min(std::time::Duration::from_millis(20))
            }
            None => std::time::Duration::from_millis(20),
        };

        match monoio::time::timeout(check_dur, rx.recv_async()).await {
            Ok(Ok(_)) => return (true, false),
            Ok(Err(_)) => return (false, false),
            Err(_) => {
                if let Some(fd) = raw_fd
                    && is_fd_closed(fd)
                {
                    return (false, true);
                }
                if let Some(dl) = deadline
                    && Instant::now() >= dl
                {
                    return (false, false);
                }
            }
        }
    }
}

static WATCHED_KEYS: std::sync::LazyLock<
    std::sync::RwLock<hashbrown::HashMap<u16, hashbrown::HashMap<Bytes, hashbrown::HashSet<u64>>>>,
> = std::sync::LazyLock::new(|| std::sync::RwLock::new(hashbrown::HashMap::new()));

static CLIENT_WATCH_TAINTED: std::sync::LazyLock<
    std::sync::RwLock<hashbrown::HashMap<(u16, u64), bool>>,
> = std::sync::LazyLock::new(|| std::sync::RwLock::new(hashbrown::HashMap::new()));

pub static CMD_STATS: std::sync::LazyLock<std::sync::RwLock<hashbrown::HashMap<String, u64>>> =
    std::sync::LazyLock::new(|| std::sync::RwLock::new(hashbrown::HashMap::new()));

#[inline]
pub fn record_cmd_stat(name: &str) {
    if let Ok(mut map) = CMD_STATS.write() {
        *map.entry(name.to_lowercase()).or_insert(0) += 1;
    }
}

pub fn watch_keys(port: u16, client_id: u64, keys: &[Bytes]) {
    let mut map = WATCHED_KEYS.write().unwrap();
    let port_map = map.entry(port).or_default();
    for k in keys {
        port_map.entry(k.clone()).or_default().insert(client_id);
    }
    CLIENT_WATCH_TAINTED
        .write()
        .unwrap()
        .entry((port, client_id))
        .or_insert(false);
}

pub fn unwatch_keys(port: u16, client_id: u64) {
    let mut map = WATCHED_KEYS.write().unwrap();
    if let Some(port_map) = map.get_mut(&port) {
        for set in port_map.values_mut() {
            set.remove(&client_id);
        }
    }
    CLIENT_WATCH_TAINTED
        .write()
        .unwrap()
        .remove(&(port, client_id));
}

pub fn is_watch_tainted(port: u16, client_id: u64) -> bool {
    CLIENT_WATCH_TAINTED
        .read()
        .unwrap()
        .get(&(port, client_id))
        .copied()
        .unwrap_or(false)
}

pub fn touch_watched_key(port: u16, key: &[u8]) {
    let map = WATCHED_KEYS.read().unwrap();
    if let Some(port_map) = map.get(&port)
        && let Some(clients) = port_map.get(key)
    {
        let mut tainted = CLIENT_WATCH_TAINTED.write().unwrap();
        for &cid in clients {
            tainted.insert((port, cid), true);
        }
    }
}

static TRACKING_CLIENTS: std::sync::LazyLock<
    std::sync::RwLock<hashbrown::HashMap<(u16, u64), ClientTracker>>,
> = std::sync::LazyLock::new(|| std::sync::RwLock::new(hashbrown::HashMap::new()));

pub fn register_client_tracking(
    port: u16,
    client_id: u64,
    bcast: bool,
    prefixes: Vec<Bytes>,
    sender: flume::Sender<Vec<u8>>,
    is_resp3: bool,
) {
    let mut map = TRACKING_CLIENTS.write().unwrap();
    map.insert(
        (port, client_id),
        ClientTracker {
            port,
            client_id,
            bcast,
            prefixes,
            tracked_keys: hashbrown::HashSet::new(),
            sender,
            is_resp3,
        },
    );
}

pub fn unregister_client_tracking(port: u16, client_id: u64) {
    let mut map = TRACKING_CLIENTS.write().unwrap();
    map.remove(&(port, client_id));
}

pub fn record_client_read(port: u16, client_id: u64, key: &[u8]) {
    let mut map = TRACKING_CLIENTS.write().unwrap();
    if let Some(tracker) = map.get_mut(&(port, client_id))
        && !tracker.bcast
    {
        tracker.tracked_keys.insert(key.to_vec());
    }
}

pub fn notify_key_invalidation(port: u16, key: &[u8], sender_client_id: u64) {
    touch_watched_key(port, key);
    let mut map = TRACKING_CLIENTS.write().unwrap();
    for tracker in map.values_mut() {
        if tracker.port != port {
            continue;
        }
        if tracker.client_id == sender_client_id && !tracker.bcast {
            continue;
        }
        if tracker.bcast {
            if !tracker.prefixes.is_empty() {
                let matched = tracker.prefixes.iter().any(|pfx| key.starts_with(pfx));
                if !matched {
                    continue;
                }
            }
        } else {
            if !tracker.tracked_keys.remove(key) {
                continue;
            }
        }
        let mut msg = Vec::new();
        if tracker.is_resp3 {
            msg.extend_from_slice(b">2\r\n$10\r\ninvalidate\r\n*1\r\n");
            write_resp_bulk(&mut msg, key);
        } else {
            msg.extend_from_slice(b"*2\r\n$10\r\ninvalidate\r\n*1\r\n");
            write_resp_bulk(&mut msg, key);
        }
        let _ = tracker.sender.send(msg);
    }
}

#[inline(always)]
pub fn write_resp_integer(out: &mut Vec<u8>, val: i64) {
    match val {
        0 => out.extend_from_slice(b":0\r\n"),
        1 => out.extend_from_slice(b":1\r\n"),
        2 => out.extend_from_slice(b":2\r\n"),
        3 => out.extend_from_slice(b":3\r\n"),
        4 => out.extend_from_slice(b":4\r\n"),
        5 => out.extend_from_slice(b":5\r\n"),
        -1 => out.extend_from_slice(b":-1\r\n"),
        _ => {
            let mut buf = [0u8; 24];
            let mut i = buf.len();
            let neg = val < 0;
            let mut uval = if neg {
                if val == i64::MIN {
                    out.extend_from_slice(b":-9223372036854775808\r\n");
                    return;
                }
                (-val) as u64
            } else {
                val as u64
            };
            while uval > 0 {
                i -= 1;
                buf[i] = b'0' + (uval % 10) as u8;
                uval /= 10;
            }
            if neg {
                i -= 1;
                buf[i] = b'-';
            }
            out.push(b':');
            out.extend_from_slice(&buf[i..]);
            out.extend_from_slice(b"\r\n");
        }
    }
}

#[inline(always)]
pub fn write_resp_bulk(out: &mut Vec<u8>, val: &[u8]) {
    out.push(b'$');
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let mut uval = val.len();
    if uval == 0 {
        out.extend_from_slice(b"0\r\n");
    } else {
        while uval > 0 {
            i -= 1;
            buf[i] = b'0' + (uval % 10) as u8;
            uval /= 10;
        }
        out.extend_from_slice(&buf[i..]);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(val);
    out.extend_from_slice(b"\r\n");
}

#[inline]
pub fn write_resp_err(out: &mut Vec<u8>, err: impl AsRef<str>) {
    let err = err.as_ref();
    if err.starts_with("WRONGTYPE")
        || err.starts_with("CROSSSLOT")
        || err.starts_with("MOVED")
        || err.starts_with("ASK")
        || err.starts_with("NOSCRIPT")
        || err.starts_with("EXECABORT")
        || err.starts_with("BUSYGROUP")
        || err.starts_with("ERR")
    {
        out.extend_from_slice(b"-");
        out.extend_from_slice(err.as_bytes());
        out.extend_from_slice(b"\r\n");
    } else {
        out.extend_from_slice(b"-ERR ");
        out.extend_from_slice(err.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
}

pub static HASH_MAX_ENTRIES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(512);
pub static HASH_MAX_VALUE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(64);
pub static ALLOW_ACCESS_EXPIRED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub async fn handle_connection(
    mut stream: TcpStream,
    client_addr: SocketAddr,
    client_id: u64,
    client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    router: Rc<Router>,
) {
    let raw_fd = stream.as_raw_fd();
    let now = Instant::now();
    let (track_tx, track_rx) = flume::unbounded::<Vec<u8>>();
    client_registry.borrow_mut().insert(
        client_id,
        ClientInfo {
            id: client_id,
            addr: client_addr,
            name: None,
            connected_at: now,
            last_active: now,
            last_cmd: "NONE".to_string(),
            is_resp3: false,
            track_tx: Some(track_tx),
            raw_fd,
        },
    );

    struct ClientCleanup {
        port: u16,
        client_id: u64,
        registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
        pubsub: Rc<RefCell<crate::pubsub::PubSubHub>>,
    }
    impl Drop for ClientCleanup {
        fn drop(&mut self) {
            self.registry.borrow_mut().remove(&self.client_id);
            self.pubsub.borrow_mut().remove_client(self.client_id);
            unregister_client_tracking(self.port, self.client_id);
            let hub_arc = crate::block::get_block_hub_for_port(self.port);
            let mut hub = hub_arc.lock().unwrap();
            hub.unregister_blocked_client(self.client_id);
        }
    }
    let _cleanup = ClientCleanup {
        port: router.port,
        client_id,
        registry: client_registry.clone(),
        pubsub: router.pubsub.clone(),
    };

    let mut buf = BytesMut::with_capacity(131072);
    let mut read_buf = vec![0u8; READ_BUFFER_SIZE];
    let mut out_buf = Vec::with_capacity(65536);

    // Pre-allocated reusable channel responders (1 per shard, 0 allocations per hop in steady-state)
    let responders: Vec<ResponderChannel> =
        (0..router.num_shards).map(|_| flume::bounded(1)).collect();
    let mut remote_batches: Vec<Vec<(usize, Command)>> = (0..router.num_shards)
        .map(|_| Vec::with_capacity(64))
        .collect();

    let mut asking = false;
    let mut in_multi = false;
    let mut tx_queue: Vec<Command> = Vec::new();
    let mut tx_has_error = false;
    let mut authenticated = !crate::acl::get_acl_for_port(router.port)
        .read()
        .unwrap()
        .is_auth_required_for_default();
    let mut auth_user = "default".to_string();

    loop {
        // Rent buffer to monoio's io_uring driver
        let (res, returned_buf) = stream.read(read_buf).await;
        read_buf = returned_buf;

        match res {
            Ok(0) => {
                // Client disconnected
                break;
            }
            Ok(n) => {
                buf.extend_from_slice(&read_buf[..n]);

                // 1. Parse all complete commands currently in the buffer
                let mut commands = Vec::new();
                let mut should_quit = false;
                while !buf.is_empty() {
                    match parse_command(&mut buf) {
                        Ok(Some(cmd)) => {
                            commands.push(cmd);
                        }
                        Ok(None) => {
                            // Incomplete frame, need more data
                            break;
                        }
                        Err(err) => {
                            write_resp_err(&mut out_buf, &err);
                            if in_multi {
                                tx_has_error = true;
                            }
                        }
                    }
                }

                // 2. Transition to Pub/Sub mode if SUBSCRIBE or PSUBSCRIBE is received
                if let Some(sub_idx) = commands
                    .iter()
                    .position(|c| matches!(c, Command::Subscribe(_) | Command::Psubscribe(_)))
                {
                    for c in commands.drain(..sub_idx) {
                        let _ = execute_command(
                            c,
                            &router,
                            client_id,
                            &client_registry,
                            &mut out_buf,
                            &mut asking,
                            &mut authenticated,
                            &mut auth_user,
                        )
                        .await;
                    }
                    if !out_buf.is_empty() {
                        let write_chunk = std::mem::take(&mut out_buf);
                        let _ = stream.write_all(write_chunk).await.0;
                    }
                    let initial_sub = commands.remove(0);
                    run_pubsub_loop(
                        stream,
                        client_id,
                        client_registry,
                        router,
                        initial_sub,
                        commands,
                        buf,
                    )
                    .await;
                    return;
                }

                // 2.5 Transition to Replica Stream mode if PSYNC is received
                if let Some(psync_idx) = commands
                    .iter()
                    .position(|c| matches!(c, Command::Psync { .. }))
                {
                    for c in commands.drain(..psync_idx) {
                        let _ = execute_command(
                            c,
                            &router,
                            client_id,
                            &client_registry,
                            &mut out_buf,
                            &mut asking,
                            &mut authenticated,
                            &mut auth_user,
                        )
                        .await;
                    }
                    if !out_buf.is_empty() {
                        let write_chunk = std::mem::take(&mut out_buf);
                        let _ = stream.write_all(write_chunk).await.0;
                    }
                    let psync_cmd = commands.remove(0);
                    run_master_replica_stream(
                        stream,
                        client_id,
                        client_registry,
                        router,
                        psync_cmd,
                    )
                    .await;
                    return;
                }

                // 3. Execute parsed commands with transaction support and pipeline squashing
                if !commands.is_empty() {
                    let has_tx = in_multi
                        || commands.iter().any(|c| {
                            matches!(
                                c,
                                Command::Multi
                                    | Command::Exec
                                    | Command::Discard
                                    | Command::Watch(_)
                                    | Command::Unwatch
                            )
                        });

                    if has_tx {
                        for cmd in commands {
                            if !IN_TX.get()
                                && let Some(c) = client_registry.borrow_mut().get_mut(&client_id)
                            {
                                c.last_active = Instant::now();
                                c.last_cmd = get_cmd_name(&cmd).to_lowercase();
                            }
                            if in_multi {
                                match cmd {
                                    Command::Multi => {
                                        out_buf.extend_from_slice(
                                            b"-ERR MULTI calls can not be nested\r\n",
                                        );
                                    }
                                    Command::Watch(_) => {
                                        out_buf.extend_from_slice(
                                            b"-ERR WATCH inside MULTI is not allowed\r\n",
                                        );
                                    }
                                    Command::Unwatch => {
                                        out_buf.extend_from_slice(b"+OK\r\n");
                                    }
                                    Command::Discard => {
                                        in_multi = false;
                                        tx_queue.clear();
                                        tx_has_error = false;
                                        unwatch_keys(router.port, client_id);
                                        crate::block::get_block_hub_for_port(router.port)
                                            .lock()
                                            .unwrap()
                                            .clear_pending_notifies();
                                        out_buf.extend_from_slice(b"+OK\r\n");
                                    }
                                    Command::Reset => {
                                        in_multi = false;
                                        tx_queue.clear();
                                        tx_has_error = false;
                                        unwatch_keys(router.port, client_id);
                                        crate::block::get_block_hub_for_port(router.port)
                                            .lock()
                                            .unwrap()
                                            .clear_pending_notifies();
                                        out_buf.extend_from_slice(b"+RESET\r\n");
                                    }
                                    Command::Exec => {
                                        in_multi = false;
                                        if tx_has_error {
                                            tx_queue.clear();
                                            tx_has_error = false;
                                            unwatch_keys(router.port, client_id);
                                            crate::block::get_block_hub_for_port(router.port)
                                                .lock()
                                                .unwrap()
                                                .clear_pending_notifies();
                                            out_buf.extend_from_slice(
                                                b"-EXECABORT Transaction discarded because of previous errors.\r\n",
                                            );
                                        } else if is_watch_tainted(router.port, client_id) {
                                            tx_queue.clear();
                                            tx_has_error = false;
                                            unwatch_keys(router.port, client_id);
                                            crate::block::get_block_hub_for_port(router.port)
                                                .lock()
                                                .unwrap()
                                                .clear_pending_notifies();
                                            write_resp_null_array(&mut out_buf);
                                        } else {
                                            unwatch_keys(router.port, client_id);
                                            let mut shards = hashbrown::HashSet::new();
                                            for cmd in &tx_queue {
                                                for k in cmd_keys(cmd) {
                                                    shards.insert(router.target_shard(k));
                                                }
                                            }
                                            let mut sorted_shards: Vec<usize> =
                                                shards.into_iter().collect();
                                            sorted_shards.sort_unstable();

                                            let use_vll = sorted_shards.len() > 1;
                                            let tx_id = if use_vll {
                                                static NEXT_TX: std::sync::atomic::AtomicU64 =
                                                    std::sync::atomic::AtomicU64::new(1);
                                                let id = NEXT_TX.fetch_add(
                                                    1,
                                                    std::sync::atomic::Ordering::Relaxed,
                                                );
                                                router.acquire_tx_locks(&sorted_shards, id).await;
                                                id
                                            } else {
                                                0
                                            };

                                            let hub_arc =
                                                crate::block::get_block_hub_for_port(router.port);
                                            hub_arc.lock().unwrap().pause();

                                            let count = tx_queue.len();
                                            out_buf.extend_from_slice(
                                                format!("*{}\r\n", count).as_bytes(),
                                            );
                                            let queued = std::mem::take(&mut tx_queue);
                                            IN_TX.set(true);
                                            for q_cmd in queued {
                                                let quit = execute_command(
                                                    q_cmd,
                                                    &router,
                                                    client_id,
                                                    &client_registry,
                                                    &mut out_buf,
                                                    &mut asking,
                                                    &mut authenticated,
                                                    &mut auth_user,
                                                )
                                                .await;
                                                if quit {
                                                    should_quit = true;
                                                    break;
                                                }
                                            }
                                            IN_TX.set(false);
                                            if let Some(c) =
                                                client_registry.borrow_mut().get_mut(&client_id)
                                            {
                                                c.last_active = Instant::now();
                                                c.last_cmd = "exec".to_string();
                                            }

                                            if use_vll {
                                                router
                                                    .release_tx_locks(&sorted_shards, tx_id)
                                                    .await;
                                            }

                                            let pending = hub_arc.lock().unwrap().resume();
                                            for k in pending {
                                                let shard_id = router.target_shard(&k);
                                                if shard_id == router.shard_id {
                                                    let mut hub = hub_arc.lock().unwrap();
                                                    hub.notify_list(
                                                        &mut router.local_db.borrow_mut().table,
                                                        &k,
                                                    );
                                                    hub.notify_zset(
                                                        &mut router.local_db.borrow_mut().table,
                                                        &k,
                                                    );
                                                } else {
                                                    let _ = router.senders[shard_id].send(
                                                        ShardMessage::NotifyList { keys: vec![k] },
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    Command::Quit => {
                                        out_buf.extend_from_slice(b"+OK\r\n");
                                        should_quit = true;
                                        break;
                                    }
                                    _ => {
                                        tx_queue.push(cmd);
                                        out_buf.extend_from_slice(b"+QUEUED\r\n");
                                    }
                                }
                            } else {
                                match cmd {
                                    Command::Multi => {
                                        in_multi = true;
                                        tx_queue.clear();
                                        tx_has_error = false;
                                        out_buf.extend_from_slice(b"+OK\r\n");
                                    }
                                    Command::Discard => {
                                        out_buf
                                            .extend_from_slice(b"-ERR DISCARD without MULTI\r\n");
                                    }
                                    Command::Exec => {
                                        out_buf.extend_from_slice(b"-ERR EXEC without MULTI\r\n");
                                    }
                                    Command::Watch(keys) => {
                                        watch_keys(router.port, client_id, &keys);
                                        out_buf.extend_from_slice(b"+OK\r\n");
                                    }
                                    Command::Unwatch => {
                                        unwatch_keys(router.port, client_id);
                                        out_buf.extend_from_slice(b"+OK\r\n");
                                    }
                                    _ => {
                                        let quit = execute_command(
                                            cmd,
                                            &router,
                                            client_id,
                                            &client_registry,
                                            &mut out_buf,
                                            &mut asking,
                                            &mut authenticated,
                                            &mut auth_user,
                                        )
                                        .await;
                                        if quit {
                                            should_quit = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    } else if commands.iter().any(|c| {
                        matches!(
                            c,
                            Command::Blpop { .. }
                                | Command::Brpop { .. }
                                | Command::Blmove { .. }
                                | Command::Blmpop { .. }
                                | Command::Bzpopmin { .. }
                                | Command::Bzpopmax { .. }
                                | Command::Bzmpop { .. }
                                | Command::Xread {
                                    block_ms: Some(_),
                                    ..
                                }
                                | Command::Xreadgroup {
                                    block_ms: Some(_),
                                    ..
                                }
                        )
                    }) {
                        for cmd in commands {
                            if matches!(
                                cmd,
                                Command::Blpop { .. }
                                    | Command::Brpop { .. }
                                    | Command::Blmove { .. }
                                    | Command::Blmpop { .. }
                                    | Command::Bzpopmin { .. }
                                    | Command::Bzpopmax { .. }
                                    | Command::Bzmpop { .. }
                                    | Command::Xread {
                                        block_ms: Some(_),
                                        ..
                                    }
                                    | Command::Xreadgroup {
                                        block_ms: Some(_),
                                        ..
                                    }
                            ) && !out_buf.is_empty()
                            {
                                let write_chunk = std::mem::take(&mut out_buf);
                                let (res, returned_buf) = stream.write_all(write_chunk).await;
                                out_buf = returned_buf;
                                out_buf.clear();
                                if res.is_err() {
                                    should_quit = true;
                                    break;
                                }
                            }
                            let quit = execute_command(
                                cmd,
                                &router,
                                client_id,
                                &client_registry,
                                &mut out_buf,
                                &mut asking,
                                &mut authenticated,
                                &mut auth_user,
                            )
                            .await;
                            if quit {
                                should_quit = true;
                                break;
                            }
                        }
                    } else if commands.len() == 1 {
                        let quit = execute_command(
                            commands.pop().unwrap(),
                            &router,
                            client_id,
                            &client_registry,
                            &mut out_buf,
                            &mut asking,
                            &mut authenticated,
                            &mut auth_user,
                        )
                        .await;
                        if quit {
                            should_quit = true;
                        }
                    } else {
                        let quit = execute_commands_squashed(
                            commands,
                            &router,
                            &responders,
                            &mut remote_batches,
                            client_id,
                            &client_registry,
                            &mut out_buf,
                            &mut asking,
                            &mut authenticated,
                            &mut auth_user,
                        )
                        .await;
                        if quit {
                            should_quit = true;
                        }
                    }
                }

                // 4. Batch flush all accumulated responses in one io_uring write
                while let Ok(inval) = track_rx.try_recv() {
                    out_buf.extend_from_slice(&inval);
                }
                if !out_buf.is_empty() {
                    let (write_res, returned_buf) = stream.write_all(out_buf).await;
                    out_buf = returned_buf;
                    out_buf.clear();
                    if write_res.is_err() {
                        break;
                    }
                }

                if should_quit {
                    break;
                }
            }
            Err(_) => {
                // Connection read error
                break;
            }
        }
    }
    client_registry.borrow_mut().remove(&client_id);
    unwatch_keys(router.port, client_id);
    unregister_client_tracking(router.port, client_id);
}

async fn run_pubsub_loop(
    stream: TcpStream,
    client_id: u64,
    client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    router: Rc<Router>,
    initial_sub: Command,
    pending_cmds: Vec<Command>,
    mut buf: BytesMut,
) {
    let (mut reader, mut writer) = stream.into_split();
    let (write_tx, write_rx) = flume::unbounded::<Vec<u8>>();

    monoio::spawn(async move {
        while let Ok(data) = write_rx.recv_async().await {
            if writer.write_all(data).await.0.is_err() {
                break;
            }
        }
    });

    let mut read_buf = vec![0u8; READ_BUFFER_SIZE];

    let handle_cmd = |cmd: Command, out: &mut Vec<u8>| -> bool {
        let cmd_name = match &cmd {
            Command::Subscribe(_) => "SUBSCRIBE",
            Command::Unsubscribe(_) => "UNSUBSCRIBE",
            Command::Psubscribe(_) => "PSUBSCRIBE",
            Command::Punsubscribe(_) => "PUNSUBSCRIBE",
            Command::Ping(_) => "PING",
            Command::Quit => "QUIT",
            _ => "OTHER",
        };
        if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
            c.last_active = Instant::now();
            c.last_cmd = cmd_name.to_string();
        }
        match cmd {
            Command::Subscribe(channels) => {
                let mut hub = router.pubsub.borrow_mut();
                for ch in channels {
                    let count = hub.subscribe(client_id, ch.clone(), write_tx.clone());
                    out.extend_from_slice(b"*3\r\n$9\r\nsubscribe\r\n$");
                    out.extend_from_slice(ch.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(&ch);
                    out.extend_from_slice(b"\r\n:");
                    out.extend_from_slice(count.to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                false
            }
            Command::Unsubscribe(channels) => {
                let mut hub = router.pubsub.borrow_mut();
                if channels.is_empty() {
                    let unsubs = hub.unsubscribe_all(client_id);
                    if unsubs.is_empty() {
                        let total = hub.total_subscriptions(client_id);
                        out.extend_from_slice(
                            format!("*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:{}\r\n", total).as_bytes(),
                        );
                    } else {
                        for (ch, remaining) in unsubs {
                            out.extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$");
                            out.extend_from_slice(ch.len().to_string().as_bytes());
                            out.extend_from_slice(b"\r\n");
                            out.extend_from_slice(&ch);
                            out.extend_from_slice(b"\r\n:");
                            out.extend_from_slice(remaining.to_string().as_bytes());
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                } else {
                    for ch in channels {
                        let remaining = hub.unsubscribe(client_id, &ch);
                        out.extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$");
                        out.extend_from_slice(ch.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&ch);
                        out.extend_from_slice(b"\r\n:");
                        out.extend_from_slice(remaining.to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                    }
                }
                false
            }
            Command::Psubscribe(patterns) => {
                let mut hub = router.pubsub.borrow_mut();
                for pat in patterns {
                    let count = hub.psubscribe(client_id, pat.clone(), write_tx.clone());
                    out.extend_from_slice(b"*3\r\n$10\r\npsubscribe\r\n$");
                    out.extend_from_slice(pat.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(&pat);
                    out.extend_from_slice(b"\r\n:");
                    out.extend_from_slice(count.to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                false
            }
            Command::Punsubscribe(patterns) => {
                let mut hub = router.pubsub.borrow_mut();
                if patterns.is_empty() {
                    let unsubs = hub.punsubscribe_all(client_id);
                    if unsubs.is_empty() {
                        let total = hub.total_subscriptions(client_id);
                        out.extend_from_slice(
                            format!("*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:{}\r\n", total)
                                .as_bytes(),
                        );
                    } else {
                        for (pat, remaining) in unsubs {
                            out.extend_from_slice(b"*3\r\n$12\r\npunsubscribe\r\n$");
                            out.extend_from_slice(pat.len().to_string().as_bytes());
                            out.extend_from_slice(b"\r\n");
                            out.extend_from_slice(&pat);
                            out.extend_from_slice(b"\r\n:");
                            out.extend_from_slice(remaining.to_string().as_bytes());
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                } else {
                    for pat in patterns {
                        let remaining = hub.punsubscribe(client_id, &pat);
                        out.extend_from_slice(b"*3\r\n$12\r\npunsubscribe\r\n$");
                        out.extend_from_slice(pat.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&pat);
                        out.extend_from_slice(b"\r\n:");
                        out.extend_from_slice(remaining.to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                    }
                }
                false
            }
            Command::Ping(msg) => {
                match msg {
                    Some(m) => {
                        out.extend_from_slice(
                            format!("*2\r\n$4\r\npong\r\n${}\r\n", m.len()).as_bytes(),
                        );
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                    }
                    None => {
                        out.extend_from_slice(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n");
                    }
                }
                false
            }
            Command::Quit => {
                out.extend_from_slice(b"+OK\r\n");
                true
            }
            other => {
                let name = match &other {
                    Command::Publish { .. } => "PUBLISH",
                    Command::Get(_) => "GET",
                    Command::Set { .. } => "SET",
                    _ => "UNKNOWN",
                };
                out.extend_from_slice(
                    format!("-ERR Can't execute '{}' in subscribed mode\r\n", name).as_bytes(),
                );
                false
            }
        }
    };

    let mut out = Vec::new();
    let q = handle_cmd(initial_sub, &mut out);
    if !out.is_empty() {
        let _ = write_tx.send(out);
    }
    if q {
        return;
    }

    for cmd in pending_cmds {
        let mut out = Vec::new();
        let q = handle_cmd(cmd, &mut out);
        if !out.is_empty() {
            let _ = write_tx.send(out);
        }
        if q {
            return;
        }
    }

    while !buf.is_empty() {
        match parse_command(&mut buf) {
            Ok(Some(cmd)) => {
                let mut out = Vec::new();
                let q = handle_cmd(cmd, &mut out);
                if !out.is_empty() {
                    let _ = write_tx.send(out);
                }
                if q {
                    return;
                }
            }
            Ok(None) => break,
            Err(e) => {
                let mut out = Vec::new();
                write_resp_err(&mut out, &e);
                let _ = write_tx.send(out);
                return;
            }
        }
    }

    loop {
        let (res, returned_buf) = reader.read(read_buf).await;
        read_buf = returned_buf;
        match res {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&read_buf[..n]);
                while !buf.is_empty() {
                    match parse_command(&mut buf) {
                        Ok(Some(cmd)) => {
                            let mut out = Vec::new();
                            let q = handle_cmd(cmd, &mut out);
                            if !out.is_empty() {
                                let _ = write_tx.send(out);
                            }
                            if q {
                                return;
                            }
                        }
                        Ok(None) => break,
                        Err(e) => {
                            let mut out = Vec::new();
                            write_resp_err(&mut out, &e);
                            let _ = write_tx.send(out);
                            return;
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
}

async fn run_master_replica_stream(
    stream: TcpStream,
    client_id: u64,
    _client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    router: Rc<Router>,
    psync_cmd: Command,
) {
    let hub = crate::replication::get_replication_hub(router.port);
    let (mut reader, mut writer) = stream.into_split();
    let (write_tx, write_rx) = flume::unbounded::<Vec<u8>>();

    let (req_replid, req_offset) = match &psync_cmd {
        Command::Psync { replid, offset } => (
            std::str::from_utf8(replid).unwrap_or(""),
            *offset,
        ),
        _ => ("", -1),
    };

    let partial = hub.try_partial_resync(client_id, write_tx.clone(), req_replid, req_offset);
    if let Some((replid, diff, _repl)) = partial {
        let mut initial_msg = format!("+CONTINUE {}\r\n", replid).into_bytes();
        initial_msg.extend_from_slice(&diff);
        if writer.write_all(initial_msg).await.0.is_err() {
            hub.unregister_replica(client_id);
            return;
        }
    } else {
        let rdb = router.generate_full_rdb().await;
        let _repl = hub.register_replica(client_id, write_tx.clone());

        let replid = hub.master_replid.clone();
        let offset = hub
            .master_repl_offset
            .load(std::sync::atomic::Ordering::SeqCst);
        let mut initial_msg =
            format!("+FULLRESYNC {} {}\r\n${}\r\n", replid, offset, rdb.len()).into_bytes();
        initial_msg.extend_from_slice(&rdb);
        if writer.write_all(initial_msg).await.0.is_err() {
            hub.unregister_replica(client_id);
            return;
        }
    }

    let writer_hub = hub.clone();
    monoio::spawn(async move {
        while let Ok(data) = write_rx.recv_async().await {
            if writer.write_all(data).await.0.is_err() {
                break;
            }
        }
        writer_hub.unregister_replica(client_id);
    });

    let mut read_buf = vec![0u8; READ_BUFFER_SIZE];
    let mut buf = BytesMut::with_capacity(32768);
    loop {
        let (res, returned_buf) = reader.read(read_buf).await;
        read_buf = returned_buf;
        match res {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&read_buf[..n]);
                while !buf.is_empty() {
                    match crate::resp::parse_command(&mut buf) {
                        Ok(Some(cmd)) => {
                            if let Command::Replconf(args) = cmd
                                && args.len() >= 2
                                && args[0].eq_ignore_ascii_case(b"ack")
                                && let Ok(s) = std::str::from_utf8(&args[1])
                                && let Ok(ack_off) = s.parse::<u64>()
                            {
                                hub.update_replica_ack(client_id, ack_off);
                            }
                        }
                        Ok(None) => break,
                        Err(_) => {
                            buf.clear();
                            break;
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
    hub.unregister_replica(client_id);
}

pub fn cmd_primary_key(cmd: &Command) -> Option<&bytes::Bytes> {
    match cmd {
        Command::Get(key)
        | Command::Getex { key, .. }
        | Command::Set { key, .. }
        | Command::IncrBy(key, _)
        | Command::Expire(key, _)
        | Command::Persist(key)
        | Command::Ttl(key, _)
        | Command::Hset { key, .. }
        | Command::Hsetnx { key, .. }
        | Command::Hmset { key, .. }
        | Command::Hget { key, .. }
        | Command::Hmget { key, .. }
        | Command::Hdel { key, .. }
        | Command::Hexists { key, .. }
        | Command::Hlen(key)
        | Command::Hgetall(key)
        | Command::Hkeys(key)
        | Command::Hvals(key)
        | Command::Hstrlen { key, .. }
        | Command::Hgetdel { key, .. }
        | Command::Lpush { key, .. }
        | Command::Rpush { key, .. }
        | Command::Lpushx { key, .. }
        | Command::Rpushx { key, .. }
        | Command::Lpop { key, .. }
        | Command::Rpop { key, .. }
        | Command::Lrange { key, .. }
        | Command::Llen(key)
        | Command::Lindex { key, .. }
        | Command::Sadd { key, .. }
        | Command::Srem { key, .. }
        | Command::Smembers(key)
        | Command::Sismember { key, .. }
        | Command::Scard(key)
        | Command::Spop { key, .. }
        | Command::Zadd { key, .. }
        | Command::Zrem { key, .. }
        | Command::Zscore { key, .. }
        | Command::Zcard(key)
        | Command::Zrank { key, .. }
        | Command::Zrevrank { key, .. }
        | Command::Zcount { key, .. }
        | Command::Zincrby { key, .. }
        | Command::Zrange { key, .. }
        | Command::Zrangestore { dst: key, .. }
        | Command::Zpopmin { key, .. }
        | Command::Zpopmax { key, .. }
        | Command::Type(key)
        | Command::Sort { key, .. }
        | Command::Setnx { key, .. }
        | Command::Getset { key, .. }
        | Command::Getdel(key)
        | Command::Append { key, .. }
        | Command::Strlen(key)
        | Command::Expiretime(key, _)
        | Command::Rename { key, .. }
        | Command::Setbit { key, .. }
        | Command::Getbit { key, .. }
        | Command::Bitcount { key, .. }
        | Command::Bitpos { key, .. }
        | Command::Pfadd { key, .. }
        | Command::Dump(key)
        | Command::Restore { key, .. }
        | Command::Xadd { key, .. }
        | Command::Xlen(key)
        | Command::Xrange { key, .. }
        | Command::Xrevrange { key, .. }
        | Command::Xdel { key, .. }
        | Command::Xtrim { key, .. }
        | Command::XgroupCreate { key, .. }
        | Command::XgroupDestroy { key, .. }
        | Command::XgroupCreateConsumer { key, .. }
        | Command::XgroupDelConsumer { key, .. }
        | Command::Xack { key, .. }
        | Command::Xpending { key, .. }
        | Command::Hincrby { key, .. }
        | Command::Hincrbyfloat { key, .. }
        | Command::Hrandfield { key, .. }
        | Command::Hscan { key, .. }
        | Command::Smismember { key, .. }
        | Command::Srandmember { key, .. }
        | Command::Sscan { key, .. }
        | Command::Zmscore { key, .. }
        | Command::Zrandmember { key, .. }
        | Command::Zremrangebyrank { key, .. }
        | Command::Zremrangebyscore { key, .. }
        | Command::Zremrangebylex { key, .. }
        | Command::Zlexcount { key, .. }
        | Command::Zscan { key, .. }
        | Command::Ltrim { key, .. }
        | Command::Lset { key, .. }
        | Command::Lrem { key, .. }
        | Command::Lpos { key, .. }
        | Command::Linsert { key, .. }
        | Command::Incrbyfloat { key, .. }
        | Command::Setrange { key, .. }
        | Command::Getrange { key, .. }
        | Command::Vadd { key, .. }
        | Command::Vdel { key, .. }
        | Command::JsonSet { key, .. }
        | Command::JsonGet { key, .. }
        | Command::JsonDel { key, .. }
        | Command::JsonType { key, .. }
        | Command::JsonNumIncrBy { key, .. }
        | Command::JsonNumMultBy { key, .. }
        | Command::JsonStrAppend { key, .. }
        | Command::JsonStrLen { key, .. }
        | Command::JsonArrAppend { key, .. }
        | Command::JsonArrLen { key, .. }
        | Command::JsonArrPop { key, .. }
        | Command::JsonObjKeys { key, .. }
        | Command::JsonObjLen { key, .. }
        | Command::JsonToggle { key, .. }
        | Command::JsonClear { key, .. }
        | Command::Geoadd { key, .. }
        | Command::Geodist { key, .. }
        | Command::Geopos { key, .. }
        | Command::Geohash { key, .. }
        | Command::Georadius { key, .. }
        | Command::Georadiusbymember { key, .. }
        | Command::Geosearch { key, .. }
        | Command::BfReserve { key, .. }
        | Command::BfAdd { key, .. }
        | Command::BfMadd { key, .. }
        | Command::BfExists { key, .. }
        | Command::BfMexists { key, .. }
        | Command::BfInfo(key)
        | Command::CfReserve { key, .. }
        | Command::CfAdd { key, .. }
        | Command::CfAddnx { key, .. }
        | Command::CfExists { key, .. }
        | Command::CfDel { key, .. }
        | Command::CfInfo(key)
        | Command::CmsInitbydim { key, .. }
        | Command::CmsInitbyprob { key, .. }
        | Command::CmsIncrby { key, .. }
        | Command::CmsQuery { key, .. }
        | Command::CmsInfo(key)
        | Command::TopkReserve { key, .. }
        | Command::TopkAdd { key, .. }
        | Command::TopkQuery { key, .. }
        | Command::TopkList(key)
        | Command::TopkInfo(key)
        | Command::CrdtSet { key, .. }
        | Command::CrdtGet(key)
        | Command::CrdtDel(key)
        | Command::CrdtIncrby { key, .. }
        | Command::CrdtSadd { key, .. }
        | Command::CrdtSmembers(key)
        | Command::CrdtSrem { key, .. } => Some(key),

        Command::Smove { source, .. }
        | Command::Lmove { source, .. }
        | Command::Blmove { source, .. } => Some(source),
        Command::Touch(keys) | Command::Del(keys) | Command::Exists(keys) | Command::Mget(keys) => {
            keys.first()
        }
        Command::Pfcount { keys } => keys.first(),
        Command::Xread { keys, .. } | Command::Xreadgroup { keys, .. } => keys.first(),
        Command::Blpop { keys, .. }
        | Command::Brpop { keys, .. }
        | Command::Bzpopmin { keys, .. }
        | Command::Bzpopmax { keys, .. }
        | Command::Zmpop { keys, .. }
        | Command::Bzmpop { keys, .. } => keys.first(),
        Command::Sinter(keys)
        | Command::Sunion(keys)
        | Command::Sdiff(keys)
        | Command::Sintercard { keys, .. }
        | Command::Sunioncard { keys, .. }
        | Command::Sdiffcard { keys, .. } => keys.first(),
        Command::Sinterstore { destination, .. }
        | Command::Sunionstore { destination, .. }
        | Command::Sdiffstore { destination, .. } => Some(destination),
        Command::Zunionstore { destination, .. }
        | Command::Zinterstore { destination, .. }
        | Command::Zdiffstore { destination, .. } => Some(destination),
        Command::Zdiff { keys, .. }
        | Command::Zinter { keys, .. }
        | Command::Zunion { keys, .. }
        | Command::Zintercard { keys, .. } => keys.first(),
        Command::Mset(pairs) | Command::Msetnx(pairs) | Command::Msetex { pairs, .. } => {
            pairs.first().map(|(k, _)| k)
        }
        Command::Lcs { key1, .. } => Some(key1),
        Command::Bitop { destkey, .. } | Command::Pfmerge { destkey, .. } => Some(destkey),
        Command::Eval { keys, .. } | Command::Evalsha { keys, .. } => keys.first(),
        Command::Sticky(key)
        | Command::Digest(key)
        | Command::Delex { key, .. }
        | Command::MemcachedSet { key, .. }
        | Command::MemcachedAdd { key, .. }
        | Command::MemcachedReplace { key, .. }
        | Command::MemcachedDelete { key, .. }
        | Command::MemcachedIncr { key, .. }
        | Command::MemcachedDecr { key, .. } => Some(key),
        Command::MemcachedGet { keys } => keys.first(),
        _ => None,
    }
}

pub fn cmd_keys(cmd: &Command) -> Vec<&[u8]> {
    match cmd {
        Command::Get(k)
        | Command::IncrBy(k, _)
        | Command::Expire(k, _)
        | Command::Persist(k)
        | Command::Ttl(k, _)
        | Command::Hlen(k)
        | Command::Hgetall(k)
        | Command::Hkeys(k)
        | Command::Hvals(k)
        | Command::Llen(k)
        | Command::Smembers(k)
        | Command::Scard(k)
        | Command::Zcard(k)
        | Command::Type(k)
        | Command::Getdel(k)
        | Command::Strlen(k)
        | Command::Expiretime(k, _)
        | Command::Dump(k)
        | Command::Xlen(k) => vec![k.as_ref()],

        Command::Set { key, .. }
        | Command::Hget { key, .. }
        | Command::Lpop { key, .. }
        | Command::Rpop { key, .. }
        | Command::Lrange { key, .. }
        | Command::Lindex { key, .. }
        | Command::Sismember { key, .. }
        | Command::Spop { key, .. }
        | Command::Zscore { key, .. }
        | Command::Zrank { key, .. }
        | Command::Zrevrank { key, .. }
        | Command::Zcount { key, .. }
        | Command::Zincrby { key, .. }
        | Command::Zrange { key, .. }
        | Command::Zrangestore { dst: key, .. }
        | Command::Zpopmin { key, .. }
        | Command::Zpopmax { key, .. }
        | Command::Setnx { key, .. }
        | Command::Getset { key, .. }
        | Command::Append { key, .. }
        | Command::Hset { key, .. }
        | Command::Hsetnx { key, .. }
        | Command::Hmset { key, .. }
        | Command::Hexists { key, .. }
        | Command::Hstrlen { key, .. }
        | Command::Hgetdel { key, .. }
        | Command::Lpush { key, .. }
        | Command::Rpush { key, .. }
        | Command::Lpushx { key, .. }
        | Command::Rpushx { key, .. }
        | Command::Sadd { key, .. }
        | Command::Srem { key, .. }
        | Command::Zadd { key, .. }
        | Command::Zrem { key, .. }
        | Command::Setbit { key, .. }
        | Command::Getbit { key, .. }
        | Command::Bitcount { key, .. }
        | Command::Bitpos { key, .. }
        | Command::Pfadd { key, .. }
        | Command::Restore { key, .. }
        | Command::Xadd { key, .. }
        | Command::Xrange { key, .. }
        | Command::Xrevrange { key, .. }
        | Command::Xdel { key, .. }
        | Command::Xtrim { key, .. }
        | Command::XgroupCreate { key, .. }
        | Command::XgroupDestroy { key, .. }
        | Command::XgroupCreateConsumer { key, .. }
        | Command::XgroupDelConsumer { key, .. }
        | Command::Xack { key, .. }
        | Command::Xpending { key, .. }
        | Command::Hincrby { key, .. }
        | Command::Hincrbyfloat { key, .. }
        | Command::Hrandfield { key, .. }
        | Command::Hscan { key, .. }
        | Command::Smismember { key, .. }
        | Command::Srandmember { key, .. }
        | Command::Sscan { key, .. }
        | Command::Zmscore { key, .. }
        | Command::Zrandmember { key, .. }
        | Command::Zremrangebyrank { key, .. }
        | Command::Zremrangebyscore { key, .. }
        | Command::Zremrangebylex { key, .. }
        | Command::Zlexcount { key, .. }
        | Command::Zscan { key, .. }
        | Command::Ltrim { key, .. }
        | Command::Lset { key, .. }
        | Command::Lrem { key, .. }
        | Command::Lpos { key, .. }
        | Command::Linsert { key, .. }
        | Command::Incrbyfloat { key, .. }
        | Command::Setrange { key, .. }
        | Command::Getrange { key, .. }
        | Command::Vadd { key, .. }
        | Command::Vdel { key, .. }
        | Command::JsonSet { key, .. }
        | Command::JsonGet { key, .. }
        | Command::JsonDel { key, .. }
        | Command::JsonType { key, .. }
        | Command::JsonNumIncrBy { key, .. }
        | Command::JsonNumMultBy { key, .. }
        | Command::JsonStrAppend { key, .. }
        | Command::JsonStrLen { key, .. }
        | Command::JsonArrAppend { key, .. }
        | Command::JsonArrLen { key, .. }
        | Command::JsonArrPop { key, .. }
        | Command::JsonObjKeys { key, .. }
        | Command::JsonObjLen { key, .. }
        | Command::JsonToggle { key, .. }
        | Command::JsonClear { key, .. }
        | Command::Geoadd { key, .. }
        | Command::Geodist { key, .. }
        | Command::Geopos { key, .. }
        | Command::Geohash { key, .. }
        | Command::Georadius { key, .. }
        | Command::Georadiusbymember { key, .. }
        | Command::Geosearch { key, .. }
        | Command::BfReserve { key, .. }
        | Command::BfAdd { key, .. }
        | Command::BfMadd { key, .. }
        | Command::BfExists { key, .. }
        | Command::BfMexists { key, .. }
        | Command::BfInfo(key)
        | Command::CfReserve { key, .. }
        | Command::CfAdd { key, .. }
        | Command::CfAddnx { key, .. }
        | Command::CfExists { key, .. }
        | Command::CfDel { key, .. }
        | Command::CfInfo(key)
        | Command::CmsInitbydim { key, .. }
        | Command::CmsInitbyprob { key, .. }
        | Command::CmsIncrby { key, .. }
        | Command::CmsQuery { key, .. }
        | Command::CmsInfo(key)
        | Command::TopkReserve { key, .. }
        | Command::TopkAdd { key, .. }
        | Command::TopkQuery { key, .. }
        | Command::TopkList(key)
        | Command::TopkInfo(key) => vec![key.as_ref()],

        Command::Smove {
            source,
            destination,
            ..
        }
        | Command::Lmove {
            source,
            destination,
            ..
        }
        | Command::Blmove {
            source,
            destination,
            ..
        } => {
            vec![source.as_ref(), destination.as_ref()]
        }
        Command::Sort { key, store, .. } => {
            if let Some(dest) = store {
                vec![key.as_ref(), dest.as_ref()]
            } else {
                vec![key.as_ref()]
            }
        }

        Command::Mget(keys) | Command::Del(keys) | Command::Exists(keys) | Command::Touch(keys) => {
            keys.iter().map(|k| k.as_ref()).collect()
        }
        Command::Pfcount { keys } => keys.iter().map(|k| k.as_ref()).collect(),
        Command::Xread { keys, .. } | Command::Xreadgroup { keys, .. } => {
            keys.iter().map(|k| k.as_ref()).collect()
        }
        Command::Blpop { keys, .. }
        | Command::Brpop { keys, .. }
        | Command::Bzpopmin { keys, .. }
        | Command::Bzpopmax { keys, .. }
        | Command::Zmpop { keys, .. }
        | Command::Bzmpop { keys, .. } => keys.iter().map(|k| k.as_ref()).collect(),
        Command::Sinter(keys)
        | Command::Sunion(keys)
        | Command::Sdiff(keys)
        | Command::Sintercard { keys, .. }
        | Command::Sunioncard { keys, .. }
        | Command::Sdiffcard { keys, .. } => keys.iter().map(|k| k.as_ref()).collect(),
        Command::Sinterstore { destination, keys }
        | Command::Sunionstore { destination, keys }
        | Command::Sdiffstore { destination, keys } => {
            let mut v = vec![destination.as_ref()];
            v.extend(keys.iter().map(|k| k.as_ref()));
            v
        }
        Command::Zunionstore {
            destination, keys, ..
        }
        | Command::Zinterstore {
            destination, keys, ..
        }
        | Command::Zdiffstore { destination, keys } => {
            let mut v = vec![destination.as_ref()];
            v.extend(keys.iter().map(|k| k.as_ref()));
            v
        }
        Command::Zdiff { keys, .. }
        | Command::Zinter { keys, .. }
        | Command::Zunion { keys, .. }
        | Command::Zintercard { keys, .. } => keys.iter().map(|k| k.as_ref()).collect(),

        Command::Mset(pairs) | Command::Msetnx(pairs) | Command::Msetex { pairs, .. } => {
            pairs.iter().map(|(k, _)| k.as_ref()).collect()
        }

        Command::Lcs { key1, key2, .. } => {
            vec![key1.as_ref(), key2.as_ref()]
        }

        Command::Digest(key) => {
            vec![key.as_ref()]
        }

        Command::Rename { key, newkey, .. } => {
            vec![key.as_ref(), newkey.as_ref()]
        }

        Command::Bitop {
            destkey, srckeys, ..
        }
        | Command::Pfmerge { destkey, srckeys } => {
            let mut v = vec![destkey.as_ref()];
            v.extend(srckeys.iter().map(|k| k.as_ref()));
            v
        }

        Command::Hmget { key, .. } | Command::Hdel { key, .. } => vec![key.as_ref()],
        Command::Eval { keys, .. } | Command::Evalsha { keys, .. } => {
            keys.iter().map(|k| k.as_ref()).collect()
        }

        _ => Vec::new(),
    }
}

async fn migrate_keys_to_node(
    router: &Router,
    keys: &[Bytes],
    host: &str,
    port: u16,
    copy: bool,
) -> Result<usize, String> {
    let mut dumps = Vec::new();
    for k in keys {
        if let Some(entry) = router.dump_key(k.clone()).await {
            dumps.push((k.clone(), entry));
        }
    }

    if dumps.is_empty() {
        return Ok(0);
    }

    let target_addr = format!("{}:{}", host, port);
    let socket_addr = match std::net::ToSocketAddrs::to_socket_addrs(&target_addr) {
        Ok(mut iter) => match iter.next() {
            Some(a) => a,
            None => return Err("cannot resolve destination host".to_string()),
        },
        Err(e) => return Err(format!("invalid destination address: {}", e)),
    };

    let mut stream = match TcpStream::connect(socket_addr).await {
        Ok(s) => s,
        Err(err) => return Err(format!("IOERR error connecting to destination: {}", err)),
    };

    let mut tx_buf = Vec::new();
    tx_buf.extend_from_slice(b"*1\r\n$6\r\nASKING\r\n");
    for (k, (val, ttl)) in &dumps {
        match val {
            crate::table::RudisValue::String(s) => {
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*5\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                    tx_buf.extend_from_slice(s);
                    tx_buf.extend_from_slice(
                        format!("\r\n$2\r\nPX\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                } else {
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                    tx_buf.extend_from_slice(s);
                    tx_buf.extend_from_slice(b"\r\n");
                }
            }
            crate::table::RudisValue::Int(n) => {
                let s = crate::table::RudisTable::format_i64(*n);
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*5\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                    tx_buf.extend_from_slice(&s);
                    tx_buf.extend_from_slice(
                        format!("\r\n$2\r\nPX\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                } else {
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                    tx_buf.extend_from_slice(&s);
                    tx_buf.extend_from_slice(b"\r\n");
                }
            }
            crate::table::RudisValue::SmallHash(entries) => {
                tx_buf.extend_from_slice(
                    format!(
                        "*{}\r\n$4\r\nHSET\r\n${}\r\n",
                        2 + entries.len() * 2,
                        k.len()
                    )
                    .as_bytes(),
                );
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n");
                for (f, v) in entries {
                    tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                    tx_buf.extend_from_slice(f);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", v.len()).as_bytes());
                    tx_buf.extend_from_slice(v);
                    tx_buf.extend_from_slice(b"\r\n");
                }
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::Hash(h) => {
                tx_buf.extend_from_slice(
                    format!("*{}\r\n$4\r\nHSET\r\n${}\r\n", 2 + h.len() * 2, k.len()).as_bytes(),
                );
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n");
                for (f, v) in h {
                    tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                    tx_buf.extend_from_slice(f);
                    tx_buf.extend_from_slice(format!("\r\n${}\r\n", v.len()).as_bytes());
                    tx_buf.extend_from_slice(v);
                    tx_buf.extend_from_slice(b"\r\n");
                }
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::List(l) => {
                tx_buf.extend_from_slice(
                    format!("*{}\r\n$5\r\nRPUSH\r\n${}\r\n", 2 + l.len(), k.len()).as_bytes(),
                );
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n");
                for item in l {
                    tx_buf.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
                    tx_buf.extend_from_slice(item);
                    tx_buf.extend_from_slice(b"\r\n");
                }
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::Set(s) => {
                tx_buf.extend_from_slice(
                    format!("*{}\r\n$4\r\nSADD\r\n${}\r\n", 2 + s.len(), k.len()).as_bytes(),
                );
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n");
                for item in s {
                    tx_buf.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
                    tx_buf.extend_from_slice(item);
                    tx_buf.extend_from_slice(b"\r\n");
                }
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::ZSet(zset) => {
                tx_buf.extend_from_slice(
                    format!("*{}\r\n$4\r\nZADD\r\n${}\r\n", 2 + zset.len() * 2, k.len()).as_bytes(),
                );
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n");
                zset.for_each(|m, score| {
                    let s = format_score(score);
                    tx_buf.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                    tx_buf.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                    tx_buf.extend_from_slice(m);
                    tx_buf.extend_from_slice(b"\r\n");
                });
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::HyperLogLog(regs) => {
                tx_buf.extend_from_slice(format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes());
                tx_buf.extend_from_slice(k);
                tx_buf.extend_from_slice(b"\r\n$16384\r\n");
                tx_buf.extend_from_slice(regs.as_ref());
                tx_buf.extend_from_slice(b"\r\n");
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::Stream(stream) => {
                for (id, fields) in &stream.entries {
                    tx_buf.extend_from_slice(
                        format!(
                            "*{}\r\n$4\r\nXADD\r\n${}\r\n",
                            3 + fields.len() * 2,
                            k.len()
                        )
                        .as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    let id_str = id.to_string();
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", id_str.len(), id_str).as_bytes(),
                    );
                    for (f, v) in fields {
                        tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                        tx_buf.extend_from_slice(f);
                        tx_buf.extend_from_slice(format!("\r\n${}\r\n", v.len()).as_bytes());
                        tx_buf.extend_from_slice(v);
                        tx_buf.extend_from_slice(b"\r\n");
                    }
                }
                if let Some(dur) = ttl {
                    let ms = dur.as_millis().max(1);
                    let ms_str = ms.to_string();
                    tx_buf.extend_from_slice(
                        format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                    );
                    tx_buf.extend_from_slice(k);
                    tx_buf.extend_from_slice(
                        format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                    );
                }
            }
            crate::table::RudisValue::Tiered(_) | crate::table::RudisValue::Cooled { .. } => {}
        }
    }

    if let Err(e) = stream.write_all(tx_buf).await.0 {
        return Err(format!("IOERR error sending to destination: {}", e));
    }

    let resp_buf = vec![0u8; 1024];
    let (read_res, _) = stream.read(resp_buf).await;
    if let Err(e) = read_res {
        return Err(format!("IOERR error reading from destination: {}", e));
    }

    let count = dumps.len();
    if !copy {
        for (k, _) in &dumps {
            let _ = router.del(k.clone()).await;
        }
    }

    Ok(count)
}

fn parse_bulk_str_from_resp(res: &[u8]) -> Option<Bytes> {
    if res.starts_with(b"$") && !res.starts_with(b"$-1") {
        let mut parts = res[1..].splitn(2, |&b| b == b'\r');
        let len_str = std::str::from_utf8(parts.next()?).ok()?;
        let len: usize = len_str.parse().ok()?;
        let rest = parts.next()?;
        let val_slice = rest.strip_prefix(b"\n")?;
        if val_slice.len() >= len {
            return Some(Bytes::copy_from_slice(&val_slice[..len]));
        }
    }
    None
}

fn parse_array_from_resp(res: &[u8]) -> Option<Vec<Bytes>> {
    if !res.starts_with(b"*") || res.starts_with(b"*-1") {
        return None;
    }
    let mut cur = &res[1..];
    let idx = cur.iter().position(|&b| b == b'\r')?;
    let count: usize = std::str::from_utf8(&cur[..idx]).ok()?.parse().ok()?;
    cur = &cur[idx + 2..];
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        if !cur.starts_with(b"$") || cur.starts_with(b"$-1") {
            return None;
        }
        cur = &cur[1..];
        let blk_idx = cur.iter().position(|&b| b == b'\r')?;
        let blk_len: usize = std::str::from_utf8(&cur[..blk_idx]).ok()?.parse().ok()?;
        cur = &cur[blk_idx + 2..];
        if cur.len() < blk_len + 2 {
            return None;
        }
        items.push(Bytes::copy_from_slice(&cur[..blk_len]));
        cur = &cur[blk_len + 2..];
    }
    Some(items)
}

fn parse_zpop_items(res: &[u8]) -> Option<Vec<(Bytes, f64)>> {
    if !res.starts_with(b"*") || res.starts_with(b"*-1") || res.starts_with(b"*0") {
        return None;
    }
    let mut cur = &res[1..];
    let idx = cur.iter().position(|&b| b == b'\r')?;
    let top_count: usize = std::str::from_utf8(&cur[..idx]).ok()?.parse().ok()?;
    cur = &cur[idx + 2..];
    if top_count == 0 {
        return None;
    }

    let mut items = Vec::new();
    if cur.starts_with(b"*2\r\n") {
        for _ in 0..top_count {
            if !cur.starts_with(b"*2\r\n") {
                return None;
            }
            cur = &cur[4..];
            if !cur.starts_with(b"$") {
                return None;
            }
            cur = &cur[1..];
            let m_len_idx = cur.iter().position(|&b| b == b'\r')?;
            let m_len: usize = std::str::from_utf8(&cur[..m_len_idx]).ok()?.parse().ok()?;
            cur = &cur[m_len_idx + 2..];
            let member = Bytes::copy_from_slice(&cur[..m_len]);
            cur = &cur[m_len + 2..];

            let score: f64 = if cur.starts_with(b",") {
                cur = &cur[1..];
                let s_idx = cur.iter().position(|&b| b == b'\r')?;
                let s_str = std::str::from_utf8(&cur[..s_idx]).ok()?;
                cur = &cur[s_idx + 2..];
                s_str.parse().ok()?
            } else if cur.starts_with(b"$") {
                cur = &cur[1..];
                let s_len_idx = cur.iter().position(|&b| b == b'\r')?;
                let s_len: usize = std::str::from_utf8(&cur[..s_len_idx]).ok()?.parse().ok()?;
                cur = &cur[s_len_idx + 2..];
                let s_str = std::str::from_utf8(&cur[..s_len]).ok()?;
                cur = &cur[s_len + 2..];
                s_str.parse().ok()?
            } else {
                return None;
            };
            items.push((member, score));
        }
    } else if cur.starts_with(b"$") {
        let num_pairs = top_count / 2;
        for _ in 0..num_pairs {
            if !cur.starts_with(b"$") {
                return None;
            }
            cur = &cur[1..];
            let m_len_idx = cur.iter().position(|&b| b == b'\r')?;
            let m_len: usize = std::str::from_utf8(&cur[..m_len_idx]).ok()?.parse().ok()?;
            cur = &cur[m_len_idx + 2..];
            let member = Bytes::copy_from_slice(&cur[..m_len]);
            cur = &cur[m_len + 2..];

            let score: f64 = if cur.starts_with(b",") {
                cur = &cur[1..];
                let s_idx = cur.iter().position(|&b| b == b'\r')?;
                let s_str = std::str::from_utf8(&cur[..s_idx]).ok()?;
                cur = &cur[s_idx + 2..];
                s_str.parse().ok()?
            } else if cur.starts_with(b"$") {
                cur = &cur[1..];
                let s_len_idx = cur.iter().position(|&b| b == b'\r')?;
                let s_len: usize = std::str::from_utf8(&cur[..s_len_idx]).ok()?.parse().ok()?;
                cur = &cur[s_len_idx + 2..];
                let s_str = std::str::from_utf8(&cur[..s_len]).ok()?;
                cur = &cur[s_len + 2..];
                s_str.parse().ok()?
            } else {
                return None;
            };
            items.push((member, score));
        }
    }
    Some(items)
}

pub fn get_cmd_name(cmd: &Command) -> &'static str {
    match cmd {
        Command::Auth { .. } => "AUTH",
        Command::Acl(_) => "ACL",
        Command::Blpop { .. } => "BLPOP",
        Command::Brpop { .. } => "BRPOP",
        Command::Lmpop { .. } => "LMPOP",
        Command::Blmpop { .. } => "BLMPOP",
        Command::Sinter(_) => "SINTER",
        Command::Sunion(_) => "SUNION",
        Command::Sdiff(_) => "SDIFF",
        Command::Sinterstore { .. } => "SINTERSTORE",
        Command::Sunionstore { .. } => "SUNIONSTORE",
        Command::Sdiffstore { .. } => "SDIFFSTORE",
        Command::Sintercard { .. } => "SINTERCARD",
        Command::Sunioncard { .. } => "SUNIONCARD",
        Command::Sdiffcard { .. } => "SDIFFCARD",
        Command::Zunionstore { .. } => "ZUNIONSTORE",
        Command::Zinterstore { .. } => "ZINTERSTORE",
        Command::Zdiffstore { .. } => "ZDIFFSTORE",
        Command::Zdiff { .. } => "ZDIFF",
        Command::Zinter { .. } => "ZINTER",
        Command::Zunion { .. } => "ZUNION",
        Command::Zintercard { .. } => "ZINTERCARD",
        Command::Get(_) => "GET",
        Command::Getex { .. } => "GETEX",
        Command::Set { .. } => "SET",
        Command::Mget(_) => "MGET",
        Command::Mset(_) => "MSET",
        Command::Msetex { .. } => "MSETEX",
        Command::Lcs { .. } => "LCS",
        Command::Digest(_) => "DIGEST",
        Command::Del(_) => "DEL",
        Command::Exists(_) => "EXISTS",
        Command::IncrBy(_, _) => "INCRBY",
        Command::Expire(_, _) => "EXPIRE",
        Command::Persist(_) => "PERSIST",
        Command::Ttl(_, _) => "TTL",
        Command::Cluster(_) => "CLUSTER",
        Command::Client(sub) => match sub {
            ClientSubcommand::List(_) => "client|list",
            ClientSubcommand::Info => "client|info",
            ClientSubcommand::SetName(_) => "client|setname",
            ClientSubcommand::GetName => "client|getname",
            ClientSubcommand::Id => "client|id",
            ClientSubcommand::Tracking { .. } => "client|tracking",
            ClientSubcommand::Caching(_) => "client|caching",
            ClientSubcommand::Kill(_) => "client|kill",
            ClientSubcommand::Unblock { .. } => "client|unblock",
            ClientSubcommand::Pause(_) => "client|pause",
            ClientSubcommand::Unpause => "client|unpause",
            ClientSubcommand::NoTouch(_) => "client|no-touch",
        },
        Command::Asking => "ASKING",
        Command::Migrate { .. } => "MIGRATE",
        Command::Hset { .. } => "HSET",
        Command::Hsetnx { .. } => "HSETNX",
        Command::Hmset { .. } => "HMSET",
        Command::Hget { .. } => "HGET",
        Command::Hmget { .. } => "HMGET",
        Command::Hdel { .. } => "HDEL",
        Command::Hexists { .. } => "HEXISTS",
        Command::Hlen(_) => "HLEN",
        Command::Hgetall(_) => "HGETALL",
        Command::Hkeys(_) => "HKEYS",
        Command::Hvals(_) => "HVALS",
        Command::Hstrlen { .. } => "HSTRLEN",
        Command::Hgetdel { .. } => "HGETDEL",
        Command::Hincrby { .. } => "HINCRBY",
        Command::Hincrbyfloat { .. } => "HINCRBYFLOAT",
        Command::Hrandfield { .. } => "HRANDFIELD",
        Command::Hscan { .. } => "HSCAN",
        Command::Lpush { .. } => "LPUSH",
        Command::Rpush { .. } => "RPUSH",
        Command::Lpushx { .. } => "LPUSHX",
        Command::Rpushx { .. } => "RPUSHX",
        Command::Lpop { .. } => "LPOP",
        Command::Rpop { .. } => "RPOP",
        Command::Lrange { .. } => "LRANGE",
        Command::Llen(_) => "LLEN",
        Command::Lindex { .. } => "LINDEX",
        Command::Ltrim { .. } => "LTRIM",
        Command::Lset { .. } => "LSET",
        Command::Lrem { .. } => "LREM",
        Command::Lpos { .. } => "LPOS",
        Command::Linsert { .. } => "LINSERT",
        Command::Sort { .. } => "SORT",
        Command::Lmove { .. } => "LMOVE",
        Command::Blmove { .. } => "BLMOVE",
        Command::Sadd { .. } => "SADD",
        Command::Srem { .. } => "SREM",
        Command::Smembers(_) => "SMEMBERS",
        Command::Sismember { .. } => "SISMEMBER",
        Command::Smismember { .. } => "SMISMEMBER",
        Command::Scard(_) => "SCARD",
        Command::Spop { .. } => "SPOP",
        Command::Srandmember { .. } => "SRANDMEMBER",
        Command::Smove { .. } => "SMOVE",
        Command::Sscan { .. } => "SSCAN",
        Command::Zadd { .. } => "ZADD",
        Command::Zrem { .. } => "ZREM",
        Command::Zscore { .. } => "ZSCORE",
        Command::Zmscore { .. } => "ZMSCORE",
        Command::Zcard(_) => "ZCARD",
        Command::Zrank { .. } => "ZRANK",
        Command::Zrevrank { .. } => "ZREVRANK",
        Command::Zcount { .. } => "ZCOUNT",
        Command::Zlexcount { .. } => "ZLEXCOUNT",
        Command::Zincrby { .. } => "ZINCRBY",
        Command::Zrange { .. } => "ZRANGE",
        Command::Zrangestore { .. } => "ZRANGESTORE",
        Command::Zpopmin { .. } => "ZPOPMIN",
        Command::Zpopmax { .. } => "ZPOPMAX",
        Command::Bzpopmin { .. } => "BZPOPMIN",
        Command::Bzpopmax { .. } => "BZPOPMAX",
        Command::Zmpop { .. } => "ZMPOP",
        Command::Bzmpop { .. } => "BZMPOP",
        Command::Zrandmember { .. } => "ZRANDMEMBER",
        Command::Zremrangebyrank { .. } => "ZREMRANGEBYRANK",
        Command::Zremrangebyscore { .. } => "ZREMRANGEBYSCORE",
        Command::Zremrangebylex { .. } => "ZREMRANGEBYLEX",
        Command::Zscan { .. } => "ZSCAN",
        Command::Incrbyfloat { .. } => "INCRBYFLOAT",
        Command::Setrange { .. } => "SETRANGE",
        Command::Getrange { .. } => "GETRANGE",
        Command::Hello { .. } => "HELLO",
        Command::Reset => "RESET",
        Command::Time => "TIME",
        Command::Echo(_) => "ECHO",
        Command::Type(_) => "TYPE",
        Command::Dbsize => "DBSIZE",
        Command::Select(_) => "SELECT",
        Command::Slowlog(_) => "SLOWLOG",
        Command::Flushdb => "FLUSHDB",
        Command::Flushall => "FLUSHALL",
        Command::Touch(_) => "TOUCH",
        Command::Rename { nx: false, .. } => "RENAME",
        Command::Rename { nx: true, .. } => "RENAMENX",
        Command::Setnx { .. } => "SETNX",
        Command::Getset { .. } => "GETSET",
        Command::Getdel(_) => "GETDEL",
        Command::Append { .. } => "APPEND",
        Command::Strlen(_) => "STRLEN",
        Command::Msetnx(_) => "MSETNX",
        Command::Save => "SAVE",
        Command::Bgsave => "BGSAVE",
        Command::Lastsave => "LASTSAVE",
        Command::Ping(_) => "PING",
        Command::CommandDocs => "COMMAND",
        Command::Info(_) => "INFO",
        Command::Replicaof { .. } => "REPLICAOF",
        Command::Psync { .. } => "PSYNC",
        Command::Replconf(_) => "REPLCONF",
        Command::Role => "ROLE",
        Command::Tier(_) => "TIER",
        Command::ConfigGet(_) | Command::ConfigSet(_, _) => "CONFIG",
        Command::Quit => "QUIT",
        Command::Subscribe(_) => "SUBSCRIBE",
        Command::Unsubscribe(_) => "UNSUBSCRIBE",
        Command::Psubscribe(_) => "PSUBSCRIBE",
        Command::Punsubscribe(_) => "PUNSUBSCRIBE",
        Command::Publish { .. } => "PUBLISH",
        Command::PubsubChannels(_) => "PUBSUB CHANNELS",
        Command::PubsubNumsub(_) => "PUBSUB NUMSUB",
        Command::PubsubNumpat => "PUBSUB NUMPAT",
        Command::Keys(_) => "KEYS",
        Command::Scan { .. } => "SCAN",
        Command::Randomkey => "RANDOMKEY",
        Command::Expiretime(_, false) => "EXPIRETIME",
        Command::Expiretime(_, true) => "PEXPIRETIME",
        Command::Multi => "MULTI",
        Command::Exec => "EXEC",
        Command::Discard => "DISCARD",
        Command::Watch(_) => "WATCH",
        Command::Unwatch => "UNWATCH",
        Command::Setbit { .. } => "SETBIT",
        Command::Getbit { .. } => "GETBIT",
        Command::Bitcount { .. } => "BITCOUNT",
        Command::Bitpos { .. } => "BITPOS",
        Command::Bitop { .. } => "BITOP",
        Command::Pfadd { .. } => "PFADD",
        Command::Pfcount { .. } => "PFCOUNT",
        Command::Pfmerge { .. } => "PFMERGE",
        Command::Dump(_) => "DUMP",
        Command::Restore { .. } => "RESTORE",
        Command::Xadd { .. } => "XADD",
        Command::Xlen(_) => "XLEN",
        Command::Xrange { .. } => "XRANGE",
        Command::Xrevrange { .. } => "XREVRANGE",
        Command::Xread { .. } => "XREAD",
        Command::Xdel { .. } => "XDEL",
        Command::Xtrim { .. } => "XTRIM",
        Command::XgroupCreate { .. }
        | Command::XgroupDestroy { .. }
        | Command::XgroupCreateConsumer { .. }
        | Command::XgroupDelConsumer { .. } => "XGROUP",
        Command::Xreadgroup { .. } => "XREADGROUP",
        Command::Xack { .. } => "XACK",
        Command::Xpending { .. } => "XPENDING",
        Command::Eval { .. } => "EVAL",
        Command::Evalsha { .. } => "EVALSHA",
        Command::ScriptLoad(_) | Command::ScriptExists(_) | Command::ScriptFlush => "SCRIPT",
        Command::Vadd { .. } => "VADD",
        Command::Vquery { .. } => "VQUERY",
        Command::Vsim { .. } => "VSIM",
        Command::Vdel { .. } => "VDEL",
        Command::Vinfo(_) => "VINFO",
        Command::CrdtSet { .. }
        | Command::CrdtGet(_)
        | Command::CrdtDel(_)
        | Command::CrdtIncrby { .. }
        | Command::CrdtSadd { .. }
        | Command::CrdtSmembers(_)
        | Command::CrdtSrem { .. }
        | Command::CrdtDump
        | Command::CrdtMerge(_)
        | Command::CrdtGc(_) => "CRDT",
        Command::FunctionLoad { .. }
        | Command::FunctionList
        | Command::FunctionDelete(_)
        | Command::FunctionFlush => "FUNCTION",
        Command::Fcall { .. } => "FCALL",
        Command::JsonSet { .. }
        | Command::JsonGet { .. }
        | Command::JsonDel { .. }
        | Command::JsonType { .. }
        | Command::JsonNumIncrBy { .. }
        | Command::JsonNumMultBy { .. }
        | Command::JsonStrAppend { .. }
        | Command::JsonStrLen { .. }
        | Command::JsonArrAppend { .. }
        | Command::JsonArrLen { .. }
        | Command::JsonArrPop { .. }
        | Command::JsonObjKeys { .. }
        | Command::JsonObjLen { .. }
        | Command::JsonToggle { .. }
        | Command::JsonClear { .. }
        | Command::JsonMget { .. } => "JSON",
        Command::Geoadd { .. }
        | Command::Geodist { .. }
        | Command::Geopos { .. }
        | Command::Geohash { .. }
        | Command::Georadius { .. }
        | Command::Georadiusbymember { .. }
        | Command::Geosearch { .. } => "GEO",
        Command::BfReserve { .. }
        | Command::BfAdd { .. }
        | Command::BfMadd { .. }
        | Command::BfExists { .. }
        | Command::BfMexists { .. }
        | Command::BfInfo(_) => "BF",
        Command::CfReserve { .. }
        | Command::CfAdd { .. }
        | Command::CfAddnx { .. }
        | Command::CfExists { .. }
        | Command::CfDel { .. }
        | Command::CfInfo(_) => "CF",
        Command::CmsInitbydim { .. }
        | Command::CmsInitbyprob { .. }
        | Command::CmsIncrby { .. }
        | Command::CmsQuery { .. }
        | Command::CmsInfo(_) => "CMS",
        Command::TopkReserve { .. }
        | Command::TopkAdd { .. }
        | Command::TopkQuery { .. }
        | Command::TopkList(_)
        | Command::TopkInfo(_) => "TOPK",
        Command::FtCreate { .. }
        | Command::FtSearch { .. }
        | Command::FtInfo(_)
        | Command::FtDropIndex { .. }
        | Command::FtExplain { .. }
        | Command::FtAdd { .. } => "FT",
        Command::XdpInfo
        | Command::XdpRuleAdd { .. }
        | Command::XdpRuleDel(_)
        | Command::XdpRuleList
        | Command::XdpStats
        | Command::XdpPacket(_) => "XDP",
        Command::DflyCluster(_) => "DFLYCLUSTER",
        Command::DflyMigrate(_) => "DFLYMIGRATE",
        Command::Stick(_) => "STICK",
        Command::Unstick(_) => "UNSTICK",
        Command::Sticky(_) => "STICKY",
        Command::Delex { .. } => "DELEX",
        Command::MemcachedSet { .. } => "MEMCACHED_SET",
        Command::MemcachedAdd { .. } => "MEMCACHED_ADD",
        Command::MemcachedReplace { .. } => "MEMCACHED_REPLACE",
        Command::MemcachedGet { .. } => "MEMCACHED_GET",
        Command::MemcachedDelete { .. } => "MEMCACHED_DELETE",
        Command::MemcachedIncr { .. } => "MEMCACHED_INCR",
        Command::MemcachedDecr { .. } => "MEMCACHED_DECR",
        Command::MemcachedStats => "MEMCACHED_STATS",
        Command::MemcachedVersion => "MEMCACHED_VERSION",
        Command::MemcachedQuit => "MEMCACHED_QUIT",
        Command::Memory(_) => "MEMORY",
        Command::Debug(_) => "DEBUG",
        Command::Unknown(_) => "UNKNOWN",
    }
}

async fn handle_bzpop(
    router: &Router,
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    out: &mut Vec<u8>,
    keys: Vec<Bytes>,
    timeout: f64,
    is_min: bool,
) -> bool {
    let mut popped: Option<(Bytes, Bytes, f64)> = None;
    for k in &keys {
        let target = router.target_shard(k);
        if target == router.shard_id {
            let mut db = router.local_db.borrow_mut();
            if !db.exists(k) {
                continue;
            }
            let res = if is_min {
                db.zpopmin(k, 1)
            } else {
                db.zpopmax(k, 1)
            };
            match res {
                Ok(mut items) => {
                    if let Some((m, s)) = items.pop() {
                        DIRTY_CHANGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let rep_cmd = if is_min {
                            Command::Zpopmin {
                                key: k.clone(),
                                count: Some(1),
                            }
                        } else {
                            Command::Zpopmax {
                                key: k.clone(),
                                count: Some(1),
                            }
                        };
                        if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                            if let Some(aof_w) = &router.aof {
                                aof_w.borrow_mut().append(&bytes);
                            }
                            if crate::replication::has_connected_replicas(router.port) {
                                crate::replication::propagate_bytes(router.port, &bytes);
                            }
                        }
                        popped = Some((k.clone(), m, s));
                        break;
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                    return false;
                }
            }
        } else {
            let remote_cmd = if is_min {
                Command::Zpopmin {
                    key: k.clone(),
                    count: Some(1),
                }
            } else {
                Command::Zpopmax {
                    key: k.clone(),
                    count: Some(1),
                }
            };
            let remote_res = router.execute_remote(target, remote_cmd).await;
            if remote_res.starts_with(b"-WRONGTYPE") {
                out.extend_from_slice(&remote_res);
                return false;
            }
            if let Some(mut items) = parse_zpop_items(&remote_res)
                && let Some((m, s)) = items.pop()
            {
                popped = Some((k.clone(), m, s));
                break;
            }
        }
    }

    if let Some((k, m, s)) = popped {
        format_bzpop_response(out, &k, &m, s);
        return false;
    }

    if IN_TX.get() {
        write_resp_null_array(out);
        return false;
    }

    let _guard = BlockedClientGuard {
        port: router.port,
        client_id,
    };
    let (tx, rx) = flume::bounded(1);
    let pop_type = if is_min {
        crate::block::ZSetPopType::Min
    } else {
        crate::block::ZSetPopType::Max
    };
    {
        let hub_arc = crate::block::get_block_hub_for_port(router.port);
        let mut hub = hub_arc.lock().unwrap();
        hub.register_blocked_zset_client(client_id, tx.clone());
        for k in keys {
            hub.register_zset_waiter(client_id, k, pop_type, 1, false, tx.clone());
        }
    }

    let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
    let (recv_res, client_disconnected) = wait_for_blocked_result(&rx, timeout, raw_fd).await;
    if client_disconnected {
        return true;
    }

    match recv_res {
        Some(crate::block::BlockedZSetResult::Popped { key, mut items, .. }) => {
            if let Some((m, s)) = items.pop() {
                let rep_cmd = if is_min {
                    Command::Zpopmin {
                        key: key.clone(),
                        count: Some(1),
                    }
                } else {
                    Command::Zpopmax {
                        key: key.clone(),
                        count: Some(1),
                    }
                };
                if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                    if let Some(aof_w) = &router.aof {
                        aof_w.borrow_mut().append(&bytes);
                    }
                    if crate::replication::has_connected_replicas(router.port) {
                        crate::replication::propagate_bytes(router.port, &bytes);
                    }
                }
                format_bzpop_response(out, &key, &m, s);
            } else {
                write_resp_null_array(out);
            }
        }
        Some(crate::block::BlockedZSetResult::Unblocked(
            crate::block::ClientUnblockType::Error,
        )) => {
            out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
        }
        _ => {
            write_resp_null_array(out);
        }
    }
    false
}

async fn execute_command(
    cmd: Command,
    router: &Router,
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    out: &mut Vec<u8>,
    asking: &mut bool,
    authenticated: &mut bool,
    auth_user: &mut String,
) -> bool {
    let cmd_name = get_cmd_name(&cmd);
    record_cmd_stat(cmd_name);
    let is_resp3 = client_registry
        .borrow()
        .get(&client_id)
        .map(|c| c.is_resp3)
        .unwrap_or(false);
    CURRENT_CLIENT_RESP3.set(is_resp3);
    if !IN_TX.get()
        && let Some(c) = client_registry.borrow_mut().get_mut(&client_id)
    {
        c.last_active = Instant::now();
        c.last_cmd = cmd_name.to_lowercase();
    }

    if !*authenticated
        && !matches!(
            cmd,
            Command::Auth { .. } | Command::Hello { .. } | Command::Quit
        )
    {
        out.extend_from_slice(b"-NOAUTH Authentication required.\r\n");
        return false;
    }

    if *authenticated {
        let acl = crate::acl::get_acl_for_port(router.port);
        let acl_guard = acl.read().unwrap();
        if let Some(user) = acl_guard.get_user(auth_user) {
            if !user.can_execute_command(cmd_name) {
                out.extend_from_slice(
                    format!(
                        "-NOPERM this user has no permissions to run the '{}' command\r\n",
                        cmd_name.to_lowercase()
                    )
                    .as_bytes(),
                );
                return false;
            }
            if let Some(key) = cmd_primary_key(&cmd)
                && !user.can_access_key(key.as_ref())
            {
                out.extend_from_slice(
                    b"-NOPERM this user has no permissions to access one of the keys used as arguments\r\n",
                );
                return false;
            }
        }
    }

    if let Command::Asking = cmd {
        *asking = true;
        out.extend_from_slice(b"+OK\r\n");
        return false;
    }

    let is_asking = *asking;
    *asking = false;

    if let Some(key) = cmd_primary_key(&cmd) {
        let slot = key_slot(key);
        let state = router.slot_states.borrow()[slot as usize].clone();
        match state {
            crate::shard::SlotState::Moved(target) => {
                out.extend_from_slice(format!("-MOVED {} {}\r\n", slot, target).as_bytes());
                return false;
            }
            crate::shard::SlotState::Importing(source) => {
                if !is_asking {
                    out.extend_from_slice(format!("-MOVED {} {}\r\n", slot, source).as_bytes());
                    return false;
                }
            }
            crate::shard::SlotState::Migrating(target) => {
                let key_exists = router.exists(key.clone()).await;
                if !key_exists {
                    out.extend_from_slice(format!("-ASK {} {}\r\n", slot, target).as_bytes());
                    return false;
                }
            }
            crate::shard::SlotState::Stable => {
                let hub = crate::cluster::get_cluster_hub(router.port);
                let my_slots = hub.my_slots.read().unwrap();
                let owns_slot = my_slots.iter().any(|&(s, e)| slot >= s && slot <= e);
                if !owns_slot {
                    let nodes = hub.nodes.read().unwrap();
                    if !nodes.is_empty()
                        && let Some(peer) = nodes.values().find(|n| {
                            n.flags.contains("master")
                                && !n.flags.contains("fail")
                                && n.slots.iter().any(|&(s, e)| slot >= s && slot <= e)
                        })
                    {
                        out.extend_from_slice(
                            format!("-MOVED {} {}:{}\r\n", slot, peer.ip, peer.port).as_bytes(),
                        );
                        return false;
                    }
                }
            }
        }
    }

    if crate::replication::get_replication_hub(router.port).is_slave()
        && crate::aof::command_to_resp(&cmd).is_some()
    {
        out.extend_from_slice(b"-READONLY You can't write against a read only replica.\r\n");
        return false;
    }

    match cmd {
        Command::Get(key) => {
            record_client_read(router.port, client_id, key.as_ref());
            let val = router.get(key).await;
            match val {
                Some(v) => {
                    out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                    out.extend_from_slice(&v);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Getex {
            key,
            expire_in,
            persist,
        } => {
            record_client_read(router.port, client_id, key.as_ref());
            let val = router.get(key.clone()).await;
            match val {
                Some(v) => {
                    if persist {
                        let _ = router.persist(key).await;
                    } else if let Some(exp) = expire_in {
                        let _ = router.expire(key, exp).await;
                    }
                    out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                    out.extend_from_slice(&v);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Set {
            key,
            value,
            expire_in,
            condition,
            get,
            keepttl,
            past_expired,
        } => {
            let target = target_shard(&key, router.num_shards);
            if target == router.shard_id {
                let mut db = router.local_db.borrow_mut();
                let current_val = match db.table.get(&key) {
                    Ok(val) => val,
                    Err(err) => {
                        if get
                            || matches!(
                                condition,
                                crate::resp::SetCondition::Ifeq(_)
                                    | crate::resp::SetCondition::Ifne(_)
                                    | crate::resp::SetCondition::Ifdeq(_)
                                    | crate::resp::SetCondition::Ifdne(_)
                            )
                        {
                            write_resp_err(out, err);
                            return false;
                        }
                        None
                    }
                };

                let exists = db.exists(&key);
                let condition_met = match &condition {
                    crate::resp::SetCondition::None => true,
                    crate::resp::SetCondition::Nx => !exists,
                    crate::resp::SetCondition::Xx => exists,
                    crate::resp::SetCondition::Ifeq(expected) => {
                        current_val.as_ref() == Some(expected)
                    }
                    crate::resp::SetCondition::Ifne(expected) => match &current_val {
                        None => true,
                        Some(val) => val != expected,
                    },
                    crate::resp::SetCondition::Ifdeq(expected_digest) => match &current_val {
                        None => false,
                        Some(val) => {
                            if expected_digest.len() != 16
                                || !expected_digest.iter().all(|b| b.is_ascii_hexdigit())
                            {
                                write_resp_err(
                                    out,
                                    "ERR digest must be exactly 16 hexadecimal characters",
                                );
                                return false;
                            }
                            let d = crate::table::compute_digest(val);
                            d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected_digest))
                        }
                    },
                    crate::resp::SetCondition::Ifdne(expected_digest) => match &current_val {
                        None => true,
                        Some(val) => {
                            if expected_digest.len() != 16
                                || !expected_digest.iter().all(|b| b.is_ascii_hexdigit())
                            {
                                write_resp_err(
                                    out,
                                    "ERR digest must be exactly 16 hexadecimal characters",
                                );
                                return false;
                            }
                            let d = crate::table::compute_digest(val);
                            !d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected_digest))
                        }
                    },
                };

                if !condition_met {
                    if get {
                        if let Some(v) = current_val {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                    return false;
                }

                if past_expired {
                    crate::table::inc_expired_keys();
                    if exists {
                        db.del(&key);
                        if let Some(aof) = &router.aof
                            && let Some(bytes) =
                                crate::aof::command_to_resp(&Command::Del(vec![key.clone()]))
                        {
                            aof.borrow_mut().append(&bytes);
                        }
                    }
                    if get {
                        if let Some(v) = current_val {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    return false;
                }

                notify_key_invalidation(router.port, key.as_ref(), client_id);
                db.set_extended(key.clone(), value.clone(), expire_in, keepttl);
                if let Some(aof) = &router.aof
                    && let Some(bytes) = crate::aof::command_to_resp(&Command::Set {
                        key: key.clone(),
                        value: value.clone(),
                        expire_in,
                        condition: condition.clone(),
                        get,
                        keepttl,
                        past_expired,
                    })
                {
                    aof.borrow_mut().append(&bytes);
                }
                if crate::replication::has_connected_replicas(router.port)
                    && let Some(bytes) = crate::aof::command_to_resp(&Command::Set {
                        key: key.clone(),
                        value: value.clone(),
                        expire_in,
                        condition,
                        get,
                        keepttl,
                        past_expired,
                    })
                {
                    crate::replication::propagate_bytes(router.port, &bytes);
                }

                if get {
                    if let Some(v) = current_val {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                } else {
                    out.extend_from_slice(b"+OK\r\n");
                }
                false
            } else {
                notify_key_invalidation(router.port, key.as_ref(), client_id);
                let resp = router
                    .execute_remote(
                        target,
                        Command::Set {
                            key,
                            value,
                            expire_in,
                            condition,
                            get,
                            keepttl,
                            past_expired,
                        },
                    )
                    .await;
                out.extend_from_slice(&resp);
                false
            }
        }
        Command::Mget(keys) => {
            for key in &keys {
                record_client_read(router.port, client_id, key.as_ref());
            }
            out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
            for key in keys {
                match router.get(key).await {
                    Some(v) => {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                    None => {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
            }
            false
        }
        Command::Mset(pairs) => {
            if crate::replication::has_connected_replicas(router.port)
                && let Some(bytes) = crate::aof::command_to_resp(&Command::Mset(pairs.clone()))
            {
                crate::replication::propagate_bytes(router.port, &bytes);
            }
            for (key, val) in pairs {
                notify_key_invalidation(router.port, key.as_ref(), client_id);
                router.set(key, val, None).await;
            }
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Msetex {
            pairs,
            condition,
            expiry,
        } => {
            let pass = match condition {
                MsetexCondition::None => true,
                MsetexCondition::Nx => {
                    let mut ok = true;
                    for (k, _) in &pairs {
                        if router.exists(k.clone()).await {
                            ok = false;
                            break;
                        }
                    }
                    ok
                }
                MsetexCondition::Xx => {
                    let mut ok = true;
                    for (k, _) in &pairs {
                        if !router.exists(k.clone()).await {
                            ok = false;
                            break;
                        }
                    }
                    ok
                }
            };

            if !pass {
                out.extend_from_slice(b":0\r\n");
                return false;
            }

            for (key, val) in pairs {
                notify_key_invalidation(router.port, key.as_ref(), client_id);
                let ttl = match expiry {
                    MsetexExpiry::None => None,
                    MsetexExpiry::KeepTtl => {
                        let ms = router.ttl(key.clone(), true).await;
                        if ms > 0 {
                            Some(std::time::Duration::from_millis(ms as u64))
                        } else {
                            None
                        }
                    }
                    MsetexExpiry::ExpireIn(d) => Some(d),
                };
                router.set(key, val, ttl).await;
            }
            match condition {
                MsetexCondition::None => {
                    out.extend_from_slice(b"+OK\r\n");
                }
                MsetexCondition::Nx | MsetexCondition::Xx => {
                    out.extend_from_slice(b":1\r\n");
                }
            }
            false
        }
        Command::Lcs {
            key1,
            key2,
            len_only,
            idx,
            min_match_len,
            with_match_len,
        } => {
            let val1 = match router.get(key1).await {
                Some(v) => v,
                None => Bytes::new(),
            };
            let val2 = match router.get(key2).await {
                Some(v) => v,
                None => Bytes::new(),
            };
            let s1 = val1.as_ref();
            let s2 = val2.as_ref();
            let m = s1.len();
            let n = s2.len();

            let mut dp = vec![vec![0u32; n + 1]; m + 1];
            for i in 1..=m {
                for j in 1..=n {
                    if s1[i - 1] == s2[j - 1] {
                        dp[i][j] = dp[i - 1][j - 1] + 1;
                    } else {
                        dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
                    }
                }
            }

            let lcs_len = dp[m][n] as usize;

            if len_only {
                out.extend_from_slice(format!(":{}\r\n", lcs_len).as_bytes());
                return false;
            }

            if idx {
                let mut matches = Vec::new();
                let mut i = m;
                let mut j = n;
                while i > 0 && j > 0 {
                    if s1[i - 1] == s2[j - 1] {
                        let end1 = i - 1;
                        let end2 = j - 1;
                        while i > 0 && j > 0 && s1[i - 1] == s2[j - 1] {
                            i -= 1;
                            j -= 1;
                        }
                        let start1 = i;
                        let start2 = j;
                        let match_len = end1 - start1 + 1;
                        if match_len >= min_match_len {
                            matches.push(((start1, end1), (start2, end2), match_len));
                        }
                    } else if dp[i - 1][j] >= dp[i][j - 1] {
                        i -= 1;
                    } else {
                        j -= 1;
                    }
                }

                out.extend_from_slice(b"*4\r\n$7\r\nmatches\r\n");
                out.extend_from_slice(format!("*{}\r\n", matches.len()).as_bytes());
                for ((s1_idx, e1_idx), (s2_idx, e2_idx), match_len) in matches {
                    if with_match_len {
                        out.extend_from_slice(
                            format!(
                                "*3\r\n*2\r\n:{}\r\n:{}\r\n*2\r\n:{}\r\n:{}\r\n:{}\r\n",
                                s1_idx, e1_idx, s2_idx, e2_idx, match_len
                            )
                            .as_bytes(),
                        );
                    } else {
                        out.extend_from_slice(
                            format!(
                                "*2\r\n*2\r\n:{}\r\n:{}\r\n*2\r\n:{}\r\n:{}\r\n",
                                s1_idx, e1_idx, s2_idx, e2_idx
                            )
                            .as_bytes(),
                        );
                    }
                }
                out.extend_from_slice(format!("$3\r\nlen\r\n:{}\r\n", lcs_len).as_bytes());
                return false;
            }

            let mut lcs_bytes = Vec::with_capacity(lcs_len);
            let mut i = m;
            let mut j = n;
            while i > 0 && j > 0 {
                if s1[i - 1] == s2[j - 1] {
                    lcs_bytes.push(s1[i - 1]);
                    i -= 1;
                    j -= 1;
                } else if dp[i - 1][j] >= dp[i][j - 1] {
                    i -= 1;
                } else {
                    j -= 1;
                }
            }
            lcs_bytes.reverse();
            out.extend_from_slice(format!("${}\r\n", lcs_bytes.len()).as_bytes());
            out.extend_from_slice(&lcs_bytes);
            out.extend_from_slice(b"\r\n");
            false
        }
        Command::Del(keys) => {
            let mut count = 0usize;
            for key in keys.clone() {
                notify_key_invalidation(router.port, key.as_ref(), client_id);
                if router.del(key.clone()).await {
                    count += 1;
                    crate::search::delete_document_hook(&String::from_utf8_lossy(&key));
                }
            }

            if count > 0
                && crate::replication::has_connected_replicas(router.port)
                && let Some(bytes) = crate::aof::command_to_resp(&Command::Del(keys))
            {
                crate::replication::propagate_bytes(router.port, &bytes);
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::Exists(keys) => {
            let mut count = 0usize;
            for key in keys {
                if router.exists(key).await {
                    count += 1;
                }
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::IncrBy(key, delta) => {
            match router.incr_by(key.clone(), delta).await {
                Ok(val) => {
                    if let Some(bytes) = crate::aof::command_to_resp(&Command::IncrBy(key, delta)) {
                        crate::replication::propagate_bytes(router.port, &bytes);
                    }
                    write_resp_integer(out, val);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Expire(key, duration) => {
            let res = router.expire(key.clone(), duration).await;
            if res {
                if let Some(bytes) = crate::aof::command_to_resp(&Command::Expire(key, duration)) {
                    crate::replication::propagate_bytes(router.port, &bytes);
                }
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Persist(key) => {
            let res = router.persist(key.clone()).await;
            if res {
                if let Some(bytes) = crate::aof::command_to_resp(&Command::Persist(key)) {
                    crate::replication::propagate_bytes(router.port, &bytes);
                }
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Ttl(key, in_millis) => {
            let res = router.ttl(key, in_millis).await;
            out.extend_from_slice(format!(":{}\r\n", res).as_bytes());
            false
        }
        Command::Ping(msg) => {
            match msg {
                Some(m) => {
                    out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                    out.extend_from_slice(&m);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"+PONG\r\n");
                }
            }
            false
        }
        Command::CommandDocs => {
            out.extend_from_slice(b"*0\r\n");
            false
        }
        Command::Info(section) => {
            let hub = crate::replication::get_replication_hub(router.port);
            let stats = crate::tiering::get_tier_stats(router.port);
            let max_mem = crate::tiering::get_max_memory(router.port);
            let used_mem = router.get_total_used_memory().await;
            let memory_str = crate::allocator::format_memory_info(
                used_mem,
                max_mem,
                stats.cooled_keys.load(std::sync::atomic::Ordering::Relaxed),
                stats.tiered_keys.load(std::sync::atomic::Ordering::Relaxed),
            );
            let storage_str = format!(
                "# Storage\r\ntier_enabled:1\r\nmaxmemory:{}\r\nmaxmemory_human:{}\r\nused_memory:{}\r\nused_memory_human:{}\r\ncooled_keys:{}\r\ntiered_keys:{}\r\ntiered_bytes:{}\r\nram_saved_bytes:{}\r\ndisk_reads:{}\r\ndisk_writes:{}\r\ndead_bytes:{}\r\ndecommit_count:{}\r\ncoalesced_reads:{}\r\nbin_pages:{}\r\ntotal_stashes:{}\r\ntotal_fetches:{}\r\ntotal_deletes:{}\r\nram_hits:{}\r\nram_misses:{}\r\nstreaming_reads:{}\r\noffload_threshold_pct:{}\r\nupload_threshold_pct:{}\r\n",
                max_mem,
                crate::tiering::format_bytes_human(max_mem),
                used_mem,
                crate::tiering::format_bytes_human(used_mem as u64),
                stats.cooled_keys.load(std::sync::atomic::Ordering::Relaxed),
                stats.tiered_keys.load(std::sync::atomic::Ordering::Relaxed),
                stats
                    .tiered_bytes
                    .load(std::sync::atomic::Ordering::Relaxed),
                stats
                    .ram_saved_bytes
                    .load(std::sync::atomic::Ordering::Relaxed),
                stats.disk_reads.load(std::sync::atomic::Ordering::Relaxed),
                stats.disk_writes.load(std::sync::atomic::Ordering::Relaxed),
                stats.dead_bytes.load(std::sync::atomic::Ordering::Relaxed),
                stats
                    .decommit_count
                    .load(std::sync::atomic::Ordering::Relaxed),
                stats
                    .coalesced_reads
                    .load(std::sync::atomic::Ordering::Relaxed),
                stats.bin_pages.load(std::sync::atomic::Ordering::Relaxed),
                stats
                    .total_stashes
                    .load(std::sync::atomic::Ordering::Relaxed),
                stats
                    .total_fetches
                    .load(std::sync::atomic::Ordering::Relaxed),
                stats
                    .total_deletes
                    .load(std::sync::atomic::Ordering::Relaxed),
                stats.ram_hits.load(std::sync::atomic::Ordering::Relaxed),
                stats.ram_misses.load(std::sync::atomic::Ordering::Relaxed),
                stats
                    .streaming_reads
                    .load(std::sync::atomic::Ordering::Relaxed),
                stats
                    .offload_threshold_pct
                    .load(std::sync::atomic::Ordering::Relaxed),
                stats
                    .upload_threshold_pct
                    .load(std::sync::atomic::Ordering::Relaxed),
            );
            let stats_str = format!(
                "# Stats\r\ntotal_connections_received:0\r\ntotal_commands_processed:0\r\ninstantaneous_ops_per_sec:0\r\ntotal_net_input_bytes:0\r\ntotal_net_output_bytes:0\r\ninstantaneous_input_kbps:0.00\r\ninstantaneous_output_kbps:0.00\r\nrejected_connections:0\r\nsync_full:0\r\nsync_partial_ok:0\r\nsync_partial_err:0\r\nexpired_keys:{}\r\nevicted_keys:0\r\nkeyspace_hits:0\r\nkeyspace_misses:0\r\npubsub_channels:0\r\npubsub_patterns:0\r\nlatest_fork_usec:0\r\n",
                crate::table::get_expired_keys()
            );
            let blocked_clients_count = {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                hub_arc.lock().unwrap().blocked_clients_count()
            };
            let clients_str = format!(
                "# Clients\r\nconnected_clients:{}\r\nblocked_clients:{}\r\ntracking_clients:0\r\n",
                client_registry.borrow().len(),
                blocked_clients_count
            );
            let persistence_str = format!(
                "# Persistence\r\nloading:0\r\nrdb_changes_since_last_save:{}\r\nrdb_bgsave_in_progress:0\r\nrdb_last_save_time:0\r\nrdb_last_bgsave_status:ok\r\n",
                DIRTY_CHANGES.load(std::sync::atomic::Ordering::Relaxed)
            );
            let cmdstat_str = {
                let mut s = String::from("# Commandstats\r\n");
                if let Ok(map) = CMD_STATS.read() {
                    let mut entries: Vec<_> = map.iter().collect();
                    entries.sort_by_key(|(k, _)| *k);
                    for (cmd, calls) in entries {
                        s.push_str(&format!(
                            "cmdstat_{}:calls={},usec=100,usec_per_call=100.00,rejected_calls=0,failed_calls=0\r\n",
                            cmd, calls
                        ));
                    }
                }
                s
            };
            let info_str = match section.as_deref() {
                Some(b"clients") | Some(b"CLIENTS") => clients_str,
                Some(b"persistence") | Some(b"PERSISTENCE") => persistence_str,
                Some(b"replication") | Some(b"REPLICATION") => hub.format_info_replication(),
                Some(b"storage") | Some(b"STORAGE") | Some(b"tiered") | Some(b"TIERED") => {
                    storage_str
                }
                Some(b"memory") | Some(b"MEMORY") => memory_str,
                Some(b"stats") | Some(b"STATS") => stats_str,
                Some(b"commandstats") | Some(b"COMMANDSTATS") => cmdstat_str,
                _ => {
                    format!(
                        "# Server\r\nrudis_version:0.1.0\r\narch:shared-nothing-io_uring\r\nshard_id:{}\r\nnum_shards:{}\r\n\
                         {}\
                         {}\
                         # Replication\r\n{}\
                         {}\
                         {}\
                         {}\
                         {}",
                        router.shard_id,
                        router.num_shards,
                        clients_str,
                        persistence_str,
                        hub.format_info_replication(),
                        memory_str,
                        stats_str,
                        storage_str,
                        cmdstat_str
                    )
                }
            };
            out.extend_from_slice(format!("${}\r\n", info_str.len()).as_bytes());
            out.extend_from_slice(info_str.as_bytes());
            out.extend_from_slice(b"\r\n");
            false
        }
        Command::Role => {
            let hub = crate::replication::get_replication_hub(router.port);
            out.extend_from_slice(&hub.format_role_resp());
            false
        }
        Command::Replconf(args) => {
            if args.is_empty() {
                out.extend_from_slice(b"-ERR wrong number of arguments for 'replconf' command\r\n");
            } else if args[0].eq_ignore_ascii_case(b"listening-port") {
                if args.len() >= 2
                    && let Ok(s) = std::str::from_utf8(&args[1])
                    && let Ok(rport) = s.parse::<u16>()
                {
                    let hub = crate::replication::get_replication_hub(router.port);
                    hub.set_replica_port(client_id, rport);
                }
                out.extend_from_slice(b"+OK\r\n");
            } else if args[0].eq_ignore_ascii_case(b"capa") {
                out.extend_from_slice(b"+OK\r\n");
            } else if args[0].eq_ignore_ascii_case(b"ack") {
                if args.len() >= 2
                    && let Ok(s) = std::str::from_utf8(&args[1])
                    && let Ok(off) = s.parse::<u64>()
                {
                    let hub = crate::replication::get_replication_hub(router.port);
                    hub.update_replica_ack(client_id, off);
                }
            } else if args[0].eq_ignore_ascii_case(b"getack") {
                let hub = crate::replication::get_replication_hub(router.port);
                let off = hub
                    .master_repl_offset
                    .load(std::sync::atomic::Ordering::SeqCst)
                    .to_string();
                out.extend_from_slice(
                    format!(
                        "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                        off.len(),
                        off
                    )
                    .as_bytes(),
                );
            } else {
                out.extend_from_slice(b"+OK\r\n");
            }
            false
        }
        Command::Replicaof { host, port } => {
            let hub = crate::replication::get_replication_hub(router.port);
            if host.eq_ignore_ascii_case(b"no") && port.eq_ignore_ascii_case(b"one") {
                hub.stop_sync();
                hub.make_master();
                out.extend_from_slice(b"+OK\r\n");
            } else {
                let host_str = String::from_utf8_lossy(&host).to_string();
                let port_str = String::from_utf8_lossy(&port);
                if let Ok(mport) = port_str.parse::<u16>() {
                    crate::replication::start_replica_sync(
                        router.port,
                        host_str,
                        mport,
                        router.clone(),
                    );
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    out.extend_from_slice(b"-ERR invalid port\r\n");
                }
            }
            false
        }
        Command::Psync { .. } => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Tier(sub) => {
            match sub {
                crate::resp::TierSubcommand::Spill(key) => {
                    let ok = router.spill_key(&key).await;
                    out.extend_from_slice(if ok { b":1\r\n" } else { b":0\r\n" });
                }
                crate::resp::TierSubcommand::Cool(key) => {
                    let ok = router.cool_key(&key).await;
                    out.extend_from_slice(if ok { b":1\r\n" } else { b":0\r\n" });
                }
                crate::resp::TierSubcommand::Decommit(key) => {
                    let count = router.decommit(key.as_deref()).await;
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                crate::resp::TierSubcommand::Load(key) => {
                    let ok = router.ensure_loaded(&key).await;
                    out.extend_from_slice(if ok { b":1\r\n" } else { b":0\r\n" });
                }
                crate::resp::TierSubcommand::SpillAll => {
                    let mut total = 0;
                    for s in 0..router.num_shards {
                        if s == router.shard_id {
                            total += router.spill_all().await;
                        } else {
                            let (tx, rx) = flume::bounded(1);
                            if router.senders[s]
                                .send(crate::shard::ShardMessage::TierSpillAll { responder: tx })
                                .is_ok()
                            {
                                total += rx.recv_async().await.unwrap_or(0);
                            }
                        }
                    }
                    out.extend_from_slice(format!(":{}\r\n", total).as_bytes());
                }
                crate::resp::TierSubcommand::Gc => {
                    let reclaimed = router.gc_all().await;
                    out.extend_from_slice(format!(":{}\r\n", reclaimed).as_bytes());
                }
                crate::resp::TierSubcommand::Snapshot(dir) => {
                    let start = std::time::Instant::now();
                    match router.tier_snapshot_all(dir).await {
                        Ok((is_reflink, total_bytes, shards)) => {
                            let ms = start.elapsed().as_millis();
                            let msg = format!(
                                "+OK snapshot created in {}ms, shards:{}, bytes:{}, reflink:{}\r\n",
                                ms, shards, total_bytes, is_reflink
                            );
                            out.extend_from_slice(msg.as_bytes());
                        }
                        Err(e) => {
                            out.extend_from_slice(
                                format!("-ERR snapshot failed: {}\r\n", e).as_bytes(),
                            );
                        }
                    }
                }

                crate::resp::TierSubcommand::Info => {
                    let stats = crate::tiering::get_tier_stats(router.port);
                    let max_mem = crate::tiering::get_max_memory(router.port);
                    let used_mem = router.get_total_used_memory().await;
                    let info = format!(
                        "# Tiered Storage (io_uring NVMe)\r\ntier_enabled:1\r\nmaxmemory:{}\r\nmaxmemory_human:{}\r\nused_memory:{}\r\nused_memory_human:{}\r\ncooled_keys:{}\r\ntiered_keys:{}\r\ntiered_bytes:{}\r\nram_saved_bytes:{}\r\ndisk_reads:{}\r\ndisk_writes:{}\r\ndead_bytes:{}\r\ngc_reclaimed_bytes:{}\r\ngc_cycles:{}\r\ndecommit_count:{}\r\ncoalesced_reads:{}\r\nbin_pages:{}\r\ntotal_stashes:{}\r\ntotal_fetches:{}\r\ntotal_deletes:{}\r\nram_hits:{}\r\nram_misses:{}\r\nstreaming_reads:{}\r\noffload_threshold_pct:{}\r\nupload_threshold_pct:{}\r\n",
                        max_mem,
                        crate::tiering::format_bytes_human(max_mem),
                        used_mem,
                        crate::tiering::format_bytes_human(used_mem as u64),
                        stats.cooled_keys.load(std::sync::atomic::Ordering::Relaxed),
                        stats.tiered_keys.load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .tiered_bytes
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .ram_saved_bytes
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats.disk_reads.load(std::sync::atomic::Ordering::Relaxed),
                        stats.disk_writes.load(std::sync::atomic::Ordering::Relaxed),
                        stats.dead_bytes.load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .gc_reclaimed_bytes
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats.gc_cycles.load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .decommit_count
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .coalesced_reads
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats.bin_pages.load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .total_stashes
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .total_fetches
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .total_deletes
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats.ram_hits.load(std::sync::atomic::Ordering::Relaxed),
                        stats.ram_misses.load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .streaming_reads
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .offload_threshold_pct
                            .load(std::sync::atomic::Ordering::Relaxed),
                        stats
                            .upload_threshold_pct
                            .load(std::sync::atomic::Ordering::Relaxed),
                    );
                    out.extend_from_slice(format!("${}\r\n{}\r\n", info.len(), info).as_bytes());
                }
            }
            false
        }
        Command::ConfigGet(param) => {
            let p_str = String::from_utf8_lossy(&param).to_lowercase();
            if p_str == "maxmemory" {
                let max_mem = crate::tiering::get_max_memory(router.port).to_string();
                let resp = format!(
                    "*2\r\n$9\r\nmaxmemory\r\n${}\r\n{}\r\n",
                    max_mem.len(),
                    max_mem
                );
                out.extend_from_slice(resp.as_bytes());
            } else if p_str == "tiered-offload-threshold" {
                let val = crate::tiering::get_offload_threshold_pct(router.port).to_string();
                let resp = format!(
                    "*2\r\n$24\r\ntiered-offload-threshold\r\n${}\r\n{}\r\n",
                    val.len(),
                    val
                );
                out.extend_from_slice(resp.as_bytes());
            } else if p_str == "tiered-upload-threshold" {
                let val = crate::tiering::get_upload_threshold_pct(router.port).to_string();
                let resp = format!(
                    "*2\r\n$23\r\ntiered-upload-threshold\r\n${}\r\n{}\r\n",
                    val.len(),
                    val
                );
                out.extend_from_slice(resp.as_bytes());
            } else if p_str == "hash-max-listpack-entries" || p_str == "hash-max-ziplist-entries" {
                let val = HASH_MAX_ENTRIES
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .to_string();
                let resp = format!(
                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    p_str.len(),
                    p_str,
                    val.len(),
                    val
                );
                out.extend_from_slice(resp.as_bytes());
            } else if p_str == "hash-max-listpack-value" || p_str == "hash-max-ziplist-value" {
                let val = HASH_MAX_VALUE
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .to_string();
                let resp = format!(
                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    p_str.len(),
                    p_str,
                    val.len(),
                    val
                );
                out.extend_from_slice(resp.as_bytes());
            } else if p_str == "*" {
                let max_mem = crate::tiering::get_max_memory(router.port).to_string();
                let offload = crate::tiering::get_offload_threshold_pct(router.port).to_string();
                let upload = crate::tiering::get_upload_threshold_pct(router.port).to_string();
                let resp = format!(
                    "*6\r\n$9\r\nmaxmemory\r\n${}\r\n{}\r\n$24\r\ntiered-offload-threshold\r\n${}\r\n{}\r\n$23\r\ntiered-upload-threshold\r\n${}\r\n{}\r\n",
                    max_mem.len(),
                    max_mem,
                    offload.len(),
                    offload,
                    upload.len(),
                    upload
                );
                out.extend_from_slice(resp.as_bytes());
            } else {
                out.extend_from_slice(b"*0\r\n");
            }
            false
        }
        Command::ConfigSet(param, val) => {
            let p_str = String::from_utf8_lossy(&param).to_lowercase();
            let val_str = String::from_utf8_lossy(&val);
            if p_str == "maxmemory" {
                if let Some(bytes) = crate::tiering::parse_memory_bytes(&val_str) {
                    crate::tiering::set_max_memory(router.port, bytes);
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    out.extend_from_slice(b"-ERR Invalid argument for CONFIG SET maxmemory\r\n");
                }
            } else if p_str == "tiered-offload-threshold" {
                if let Ok(pct) = val_str.parse::<u64>() {
                    crate::tiering::set_offload_threshold_pct(router.port, pct);
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    out.extend_from_slice(
                        b"-ERR Invalid argument for CONFIG SET tiered-offload-threshold\r\n",
                    );
                }
            } else if p_str == "tiered-upload-threshold" {
                if let Ok(pct) = val_str.parse::<u64>() {
                    crate::tiering::set_upload_threshold_pct(router.port, pct);
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    out.extend_from_slice(
                        b"-ERR Invalid argument for CONFIG SET tiered-upload-threshold\r\n",
                    );
                }
            } else if p_str == "hash-max-listpack-entries" || p_str == "hash-max-ziplist-entries" {
                if let Ok(n) = val_str.parse::<usize>() {
                    HASH_MAX_ENTRIES.store(n, std::sync::atomic::Ordering::Relaxed);
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    out.extend_from_slice(b"-ERR Invalid argument for CONFIG SET\r\n");
                }
            } else if p_str == "hash-max-listpack-value" || p_str == "hash-max-ziplist-value" {
                if let Ok(n) = val_str.parse::<usize>() {
                    HASH_MAX_VALUE.store(n, std::sync::atomic::Ordering::Relaxed);
                    out.extend_from_slice(b"+OK\r\n");
                } else {
                    out.extend_from_slice(b"-ERR Invalid argument for CONFIG SET\r\n");
                }
            } else if p_str == "resetstat" {
                if let Ok(mut map) = CMD_STATS.write() {
                    map.clear();
                }
                out.extend_from_slice(b"+OK\r\n");
            } else {
                out.extend_from_slice(b"+OK\r\n");
            }
            false
        }
        Command::Cluster(sub) => {
            match sub {
                ClusterSubcommand::KeySlot(key) => {
                    let slot = key_slot(&key);
                    out.extend_from_slice(format!(":{}\r\n", slot).as_bytes());
                }
                ClusterSubcommand::CountKeysInSlot(slot) => {
                    let count = router.count_keys_in_slot(slot).await;
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                ClusterSubcommand::GetKeysInSlot(slot, count) => {
                    let keys = router.get_keys_in_slot(slot, count).await;
                    out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
                    for k in keys {
                        out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                ClusterSubcommand::Slots => {
                    let mut slots_bytes = Vec::new();
                    router.cluster_slots(&mut slots_bytes);
                    out.extend_from_slice(&slots_bytes);
                }
                ClusterSubcommand::Shards => {
                    let mut shards_bytes = Vec::new();
                    router.cluster_shards(&mut shards_bytes);
                    out.extend_from_slice(&shards_bytes);
                }
                ClusterSubcommand::Links => {
                    let mut links_bytes = Vec::new();
                    router.cluster_links(&mut links_bytes);
                    out.extend_from_slice(&links_bytes);
                }
                ClusterSubcommand::AddSlots(slots) => match router.cluster_addslots(&slots) {
                    Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
                },
                ClusterSubcommand::DelSlots(slots) => match router.cluster_delslots(&slots) {
                    Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
                },
                ClusterSubcommand::AddSlotsRange(ranges) => {
                    match router.cluster_addslotsrange(&ranges) {
                        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
                    }
                }
                ClusterSubcommand::DelSlotsRange(ranges) => {
                    match router.cluster_delslotsrange(&ranges) {
                        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
                    }
                }
                ClusterSubcommand::Nodes => {
                    let nodes = router.cluster_nodes();
                    out.extend_from_slice(format!("${}\r\n", nodes.len()).as_bytes());
                    out.extend_from_slice(nodes.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClusterSubcommand::Info => {
                    let info = router.cluster_info();
                    out.extend_from_slice(format!("${}\r\n", info.len()).as_bytes());
                    out.extend_from_slice(info.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClusterSubcommand::MyId => {
                    let id = router.my_id();
                    out.extend_from_slice(format!("${}\r\n", id.len()).as_bytes());
                    out.extend_from_slice(id.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClusterSubcommand::Meet { ip, port } => match router.cluster_meet(ip, port) {
                    Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => {
                        let resp = format!("-ERR {}\r\n", e);
                        out.extend_from_slice(resp.as_bytes());
                    }
                },
                ClusterSubcommand::SetSlot(slot, sub_cmd) => match sub_cmd {
                    SetSlotSubcommand::Migrating(node) => {
                        router.set_slot_state(slot, crate::shard::SlotState::Migrating(node));
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    SetSlotSubcommand::Importing(node) => {
                        router.set_slot_state(slot, crate::shard::SlotState::Importing(node));
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    SetSlotSubcommand::Stable => {
                        router.set_slot_state(slot, crate::shard::SlotState::Stable);
                        out.extend_from_slice(b"+OK\r\n");
                    }
                    SetSlotSubcommand::Node(node) => {
                        let is_myself = node == "myself"
                            || (0..router.num_shards).any(|s| node == format!("{:040x}", s + 1));
                        if is_myself {
                            let shard = (0..router.num_shards)
                                .find(|&s| node == format!("{:040x}", s + 1))
                                .unwrap_or_else(|| {
                                    crate::router::slot_to_shard(slot, router.num_shards)
                                });
                            router.set_slot_owner(slot, shard);
                        } else {
                            router.set_slot_state(slot, crate::shard::SlotState::Moved(node));
                        }
                        out.extend_from_slice(b"+OK\r\n");
                    }
                },
                ClusterSubcommand::MigrateSlot { slot, host, port } => {
                    let target_addr = format!("{}:{}", host, port);
                    // 1. Mark local slot as Migrating
                    router.set_slot_state(
                        slot,
                        crate::shard::SlotState::Migrating(target_addr.clone()),
                    );

                    // 2. Notify remote node: CLUSTER SETSLOT <slot> IMPORTING <my_id>
                    let my_id = router.my_id();
                    let target_sock = match std::net::ToSocketAddrs::to_socket_addrs(&target_addr) {
                        Ok(mut iter) => iter.next(),
                        Err(_) => None,
                    };
                    if let Some(sock_addr) = target_sock
                        && let Ok(mut stream) = monoio::net::TcpStream::connect(sock_addr).await
                    {
                        let slot_str = slot.to_string();
                        let setslot_import = format!(
                            "*4\r\n$7\r\nCLUSTER\r\n$7\r\nSETSLOT\r\n${}\r\n{}\r\n$9\r\nIMPORTING\r\n${}\r\n{}\r\n",
                            slot_str.len(),
                            slot_str,
                            my_id.len(),
                            my_id
                        );
                        let (write_res, _) = stream.write_all(setslot_import.into_bytes()).await;
                        if write_res.is_ok() {
                            let buf = vec![0u8; 64];
                            let _ = stream.read(buf).await;
                        }
                    }

                    // 3. Migrate keys belonging to this slot in batches
                    loop {
                        let keys = router.get_keys_in_slot(slot, 100).await;
                        if keys.is_empty() {
                            break;
                        }
                        if let Err(e) =
                            migrate_keys_to_node(router, &keys, &host, port, false).await
                        {
                            out.extend_from_slice(
                                format!("-ERR migration failed: {}\r\n", e).as_bytes(),
                            );
                            return false;
                        }
                    }

                    // 4. Notify remote node to take final ownership: CLUSTER SETSLOT <slot> NODE myself
                    if let Some(sock_addr) = target_sock
                        && let Ok(mut stream) = monoio::net::TcpStream::connect(sock_addr).await
                    {
                        let slot_str = slot.to_string();
                        let setslot_node = format!(
                            "*4\r\n$7\r\nCLUSTER\r\n$7\r\nSETSLOT\r\n${}\r\n{}\r\n$4\r\nNODE\r\n$6\r\nmyself\r\n",
                            slot_str.len(),
                            slot_str
                        );
                        let (write_res, _) = stream.write_all(setslot_node.into_bytes()).await;
                        if write_res.is_ok() {
                            let buf = vec![0u8; 64];
                            let _ = stream.read(buf).await;
                        }
                    }

                    // 5. Update local state to Moved
                    router.set_slot_state(slot, crate::shard::SlotState::Moved(target_addr));
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClusterSubcommand::Rebalance { host, port, slots } => {
                    let num_slots = slots.unwrap_or(1);
                    let mut migrated_count = 0;
                    for s in 0..16384u16 {
                        if migrated_count >= num_slots {
                            break;
                        }
                        let is_stable = matches!(
                            router.slot_states.borrow()[s as usize],
                            crate::shard::SlotState::Stable
                        );
                        if is_stable {
                            let keys = router.get_keys_in_slot(s, 100).await;
                            if !keys.is_empty() {
                                let _ =
                                    migrate_keys_to_node(router, &keys, &host, port, false).await;
                            }
                            let target_addr = format!("{}:{}", host, port);
                            router.set_slot_state(s, crate::shard::SlotState::Moved(target_addr));
                            migrated_count += 1;
                        }
                    }
                    out.extend_from_slice(format!(":{}\r\n", migrated_count).as_bytes());
                }
                ClusterSubcommand::Failover { force } => match router.cluster_failover(force) {
                    Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => {
                        let resp = format!("-ERR {}\r\n", e);
                        out.extend_from_slice(resp.as_bytes());
                    }
                },
                ClusterSubcommand::Reset { hard } => match router.cluster_reset(hard) {
                    Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => {
                        let resp = format!("-ERR {}\r\n", e);
                        out.extend_from_slice(resp.as_bytes());
                    }
                },
                ClusterSubcommand::Forget(node_id) => match router.cluster_forget(&node_id) {
                    Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => {
                        let resp = format!("-ERR {}\r\n", e);
                        out.extend_from_slice(resp.as_bytes());
                    }
                },
                ClusterSubcommand::Replicate(node_id) => match router.cluster_replicate(&node_id) {
                    Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                    Err(e) => {
                        let resp = format!("-ERR {}\r\n", e);
                        out.extend_from_slice(resp.as_bytes());
                    }
                },
                ClusterSubcommand::SaveConfig => {
                    out.extend_from_slice(b"+OK\r\n");
                }
            }
            false
        }
        Command::Client(sub) => {
            match sub {
                ClientSubcommand::List(ref filter_ids) => {
                    let list = router.client_list(client_registry, filter_ids).await;
                    out.extend_from_slice(format!("${}\r\n", list.len()).as_bytes());
                    out.extend_from_slice(list.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
                ClientSubcommand::Info => {
                    let reg = client_registry.borrow();
                    if let Some(c) = reg.get(&client_id) {
                        let now = std::time::Instant::now();
                        let age = now.duration_since(c.connected_at).as_secs();
                        let idle = now.duration_since(c.last_active).as_secs();
                        let is_blocked = crate::block::get_block_hub_for_port(router.port)
                            .lock()
                            .unwrap()
                            .is_blocked(c.id);
                        let flags = if is_blocked { "b" } else { "N" };
                        let info = format!(
                            "id={} addr={} laddr=127.0.0.1:{} fd=8 name={} age={} idle={} flags={} db=0 sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=20448 argv-mem=10 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 omem-shared=0 omem-unshared=0 tot-mem=22306 events=r cmd={} user=default redir=-1 resp=2 lib-name= lib-ver= io-thread=0 tot-net-in=0 tot-net-out=0 tot-cmds=0 read-events=0 avg-pipeline-len-sum=0 avg-pipeline-len-cnt=0\n",
                            c.id,
                            c.addr,
                            router.port,
                            c.name.as_deref().unwrap_or(""),
                            age,
                            idle,
                            flags,
                            c.last_cmd.to_lowercase()
                        );
                        out.extend_from_slice(format!("${}\r\n", info.len()).as_bytes());
                        out.extend_from_slice(info.as_bytes());
                        out.extend_from_slice(b"\r\n");
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
                ClientSubcommand::SetName(name) => {
                    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                        c.name = Some(name);
                    }
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClientSubcommand::GetName => {
                    let name = client_registry
                        .borrow()
                        .get(&client_id)
                        .and_then(|c| c.name.clone());
                    match name {
                        Some(n) => {
                            out.extend_from_slice(format!("${}\r\n", n.len()).as_bytes());
                            out.extend_from_slice(n.as_bytes());
                            out.extend_from_slice(b"\r\n");
                        }
                        None => {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    }
                }
                ClientSubcommand::Id => {
                    out.extend_from_slice(format!(":{}\r\n", client_id).as_bytes());
                }
                ClientSubcommand::Tracking {
                    enabled,
                    bcast,
                    prefixes,
                } => {
                    if enabled {
                        let reg = client_registry.borrow();
                        if let Some(c) = reg.get(&client_id)
                            && let Some(tx) = &c.track_tx
                        {
                            register_client_tracking(
                                router.port,
                                client_id,
                                bcast,
                                prefixes,
                                tx.clone(),
                                c.is_resp3,
                            );
                        }
                    } else {
                        unregister_client_tracking(router.port, client_id);
                    }
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClientSubcommand::Caching(_) => {
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClientSubcommand::Kill(_) => {
                    out.extend_from_slice(b"+OK\r\n");
                }
                ClientSubcommand::Unblock {
                    client_id: target_id,
                    unblock_type,
                } => {
                    let hub_arc = crate::block::get_block_hub_for_port(router.port);
                    let mut hub = hub_arc.lock().unwrap();
                    let unblocked = hub.unblock_client(target_id, unblock_type);
                    if unblocked {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                ClientSubcommand::Pause(_)
                | ClientSubcommand::Unpause
                | ClientSubcommand::NoTouch(_) => {
                    out.extend_from_slice(b"+OK\r\n");
                }
            }
            false
        }
        Command::Hset { .. }
        | Command::Hsetnx { .. }
        | Command::Hmset { .. }
        | Command::Hget { .. }
        | Command::Hmget { .. }
        | Command::Hdel { .. }
        | Command::Hexists { .. }
        | Command::Hlen(_)
        | Command::Hgetall(_)
        | Command::Hkeys(_)
        | Command::Hvals(_)
        | Command::Hstrlen { .. }
        | Command::Hgetdel { .. }
        | Command::Lpush { .. }
        | Command::Rpush { .. }
        | Command::Lpushx { .. }
        | Command::Rpushx { .. }
        | Command::Lpop { .. }
        | Command::Rpop { .. }
        | Command::Lrange { .. }
        | Command::Llen(_)
        | Command::Lindex { .. }
        | Command::Sadd { .. }
        | Command::Srem { .. }
        | Command::Smembers(_)
        | Command::Sismember { .. }
        | Command::Scard(_)
        | Command::Spop { .. }
        | Command::Zadd { .. }
        | Command::Zrem { .. }
        | Command::Zscore { .. }
        | Command::Zcard(_)
        | Command::Zrank { .. }
        | Command::Zrevrank { .. }
        | Command::Zcount { .. }
        | Command::Zincrby { .. }
        | Command::Zrange { .. }
        | Command::Zrangestore { .. }
        | Command::Zpopmin { .. }
        | Command::Zpopmax { .. }
        | Command::Type(_)
        | Command::Setnx { .. }
        | Command::Getset { .. }
        | Command::Getdel(_)
        | Command::Append { .. }
        | Command::Strlen(_)
        | Command::Setbit { .. }
        | Command::Getbit { .. }
        | Command::Bitcount { .. }
        | Command::Bitpos { .. }
        | Command::Pfadd { .. }
        | Command::Dump(_)
        | Command::Restore { .. }
        | Command::Xadd { .. }
        | Command::Xlen(_)
        | Command::Xrange { .. }
        | Command::Xrevrange { .. }
        | Command::Xdel { .. }
        | Command::Xtrim { .. }
        | Command::XgroupCreate { .. }
        | Command::XgroupDestroy { .. }
        | Command::XgroupCreateConsumer { .. }
        | Command::XgroupDelConsumer { .. }
        | Command::Xack { .. }
        | Command::Xpending { .. }
        | Command::Hincrby { .. }
        | Command::Hincrbyfloat { .. }
        | Command::Hrandfield { .. }
        | Command::Hscan { .. }
        | Command::Ltrim { .. }
        | Command::Lset { .. }
        | Command::Lrem { .. }
        | Command::Lpos { .. }
        | Command::Linsert { .. }
        | Command::Smismember { .. }
        | Command::Srandmember { .. }
        | Command::Sscan { .. }
        | Command::Zmscore { .. }
        | Command::Zlexcount { .. }
        | Command::Zrandmember { .. }
        | Command::Zremrangebyrank { .. }
        | Command::Zremrangebyscore { .. }
        | Command::Zremrangebylex { .. }
        | Command::Zscan { .. }
        | Command::Incrbyfloat { .. }
        | Command::Setrange { .. }
        | Command::Getrange { .. }
        | Command::JsonSet { .. }
        | Command::JsonGet { .. }
        | Command::JsonDel { .. }
        | Command::JsonType { .. }
        | Command::JsonNumIncrBy { .. }
        | Command::JsonNumMultBy { .. }
        | Command::JsonStrAppend { .. }
        | Command::JsonStrLen { .. }
        | Command::JsonArrAppend { .. }
        | Command::JsonArrLen { .. }
        | Command::JsonArrPop { .. }
        | Command::JsonObjKeys { .. }
        | Command::JsonObjLen { .. }
        | Command::JsonToggle { .. }
        | Command::JsonClear { .. }
        | Command::Geoadd { .. }
        | Command::Geodist { .. }
        | Command::Geopos { .. }
        | Command::Geohash { .. }
        | Command::Georadius { .. }
        | Command::Georadiusbymember { .. }
        | Command::Geosearch { .. }
        | Command::BfReserve { .. }
        | Command::BfAdd { .. }
        | Command::BfMadd { .. }
        | Command::BfExists { .. }
        | Command::BfMexists { .. }
        | Command::BfInfo(_)
        | Command::CfReserve { .. }
        | Command::CfAdd { .. }
        | Command::CfAddnx { .. }
        | Command::CfExists { .. }
        | Command::CfDel { .. }
        | Command::CfInfo(_)
        | Command::CmsInitbydim { .. }
        | Command::CmsInitbyprob { .. }
        | Command::CmsIncrby { .. }
        | Command::CmsQuery { .. }
        | Command::CmsInfo(_)
        | Command::TopkReserve { .. }
        | Command::TopkAdd { .. }
        | Command::TopkQuery { .. }
        | Command::TopkList(_)
        | Command::TopkInfo(_)
        | Command::Digest(_)
        | Command::Delex { .. }
        | Command::CrdtSet { .. }
        | Command::CrdtGet(_)
        | Command::CrdtDel(_)
        | Command::CrdtIncrby { .. }
        | Command::CrdtSadd { .. }
        | Command::CrdtSmembers(_)
        | Command::CrdtSrem { .. } => {
            if let Some(target) = target_shard_of_cmd(&cmd, router.num_shards) {
                if target == router.shard_id {
                    execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        out,
                        router.aof.as_deref(),
                    );
                } else {
                    let res = router.execute_remote(target, cmd).await;
                    out.extend_from_slice(&res);
                }
            }
            false
        }
        Command::JsonMget { keys, path } => {
            out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
            for k in keys {
                let single_cmd = Command::JsonGet {
                    key: k.clone(),
                    paths: vec![path.clone()],
                };
                if let Some(target) = target_shard_of_cmd(&single_cmd, router.num_shards) {
                    if target == router.shard_id {
                        let mut tmp = Vec::new();
                        execute_local_command(
                            &single_cmd,
                            &mut router.local_db.borrow_mut(),
                            &mut tmp,
                            None,
                        );
                        out.extend_from_slice(&tmp);
                    } else {
                        let res = router.execute_remote(target, single_cmd).await;
                        out.extend_from_slice(&res);
                    }
                } else {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }

        Command::Xread {
            ref keys, block_ms, ..
        }
        | Command::Xreadgroup {
            ref keys, block_ms, ..
        } => {
            if keys.is_empty() {
                out.extend_from_slice(b"$-1\r\n");
                return false;
            }
            let first_target = router.target_shard(&keys[0]);
            for k in &keys[1..] {
                if router.target_shard(k) != first_target {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                    return false;
                }
            }

            let start_len = out.len();
            if first_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_target, cmd.clone()).await;
                out.extend_from_slice(&res);
            }

            let produced_empty = &out[start_len..] == b"$-1\r\n" || &out[start_len..] == b"*0\r\n";
            if let Some(wait_ms) = block_ms
                && produced_empty
            {
                out.truncate(start_len);
                let (tx, rx) = flume::bounded(1);
                {
                    let hub_arc = crate::block::get_block_hub_for_port(router.port);
                    let mut hub = hub_arc.lock().unwrap();
                    for k in keys {
                        hub.register_stream_waiter(k.clone(), tx.clone());
                    }
                }
                let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
                let (wait_res, client_disconnected) =
                    wait_for_stream_result(&rx, wait_ms, raw_fd).await;
                if client_disconnected {
                    return true;
                }
                if wait_res {
                    let mut unblocked_cmd = cmd.clone();
                    match &mut unblocked_cmd {
                        Command::Xread { block_ms: b, .. }
                        | Command::Xreadgroup { block_ms: b, .. } => {
                            *b = None;
                        }
                        _ => {}
                    }
                    if first_target == router.shard_id {
                        execute_local_command(
                            &unblocked_cmd,
                            &mut router.local_db.borrow_mut(),
                            out,
                            router.aof.as_deref(),
                        );
                    } else {
                        let res = router.execute_remote(first_target, unblocked_cmd).await;
                        out.extend_from_slice(&res);
                    }
                } else {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Auth { username, password } => {
            let uname = username.as_deref().unwrap_or("default");
            let pass = password.as_str();
            let acl = crate::acl::get_acl_for_port(router.port);
            let acl_guard = acl.read().unwrap();
            if let Ok(authed_user) = acl_guard.check_auth(Some(uname), pass) {
                *authenticated = true;
                *auth_user = authed_user;
                out.extend_from_slice(b"+OK\r\n");
            } else {
                out.extend_from_slice(
                    b"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
                );
            }
            false
        }
        Command::Acl(subcmd) => {
            let acl = crate::acl::get_acl_for_port(router.port);
            match subcmd {
                crate::resp::AclSubcommand::WhoAmI => {
                    out.extend_from_slice(
                        format!("${}\r\n{}\r\n", auth_user.len(), auth_user).as_bytes(),
                    );
                }
                crate::resp::AclSubcommand::Users => {
                    let users = acl.read().unwrap().users();
                    out.extend_from_slice(format!("*{}\r\n", users.len()).as_bytes());
                    for u in users {
                        out.extend_from_slice(format!("${}\r\n{}\r\n", u.len(), u).as_bytes());
                    }
                }
                crate::resp::AclSubcommand::List => {
                    let list = acl.read().unwrap().list();
                    out.extend_from_slice(format!("*{}\r\n", list.len()).as_bytes());
                    for line in list {
                        out.extend_from_slice(
                            format!("${}\r\n{}\r\n", line.len(), line).as_bytes(),
                        );
                    }
                }
                crate::resp::AclSubcommand::GetUser(username) => {
                    if let Some(user) = acl.read().unwrap().get_user(&username) {
                        out.extend_from_slice(b"*8\r\n");
                        out.extend_from_slice(b"$5\r\nflags\r\n");
                        let flags = user.flags();
                        out.extend_from_slice(format!("*{}\r\n", flags.len()).as_bytes());
                        for f in flags {
                            out.extend_from_slice(format!("${}\r\n{}\r\n", f.len(), f).as_bytes());
                        }
                        out.extend_from_slice(b"$9\r\npasswords\r\n");
                        out.extend_from_slice(format!("*{}\r\n", user.passwords.len()).as_bytes());
                        for p in &user.passwords {
                            out.extend_from_slice(format!("${}\r\n{}\r\n", p.len(), p).as_bytes());
                        }
                        out.extend_from_slice(b"$8\r\ncommands\r\n");
                        let cmd_str = if user.all_commands { "+@all" } else { "-@all" };
                        out.extend_from_slice(
                            format!("${}\r\n{}\r\n", cmd_str.len(), cmd_str).as_bytes(),
                        );
                        out.extend_from_slice(b"$4\r\nkeys\r\n");
                        let key_str = if user.all_keys { "~*" } else { "" };
                        out.extend_from_slice(
                            format!("${}\r\n{}\r\n", key_str.len(), key_str).as_bytes(),
                        );
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
                crate::resp::AclSubcommand::SetUser { username, rules } => {
                    match acl.write().unwrap().set_user(&username, &rules) {
                        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
                    }
                }
                crate::resp::AclSubcommand::DelUser(usernames) => {
                    let count = acl.write().unwrap().del_user(&usernames);
                    write_resp_integer(out, count as i64);
                }
                crate::resp::AclSubcommand::Cat => {
                    let cats = [
                        "keyspace",
                        "read",
                        "write",
                        "set",
                        "sortedset",
                        "list",
                        "hash",
                        "string",
                        "bitmap",
                        "hyperloglog",
                        "geo",
                        "stream",
                        "pubsub",
                        "admin",
                        "fast",
                        "slow",
                        "blocking",
                        "dangerous",
                        "connection",
                        "transaction",
                        "scripting",
                    ];
                    out.extend_from_slice(format!("*{}\r\n", cats.len()).as_bytes());
                    for cat in cats {
                        out.extend_from_slice(format!("${}\r\n{}\r\n", cat.len(), cat).as_bytes());
                    }
                }
            }
            false
        }
        Command::Blpop { keys, timeout } => {
            let mut popped: Option<(Bytes, Bytes)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    let exists = db.exists(k);
                    if !exists {
                        continue;
                    }
                    match db.lpop(k, 1) {
                        Ok(mut vals) => {
                            if let Some(v) = vals.pop() {
                                DIRTY_CHANGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                popped = Some((k.clone(), v));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_res = router
                        .execute_remote(
                            target,
                            Command::Lpop {
                                key: k.clone(),
                                count: None,
                            },
                        )
                        .await;
                    if remote_res.starts_with(b"-") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if let Some(v) = parse_bulk_str_from_resp(&remote_res) {
                        popped = Some((k.clone(), v));
                        break;
                    }
                }
            }

            if let Some((k, v)) = popped {
                let rep_cmd = Command::Lpop {
                    key: k.clone(),
                    count: None,
                };
                if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                    if let Some(aof_w) = &router.aof {
                        aof_w.borrow_mut().append(&bytes);
                    }
                    if crate::replication::has_connected_replicas(router.port) {
                        crate::replication::propagate_bytes(router.port, &bytes);
                    }
                }
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n$");
                out.extend_from_slice(v.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&v);
                out.extend_from_slice(b"\r\n");
                return false;
            }

            if IN_TX.get() {
                write_resp_null_array(out);
                return false;
            }

            let _guard = BlockedClientGuard {
                port: router.port,
                client_id,
            };
            let (tx, rx) = flume::bounded(1);
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock().unwrap();
                hub.register_blocked_client(client_id, tx.clone());
                for k in &keys {
                    hub.register_list_waiter(
                        client_id,
                        k.clone(),
                        crate::block::ListPopType::Left,
                        1,
                        tx.clone(),
                    );
                }
            }

            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
            let (recv_res, client_disconnected) =
                wait_for_blocked_result(&rx, timeout, raw_fd).await;
            if client_disconnected {
                return true;
            }

            match recv_res {
                Some(crate::block::BlockedListResult::Popped(k, mut vals)) => {
                    if let Some(v) = vals.pop() {
                        out.extend_from_slice(b"*2\r\n$");
                        out.extend_from_slice(k.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n$");
                        out.extend_from_slice(v.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        write_resp_null_array(out);
                    }
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::WrongType,
                )) => {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Error,
                )) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                }
                _ => {
                    write_resp_null_array(out);
                }
            }
            false
        }
        Command::Brpop { keys, timeout } => {
            let mut popped: Option<(Bytes, Bytes)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    if !db.exists(k) {
                        continue;
                    }
                    match db.rpop(k, 1) {
                        Ok(mut vals) => {
                            if let Some(v) = vals.pop() {
                                DIRTY_CHANGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                popped = Some((k.clone(), v));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_res = router
                        .execute_remote(
                            target,
                            Command::Rpop {
                                key: k.clone(),
                                count: None,
                            },
                        )
                        .await;
                    if remote_res.starts_with(b"-") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if let Some(v) = parse_bulk_str_from_resp(&remote_res) {
                        popped = Some((k.clone(), v));
                        break;
                    }
                }
            }

            if let Some((k, v)) = popped {
                let rep_cmd = Command::Rpop {
                    key: k.clone(),
                    count: None,
                };
                if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                    if let Some(aof_w) = &router.aof {
                        aof_w.borrow_mut().append(&bytes);
                    }
                    if crate::replication::has_connected_replicas(router.port) {
                        crate::replication::propagate_bytes(router.port, &bytes);
                    }
                }
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n$");
                out.extend_from_slice(v.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&v);
                out.extend_from_slice(b"\r\n");
                return false;
            }

            if IN_TX.get() {
                write_resp_null_array(out);
                return false;
            }

            let _guard = BlockedClientGuard {
                port: router.port,
                client_id,
            };
            let (tx, rx) = flume::bounded(1);
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock().unwrap();
                hub.register_blocked_client(client_id, tx.clone());
                for k in &keys {
                    hub.register_list_waiter(
                        client_id,
                        k.clone(),
                        crate::block::ListPopType::Right,
                        1,
                        tx.clone(),
                    );
                }
            }

            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
            let (recv_res, client_disconnected) =
                wait_for_blocked_result(&rx, timeout, raw_fd).await;
            if client_disconnected {
                return true;
            }

            match recv_res {
                Some(crate::block::BlockedListResult::Popped(k, mut vals)) => {
                    if let Some(v) = vals.pop() {
                        out.extend_from_slice(b"*2\r\n$");
                        out.extend_from_slice(k.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n$");
                        out.extend_from_slice(v.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        write_resp_null_array(out);
                    }
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::WrongType,
                )) => {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Error,
                )) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                }
                _ => {
                    write_resp_null_array(out);
                }
            }
            false
        }
        Command::Smove {
            ref source,
            ref destination,
            ..
        } => {
            let s_target = router.target_shard(source);
            let d_target = router.target_shard(destination);
            if s_target != d_target {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if s_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(s_target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Sort {
            ref key, ref store, ..
        } => {
            let s_target = router.target_shard(key);
            if let Some(dest) = store {
                let d_target = router.target_shard(dest);
                if s_target != d_target {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                    return false;
                }
            }
            if s_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(s_target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Lmove {
            ref source,
            ref destination,
            ..
        } => {
            let s_target = router.target_shard(source);
            let d_target = router.target_shard(destination);
            if s_target != d_target {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if s_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(s_target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Blmove {
            ref source,
            ref destination,
            where_from,
            where_to,
            timeout,
        } => {
            let s_target = router.target_shard(source);
            let d_target = router.target_shard(destination);
            if s_target != d_target {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            let start_len = out.len();
            if s_target == router.shard_id {
                execute_local_command(
                    &Command::Lmove {
                        source: source.clone(),
                        destination: destination.clone(),
                        where_from,
                        where_to,
                    },
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router
                    .execute_remote(
                        s_target,
                        Command::Lmove {
                            source: source.clone(),
                            destination: destination.clone(),
                            where_from,
                            where_to,
                        },
                    )
                    .await;
                out.extend_from_slice(&res);
            }

            if &out[start_len..] != b"$-1\r\n" {
                return false;
            }
            if IN_TX.get() {
                return false;
            }
            out.truncate(start_len);

            let _guard = BlockedClientGuard {
                port: router.port,
                client_id,
            };
            let (tx, rx) = flume::bounded(1);
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock().unwrap();
                hub.register_blocked_client(client_id, tx.clone());
                let from_type = match where_from {
                    crate::table::ListDirection::Left => crate::block::ListPopType::Left,
                    crate::table::ListDirection::Right => crate::block::ListPopType::Right,
                };
                let to_type = match where_to {
                    crate::table::ListDirection::Left => crate::block::ListPopType::Left,
                    crate::table::ListDirection::Right => crate::block::ListPopType::Right,
                };
                hub.register_move_waiter(
                    client_id,
                    source.clone(),
                    from_type,
                    to_type,
                    destination.clone(),
                    tx,
                );
            }

            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
            let (recv_res, client_disconnected) =
                wait_for_blocked_result(&rx, timeout, raw_fd).await;
            if client_disconnected {
                return true;
            }

            match recv_res {
                Some(crate::block::BlockedListResult::Popped(_, mut vals)) => {
                    if let Some(val) = vals.pop() {
                        let rep_cmd = Command::Lmove {
                            source: source.clone(),
                            destination: destination.clone(),
                            where_from,
                            where_to,
                        };
                        if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                            if let Some(aof_w) = &router.aof {
                                aof_w.borrow_mut().append(&bytes);
                            }
                            if crate::replication::has_connected_replicas(router.port) {
                                crate::replication::propagate_bytes(router.port, &bytes);
                            }
                        }
                        write_resp_bulk(out, &val);
                    } else {
                        write_resp_null(out);
                    }
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::WrongType,
                )) => {
                    out.extend_from_slice(
                        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                    );
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Error,
                )) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                }
                _ => {
                    write_resp_null(out);
                }
            }
            false
        }
        Command::Lmpop {
            keys,
            where_from,
            count,
        } => {
            let mut popped: Option<(Bytes, Vec<Bytes>)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    if !db.exists(k) {
                        continue;
                    }
                    let res = match where_from {
                        crate::table::ListDirection::Left => db.lpop(k, count),
                        crate::table::ListDirection::Right => db.rpop(k, count),
                    };
                    match res {
                        Ok(vals) => {
                            if !vals.is_empty() {
                                DIRTY_CHANGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let rep_cmd = match where_from {
                                    crate::table::ListDirection::Left => Command::Lpop {
                                        key: k.clone(),
                                        count: Some(vals.len()),
                                    },
                                    crate::table::ListDirection::Right => Command::Rpop {
                                        key: k.clone(),
                                        count: Some(vals.len()),
                                    },
                                };
                                if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                                    if let Some(aof_w) = &router.aof {
                                        aof_w.borrow_mut().append(&bytes);
                                    }
                                    if crate::replication::has_connected_replicas(router.port) {
                                        crate::replication::propagate_bytes(router.port, &bytes);
                                    }
                                }
                                popped = Some((k.clone(), vals));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_cmd = match where_from {
                        crate::table::ListDirection::Left => Command::Lpop {
                            key: k.clone(),
                            count: Some(count),
                        },
                        crate::table::ListDirection::Right => Command::Rpop {
                            key: k.clone(),
                            count: Some(count),
                        },
                    };
                    let remote_res = router.execute_remote(target, remote_cmd).await;
                    if remote_res.starts_with(b"-WRONGTYPE") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if remote_res.starts_with(b"*")
                        && !remote_res.starts_with(b"*-1")
                        && !remote_res.starts_with(b"*0")
                        && let Some(vals) = parse_array_from_resp(&remote_res)
                        && !vals.is_empty()
                    {
                        popped = Some((k.clone(), vals));
                        break;
                    }
                }
            }

            if let Some((k, vals)) = popped {
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n*");
                out.extend_from_slice(vals.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for v in &vals {
                    out.extend_from_slice(b"$");
                    out.extend_from_slice(v.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(v);
                    out.extend_from_slice(b"\r\n");
                }
            } else {
                write_resp_null_array(out);
            }
            false
        }
        Command::Blmpop {
            timeout,
            keys,
            where_from,
            count,
        } => {
            let mut popped: Option<(Bytes, Vec<Bytes>)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    if !db.exists(k) {
                        continue;
                    }
                    let res = match where_from {
                        crate::table::ListDirection::Left => db.lpop(k, count),
                        crate::table::ListDirection::Right => db.rpop(k, count),
                    };
                    match res {
                        Ok(vals) => {
                            if !vals.is_empty() {
                                DIRTY_CHANGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let rep_cmd = match where_from {
                                    crate::table::ListDirection::Left => Command::Lpop {
                                        key: k.clone(),
                                        count: Some(vals.len()),
                                    },
                                    crate::table::ListDirection::Right => Command::Rpop {
                                        key: k.clone(),
                                        count: Some(vals.len()),
                                    },
                                };
                                if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                                    if let Some(aof_w) = &router.aof {
                                        aof_w.borrow_mut().append(&bytes);
                                    }
                                    if crate::replication::has_connected_replicas(router.port) {
                                        crate::replication::propagate_bytes(router.port, &bytes);
                                    }
                                }
                                popped = Some((k.clone(), vals));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_cmd = match where_from {
                        crate::table::ListDirection::Left => Command::Lpop {
                            key: k.clone(),
                            count: Some(count),
                        },
                        crate::table::ListDirection::Right => Command::Rpop {
                            key: k.clone(),
                            count: Some(count),
                        },
                    };
                    let remote_res = router.execute_remote(target, remote_cmd).await;
                    if remote_res.starts_with(b"-WRONGTYPE") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if remote_res.starts_with(b"*")
                        && !remote_res.starts_with(b"*-1")
                        && !remote_res.starts_with(b"*0")
                        && let Some(vals) = parse_array_from_resp(&remote_res)
                        && !vals.is_empty()
                    {
                        popped = Some((k.clone(), vals));
                        break;
                    }
                }
            }

            if let Some((k, vals)) = popped {
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n*");
                out.extend_from_slice(vals.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for v in &vals {
                    out.extend_from_slice(b"$");
                    out.extend_from_slice(v.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(v);
                    out.extend_from_slice(b"\r\n");
                }
                return false;
            }

            if IN_TX.get() {
                write_resp_null_array(out);
                return false;
            }

            let _guard = BlockedClientGuard {
                port: router.port,
                client_id,
            };
            let (tx, rx) = flume::bounded(1);
            let pop_type = match where_from {
                crate::table::ListDirection::Left => crate::block::ListPopType::Left,
                crate::table::ListDirection::Right => crate::block::ListPopType::Right,
            };
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock().unwrap();
                hub.register_blocked_client(client_id, tx.clone());
                for k in &keys {
                    hub.register_list_waiter(client_id, k.clone(), pop_type, count, tx.clone());
                }
            }

            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
            let (recv_res, client_disconnected) =
                wait_for_blocked_result(&rx, timeout, raw_fd).await;
            if client_disconnected {
                return true;
            }

            match recv_res {
                Some(crate::block::BlockedListResult::Popped(k, vals)) => {
                    let rep_cmd = match where_from {
                        crate::table::ListDirection::Left => Command::Lpop {
                            key: k.clone(),
                            count: Some(vals.len()),
                        },
                        crate::table::ListDirection::Right => Command::Rpop {
                            key: k.clone(),
                            count: Some(vals.len()),
                        },
                    };
                    if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                        if let Some(aof_w) = &router.aof {
                            aof_w.borrow_mut().append(&bytes);
                        }
                        if crate::replication::has_connected_replicas(router.port) {
                            crate::replication::propagate_bytes(router.port, &bytes);
                        }
                    }
                    out.extend_from_slice(b"*2\r\n$");
                    out.extend_from_slice(k.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(&k);
                    out.extend_from_slice(b"\r\n*");
                    out.extend_from_slice(vals.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    for v in &vals {
                        out.extend_from_slice(b"$");
                        out.extend_from_slice(v.len().to_string().as_bytes());
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(v);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Some(crate::block::BlockedListResult::Unblocked(
                    crate::block::ClientUnblockType::Error,
                )) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                }
                _ => {
                    write_resp_null_array(out);
                }
            }
            false
        }
        Command::Bzpopmin { keys, timeout } => {
            handle_bzpop(router, client_id, client_registry, out, keys, timeout, true).await
        }
        Command::Bzpopmax { keys, timeout } => {
            handle_bzpop(
                router,
                client_id,
                client_registry,
                out,
                keys,
                timeout,
                false,
            )
            .await
        }
        Command::Zmpop {
            keys,
            is_min,
            count,
        } => {
            let mut popped: Option<(Bytes, Vec<(Bytes, f64)>)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    if !db.exists(k) {
                        continue;
                    }
                    let res = if is_min {
                        db.zpopmin(k, count)
                    } else {
                        db.zpopmax(k, count)
                    };
                    match res {
                        Ok(items) => {
                            if !items.is_empty() {
                                DIRTY_CHANGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let rep_cmd = if is_min {
                                    Command::Zpopmin {
                                        key: k.clone(),
                                        count: Some(items.len()),
                                    }
                                } else {
                                    Command::Zpopmax {
                                        key: k.clone(),
                                        count: Some(items.len()),
                                    }
                                };
                                if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                                    if let Some(aof_w) = &router.aof {
                                        aof_w.borrow_mut().append(&bytes);
                                    }
                                    if crate::replication::has_connected_replicas(router.port) {
                                        crate::replication::propagate_bytes(router.port, &bytes);
                                    }
                                }
                                popped = Some((k.clone(), items));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_cmd = if is_min {
                        Command::Zpopmin {
                            key: k.clone(),
                            count: Some(count),
                        }
                    } else {
                        Command::Zpopmax {
                            key: k.clone(),
                            count: Some(count),
                        }
                    };
                    let remote_res = router.execute_remote(target, remote_cmd).await;
                    if remote_res.starts_with(b"-WRONGTYPE") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if let Some(items) = parse_zpop_items(&remote_res)
                        && !items.is_empty()
                    {
                        popped = Some((k.clone(), items));
                        break;
                    }
                }
            }

            if let Some((k, items)) = popped {
                format_zmpop_response(out, &k, &items);
            } else {
                write_resp_null_array(out);
            }
            false
        }
        Command::Bzmpop {
            timeout,
            keys,
            is_min,
            count,
        } => {
            let mut popped: Option<(Bytes, Vec<(Bytes, f64)>)> = None;
            for k in &keys {
                let target = router.target_shard(k);
                if target == router.shard_id {
                    let mut db = router.local_db.borrow_mut();
                    if !db.exists(k) {
                        continue;
                    }
                    let res = if is_min {
                        db.zpopmin(k, count)
                    } else {
                        db.zpopmax(k, count)
                    };
                    match res {
                        Ok(items) => {
                            if !items.is_empty() {
                                DIRTY_CHANGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                let rep_cmd = if is_min {
                                    Command::Zpopmin {
                                        key: k.clone(),
                                        count: Some(items.len()),
                                    }
                                } else {
                                    Command::Zpopmax {
                                        key: k.clone(),
                                        count: Some(items.len()),
                                    }
                                };
                                if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                                    if let Some(aof_w) = &router.aof {
                                        aof_w.borrow_mut().append(&bytes);
                                    }
                                    if crate::replication::has_connected_replicas(router.port) {
                                        crate::replication::propagate_bytes(router.port, &bytes);
                                    }
                                }
                                popped = Some((k.clone(), items));
                                break;
                            }
                        }
                        Err(err) => {
                            write_resp_err(out, err);
                            return false;
                        }
                    }
                } else {
                    let remote_cmd = if is_min {
                        Command::Zpopmin {
                            key: k.clone(),
                            count: Some(count),
                        }
                    } else {
                        Command::Zpopmax {
                            key: k.clone(),
                            count: Some(count),
                        }
                    };
                    let remote_res = router.execute_remote(target, remote_cmd).await;
                    if remote_res.starts_with(b"-WRONGTYPE") {
                        out.extend_from_slice(&remote_res);
                        return false;
                    }
                    if let Some(items) = parse_zpop_items(&remote_res)
                        && !items.is_empty()
                    {
                        popped = Some((k.clone(), items));
                        break;
                    }
                }
            }

            if let Some((k, items)) = popped {
                format_zmpop_response(out, &k, &items);
                return false;
            }

            if IN_TX.get() {
                write_resp_null_array(out);
                return false;
            }

            let _guard = BlockedClientGuard {
                port: router.port,
                client_id,
            };
            let (tx, rx) = flume::bounded(1);
            let pop_type = if is_min {
                crate::block::ZSetPopType::Min
            } else {
                crate::block::ZSetPopType::Max
            };
            {
                let hub_arc = crate::block::get_block_hub_for_port(router.port);
                let mut hub = hub_arc.lock().unwrap();
                hub.register_blocked_zset_client(client_id, tx.clone());
                for k in keys {
                    hub.register_zset_waiter(client_id, k, pop_type, count, true, tx.clone());
                }
            }

            let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
            let (recv_res, client_disconnected) =
                wait_for_blocked_result(&rx, timeout, raw_fd).await;
            if client_disconnected {
                return true;
            }

            match recv_res {
                Some(crate::block::BlockedZSetResult::Popped { key, items, .. }) => {
                    let rep_cmd = if is_min {
                        Command::Zpopmin {
                            key: key.clone(),
                            count: Some(items.len()),
                        }
                    } else {
                        Command::Zpopmax {
                            key: key.clone(),
                            count: Some(items.len()),
                        }
                    };
                    if let Some(bytes) = crate::aof::command_to_resp(&rep_cmd) {
                        if let Some(aof_w) = &router.aof {
                            aof_w.borrow_mut().append(&bytes);
                        }
                        if crate::replication::has_connected_replicas(router.port) {
                            crate::replication::propagate_bytes(router.port, &bytes);
                        }
                    }
                    format_zmpop_response(out, &key, &items);
                }
                Some(crate::block::BlockedZSetResult::Unblocked(
                    crate::block::ClientUnblockType::Error,
                )) => {
                    out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                }
                _ => {
                    write_resp_null_array(out);
                }
            }
            false
        }
        Command::Hello {
            proto,
            ref auth,
            ref setname,
        } => {
            let acl = crate::acl::get_acl_for_port(router.port);
            let default_requires_auth = acl
                .read()
                .unwrap()
                .get_user("default")
                .map(|u| !u.passwords.is_empty())
                .unwrap_or(false);

            if let Some((uname, pass)) = auth {
                if let Ok(user) = acl.read().unwrap().check_auth(Some(uname), pass) {
                    *authenticated = true;
                    *auth_user = user;
                } else {
                    out.extend_from_slice(
                        b"-WRONGPASS invalid username-password pair or user is disabled.\r\n",
                    );
                    return false;
                }
            } else if !*authenticated && default_requires_auth {
                out.extend_from_slice(
                    b"-NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and set the protocol.\r\n",
                );
                return false;
            }

            if let Some(name) = setname
                && let Some(c) = client_registry.borrow_mut().get_mut(&client_id)
            {
                c.name = Some(name.clone());
            }

            let proto_ver = proto.unwrap_or(2);
            if proto_ver == 3 {
                if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                    c.is_resp3 = true;
                }
                CURRENT_CLIENT_RESP3.set(true);
                out.extend_from_slice(b"%7\r\n");
            } else {
                if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                    c.is_resp3 = false;
                }
                CURRENT_CLIENT_RESP3.set(false);
                out.extend_from_slice(b"*14\r\n");
            }
            out.extend_from_slice(b"$6\r\nserver\r\n$6\r\nvalkey\r\n");
            out.extend_from_slice(b"$7\r\nversion\r\n$5\r\n7.2.0\r\n");
            out.extend_from_slice(format!("$5\r\nproto\r\n:{}\r\n", proto_ver).as_bytes());
            out.extend_from_slice(format!("$2\r\nid\r\n:{}\r\n", client_id).as_bytes());
            out.extend_from_slice(b"$4\r\nmode\r\n$10\r\nstandalone\r\n");
            out.extend_from_slice(b"$4\r\nrole\r\n$6\r\nmaster\r\n");
            out.extend_from_slice(b"$7\r\nmodules\r\n*0\r\n");
            false
        }
        Command::Reset => {
            if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                c.name = None;
                c.is_resp3 = false;
            }
            unregister_client_tracking(router.port, client_id);
            *asking = false;

            let acl = crate::acl::get_acl_for_port(router.port);
            let default_requires_auth = acl
                .read()
                .unwrap()
                .get_user("default")
                .map(|u| !u.passwords.is_empty())
                .unwrap_or(false);
            *authenticated = !default_requires_auth;
            *auth_user = "default".to_string();
            out.extend_from_slice(b"+RESET\r\n");
            false
        }
        Command::Time => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let secs = now.as_secs();
            let micros = now.subsec_micros();
            out.extend_from_slice(
                format!(
                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    secs.to_string().len(),
                    secs,
                    micros.to_string().len(),
                    micros
                )
                .as_bytes(),
            );
            false
        }
        Command::Echo(ref msg) => {
            write_resp_bulk(out, msg);
            false
        }
        Command::Sinter(ref keys)
        | Command::Sunion(ref keys)
        | Command::Sdiff(ref keys)
        | Command::Sintercard { ref keys, .. }
        | Command::Sunioncard { ref keys, .. }
        | Command::Sdiffcard { ref keys, .. }
        | Command::Zdiff { ref keys, .. }
        | Command::Zinter { ref keys, .. }
        | Command::Zunion { ref keys, .. }
        | Command::Zintercard { ref keys, .. } => {
            if keys.is_empty() {
                out.extend_from_slice(b"*0\r\n");
                return false;
            }
            let first_target = router.target_shard(&keys[0]);
            for k in &keys[1..] {
                if router.target_shard(k) != first_target {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                    return false;
                }
            }
            if first_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Sinterstore {
            ref destination,
            ref keys,
        }
        | Command::Sunionstore {
            ref destination,
            ref keys,
        }
        | Command::Sdiffstore {
            ref destination,
            ref keys,
        }
        | Command::Zunionstore {
            ref destination,
            ref keys,
            ..
        }
        | Command::Zinterstore {
            ref destination,
            ref keys,
            ..
        }
        | Command::Zdiffstore {
            ref destination,
            ref keys,
        } => {
            let dest_target = router.target_shard(destination);
            for k in keys {
                if router.target_shard(k) != dest_target {
                    out.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                    return false;
                }
            }
            if dest_target == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(dest_target, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Dbsize => {
            let count = router.dbsize().await;
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::Select(_) => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Slowlog(sub) => {
            if sub.eq_ignore_ascii_case(b"reset") {
                out.extend_from_slice(b"+OK\r\n");
            } else if sub.eq_ignore_ascii_case(b"len") {
                out.extend_from_slice(b":0\r\n");
            } else {
                out.extend_from_slice(b"*0\r\n");
            }
            false
        }
        Command::Flushdb | Command::Flushall => {
            router.flushdb().await;
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Touch(keys) => {
            let mut count = 0usize;
            for k in keys {
                let target = router.target_shard(&k);
                if target == router.shard_id {
                    count += router.local_db.borrow_mut().touch(&[k]);
                } else {
                    let res = router.execute_remote(target, Command::Touch(vec![k])).await;
                    if res == b":1\r\n" {
                        count += 1;
                    }
                }
            }
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::Rename {
            ref key,
            ref newkey,
            ..
        } => {
            let target_src = router.target_shard(key);
            let target_dst = router.target_shard(newkey);
            if target_src != target_dst {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if target_src == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(target_src, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Msetnx(ref pairs) => {
            if pairs.is_empty() {
                out.extend_from_slice(b":0\r\n");
                return false;
            }
            let first_shard = router.target_shard(&pairs[0].0);
            let all_same_shard = pairs
                .iter()
                .all(|(k, _)| router.target_shard(k) == first_shard);
            if !all_same_shard {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if first_shard == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_shard, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Pfcount { ref keys } => {
            if keys.is_empty() {
                out.extend_from_slice(b":0\r\n");
                return false;
            }
            let first_shard = router.target_shard(&keys[0]);
            let all_same = keys.iter().all(|k| router.target_shard(k) == first_shard);
            if !all_same {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if first_shard == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_shard, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Bitop {
            ref destkey,
            ref srckeys,
            ..
        } => {
            let first_shard = router.target_shard(destkey);
            let all_same = srckeys
                .iter()
                .all(|k| router.target_shard(k) == first_shard);
            if !all_same {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if first_shard == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_shard, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Pfmerge {
            ref destkey,
            ref srckeys,
        } => {
            let first_shard = router.target_shard(destkey);
            let all_same = srckeys
                .iter()
                .all(|k| router.target_shard(k) == first_shard);
            if !all_same {
                out.extend_from_slice(
                    b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                );
                return false;
            }
            if first_shard == router.shard_id {
                execute_local_command(
                    &cmd,
                    &mut router.local_db.borrow_mut(),
                    out,
                    router.aof.as_deref(),
                );
            } else {
                let res = router.execute_remote(first_shard, cmd).await;
                out.extend_from_slice(&res);
            }
            false
        }
        Command::Save => {
            match router.save_rdb().await {
                Ok(_) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => {
                    let err = format!("-ERR {}\r\n", e);
                    out.extend_from_slice(err.as_bytes());
                }
            }
            false
        }
        Command::Bgsave => {
            match router.bgsave().await {
                Ok(_) => {
                    out.extend_from_slice(b"+Background saving started\r\n");
                }
                Err(e) => {
                    let err = format!("-ERR {}\r\n", e);
                    out.extend_from_slice(err.as_bytes());
                }
            }
            false
        }
        Command::Lastsave => {
            let ts = router.lastsave();
            out.extend_from_slice(format!(":{}\r\n", ts).as_bytes());
            false
        }
        Command::Asking => {
            // Already handled at start of execute_command
            false
        }
        Command::Migrate {
            host,
            port,
            key,
            keys,
            destination_db: _,
            timeout_ms: _,
            copy,
            replace: _,
        } => {
            let mut migrate_keys = Vec::new();
            if let Some(k) = key
                && !k.is_empty()
            {
                migrate_keys.push(k);
            }
            for k in keys {
                if !migrate_keys.contains(&k) {
                    migrate_keys.push(k);
                }
            }
            if migrate_keys.is_empty() {
                out.extend_from_slice(b"+NOKEY\r\n");
                return false;
            }

            let mut dumps = Vec::new();
            for k in &migrate_keys {
                if let Some(entry) = router.dump_key(k.clone()).await {
                    dumps.push((k.clone(), entry));
                }
            }

            if dumps.is_empty() {
                out.extend_from_slice(b"+NOKEY\r\n");
                return false;
            }

            let target_addr = format!("{}:{}", host, port);
            let socket_addr = match std::net::ToSocketAddrs::to_socket_addrs(&target_addr) {
                Ok(mut iter) => match iter.next() {
                    Some(a) => a,
                    None => {
                        out.extend_from_slice(b"-ERR cannot resolve destination host\r\n");
                        return false;
                    }
                },
                Err(_) => {
                    out.extend_from_slice(b"-ERR invalid destination address\r\n");
                    return false;
                }
            };

            let mut stream = match monoio::net::TcpStream::connect(socket_addr).await {
                Ok(s) => s,
                Err(err) => {
                    out.extend_from_slice(
                        format!("-IOERR error connecting to destination: {}\r\n", err).as_bytes(),
                    );
                    return false;
                }
            };

            // Send ASKING first to allow import if destination slot is in IMPORTING state
            let mut tx_buf = Vec::new();
            tx_buf.extend_from_slice(b"*1\r\n$6\r\nASKING\r\n");
            for (k, (val, ttl)) in &dumps {
                match val {
                    crate::table::RudisValue::String(s) => {
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*5\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                            tx_buf.extend_from_slice(s);
                            tx_buf.extend_from_slice(
                                format!("\r\n$2\r\nPX\r\n${}\r\n{}\r\n", ms_str.len(), ms_str)
                                    .as_bytes(),
                            );
                        } else {
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                            tx_buf.extend_from_slice(s);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                    }
                    crate::table::RudisValue::Int(n) => {
                        let s = crate::table::RudisTable::format_i64(*n);
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*5\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                            tx_buf.extend_from_slice(&s);
                            tx_buf.extend_from_slice(
                                format!("\r\n$2\r\nPX\r\n${}\r\n{}\r\n", ms_str.len(), ms_str)
                                    .as_bytes(),
                            );
                        } else {
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(format!("\r\n${}\r\n", s.len()).as_bytes());
                            tx_buf.extend_from_slice(&s);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                    }
                    crate::table::RudisValue::SmallHash(fields) => {
                        tx_buf.extend_from_slice(
                            format!(
                                "*{}\r\n$4\r\nHSET\r\n${}\r\n",
                                2 + fields.len() * 2,
                                k.len()
                            )
                            .as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n");
                        for (f, v) in fields {
                            tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                            tx_buf.extend_from_slice(f);
                            tx_buf.extend_from_slice(b"\r\n");
                            tx_buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            tx_buf.extend_from_slice(v);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::Hash(fields) => {
                        tx_buf.extend_from_slice(
                            format!(
                                "*{}\r\n$4\r\nHSET\r\n${}\r\n",
                                2 + fields.len() * 2,
                                k.len()
                            )
                            .as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n");
                        for (f, v) in fields {
                            tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                            tx_buf.extend_from_slice(f);
                            tx_buf.extend_from_slice(b"\r\n");
                            tx_buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            tx_buf.extend_from_slice(v);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::List(deque) => {
                        tx_buf.extend_from_slice(
                            format!("*{}\r\n$5\r\nRPUSH\r\n${}\r\n", 2 + deque.len(), k.len())
                                .as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n");
                        for v in deque {
                            tx_buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            tx_buf.extend_from_slice(v);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::Set(set) => {
                        tx_buf.extend_from_slice(
                            format!("*{}\r\n$4\r\nSADD\r\n${}\r\n", 2 + set.len(), k.len())
                                .as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n");
                        for m in set {
                            tx_buf.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            tx_buf.extend_from_slice(m);
                            tx_buf.extend_from_slice(b"\r\n");
                        }
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::ZSet(zset) => {
                        tx_buf.extend_from_slice(
                            format!("*{}\r\n$4\r\nZADD\r\n${}\r\n", 2 + zset.len() * 2, k.len())
                                .as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n");
                        zset.for_each(|m, score| {
                            let s = format_score(score);
                            tx_buf
                                .extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                            tx_buf.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            tx_buf.extend_from_slice(m);
                            tx_buf.extend_from_slice(b"\r\n");
                        });
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::HyperLogLog(regs) => {
                        tx_buf.extend_from_slice(
                            format!("*3\r\n$3\r\nSET\r\n${}\r\n", k.len()).as_bytes(),
                        );
                        tx_buf.extend_from_slice(k);
                        tx_buf.extend_from_slice(b"\r\n$16384\r\n");
                        tx_buf.extend_from_slice(regs.as_ref());
                        tx_buf.extend_from_slice(b"\r\n");
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::Stream(stream) => {
                        for (id, fields) in &stream.entries {
                            tx_buf.extend_from_slice(
                                format!(
                                    "*{}\r\n$4\r\nXADD\r\n${}\r\n",
                                    3 + fields.len() * 2,
                                    k.len()
                                )
                                .as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(b"\r\n");
                            let id_str = id.to_string();
                            tx_buf.extend_from_slice(
                                format!("${}\r\n{}\r\n", id_str.len(), id_str).as_bytes(),
                            );
                            for (f, v) in fields {
                                tx_buf.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                                tx_buf.extend_from_slice(f);
                                tx_buf.extend_from_slice(b"\r\n");
                                tx_buf.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                                tx_buf.extend_from_slice(v);
                                tx_buf.extend_from_slice(b"\r\n");
                            }
                        }
                        if let Some(dur) = ttl {
                            let ms = dur.as_millis().max(1);
                            let ms_str = ms.to_string();
                            tx_buf.extend_from_slice(
                                format!("*3\r\n$7\r\nPEXPIRE\r\n${}\r\n", k.len()).as_bytes(),
                            );
                            tx_buf.extend_from_slice(k);
                            tx_buf.extend_from_slice(
                                format!("\r\n${}\r\n{}\r\n", ms_str.len(), ms_str).as_bytes(),
                            );
                        }
                    }
                    crate::table::RudisValue::Tiered(_)
                    | crate::table::RudisValue::Cooled { .. } => {}
                }
            }

            if let Err(e) = stream.write_all(tx_buf).await.0 {
                out.extend_from_slice(
                    format!("-IOERR error sending to destination: {}\r\n", e).as_bytes(),
                );
                return false;
            }

            let resp_buf = vec![0u8; 1024];
            let (read_res, _) = stream.read(resp_buf).await;
            if let Err(e) = read_res {
                out.extend_from_slice(
                    format!("-IOERR error reading from destination: {}\r\n", e).as_bytes(),
                );
                return false;
            }

            if !copy {
                for (k, _) in dumps {
                    let _ = router.del(k).await;
                }
            }

            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Subscribe(_) | Command::Psubscribe(_) => false,
        Command::Unsubscribe(channels) => {
            if channels.is_empty() {
                out.extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n");
            } else {
                for ch in channels {
                    out.extend_from_slice(
                        format!(
                            "*3\r\n$11\r\nunsubscribe\r\n${}\r\n{}\r\n:0\r\n",
                            ch.len(),
                            String::from_utf8_lossy(&ch)
                        )
                        .as_bytes(),
                    );
                }
            }
            false
        }
        Command::Punsubscribe(patterns) => {
            if patterns.is_empty() {
                out.extend_from_slice(b"*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:0\r\n");
            } else {
                for pat in patterns {
                    out.extend_from_slice(
                        format!(
                            "*3\r\n$12\r\npunsubscribe\r\n${}\r\n{}\r\n:0\r\n",
                            pat.len(),
                            String::from_utf8_lossy(&pat)
                        )
                        .as_bytes(),
                    );
                }
            }
            false
        }
        Command::Publish { channel, message } => {
            let count = router.publish(channel, message).await;
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::PubsubChannels(pattern) => {
            let channels = router.pubsub_channels(pattern).await;
            out.extend_from_slice(format!("*{}\r\n", channels.len()).as_bytes());
            for ch in channels {
                out.extend_from_slice(format!("${}\r\n", ch.len()).as_bytes());
                out.extend_from_slice(&ch);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::PubsubNumsub(channels) => {
            let counts = router.pubsub_numsub(channels).await;
            out.extend_from_slice(format!("*{}\r\n", counts.len() * 2).as_bytes());
            for (ch, cnt) in counts {
                out.extend_from_slice(format!("${}\r\n", ch.len()).as_bytes());
                out.extend_from_slice(&ch);
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(format!(":{}\r\n", cnt).as_bytes());
            }
            false
        }
        Command::PubsubNumpat => {
            let count = router.pubsub_numpat().await;
            out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
            false
        }
        Command::Keys(pattern) => {
            let keys = router.keys(&pattern).await;
            out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
            for k in keys {
                out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::Scan {
            cursor,
            pattern,
            count,
        } => {
            let cnt = count.unwrap_or(10);
            let (next_cursor, keys) = router.scan(cursor, pattern.as_deref(), cnt).await;
            let cursor_str = next_cursor.to_string();
            out.extend_from_slice(b"*2\r\n$");
            out.extend_from_slice(cursor_str.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(cursor_str.as_bytes());
            out.extend_from_slice(b"\r\n*");
            out.extend_from_slice(keys.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for k in keys {
                out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::Randomkey => {
            match router.random_key().await {
                Some(k) => {
                    out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                    out.extend_from_slice(&k);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Expiretime(key, in_millis) => {
            let ts = router.expiretime(key, in_millis).await;
            out.extend_from_slice(format!(":{}\r\n", ts).as_bytes());
            false
        }
        Command::Multi => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Exec => {
            out.extend_from_slice(b"-ERR EXEC without MULTI\r\n");
            false
        }
        Command::Discard => {
            out.extend_from_slice(b"-ERR DISCARD without MULTI\r\n");
            false
        }
        Command::Watch(keys) => {
            watch_keys(router.port, client_id, &keys);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Unwatch => {
            unwatch_keys(router.port, client_id);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Eval { script, keys, args } => {
            let script_str = String::from_utf8_lossy(&script);
            crate::scripting::load_script(&script);
            match crate::scripting::eval_script(
                &script_str,
                &keys,
                &args,
                &router.local_db,
                router.aof.as_deref(),
            ) {
                Ok(resp) => out.extend_from_slice(&resp),
                Err(e) => {
                    let err_resp = format!("-{}\r\n", e);
                    out.extend_from_slice(err_resp.as_bytes());
                }
            }
            false
        }
        Command::Evalsha { sha, keys, args } => {
            let sha_str = String::from_utf8_lossy(&sha);
            if let Some(script) = crate::scripting::get_script(&sha_str) {
                match crate::scripting::eval_script(
                    &script,
                    &keys,
                    &args,
                    &router.local_db,
                    router.aof.as_deref(),
                ) {
                    Ok(resp) => out.extend_from_slice(&resp),
                    Err(e) => {
                        let err_resp = format!("-{}\r\n", e);
                        out.extend_from_slice(err_resp.as_bytes());
                    }
                }
            } else {
                out.extend_from_slice(b"-NOSCRIPT No matching script. Please use EVAL.\r\n");
            }
            false
        }
        Command::ScriptLoad(script) => {
            let sha = crate::scripting::load_script(&script);
            write_resp_bulk(out, sha.as_bytes());
            false
        }
        Command::ScriptExists(shas) => {
            let exists = crate::scripting::script_exists(&shas);
            out.extend_from_slice(format!("*{}\r\n", exists.len()).as_bytes());
            for b in exists {
                if b {
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
            }
            false
        }
        Command::ScriptFlush => {
            crate::scripting::flush_scripts();
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Vadd {
            index,
            key,
            vector,
            metric,
            quantize,
            pq,
            tiered,
        } => {
            match router.local_db.borrow_mut().vadd(
                &index,
                key.clone(),
                vector,
                metric,
                quantize,
                pq,
                tiered,
            ) {
                Ok(()) => {
                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Vquery {
            index,
            k,
            query,
            rerank,
        } => {
            let results = router.local_db.borrow().vquery(&index, &query, k, rerank);
            out.extend_from_slice(format!("*{}\r\n", results.len() * 2).as_bytes());
            for (key, dist) in results {
                write_resp_bulk(out, &key);
                let s = format!("{:.6}", dist);
                write_resp_bulk(out, s.as_bytes());
            }
            false
        }
        Command::Vsim {
            index,
            k1,
            k2,
            metric,
        } => {
            match router.local_db.borrow().vsim(&index, &k1, &k2, metric) {
                Ok(dist) => {
                    let s = format!("{:.6}", dist);
                    write_resp_bulk(out, s.as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Vdel { index, key } => {
            let removed = router.local_db.borrow_mut().vdel(&index, &key);
            if removed {
                notify_key_invalidation(router.port, key.as_ref(), client_id);
                write_resp_integer(out, 1);
            } else {
                write_resp_integer(out, 0);
            }
            false
        }
        Command::Vinfo(index) => {
            if let Some((count, dim, metric, max_layer)) = router.local_db.borrow().vinfo(&index) {
                out.extend_from_slice(b"*8\r\n");
                write_resp_bulk(out, b"num_elements");
                write_resp_integer(out, count as i64);
                write_resp_bulk(out, b"dimension");
                write_resp_integer(out, dim as i64);
                write_resp_bulk(out, b"metric");
                write_resp_bulk(out, metric.as_bytes());
                write_resp_bulk(out, b"max_layer");
                write_resp_integer(out, max_layer as i64);
            } else {
                out.extend_from_slice(b"$-1\r\n");
            }
            false
        }
        Command::CrdtDump => {
            let mut payload = router.local_db.borrow().crdt_dump();
            for sid in 0..router.num_shards {
                if sid != router.shard_id {
                    let res = router.execute_remote(sid, Command::CrdtDump).await;
                    if let Some(first_nl) = res.iter().position(|&b| b == b'\n')
                        && res.len() > first_nl + 2
                    {
                        let chunk = &res[first_nl + 1..res.len() - 2];
                        payload.extend_from_slice(chunk);
                    }
                }
            }
            write_resp_bulk(out, &payload);
            false
        }
        Command::CrdtMerge(payload) => {
            let mut total_merged = match router.local_db.borrow_mut().crdt_merge(&payload) {
                Ok(count) => count,
                Err(e) => {
                    let err_resp = format!("-ERR {}\r\n", e);
                    out.extend_from_slice(err_resp.as_bytes());
                    return false;
                }
            };
            for sid in 0..router.num_shards {
                if sid != router.shard_id {
                    let res = router
                        .execute_remote(sid, Command::CrdtMerge(payload.clone()))
                        .await;
                    if let Ok(s) = std::str::from_utf8(&res)
                        && let Some(num_str) =
                            s.strip_prefix(':').and_then(|x| x.split("\r\n").next())
                        && let Ok(n) = num_str.parse::<usize>()
                    {
                        total_merged += n;
                    }
                }
            }
            write_resp_integer(out, total_merged as i64);
            false
        }
        Command::CrdtGc(ttl_ms) => {
            let (mut total_regs, mut total_sets) = router.local_db.borrow_mut().crdt_gc(ttl_ms);
            for sid in 0..router.num_shards {
                if sid != router.shard_id {
                    let res = router.execute_remote(sid, Command::CrdtGc(ttl_ms)).await;
                    if let Ok(s) = std::str::from_utf8(&res) {
                        let parts: Vec<&str> = s.split("\r\n").collect();
                        if parts.len() >= 6 {
                            if let Ok(r) = parts[3].trim_start_matches(':').parse::<usize>() {
                                total_regs += r;
                            }
                            if let Ok(st) = parts[5].trim_start_matches(':').parse::<usize>() {
                                total_sets += st;
                            }
                        }
                    }
                }
            }
            out.extend_from_slice(b"*4\r\n");
            write_resp_bulk(out, b"registers_pruned");
            write_resp_integer(out, total_regs as i64);
            write_resp_bulk(out, b"set_tombstones_pruned");
            write_resp_integer(out, total_sets as i64);
            false
        }
        Command::FunctionLoad { replace, code } => {
            let code_str = String::from_utf8_lossy(&code);
            match crate::scripting::load_function(&code_str, replace) {
                Ok(lib_name) => {
                    write_resp_bulk(out, lib_name.as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Fcall {
            function,
            keys,
            args,
        } => {
            match crate::scripting::call_function(
                &function,
                &keys,
                &args,
                &router.local_db,
                router.aof.as_deref(),
            ) {
                Ok(res) => {
                    for k in &keys {
                        notify_key_invalidation(router.port, k.as_ref(), client_id);
                    }
                    out.extend_from_slice(&res);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::FunctionList => {
            let libs = crate::scripting::list_functions();
            out.extend_from_slice(format!("*{}\r\n", libs.len()).as_bytes());
            for lib in libs {
                out.extend_from_slice(b"*8\r\n");
                write_resp_bulk(out, b"library_name");
                write_resp_bulk(out, lib.name.as_bytes());
                write_resp_bulk(out, b"engine");
                write_resp_bulk(out, lib.engine.as_bytes());
                write_resp_bulk(out, b"functions");
                out.extend_from_slice(format!("*{}\r\n", lib.functions.len()).as_bytes());
                for f in &lib.functions {
                    write_resp_bulk(out, f.as_bytes());
                }
                write_resp_bulk(out, b"raw_code");
                write_resp_bulk(out, lib.raw_code.as_bytes());
            }
            false
        }
        Command::FunctionDelete(lib) => {
            if crate::scripting::delete_function(&lib) {
                out.extend_from_slice(b"+OK\r\n");
            } else {
                out.extend_from_slice(format!("-ERR Library not found: {}\r\n", lib).as_bytes());
            }
            false
        }
        Command::FunctionFlush => {
            crate::scripting::flush_functions();
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::FtCreate {
            index,
            on_type,
            prefixes,
            fields,
        } => {
            let schema = crate::search::IndexSchema {
                name: index,
                on_type,
                prefixes,
                fields,
            };
            match crate::search::create_search_index(schema) {
                Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
            }
            false
        }
        Command::FtSearch {
            index,
            query,
            options,
        } => {
            if let Some(idx_arc) = crate::search::get_search_index(&index) {
                let idx = idx_arc.read().unwrap();
                let ast = crate::search::parse_query(&query);
                let (total, hits) = crate::search::execute_search(&idx, &ast, &options);

                if options.nocontent {
                    out.extend_from_slice(
                        format!("*{}\r\n:{}\r\n", 1 + hits.len(), total).as_bytes(),
                    );
                    for hit in hits {
                        write_resp_bulk(out, hit.doc_id.as_bytes());
                    }
                } else {
                    let mut num_elems = 1;
                    for _ in &hits {
                        num_elems += 2;
                    }
                    out.extend_from_slice(format!("*{}\r\n:{}\r\n", num_elems, total).as_bytes());
                    for hit in hits {
                        write_resp_bulk(out, hit.doc_id.as_bytes());
                        out.extend_from_slice(format!("*{}\r\n", hit.fields.len() * 2).as_bytes());
                        for (k, v) in hit.fields {
                            write_resp_bulk(out, k.as_bytes());
                            write_resp_bulk(out, v.as_bytes());
                        }
                    }
                }
            } else {
                out.extend_from_slice(format!("-ERR Unknown Index name: {}\r\n", index).as_bytes());
            }
            false
        }
        Command::FtInfo(index) => {
            if let Some(idx_arc) = crate::search::get_search_index(&index) {
                let idx = idx_arc.read().unwrap();
                if let Some(schema) = &idx.schema {
                    out.extend_from_slice(b"*12\r\n");
                    write_resp_bulk(out, b"index_name");
                    write_resp_bulk(out, schema.name.as_bytes());
                    write_resp_bulk(out, b"index_options");
                    out.extend_from_slice(b"*0\r\n");
                    write_resp_bulk(out, b"num_docs");
                    write_resp_integer(out, idx.total_docs as i64);
                    write_resp_bulk(out, b"num_terms");
                    write_resp_integer(out, idx.inverted.len() as i64);
                    write_resp_bulk(out, b"total_inverted_index_blocks");
                    write_resp_integer(out, idx.total_terms as i64);
                    write_resp_bulk(out, b"indexing");
                    write_resp_bulk(out, b"0");
                } else {
                    out.extend_from_slice(b"$-1\r\n");
                }
            } else {
                out.extend_from_slice(format!("-ERR Unknown Index name: {}\r\n", index).as_bytes());
            }
            false
        }
        Command::FtDropIndex { index, dd: _ } => {
            match crate::search::drop_search_index(&index) {
                Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
            }
            false
        }
        Command::FtExplain { index: _, query } => {
            let ast = crate::search::parse_query(&query);
            let repr = format!("{:?}", ast);
            write_resp_bulk(out, repr.as_bytes());
            false
        }
        Command::FtAdd {
            index,
            doc_id,
            score: _,
            fields,
        } => {
            if let Some(idx_arc) = crate::search::get_search_index(&index) {
                let mut idx = idx_arc.write().unwrap();
                let map: std::collections::HashMap<String, String> = fields.into_iter().collect();
                idx.add_document(&doc_id, map, None);
                out.extend_from_slice(b"+OK\r\n");
            } else {
                out.extend_from_slice(format!("-ERR Unknown Index name: {}\r\n", index).as_bytes());
            }
            false
        }
        Command::XdpInfo => {
            let info = crate::xdp::get_xdp_engine().info();
            write_resp_bulk(out, info.as_bytes());
            false
        }
        Command::XdpRuleAdd { action, cidr } => {
            match crate::xdp::get_xdp_engine().add_rule(action, &cidr) {
                Ok(id) => write_resp_integer(out, id as i64),
                Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
            }
            false
        }
        Command::XdpRuleDel(id) => {
            match crate::xdp::get_xdp_engine().del_rule(id) {
                Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                Err(e) => out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes()),
            }
            false
        }
        Command::XdpRuleList => {
            let rules = crate::xdp::get_xdp_engine().list_rules();
            out.extend_from_slice(format!("*{}\r\n", rules.len()).as_bytes());
            for r in rules {
                let line = format!("id:{} action:{} cidr:{}", r.id, r.action, r.cidr);
                write_resp_bulk(out, line.as_bytes());
            }
            false
        }
        Command::XdpStats => {
            use std::sync::atomic::Ordering;
            let engine = crate::xdp::get_xdp_engine();
            out.extend_from_slice(b"*12\r\n");
            write_resp_bulk(out, b"rx_packets");
            write_resp_integer(out, engine.rx_packets.load(Ordering::Relaxed) as i64);
            write_resp_bulk(out, b"rx_bytes");
            write_resp_integer(out, engine.rx_bytes.load(Ordering::Relaxed) as i64);
            write_resp_bulk(out, b"dropped_packets");
            write_resp_integer(out, engine.dropped_packets.load(Ordering::Relaxed) as i64);
            write_resp_bulk(out, b"redirected_packets");
            write_resp_integer(
                out,
                engine.redirected_packets.load(Ordering::Relaxed) as i64,
            );
            write_resp_bulk(out, b"pass_packets");
            write_resp_integer(out, engine.pass_packets.load(Ordering::Relaxed) as i64);
            write_resp_bulk(out, b"rate_limit_drops");
            write_resp_integer(out, engine.rate_limit_drops.load(Ordering::Relaxed) as i64);
            false
        }
        Command::XdpPacket(payload) => {
            let action = crate::xdp::get_xdp_engine().process_packet(&payload);
            let s = format!("+{}\r\n", action);
            out.extend_from_slice(s.as_bytes());
            false
        }
        Command::DflyCluster(sub) => {
            match sub {
                crate::resp::DflyClusterSubcommand::MyId => {
                    let hub = crate::cluster::get_cluster_hub(router.port);
                    let id = hub.my_id();
                    write_resp_bulk(out, id.as_bytes());
                }
                crate::resp::DflyClusterSubcommand::Config(json) => {
                    let hub = crate::cluster::get_cluster_hub(router.port);
                    match hub.dfly_cluster_config(&json) {
                        Ok(()) => out.extend_from_slice(b"+OK\r\n"),
                        Err(e) => out.extend_from_slice(format!("-{}\r\n", e).as_bytes()),
                    }
                }
                crate::resp::DflyClusterSubcommand::GetSlotInfo(slots) => {
                    out.extend_from_slice(format!("*{}\r\n", slots.len()).as_bytes());
                    for s in slots {
                        let key_count = router.count_keys_in_slot(s).await;
                        let mem_bytes = key_count * 128;
                        out.extend_from_slice(b"*3\r\n");
                        write_resp_integer(out, s as i64);
                        write_resp_integer(out, key_count as i64);
                        write_resp_integer(out, mem_bytes as i64);
                    }
                }
                crate::resp::DflyClusterSubcommand::FlushSlots(ranges) => {
                    let _ = router.flush_slots(&ranges).await;
                    out.extend_from_slice(b"+OK\r\n");
                }
                crate::resp::DflyClusterSubcommand::SlotMigrationStatus => {
                    let hub = crate::cluster::get_cluster_hub(router.port);
                    hub.dfly_slot_migration_status(out);
                }
            }
            false
        }
        Command::DflyMigrate(sub) => {
            let hub = crate::cluster::get_cluster_hub(router.port);
            match sub {
                crate::resp::DflyMigrateSubcommand::Init {
                    source_id,
                    num_shards,
                    slots,
                } => {
                    hub.dfly_migrate_init(&source_id, num_shards, &slots);
                    out.extend_from_slice(b"+OK\r\n");
                }
                crate::resp::DflyMigrateSubcommand::Flow { source_id, flow_id } => {
                    hub.dfly_migrate_flow(&source_id, flow_id);
                    out.extend_from_slice(b"+OK\r\n");
                }
                crate::resp::DflyMigrateSubcommand::Ack { flow_id } => {
                    hub.dfly_migrate_ack(flow_id);
                    out.extend_from_slice(b"+OK\r\n");
                }
            }
            false
        }
        Command::Stick(keys) => {
            let count = router.stick(&keys).await;
            write_resp_integer(out, count as i64);
            false
        }
        Command::Unstick(keys) => {
            let count = router.unstick(&keys).await;
            write_resp_integer(out, count as i64);
            false
        }
        Command::Sticky(key) => {
            let sticky = router.is_sticky(&key).await;
            write_resp_integer(out, if sticky { 1 } else { 0 });
            false
        }
        Command::MemcachedSet {
            key,
            flags: _,
            exptime,
            bytes: _,
            noreply,
            data,
        } => {
            let dur = if exptime > 0 {
                Some(std::time::Duration::from_secs(exptime as u64))
            } else {
                None
            };
            router.set(key, data, dur).await;
            if !noreply {
                out.extend_from_slice(b"STORED\r\n");
            }
            false
        }
        Command::MemcachedAdd {
            key,
            flags: _,
            exptime,
            bytes: _,
            noreply,
            data,
        } => {
            if router.exists(key.clone()).await {
                if !noreply {
                    out.extend_from_slice(b"NOT_STORED\r\n");
                }
            } else {
                let dur = if exptime > 0 {
                    Some(std::time::Duration::from_secs(exptime as u64))
                } else {
                    None
                };
                router.set(key, data, dur).await;
                if !noreply {
                    out.extend_from_slice(b"STORED\r\n");
                }
            }
            false
        }
        Command::MemcachedReplace {
            key,
            flags: _,
            exptime,
            bytes: _,
            noreply,
            data,
        } => {
            if router.exists(key.clone()).await {
                let dur = if exptime > 0 {
                    Some(std::time::Duration::from_secs(exptime as u64))
                } else {
                    None
                };
                router.set(key, data, dur).await;
                if !noreply {
                    out.extend_from_slice(b"STORED\r\n");
                }
            } else {
                if !noreply {
                    out.extend_from_slice(b"NOT_STORED\r\n");
                }
            }
            false
        }
        Command::MemcachedGet { keys } => {
            for k in keys {
                if let Some(val) = router.get(k.clone()).await {
                    out.extend_from_slice(
                        format!("VALUE {} 0 {}\r\n", String::from_utf8_lossy(&k), val.len())
                            .as_bytes(),
                    );
                    out.extend_from_slice(&val);
                    out.extend_from_slice(b"\r\n");
                }
            }
            out.extend_from_slice(b"END\r\n");
            false
        }
        Command::MemcachedDelete { key, noreply } => {
            let deleted = router.del(key).await;
            if !noreply {
                if deleted {
                    out.extend_from_slice(b"DELETED\r\n");
                } else {
                    out.extend_from_slice(b"NOT_FOUND\r\n");
                }
            }
            false
        }
        Command::MemcachedIncr {
            key,
            value,
            noreply,
        } => {
            match router.incr_by(key, value as i64).await {
                Ok(new_val) => {
                    if !noreply {
                        out.extend_from_slice(format!("{}\r\n", new_val).as_bytes());
                    }
                }
                Err(_) => {
                    if !noreply {
                        out.extend_from_slice(b"NOT_FOUND\r\n");
                    }
                }
            }
            false
        }
        Command::MemcachedDecr {
            key,
            value,
            noreply,
        } => {
            match router.incr_by(key, -(value as i64)).await {
                Ok(new_val) => {
                    let clamped = new_val.max(0);
                    if !noreply {
                        out.extend_from_slice(format!("{}\r\n", clamped).as_bytes());
                    }
                }
                Err(_) => {
                    if !noreply {
                        out.extend_from_slice(b"NOT_FOUND\r\n");
                    }
                }
            }
            false
        }
        Command::MemcachedStats => {
            let dbsize = router.dbsize().await;
            out.extend_from_slice(format!("STAT pid {}\r\nSTAT uptime 3600\r\nSTAT version 1.6.0-rudis-dragonfly\r\nSTAT curr_items {}\r\nEND\r\n", std::process::id(), dbsize).as_bytes());
            false
        }
        Command::MemcachedVersion => {
            out.extend_from_slice(b"VERSION 1.6.0-rudis-dragonfly\r\n");
            false
        }
        Command::MemcachedQuit => true,
        Command::Quit => {
            out.extend_from_slice(b"+OK\r\n");
            true
        }
        Command::Unknown(cmd_name) => {
            let resp = format!("-ERR unknown command '{}'\r\n", cmd_name);
            out.extend_from_slice(resp.as_bytes());
            false
        }
        Command::Memory(sub) => {
            match sub {
                MemorySubcommand::Usage { key } => {
                    let target = router.target_shard(&key);
                    if target == router.shard_id {
                        if let Some((val, _)) = router.local_db.borrow_mut().get_entry(&key) {
                            let size = 24 + key.len() + val.approx_bytes();
                            write_resp_integer(out, size as i64);
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        if let Some(val) = router.get(key.clone()).await {
                            let size = 24 + key.len() + val.len();
                            write_resp_integer(out, size as i64);
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    }
                }
                MemorySubcommand::Stats => {
                    out.extend_from_slice(b"*2\r\n$10\r\npeak.alloc\r\n:1048576\r\n");
                }
                MemorySubcommand::Purge => {
                    out.extend_from_slice(b"+OK\r\n");
                }
                MemorySubcommand::Doctor => {
                    out.extend_from_slice(
                        b"+Hi Sam, I can't find any memory issues in your instance.\r\n",
                    );
                }
            }
            false
        }
        Command::Debug(ref args) => {
            if let Some(sub) = args.first() {
                if sub.eq_ignore_ascii_case(b"object") {
                    if let Some(key) = args.get(1) {
                        let shard_id = router.target_shard(key);
                        if shard_id == router.shard_id {
                            if let Some(enc) = router.local_db.borrow_mut().object_encoding(key) {
                                out.extend_from_slice(format!("+Value at:0x12345678 refcount:1 encoding:{} serializedlength:10 lru:0 lru_seconds_idle:0\r\n", enc).as_bytes());
                            } else {
                                out.extend_from_slice(b"-ERR no such key\r\n");
                            }
                        } else {
                            let res = router.execute_remote(shard_id, cmd).await;
                            out.extend_from_slice(&res);
                        }
                        return false;
                    }
                } else if sub.eq_ignore_ascii_case(b"set-allow-access-expired") {
                    let flag = args.get(1).map(|v| v.as_ref() == b"1").unwrap_or(false);
                    ALLOW_ACCESS_EXPIRED.store(flag, std::sync::atomic::Ordering::Relaxed);
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"set-active-expire") {
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"sleep") {
                    if let Some(arg) = args.get(1)
                        && let Ok(s) = std::str::from_utf8(arg)
                        && let Ok(secs) = s.parse::<f64>()
                    {
                        monoio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
                    }
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                }
            }
            out.extend_from_slice(b"+OK\r\n");
            false
        }
    }
}

pub fn target_shard_of_cmd(cmd: &Command, num_shards: usize) -> Option<usize> {
    match cmd {
        Command::Get(key)
        | Command::Getex { key, .. }
        | Command::Set { key, .. }
        | Command::Digest(key)
        | Command::IncrBy(key, _)
        | Command::Expire(key, _)
        | Command::Persist(key)
        | Command::Ttl(key, _)
        | Command::Hset { key, .. }
        | Command::Hsetnx { key, .. }
        | Command::Hmset { key, .. }
        | Command::Hget { key, .. }
        | Command::Hmget { key, .. }
        | Command::Hdel { key, .. }
        | Command::Hexists { key, .. }
        | Command::Hlen(key)
        | Command::Hgetall(key)
        | Command::Hkeys(key)
        | Command::Hvals(key)
        | Command::Hstrlen { key, .. }
        | Command::Hgetdel { key, .. }
        | Command::Lpush { key, .. }
        | Command::Rpush { key, .. }
        | Command::Lpushx { key, .. }
        | Command::Rpushx { key, .. }
        | Command::Lpop { key, .. }
        | Command::Rpop { key, .. }
        | Command::Lrange { key, .. }
        | Command::Llen(key)
        | Command::Lindex { key, .. }
        | Command::Sadd { key, .. }
        | Command::Srem { key, .. }
        | Command::Smembers(key)
        | Command::Sismember { key, .. }
        | Command::Scard(key)
        | Command::Spop { key, .. }
        | Command::Zadd { key, .. }
        | Command::Zrem { key, .. }
        | Command::Zscore { key, .. }
        | Command::Zcard(key)
        | Command::Zrank { key, .. }
        | Command::Zrevrank { key, .. }
        | Command::Zcount { key, .. }
        | Command::Zincrby { key, .. }
        | Command::Zrange { key, .. }
        | Command::Zrangestore { dst: key, .. }
        | Command::Zpopmin { key, .. }
        | Command::Zpopmax { key, .. }
        | Command::Type(key)
        | Command::Setnx { key, .. }
        | Command::Getset { key, .. }
        | Command::Getdel(key)
        | Command::Append { key, .. }
        | Command::Strlen(key)
        | Command::Expiretime(key, _)
        | Command::Setbit { key, .. }
        | Command::Getbit { key, .. }
        | Command::Bitcount { key, .. }
        | Command::Bitpos { key, .. }
        | Command::Pfadd { key, .. }
        | Command::Dump(key)
        | Command::Restore { key, .. }
        | Command::Xadd { key, .. }
        | Command::Xlen(key)
        | Command::Xrange { key, .. }
        | Command::Xrevrange { key, .. }
        | Command::Xdel { key, .. }
        | Command::Xtrim { key, .. }
        | Command::XgroupCreate { key, .. }
        | Command::XgroupDestroy { key, .. }
        | Command::XgroupCreateConsumer { key, .. }
        | Command::XgroupDelConsumer { key, .. }
        | Command::Xack { key, .. }
        | Command::Xpending { key, .. }
        | Command::Hincrby { key, .. }
        | Command::Hincrbyfloat { key, .. }
        | Command::Hrandfield { key, .. }
        | Command::Hscan { key, .. }
        | Command::Smismember { key, .. }
        | Command::Srandmember { key, .. }
        | Command::Sscan { key, .. }
        | Command::Zmscore { key, .. }
        | Command::Zrandmember { key, .. }
        | Command::Zremrangebyrank { key, .. }
        | Command::Zremrangebyscore { key, .. }
        | Command::Zremrangebylex { key, .. }
        | Command::Zlexcount { key, .. }
        | Command::Zscan { key, .. }
        | Command::Ltrim { key, .. }
        | Command::Lset { key, .. }
        | Command::Lrem { key, .. }
        | Command::Lpos { key, .. }
        | Command::Linsert { key, .. }
        | Command::Incrbyfloat { key, .. }
        | Command::Setrange { key, .. }
        | Command::Getrange { key, .. }
        | Command::JsonSet { key, .. }
        | Command::JsonGet { key, .. }
        | Command::JsonDel { key, .. }
        | Command::JsonType { key, .. }
        | Command::JsonNumIncrBy { key, .. }
        | Command::JsonNumMultBy { key, .. }
        | Command::JsonStrAppend { key, .. }
        | Command::JsonStrLen { key, .. }
        | Command::JsonArrAppend { key, .. }
        | Command::JsonArrLen { key, .. }
        | Command::JsonArrPop { key, .. }
        | Command::JsonObjKeys { key, .. }
        | Command::JsonObjLen { key, .. }
        | Command::JsonToggle { key, .. }
        | Command::JsonClear { key, .. }
        | Command::Geoadd { key, .. }
        | Command::Geodist { key, .. }
        | Command::Geopos { key, .. }
        | Command::Geohash { key, .. }
        | Command::Georadius { key, .. }
        | Command::Georadiusbymember { key, .. }
        | Command::Geosearch { key, .. }
        | Command::BfReserve { key, .. }
        | Command::BfAdd { key, .. }
        | Command::BfMadd { key, .. }
        | Command::BfExists { key, .. }
        | Command::BfMexists { key, .. }
        | Command::BfInfo(key)
        | Command::CfReserve { key, .. }
        | Command::CfAdd { key, .. }
        | Command::CfAddnx { key, .. }
        | Command::CfExists { key, .. }
        | Command::CfDel { key, .. }
        | Command::CfInfo(key)
        | Command::CmsInitbydim { key, .. }
        | Command::CmsInitbyprob { key, .. }
        | Command::CmsIncrby { key, .. }
        | Command::CmsQuery { key, .. }
        | Command::CmsInfo(key)
        | Command::TopkReserve { key, .. }
        | Command::TopkAdd { key, .. }
        | Command::TopkQuery { key, .. }
        | Command::TopkList(key)
        | Command::TopkInfo(key)
        | Command::Sticky(key)
        | Command::Delex { key, .. }
        | Command::MemcachedSet { key, .. }
        | Command::MemcachedAdd { key, .. }
        | Command::MemcachedReplace { key, .. }
        | Command::MemcachedDelete { key, .. }
        | Command::MemcachedIncr { key, .. }
        | Command::MemcachedDecr { key, .. }
        | Command::CrdtSet { key, .. }
        | Command::CrdtGet(key)
        | Command::CrdtDel(key)
        | Command::CrdtIncrby { key, .. }
        | Command::CrdtSadd { key, .. }
        | Command::CrdtSmembers(key)
        | Command::CrdtSrem { key, .. } => Some(target_shard(key, num_shards)),
        Command::Smove {
            source,
            destination,
            ..
        } => {
            let s1 = target_shard(source, num_shards);
            let s2 = target_shard(destination, num_shards);
            if s1 == s2 { Some(s1) } else { None }
        }
        Command::Lmove {
            source,
            destination,
            ..
        } => {
            let s1 = target_shard(source, num_shards);
            let s2 = target_shard(destination, num_shards);
            if s1 == s2 { Some(s1) } else { None }
        }
        Command::Sort {
            key,
            store: Some(dest),
            ..
        } => {
            let s1 = target_shard(key, num_shards);
            let s2 = target_shard(dest, num_shards);
            if s1 == s2 { Some(s1) } else { None }
        }
        Command::Sort {
            key, store: None, ..
        } => Some(target_shard(key, num_shards)),
        Command::Pfcount { keys } if keys.len() == 1 => Some(target_shard(&keys[0], num_shards)),
        Command::Rename { key, newkey, .. } => {
            let s1 = target_shard(key, num_shards);
            let s2 = target_shard(newkey, num_shards);
            if s1 == s2 { Some(s1) } else { None }
        }
        Command::Pfmerge { destkey, srckeys }
            if srckeys
                .iter()
                .all(|k| target_shard(k, num_shards) == target_shard(destkey, num_shards)) =>
        {
            Some(target_shard(destkey, num_shards))
        }
        Command::Bitop {
            destkey, srckeys, ..
        } if srckeys
            .iter()
            .all(|k| target_shard(k, num_shards) == target_shard(destkey, num_shards)) =>
        {
            Some(target_shard(destkey, num_shards))
        }
        Command::Touch(keys) | Command::Del(keys) | Command::Exists(keys) if keys.len() == 1 => {
            Some(target_shard(&keys[0], num_shards))
        }
        Command::Xread { keys, .. } | Command::Xreadgroup { keys, .. }
            if !keys.is_empty()
                && keys
                    .iter()
                    .all(|k| target_shard(k, num_shards) == target_shard(&keys[0], num_shards)) =>
        {
            Some(target_shard(&keys[0], num_shards))
        }
        Command::Sinter(keys)
        | Command::Sunion(keys)
        | Command::Sdiff(keys)
        | Command::Zdiff { keys, .. }
        | Command::Zinter { keys, .. }
        | Command::Zunion { keys, .. }
            if !keys.is_empty()
                && keys
                    .iter()
                    .all(|k| target_shard(k, num_shards) == target_shard(&keys[0], num_shards)) =>
        {
            Some(target_shard(&keys[0], num_shards))
        }
        Command::Sinterstore { destination, keys }
        | Command::Sunionstore { destination, keys }
        | Command::Sdiffstore { destination, keys }
        | Command::Zunionstore {
            destination, keys, ..
        }
        | Command::Zinterstore {
            destination, keys, ..
        }
        | Command::Zdiffstore { destination, keys }
            if keys
                .iter()
                .all(|k| target_shard(k, num_shards) == target_shard(destination, num_shards)) =>
        {
            Some(target_shard(destination, num_shards))
        }
        _ => None,
    }
}

pub fn execute_local_command(
    cmd: &Command,
    db: &mut ShardDb,
    out: &mut Vec<u8>,
    aof: Option<&RefCell<crate::aof::AofWriter>>,
) -> bool {
    macro_rules! record_change {
        ($cmd_expr:expr) => {
            DIRTY_CHANGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            for k in cmd_keys($cmd_expr) {
                touch_watched_key(db.port, k.as_ref());
            }
            let need_aof = aof.is_some();
            let need_rep = crate::replication::has_connected_replicas(db.port);
            if need_aof || need_rep {
                if let Some(bytes) = crate::aof::command_to_resp($cmd_expr) {
                    if let Some(aof_w) = aof {
                        aof_w.borrow_mut().append(&bytes);
                    }
                    if need_rep {
                        crate::replication::propagate_bytes(db.port, &bytes);
                    }
                }
            }
        };
    }
    match cmd {
        Command::Get(key) => {
            match db.get(key) {
                Some(v) => {
                    write_resp_bulk(out, &v);
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Getex {
            key,
            expire_in,
            persist,
        } => {
            let val = db.get(key.as_ref());
            match val {
                Some(v) => {
                    if *persist {
                        db.persist(key.as_ref());
                    } else if let Some(exp) = expire_in {
                        db.expire(key.as_ref(), *exp);
                    }
                    write_resp_bulk(out, &v);
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Mget(keys) => {
            out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
            for k in keys {
                match db.get(k) {
                    Some(v) => {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                    None => {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
            }
            false
        }
        Command::Set {
            key,
            value,
            expire_in,
            condition,
            get,
            keepttl,
            past_expired,
        } => {
            let current_val = match db.table.get(key) {
                Ok(val) => val,
                Err(err) => {
                    if *get
                        || matches!(
                            condition,
                            crate::resp::SetCondition::Ifeq(_)
                                | crate::resp::SetCondition::Ifne(_)
                                | crate::resp::SetCondition::Ifdeq(_)
                                | crate::resp::SetCondition::Ifdne(_)
                        )
                    {
                        write_resp_err(out, err);
                        return false;
                    }
                    None
                }
            };

            let exists = db.exists(key);
            let condition_met = match condition {
                crate::resp::SetCondition::None => true,
                crate::resp::SetCondition::Nx => !exists,
                crate::resp::SetCondition::Xx => exists,
                crate::resp::SetCondition::Ifeq(expected) => current_val.as_ref() == Some(expected),
                crate::resp::SetCondition::Ifne(expected) => match &current_val {
                    None => true,
                    Some(val) => val != expected,
                },
                crate::resp::SetCondition::Ifdeq(expected_digest) => match &current_val {
                    None => false,
                    Some(val) => {
                        if expected_digest.len() != 16
                            || !expected_digest.iter().all(|b| b.is_ascii_hexdigit())
                        {
                            write_resp_err(
                                out,
                                "ERR digest must be exactly 16 hexadecimal characters",
                            );
                            return false;
                        }
                        let d = crate::table::compute_digest(val);
                        d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected_digest))
                    }
                },
                crate::resp::SetCondition::Ifdne(expected_digest) => match &current_val {
                    None => true,
                    Some(val) => {
                        if expected_digest.len() != 16
                            || !expected_digest.iter().all(|b| b.is_ascii_hexdigit())
                        {
                            write_resp_err(
                                out,
                                "ERR digest must be exactly 16 hexadecimal characters",
                            );
                            return false;
                        }
                        let d = crate::table::compute_digest(val);
                        !d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected_digest))
                    }
                },
            };

            if !condition_met {
                if *get {
                    if let Some(v) = current_val {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                } else {
                    out.extend_from_slice(b"$-1\r\n");
                }
                return false;
            }

            if *past_expired {
                crate::table::inc_expired_keys();
                if exists {
                    db.del(key);
                    record_change!(cmd);
                }
                if *get {
                    if let Some(v) = current_val {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                } else {
                    out.extend_from_slice(b"+OK\r\n");
                }
                return false;
            }

            db.set_extended(key.clone(), value.clone(), *expire_in, *keepttl);
            record_change!(cmd);

            if *get {
                if let Some(v) = current_val {
                    out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                    out.extend_from_slice(&v);
                    out.extend_from_slice(b"\r\n");
                } else {
                    out.extend_from_slice(b"$-1\r\n");
                }
            } else {
                out.extend_from_slice(b"+OK\r\n");
            }
            false
        }
        Command::Mset(pairs) => {
            for (k, v) in pairs {
                db.set(k.clone(), v.clone(), None);
            }
            record_change!(cmd);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Msetex {
            pairs,
            condition,
            expiry,
        } => {
            let pass = match condition {
                MsetexCondition::None => true,
                MsetexCondition::Nx => pairs.iter().all(|(k, _)| !db.exists(k)),
                MsetexCondition::Xx => pairs.iter().all(|(k, _)| db.exists(k)),
            };

            if !pass {
                out.extend_from_slice(b":0\r\n");
                return false;
            }

            for (k, v) in pairs {
                let ttl = match expiry {
                    MsetexExpiry::None => None,
                    MsetexExpiry::KeepTtl => db.get_entry(k).and_then(|(_, exp)| exp),
                    MsetexExpiry::ExpireIn(d) => Some(*d),
                };
                db.set(k.clone(), v.clone(), ttl);
            }
            record_change!(cmd);
            match condition {
                MsetexCondition::None => {
                    out.extend_from_slice(b"+OK\r\n");
                }
                MsetexCondition::Nx | MsetexCondition::Xx => {
                    out.extend_from_slice(b":1\r\n");
                }
            }
            false
        }
        Command::Lcs {
            key1,
            key2,
            len_only,
            idx,
            min_match_len,
            with_match_len,
        } => {
            let val1 = match db.table.get(key1) {
                Ok(v) => v.unwrap_or_default(),
                Err(err) => {
                    write_resp_err(out, err);
                    return false;
                }
            };
            let val2 = match db.table.get(key2) {
                Ok(v) => v.unwrap_or_default(),
                Err(err) => {
                    write_resp_err(out, err);
                    return false;
                }
            };

            let s1 = val1.as_ref();
            let s2 = val2.as_ref();
            let m = s1.len();
            let n = s2.len();

            let mut dp = vec![vec![0u32; n + 1]; m + 1];
            for i in 1..=m {
                for j in 1..=n {
                    if s1[i - 1] == s2[j - 1] {
                        dp[i][j] = dp[i - 1][j - 1] + 1;
                    } else {
                        dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
                    }
                }
            }

            let lcs_len = dp[m][n] as usize;

            if *len_only {
                out.extend_from_slice(format!(":{}\r\n", lcs_len).as_bytes());
                return false;
            }

            if *idx {
                let mut matches = Vec::new();
                let mut i = m;
                let mut j = n;
                while i > 0 && j > 0 {
                    if s1[i - 1] == s2[j - 1] {
                        let end1 = i - 1;
                        let end2 = j - 1;
                        while i > 0 && j > 0 && s1[i - 1] == s2[j - 1] {
                            i -= 1;
                            j -= 1;
                        }
                        let start1 = i;
                        let start2 = j;
                        let match_len = end1 - start1 + 1;
                        if match_len >= *min_match_len {
                            matches.push(((start1, end1), (start2, end2), match_len));
                        }
                    } else if dp[i - 1][j] >= dp[i][j - 1] {
                        i -= 1;
                    } else {
                        j -= 1;
                    }
                }

                out.extend_from_slice(b"*4\r\n$7\r\nmatches\r\n");
                out.extend_from_slice(format!("*{}\r\n", matches.len()).as_bytes());
                for ((s1_idx, e1_idx), (s2_idx, e2_idx), match_len) in matches {
                    if *with_match_len {
                        out.extend_from_slice(
                            format!(
                                "*3\r\n*2\r\n:{}\r\n:{}\r\n*2\r\n:{}\r\n:{}\r\n:{}\r\n",
                                s1_idx, e1_idx, s2_idx, e2_idx, match_len
                            )
                            .as_bytes(),
                        );
                    } else {
                        out.extend_from_slice(
                            format!(
                                "*2\r\n*2\r\n:{}\r\n:{}\r\n*2\r\n:{}\r\n:{}\r\n",
                                s1_idx, e1_idx, s2_idx, e2_idx
                            )
                            .as_bytes(),
                        );
                    }
                }
                out.extend_from_slice(format!("$3\r\nlen\r\n:{}\r\n", lcs_len).as_bytes());
                return false;
            }

            let mut lcs_bytes = Vec::with_capacity(lcs_len);
            let mut i = m;
            let mut j = n;
            while i > 0 && j > 0 {
                if s1[i - 1] == s2[j - 1] {
                    lcs_bytes.push(s1[i - 1]);
                    i -= 1;
                    j -= 1;
                } else if dp[i - 1][j] >= dp[i][j - 1] {
                    i -= 1;
                } else {
                    j -= 1;
                }
            }
            lcs_bytes.reverse();
            out.extend_from_slice(format!("${}\r\n", lcs_bytes.len()).as_bytes());
            out.extend_from_slice(&lcs_bytes);
            out.extend_from_slice(b"\r\n");
            false
        }
        Command::Del(keys) => {
            let mut count = 0usize;
            for k in keys {
                if db.del(k) {
                    count += 1;
                    crate::search::delete_document_hook(&String::from_utf8_lossy(k));
                }
            }
            if count > 0 {
                record_change!(cmd);
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::Exists(keys) => {
            let mut count = 0usize;
            for k in keys {
                if db.exists(k) {
                    count += 1;
                }
            }
            write_resp_integer(out, count as i64);
            false
        }
        Command::IncrBy(key, delta) => {
            match db.incr_by(key.clone(), *delta) {
                Ok(val) => {
                    record_change!(cmd);
                    write_resp_integer(out, val);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Expire(key, duration) => {
            let res = db.expire(key, *duration);
            if res {
                record_change!(cmd);
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Persist(key) => {
            let res = db.persist(key);
            if res {
                record_change!(cmd);
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Ttl(key, in_millis) => {
            let res = db.ttl(key, *in_millis);
            write_resp_integer(out, res);
            false
        }
        Command::Hset { key, fields } => {
            match db.hset(key.clone(), fields.clone()) {
                Ok(count) => {
                    record_change!(cmd);
                    let str_fields: std::collections::HashMap<String, String> = fields
                        .iter()
                        .map(|(k, v)| {
                            (
                                String::from_utf8_lossy(k).to_string(),
                                String::from_utf8_lossy(v).to_string(),
                            )
                        })
                        .collect();
                    crate::search::index_document_hook(&String::from_utf8_lossy(key), str_fields);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hsetnx { key, field, value } => {
            match db.hsetnx(key.clone(), field.clone(), value.clone()) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hmset { key, fields } => {
            match db.hset(key.clone(), fields.clone()) {
                Ok(_) => {
                    record_change!(cmd);
                    let str_fields: std::collections::HashMap<String, String> = fields
                        .iter()
                        .map(|(k, v)| {
                            (
                                String::from_utf8_lossy(k).to_string(),
                                String::from_utf8_lossy(v).to_string(),
                            )
                        })
                        .collect();
                    crate::search::index_document_hook(&String::from_utf8_lossy(key), str_fields);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hget { key, field } => {
            match db.hget(key, field) {
                Ok(Some(v)) => {
                    out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                    out.extend_from_slice(&v);
                    out.extend_from_slice(b"\r\n");
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hmget { key, fields } => {
            match db.hmget(key, fields) {
                Ok(vals) => {
                    out.extend_from_slice(format!("*{}\r\n", vals.len()).as_bytes());
                    for v in vals {
                        match v {
                            Some(val) => {
                                out.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
                                out.extend_from_slice(&val);
                                out.extend_from_slice(b"\r\n");
                            }
                            None => {
                                out.extend_from_slice(b"$-1\r\n");
                            }
                        }
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hdel { key, fields } => {
            match db.hdel(key, fields) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hexists { key, field } => {
            match db.hexists(key, field) {
                Ok(exists) => {
                    if exists {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hlen(key) => {
            match db.hlen(key) {
                Ok(len) => {
                    write_resp_integer(out, len as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hgetall(key) => {
            match db.hgetall(key) {
                Ok(pairs) => {
                    out.extend_from_slice(format!("*{}\r\n", pairs.len() * 2).as_bytes());
                    for (k, v) in pairs {
                        out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n");
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hkeys(key) => {
            match db.hkeys(key) {
                Ok(keys) => {
                    out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
                    for k in keys {
                        out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                        out.extend_from_slice(&k);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hvals(key) => {
            match db.hvals(key) {
                Ok(vals) => {
                    out.extend_from_slice(format!("*{}\r\n", vals.len()).as_bytes());
                    for v in vals {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hstrlen { key, field } => {
            match db.hstrlen(key, field) {
                Ok(len) => {
                    write_resp_integer(out, len as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Hgetdel { key, fields } => {
            match db.hgetdel(key, fields) {
                Ok((vals, deleted_fields)) => {
                    if !deleted_fields.is_empty() {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!("*{}\r\n", vals.len()).as_bytes());
                    for v in vals {
                        match v {
                            Some(val) => {
                                out.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
                                out.extend_from_slice(&val);
                                out.extend_from_slice(b"\r\n");
                            }
                            None => {
                                out.extend_from_slice(b"$-1\r\n");
                            }
                        }
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        // LIST COMMANDS
        Command::Lpush { key, values } => {
            match db.lpush(key.clone(), values.clone()) {
                Ok(len) => {
                    record_change!(cmd);
                    notify_list_or_defer(db, key);
                    write_resp_integer(out, len as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Rpush { key, values } => {
            match db.rpush(key.clone(), values.clone()) {
                Ok(len) => {
                    record_change!(cmd);
                    notify_list_or_defer(db, key);
                    write_resp_integer(out, len as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Lpushx { key, values } => {
            match db.lpushx(key.clone(), values.clone()) {
                Ok(len) => {
                    if len > 0 {
                        record_change!(cmd);
                        notify_list_or_defer(db, key);
                    }
                    write_resp_integer(out, len as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Rpushx { key, values } => {
            match db.rpushx(key.clone(), values.clone()) {
                Ok(len) => {
                    if len > 0 {
                        record_change!(cmd);
                        notify_list_or_defer(db, key);
                    }
                    write_resp_integer(out, len as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Lpop { key, count } => {
            let n = count.unwrap_or(1);
            match db.lpop(key, n) {
                Ok(popped) => {
                    if !popped.is_empty() {
                        record_change!(cmd);
                    }
                    if count.is_some() {
                        if !popped.is_empty() {
                            out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                            for v in popped {
                                out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                                out.extend_from_slice(&v);
                                out.extend_from_slice(b"\r\n");
                            }
                        } else if *count == Some(0) && db.exists(key) {
                            out.extend_from_slice(b"*0\r\n");
                        } else {
                            write_resp_null_array(out);
                        }
                    } else if let Some(first) = popped.into_iter().next() {
                        out.extend_from_slice(format!("${}\r\n", first.len()).as_bytes());
                        out.extend_from_slice(&first);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        write_resp_null(out);
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Rpop { key, count } => {
            let n = count.unwrap_or(1);
            match db.rpop(key, n) {
                Ok(popped) => {
                    if !popped.is_empty() {
                        record_change!(cmd);
                    }
                    if count.is_some() {
                        if !popped.is_empty() {
                            out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                            for v in popped {
                                out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                                out.extend_from_slice(&v);
                                out.extend_from_slice(b"\r\n");
                            }
                        } else if *count == Some(0) && db.exists(key) {
                            out.extend_from_slice(b"*0\r\n");
                        } else {
                            write_resp_null_array(out);
                        }
                    } else if let Some(first) = popped.into_iter().next() {
                        out.extend_from_slice(format!("${}\r\n", first.len()).as_bytes());
                        out.extend_from_slice(&first);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        write_resp_null(out);
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Lmpop {
            keys,
            where_from,
            count,
        } => {
            let mut popped: Option<(Bytes, Vec<Bytes>)> = None;
            for k in keys {
                if !db.exists(k) {
                    continue;
                }
                let res = match where_from {
                    crate::table::ListDirection::Left => db.lpop(k, *count),
                    crate::table::ListDirection::Right => db.rpop(k, *count),
                };
                match res {
                    Ok(vals) => {
                        if !vals.is_empty() {
                            record_change!(cmd);
                            popped = Some((k.clone(), vals));
                            break;
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                        return false;
                    }
                }
            }
            if let Some((k, vals)) = popped {
                out.extend_from_slice(b"*2\r\n$");
                out.extend_from_slice(k.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n*");
                out.extend_from_slice(vals.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for v in &vals {
                    out.extend_from_slice(b"$");
                    out.extend_from_slice(v.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(v);
                    out.extend_from_slice(b"\r\n");
                }
            } else {
                write_resp_null_array(out);
            }
            false
        }
        Command::Blmpop { .. } => false,
        Command::Llen(key) => {
            match db.llen(key) {
                Ok(len) => {
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Lindex { key, index } => {
            match db.lindex(key, *index) {
                Ok(Some(v)) => {
                    out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                    out.extend_from_slice(&v);
                    out.extend_from_slice(b"\r\n");
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Lrange { key, start, stop } => {
            match db.lrange(key, *start, *stop) {
                Ok(items) => {
                    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                    for v in items {
                        out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                        out.extend_from_slice(&v);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        // SET COMMANDS
        Command::Sadd { key, members } => {
            match db.sadd(key.clone(), members.clone()) {
                Ok(added) => {
                    if added > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", added).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Srem { key, members } => {
            match db.srem(key, members) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Smembers(key) => {
            match db.smembers(key) {
                Ok(members) => {
                    out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
                    for m in members {
                        out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Sismember { key, member } => {
            match db.sismember(key, member) {
                Ok(is_mem) => {
                    if is_mem {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Scard(key) => {
            match db.scard(key) {
                Ok(card) => {
                    out.extend_from_slice(format!(":{}\r\n", card).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Spop { key, count } => {
            let n = count.unwrap_or(1);
            match db.spop(key, n) {
                Ok(popped) => {
                    if !popped.is_empty() {
                        let srem_cmd = Command::Srem {
                            key: key.clone(),
                            members: popped.clone(),
                        };
                        record_change!(&srem_cmd);
                    }
                    if count.is_some() {
                        out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                        for m in popped {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    } else if let Some(first) = popped.into_iter().next() {
                        out.extend_from_slice(format!("${}\r\n", first.len()).as_bytes());
                        out.extend_from_slice(&first);
                        out.extend_from_slice(b"\r\n");
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Sinter(keys) => {
            match db.sinter(keys) {
                Ok(members) => {
                    out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
                    for m in members {
                        out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Sunion(keys) => {
            match db.sunion(keys) {
                Ok(members) => {
                    out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
                    for m in members {
                        out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Sdiff(keys) => {
            match db.sdiff(keys) {
                Ok(members) => {
                    out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
                    for m in members {
                        out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                        out.extend_from_slice(&m);
                        out.extend_from_slice(b"\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Sinterstore { destination, keys } => {
            match db.sinterstore(destination.clone(), keys) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Sunionstore { destination, keys } => {
            match db.sunionstore(destination.clone(), keys) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Sdiffstore { destination, keys } => {
            match db.sdiffstore(destination.clone(), keys) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Sintercard { keys, limit } => {
            match db.sintercard(keys, *limit) {
                Ok(card) => write_resp_integer(out, card as i64),
                Err(err) => write_resp_err(out, err),
            }
            false
        }
        Command::Sunioncard { keys, limit } => {
            match db.sunioncard(keys, *limit) {
                Ok(card) => write_resp_integer(out, card as i64),
                Err(err) => write_resp_err(out, err),
            }
            false
        }
        Command::Sdiffcard { keys, limit } => {
            match db.sdiffcard(keys, *limit) {
                Ok(card) => write_resp_integer(out, card as i64),
                Err(err) => write_resp_err(out, err),
            }
            false
        }
        // ZSET COMMANDS
        Command::Zadd {
            key,
            elements,
            flags,
        } => {
            match db.zadd(key.clone(), elements.clone(), *flags) {
                Ok((count, incr_score)) => {
                    if flags.incr {
                        if incr_score.is_some() {
                            record_change!(cmd);
                            notify_zset_or_defer(db, key);
                        }
                    } else if count > 0 {
                        record_change!(cmd);
                        notify_zset_or_defer(db, key);
                    }
                    if flags.incr {
                        if let Some(score) = incr_score {
                            let s = format_score(score);
                            out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        write_resp_integer(out, count as i64);
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zrem { key, members } => {
            match db.zrem(key, members) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zscore { key, member } => {
            match db.zscore(key, member) {
                Ok(Some(score)) => {
                    let s = format_score(score);
                    out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zcard(key) => {
            match db.zcard(key) {
                Ok(card) => {
                    out.extend_from_slice(format!(":{}\r\n", card).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zrank {
            key,
            member,
            with_score,
        } => {
            match db.zrank(key, member, false, *with_score) {
                Ok(Some((rank, score))) => {
                    if *with_score {
                        let s = format_score(score.unwrap_or(0.0));
                        out.extend_from_slice(
                            format!("*2\r\n:{}\r\n${}\r\n{}\r\n", rank, s.len(), s).as_bytes(),
                        );
                    } else {
                        out.extend_from_slice(format!(":{}\r\n", rank).as_bytes());
                    }
                }
                Ok(None) => {
                    if *with_score {
                        out.extend_from_slice(b"*-1\r\n");
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zrevrank {
            key,
            member,
            with_score,
        } => {
            match db.zrank(key, member, true, *with_score) {
                Ok(Some((rank, score))) => {
                    if *with_score {
                        let s = format_score(score.unwrap_or(0.0));
                        out.extend_from_slice(
                            format!("*2\r\n:{}\r\n${}\r\n{}\r\n", rank, s.len(), s).as_bytes(),
                        );
                    } else {
                        out.extend_from_slice(format!(":{}\r\n", rank).as_bytes());
                    }
                }
                Ok(None) => {
                    if *with_score {
                        out.extend_from_slice(b"*-1\r\n");
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zcount {
            key,
            min,
            min_inc,
            max,
            max_inc,
        } => {
            match db.zcount(key, *min, *min_inc, *max, *max_inc) {
                Ok(count) => {
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zincrby { key, delta, member } => {
            match db.zincrby(key.clone(), *delta, member.clone()) {
                Ok(score) => {
                    record_change!(cmd);
                    notify_zset_or_defer(db, key);
                    let s = format_score(score);
                    out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zrange { key, opts } => {
            match db.zrange(key, opts) {
                Ok(items) => {
                    if opts.with_scores {
                        if CURRENT_CLIENT_RESP3.get() {
                            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                            for (m, s) in items {
                                out.extend_from_slice(b"*2\r\n");
                                write_resp_bulk(out, &m);
                                write_resp_score(out, s);
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", items.len() * 2).as_bytes());
                            for (m, s) in items {
                                write_resp_bulk(out, &m);
                                write_resp_score(out, s);
                            }
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (m, _) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zrangestore { dst, src, opts } => {
            match db.zrangestore(dst, src, opts) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zpopmin { key, count } => {
            let n = count.unwrap_or(1);
            match db.zpopmin(key, n) {
                Ok(popped) => {
                    if !popped.is_empty() {
                        let zrem_cmd = Command::Zrem {
                            key: key.clone(),
                            members: popped.iter().map(|(m, _)| m.clone()).collect(),
                        };
                        record_change!(&zrem_cmd);
                    }
                    if count.is_none() {
                        if popped.is_empty() {
                            out.extend_from_slice(b"*0\r\n");
                        } else {
                            out.extend_from_slice(b"*2\r\n");
                            let (m, s) = &popped[0];
                            write_resp_bulk(out, m);
                            write_resp_score(out, *s);
                        }
                    } else if CURRENT_CLIENT_RESP3.get() {
                        out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                        for (m, s) in &popped {
                            out.extend_from_slice(b"*2\r\n");
                            write_resp_bulk(out, m);
                            write_resp_score(out, *s);
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", popped.len() * 2).as_bytes());
                        for (m, s) in &popped {
                            write_resp_bulk(out, m);
                            write_resp_score(out, *s);
                        }
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zpopmax { key, count } => {
            let n = count.unwrap_or(1);
            match db.zpopmax(key, n) {
                Ok(popped) => {
                    if !popped.is_empty() {
                        let zrem_cmd = Command::Zrem {
                            key: key.clone(),
                            members: popped.iter().map(|(m, _)| m.clone()).collect(),
                        };
                        record_change!(&zrem_cmd);
                    }
                    if count.is_none() {
                        if popped.is_empty() {
                            out.extend_from_slice(b"*0\r\n");
                        } else {
                            out.extend_from_slice(b"*2\r\n");
                            let (m, s) = &popped[0];
                            write_resp_bulk(out, m);
                            write_resp_score(out, *s);
                        }
                    } else if CURRENT_CLIENT_RESP3.get() {
                        out.extend_from_slice(format!("*{}\r\n", popped.len()).as_bytes());
                        for (m, s) in &popped {
                            out.extend_from_slice(b"*2\r\n");
                            write_resp_bulk(out, m);
                            write_resp_score(out, *s);
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", popped.len() * 2).as_bytes());
                        for (m, s) in &popped {
                            write_resp_bulk(out, m);
                            write_resp_score(out, *s);
                        }
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zmpop {
            keys,
            is_min,
            count,
        } => {
            let mut popped: Option<(Bytes, Vec<(Bytes, f64)>)> = None;
            for k in keys {
                if !db.exists(k) {
                    continue;
                }
                let res = if *is_min {
                    db.zpopmin(k, *count)
                } else {
                    db.zpopmax(k, *count)
                };
                match res {
                    Ok(items) => {
                        if !items.is_empty() {
                            let rep_cmd = if *is_min {
                                Command::Zpopmin {
                                    key: k.clone(),
                                    count: Some(items.len()),
                                }
                            } else {
                                Command::Zpopmax {
                                    key: k.clone(),
                                    count: Some(items.len()),
                                }
                            };
                            record_change!(&rep_cmd);
                            popped = Some((k.clone(), items));
                            break;
                        }
                    }
                    Err(err) => {
                        write_resp_err(out, err);
                        return false;
                    }
                }
            }
            if let Some((k, items)) = popped {
                format_zmpop_response(out, &k, &items);
            } else {
                write_resp_null_array(out);
            }
            false
        }
        Command::Zunionstore {
            destination,
            keys,
            weights,
            aggregate,
        } => {
            match db.zunionstore(destination.clone(), keys, weights, *aggregate) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zinterstore {
            destination,
            keys,
            weights,
            aggregate,
        } => {
            match db.zinterstore(destination.clone(), keys, weights, *aggregate) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zdiffstore { destination, keys } => {
            match db.zdiffstore(destination.clone(), keys) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zdiff { keys, with_scores } => {
            match db.zdiff(keys, *with_scores) {
                Ok(items) => {
                    if *with_scores {
                        if CURRENT_CLIENT_RESP3.get() {
                            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                            for (m, s) in items {
                                out.extend_from_slice(b"*2\r\n");
                                write_resp_bulk(out, &m);
                                write_resp_score(out, s);
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", items.len() * 2).as_bytes());
                            for (m, s) in items {
                                write_resp_bulk(out, &m);
                                write_resp_score(out, s);
                            }
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (m, _) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zinter {
            keys,
            weights,
            aggregate,
            with_scores,
        } => {
            match db.zinter(keys, weights, *aggregate, *with_scores) {
                Ok(items) => {
                    if *with_scores {
                        if CURRENT_CLIENT_RESP3.get() {
                            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                            for (m, s) in items {
                                out.extend_from_slice(b"*2\r\n");
                                write_resp_bulk(out, &m);
                                write_resp_score(out, s);
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", items.len() * 2).as_bytes());
                            for (m, s) in items {
                                write_resp_bulk(out, &m);
                                write_resp_score(out, s);
                            }
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (m, _) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zunion {
            keys,
            weights,
            aggregate,
            with_scores,
        } => {
            match db.zunion(keys, weights, *aggregate, *with_scores) {
                Ok(items) => {
                    if *with_scores {
                        if CURRENT_CLIENT_RESP3.get() {
                            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                            for (m, s) in items {
                                out.extend_from_slice(b"*2\r\n");
                                write_resp_bulk(out, &m);
                                write_resp_score(out, s);
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", items.len() * 2).as_bytes());
                            for (m, s) in items {
                                write_resp_bulk(out, &m);
                                write_resp_score(out, s);
                            }
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (m, _) in items {
                            out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                            out.extend_from_slice(&m);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Zintercard { keys, limit } => {
            match db.zintercard(keys, *limit) {
                Ok(card) => write_resp_integer(out, card as i64),
                Err(err) => write_resp_err(out, err),
            }
            false
        }
        // GENERIC & DATABASE COMMANDS
        Command::Type(key) => {
            let t = db.type_of(key);
            out.extend_from_slice(format!("+{}\r\n", t).as_bytes());
            false
        }
        Command::Dbsize => {
            let n = db.dbsize();
            out.extend_from_slice(format!(":{}\r\n", n).as_bytes());
            false
        }
        Command::Select(_) => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Slowlog(sub) => {
            if sub.eq_ignore_ascii_case(b"reset") {
                out.extend_from_slice(b"+OK\r\n");
            } else if sub.eq_ignore_ascii_case(b"len") {
                out.extend_from_slice(b":0\r\n");
            } else {
                out.extend_from_slice(b"*0\r\n");
            }
            false
        }
        Command::Flushdb | Command::Flushall => {
            db.flushdb();
            record_change!(cmd);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Touch(keys) => {
            let n = db.touch(keys);
            out.extend_from_slice(format!(":{}\r\n", n).as_bytes());
            false
        }
        Command::Rename { key, newkey, nx } => {
            match db.rename(key, newkey.clone(), *nx) {
                Ok(success) => {
                    if success {
                        record_change!(cmd);
                        if db.type_of(newkey) == "list" {
                            notify_list_or_defer(db, newkey);
                        }
                        if *nx {
                            out.extend_from_slice(b":1\r\n");
                        } else {
                            out.extend_from_slice(b"+OK\r\n");
                        }
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        // EXTENDED STRING COMMANDS
        Command::Setnx { key, value } => {
            let set = db.setnx(key.clone(), value.clone());
            if set {
                record_change!(cmd);
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::Getset { key, value } => {
            match db.getset(key.clone(), value.clone()) {
                Ok(old) => {
                    record_change!(cmd);
                    match old {
                        Some(v) => {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                        None => out.extend_from_slice(b"$-1\r\n"),
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Getdel(key) => {
            match db.getdel(key) {
                Ok(old) => {
                    if old.is_some() {
                        record_change!(cmd);
                    }
                    match old {
                        Some(v) => {
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                        None => out.extend_from_slice(b"$-1\r\n"),
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Append { key, value } => {
            match db.append(key.clone(), value) {
                Ok(new_len) => {
                    record_change!(cmd);
                    out.extend_from_slice(format!(":{}\r\n", new_len).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Strlen(key) => {
            match db.strlen(key) {
                Ok(len) => {
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Msetnx(pairs) => {
            let any_exists = pairs.iter().any(|(k, _)| db.exists(k));
            if any_exists {
                out.extend_from_slice(b":0\r\n");
            } else {
                for (k, v) in pairs {
                    db.set(k.clone(), v.clone(), None);
                }
                record_change!(cmd);
                out.extend_from_slice(b":1\r\n");
            }
            false
        }
        Command::Ping(msg) => {
            match msg {
                Some(m) => {
                    out.extend_from_slice(format!("${}\r\n", m.len()).as_bytes());
                    out.extend_from_slice(m);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"+PONG\r\n");
                }
            }
            false
        }
        Command::CommandDocs => {
            out.extend_from_slice(b"*0\r\n");
            false
        }
        Command::Keys(pattern) => {
            let keys = db.keys(pattern);
            out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
            for k in keys {
                out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::Scan {
            cursor,
            pattern,
            count,
        } => {
            let cnt = count.unwrap_or(10);
            let (next_cursor, keys) = db.scan(*cursor as usize, pattern.as_deref(), cnt);
            let cursor_str = next_cursor.to_string();
            out.extend_from_slice(b"*2\r\n$");
            out.extend_from_slice(cursor_str.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(cursor_str.as_bytes());
            out.extend_from_slice(b"\r\n*");
            out.extend_from_slice(keys.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for k in keys {
                out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                out.extend_from_slice(&k);
                out.extend_from_slice(b"\r\n");
            }
            false
        }
        Command::Randomkey => {
            match db.random_key() {
                Some(k) => {
                    out.extend_from_slice(format!("${}\r\n", k.len()).as_bytes());
                    out.extend_from_slice(&k);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Expiretime(key, in_millis) => {
            let ts = db.expiretime(key, *in_millis);
            out.extend_from_slice(format!(":{}\r\n", ts).as_bytes());
            false
        }
        Command::Multi => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Exec => {
            out.extend_from_slice(b"-ERR EXEC without MULTI\r\n");
            false
        }
        Command::Discard => {
            out.extend_from_slice(b"-ERR DISCARD without MULTI\r\n");
            false
        }
        Command::Watch(_) | Command::Unwatch => {
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Setbit { key, offset, value } => {
            match db.setbit(key.clone(), *offset, *value) {
                Ok(old) => {
                    record_change!(cmd);
                    out.extend_from_slice(format!(":{}\r\n", old).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Getbit { key, offset } => {
            match db.getbit(key, *offset) {
                Ok(bit) => {
                    out.extend_from_slice(format!(":{}\r\n", bit).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Bitcount { key, start, end } => {
            match db.bitcount(key, *start, *end) {
                Ok(count) => {
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Bitpos {
            key,
            bit,
            start,
            end,
        } => {
            match db.bitpos(key, *bit, *start, *end) {
                Ok(pos) => {
                    out.extend_from_slice(format!(":{}\r\n", pos).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Bitop {
            op,
            destkey,
            srckeys,
        } => {
            match db.bitop(op, destkey.clone(), srckeys) {
                Ok(len) => {
                    record_change!(cmd);
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Pfadd { key, elements } => {
            match db.pfadd(key.clone(), elements) {
                Ok(updated) => {
                    if updated {
                        record_change!(cmd);
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Pfcount { keys } => {
            match db.pfcount(keys) {
                Ok(count) => {
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Pfmerge { destkey, srckeys } => {
            match db.pfmerge(destkey.clone(), srckeys) {
                Ok(()) => {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Dump(key) => {
            match db.dump(key) {
                Some(bytes) => {
                    out.extend_from_slice(format!("${}\r\n", bytes.len()).as_bytes());
                    out.extend_from_slice(&bytes);
                    out.extend_from_slice(b"\r\n");
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Restore {
            key,
            ttl_ms,
            serialized,
            replace,
            absttl,
        } => {
            match db.restore(key.clone(), *ttl_ms, serialized, *replace, *absttl) {
                Ok(()) => {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    if err.starts_with("BUSYKEY") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Xadd {
            key,
            nomkstream,
            maxlen,
            minid,
            id,
            fields,
        } => {
            match db.xadd(
                key.clone(),
                id.clone(),
                fields.clone(),
                *nomkstream,
                *maxlen,
                *minid,
            ) {
                Ok(Some(generated_id)) => {
                    let explicit_cmd = Command::Xadd {
                        key: key.clone(),
                        nomkstream: *nomkstream,
                        maxlen: *maxlen,
                        minid: *minid,
                        id: crate::table::StreamAddId::Explicit(generated_id),
                        fields: fields.clone(),
                    };
                    record_change!(&explicit_cmd);
                    let s = generated_id.to_string();
                    crate::block::get_block_hub_for_port(db.port)
                        .lock()
                        .unwrap()
                        .notify_stream(key);
                    out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Xlen(key) => {
            match db.xlen(key) {
                Ok(len) => {
                    out.extend_from_slice(format!(":{}\r\n", len).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Xrange {
            key,
            start,
            end,
            count,
        } => {
            match db.xrange(key, start, end, *count) {
                Ok(items) => {
                    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                    for (id, fields) in items {
                        let id_str = id.to_string();
                        out.extend_from_slice(
                            format!(
                                "*2\r\n${}\r\n{}\r\n*{}\r\n",
                                id_str.len(),
                                id_str,
                                fields.len() * 2
                            )
                            .as_bytes(),
                        );
                        for (f, v) in fields {
                            out.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                            out.extend_from_slice(&f);
                            out.extend_from_slice(b"\r\n");
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Xrevrange {
            key,
            end,
            start,
            count,
        } => {
            match db.xrevrange(key, end, start, *count) {
                Ok(items) => {
                    out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                    for (id, fields) in items {
                        let id_str = id.to_string();
                        out.extend_from_slice(
                            format!(
                                "*2\r\n${}\r\n{}\r\n*{}\r\n",
                                id_str.len(),
                                id_str,
                                fields.len() * 2
                            )
                            .as_bytes(),
                        );
                        for (f, v) in fields {
                            out.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                            out.extend_from_slice(&f);
                            out.extend_from_slice(b"\r\n");
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Xread {
            count, keys, ids, ..
        } => {
            match db.xread(keys, ids, *count) {
                Ok(streams) => {
                    if streams.is_empty() {
                        out.extend_from_slice(b"$-1\r\n");
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", streams.len()).as_bytes());
                        for (stream_key, entries) in streams {
                            out.extend_from_slice(
                                format!("*2\r\n${}\r\n", stream_key.len()).as_bytes(),
                            );
                            out.extend_from_slice(&stream_key);
                            out.extend_from_slice(format!("\r\n*{}\r\n", entries.len()).as_bytes());
                            for (id, fields) in entries {
                                let id_str = id.to_string();
                                out.extend_from_slice(
                                    format!(
                                        "*2\r\n${}\r\n{}\r\n*{}\r\n",
                                        id_str.len(),
                                        id_str,
                                        fields.len() * 2
                                    )
                                    .as_bytes(),
                                );
                                for (f, v) in fields {
                                    out.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                                    out.extend_from_slice(&f);
                                    out.extend_from_slice(b"\r\n");
                                    out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                                    out.extend_from_slice(&v);
                                    out.extend_from_slice(b"\r\n");
                                }
                            }
                        }
                    }
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Xdel { key, ids } => {
            match db.xdel(key, ids) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::Xtrim { key, maxlen, minid } => {
            match db.xtrim(key, *maxlen, *minid) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                }
            }
            false
        }
        Command::XgroupCreate {
            key,
            group,
            id,
            mkstream,
        } => {
            match db.xgroup_create(key.clone(), group.clone(), id, *mkstream) {
                Ok(()) => {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    if err.starts_with("BUSYGROUP") || err.starts_with("ERR") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::XgroupDestroy { key, group } => {
            match db.xgroup_destroy(key, group) {
                Ok(destroyed) => {
                    if destroyed {
                        record_change!(cmd);
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::XgroupCreateConsumer {
            key,
            group,
            consumer,
        } => {
            match db.xgroup_createconsumer(key, group, consumer.clone()) {
                Ok(created) => {
                    if created {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::XgroupDelConsumer {
            key,
            group,
            consumer,
        } => {
            match db.xgroup_delconsumer(key, group, consumer) {
                Ok(pending_count) => {
                    out.extend_from_slice(format!(":{}\r\n", pending_count).as_bytes());
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Xreadgroup {
            group,
            consumer,
            count,
            noack,
            keys,
            ids,
            ..
        } => {
            let mut all_results = Vec::new();
            let mut err = None;
            for (k, id_str) in keys.iter().zip(ids.iter()) {
                match db.xreadgroup(k, group, consumer.clone(), id_str, *count, *noack) {
                    Ok(entries) => {
                        if !entries.is_empty() {
                            all_results.push((k.clone(), entries));
                        }
                    }
                    Err(e) => {
                        err = Some(e);
                        break;
                    }
                }
            }
            if let Some(err) = err {
                if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                    out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                } else {
                    write_resp_err(out, err);
                }
            } else if all_results.is_empty() {
                out.extend_from_slice(b"$-1\r\n");
            } else {
                out.extend_from_slice(format!("*{}\r\n", all_results.len()).as_bytes());
                for (stream_key, entries) in all_results {
                    out.extend_from_slice(format!("*2\r\n${}\r\n", stream_key.len()).as_bytes());
                    out.extend_from_slice(&stream_key);
                    out.extend_from_slice(format!("\r\n*{}\r\n", entries.len()).as_bytes());
                    for (id, fields) in entries {
                        let id_str = id.to_string();
                        out.extend_from_slice(
                            format!(
                                "*2\r\n${}\r\n{}\r\n*{}\r\n",
                                id_str.len(),
                                id_str,
                                fields.len() * 2
                            )
                            .as_bytes(),
                        );
                        for (f, v) in fields {
                            out.extend_from_slice(format!("${}\r\n", f.len()).as_bytes());
                            out.extend_from_slice(&f);
                            out.extend_from_slice(b"\r\n");
                            out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                            out.extend_from_slice(&v);
                            out.extend_from_slice(b"\r\n");
                        }
                    }
                }
            }
            false
        }
        Command::Xack { key, group, ids } => {
            match db.xack(key, group, ids) {
                Ok(count) => {
                    if count > 0 {
                        record_change!(cmd);
                    }
                    out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Xpending { key, group, range } => {
            match range {
                None => match db.xpending_summary(key, group) {
                    Ok((count, min_id, max_id, consumers)) => {
                        out.extend_from_slice(b"*4\r\n");
                        out.extend_from_slice(format!(":{}\r\n", count).as_bytes());
                        if let Some(min) = min_id {
                            let s = min.to_string();
                            out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                        if let Some(max) = max_id {
                            let s = max.to_string();
                            out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                        out.extend_from_slice(format!("*{}\r\n", consumers.len()).as_bytes());
                        for (c_name, c_cnt) in consumers {
                            out.extend_from_slice(
                                format!("*2\r\n${}\r\n", c_name.len()).as_bytes(),
                            );
                            out.extend_from_slice(&c_name);
                            out.extend_from_slice(format!("\r\n:{}\r\n", c_cnt).as_bytes());
                        }
                    }
                    Err(err) => {
                        if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                            out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                        } else {
                            write_resp_err(out, err);
                        }
                    }
                },
                Some((start, end, count, consumer)) => {
                    match db.xpending_range(key, group, *start, *end, *count, consumer.as_deref()) {
                        Ok(entries) => {
                            out.extend_from_slice(format!("*{}\r\n", entries.len()).as_bytes());
                            for (id, c_name, idle, delivery_cnt) in entries {
                                let id_str = id.to_string();
                                out.extend_from_slice(
                                    format!(
                                        "*4\r\n${}\r\n{}\r\n${}\r\n",
                                        id_str.len(),
                                        id_str,
                                        c_name.len(),
                                    )
                                    .as_bytes(),
                                );
                                out.extend_from_slice(&c_name);
                                out.extend_from_slice(
                                    format!("\r\n:{}\r\n:{}\r\n", idle, delivery_cnt).as_bytes(),
                                );
                            }
                        }
                        Err(err) => {
                            if err.starts_with("ERR") || err.starts_with("NOGROUP") {
                                out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                            } else {
                                write_resp_err(out, err);
                            }
                        }
                    }
                }
            }
            false
        }
        Command::Hincrby {
            key,
            field,
            increment,
        } => {
            match db.hincrby(key.clone(), field.clone(), *increment) {
                Ok(val) => {
                    record_change!(cmd);
                    write_resp_integer(out, val);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Hincrbyfloat {
            key,
            field,
            increment,
        } => {
            match db.hincrbyfloat(key.clone(), field.clone(), *increment) {
                Ok(val) => {
                    record_change!(cmd);
                    write_resp_bulk(out, val.to_string().as_bytes());
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Hrandfield {
            key,
            count,
            with_values,
        } => {
            match db.hrandfield(key, *count, *with_values) {
                Ok(items) => {
                    if count.is_none() {
                        if let Some(f) = items.first() {
                            write_resp_bulk(out, f);
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else if *with_values && CURRENT_CLIENT_RESP3.get() {
                        let num_pairs = items.len() / 2;
                        out.extend_from_slice(format!("*{}\r\n", num_pairs).as_bytes());
                        for chunk in items.chunks(2) {
                            out.extend_from_slice(b"*2\r\n");
                            write_resp_bulk(out, &chunk[0]);
                            write_resp_bulk(out, &chunk[1]);
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for item in items {
                            write_resp_bulk(out, &item);
                        }
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Hscan {
            key,
            cursor,
            pattern,
            count,
        } => {
            match db.hscan(key, *cursor, pattern.as_deref(), count.unwrap_or(10)) {
                Ok((next_cursor, entries)) => {
                    out.extend_from_slice(b"*2\r\n");
                    let cur_str = next_cursor.to_string();
                    write_resp_bulk(out, cur_str.as_bytes());
                    out.extend_from_slice(format!("*{}\r\n", entries.len()).as_bytes());
                    for item in entries {
                        write_resp_bulk(out, &item);
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Smismember { key, members } => {
            match db.smismember(key, members) {
                Ok(bools) => {
                    out.extend_from_slice(format!("*{}\r\n", bools.len()).as_bytes());
                    for b in bools {
                        write_resp_integer(out, if b { 1 } else { 0 });
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Srandmember { key, count } => {
            match db.srandmember(key, *count) {
                Ok(items) => {
                    if count.is_none() {
                        if let Some(item) = items.first() {
                            write_resp_bulk(out, item);
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for item in items {
                            write_resp_bulk(out, &item);
                        }
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Smove {
            source,
            destination,
            member,
        } => {
            match db.smove(source, destination.clone(), member.clone()) {
                Ok(res) => {
                    if res.moved {
                        DIRTY_CHANGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        touch_watched_key(db.port, source.as_ref());
                        if res.dst_added {
                            touch_watched_key(db.port, destination.as_ref());
                        }
                        let need_aof = aof.is_some();
                        let need_rep = crate::replication::has_connected_replicas(db.port);
                        if (need_aof || need_rep)
                            && let Some(bytes) = crate::aof::command_to_resp(cmd)
                        {
                            if let Some(aof_w) = aof {
                                aof_w.borrow_mut().append(&bytes);
                            }
                            if need_rep {
                                crate::replication::propagate_bytes(db.port, &bytes);
                            }
                        }
                    }
                    write_resp_integer(out, if res.moved { 1 } else { 0 });
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Sscan {
            key,
            cursor,
            pattern,
            count,
        } => {
            match db.sscan(key, *cursor, pattern.as_deref(), count.unwrap_or(10)) {
                Ok((next_cursor, entries)) => {
                    out.extend_from_slice(b"*2\r\n");
                    let cur_str = next_cursor.to_string();
                    write_resp_bulk(out, cur_str.as_bytes());
                    out.extend_from_slice(format!("*{}\r\n", entries.len()).as_bytes());
                    for item in entries {
                        write_resp_bulk(out, &item);
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Zmscore { key, members } => {
            match db.zmscore(key, members) {
                Ok(scores) => {
                    out.extend_from_slice(format!("*{}\r\n", scores.len()).as_bytes());
                    for s in scores {
                        match s {
                            Some(val) => {
                                let formatted = format_score(val);
                                write_resp_bulk(out, formatted.as_bytes());
                            }
                            None => out.extend_from_slice(b"$-1\r\n"),
                        }
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Zrandmember {
            key,
            count,
            with_scores,
        } => {
            match db.zrandmember(key, *count, *with_scores) {
                Ok(items) => {
                    if count.is_none() {
                        if let Some((m, _)) = items.first() {
                            write_resp_bulk(out, m);
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else if *with_scores {
                        if CURRENT_CLIENT_RESP3.get() {
                            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                            for (m, s) in items {
                                out.extend_from_slice(b"*2\r\n");
                                write_resp_bulk(out, &m);
                                let formatted = format_score(s);
                                write_resp_bulk(out, formatted.as_bytes());
                            }
                        } else {
                            out.extend_from_slice(format!("*{}\r\n", items.len() * 2).as_bytes());
                            for (m, s) in items {
                                write_resp_bulk(out, &m);
                                let formatted = format_score(s);
                                write_resp_bulk(out, formatted.as_bytes());
                            }
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                        for (m, _) in items {
                            write_resp_bulk(out, &m);
                        }
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Zremrangebyrank { key, start, stop } => {
            match db.zremrangebyrank(key, *start, *stop) {
                Ok(removed) => {
                    if removed > 0 {
                        record_change!(cmd);
                    }
                    write_resp_integer(out, removed as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Zremrangebyscore {
            key,
            min_score,
            min_inc,
            max_score,
            max_inc,
        } => {
            match db.zremrangebyscore(key, *min_score, *min_inc, *max_score, *max_inc) {
                Ok(removed) => {
                    if removed > 0 {
                        record_change!(cmd);
                    }
                    write_resp_integer(out, removed as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Zremrangebylex { key, min, max } => {
            match db.zremrangebylex(key, min, max) {
                Ok(removed) => {
                    if removed > 0 {
                        record_change!(cmd);
                    }
                    write_resp_integer(out, removed as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Zlexcount { key, min, max } => {
            match db.zlexcount(key, min, max) {
                Ok(count) => {
                    write_resp_integer(out, count as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Zscan {
            key,
            cursor,
            pattern,
            count,
        } => {
            match db.zscan(key, *cursor, pattern.as_deref(), count.unwrap_or(10)) {
                Ok((next_cursor, entries)) => {
                    out.extend_from_slice(b"*2\r\n");
                    let cur_str = next_cursor.to_string();
                    write_resp_bulk(out, cur_str.as_bytes());
                    out.extend_from_slice(format!("*{}\r\n", entries.len() * 2).as_bytes());
                    for (m, s) in entries {
                        write_resp_bulk(out, &m);
                        let formatted = format_score(s);
                        write_resp_bulk(out, formatted.as_bytes());
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Ltrim { key, start, stop } => {
            match db.ltrim(key, *start, *stop) {
                Ok(()) => {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Lset {
            key,
            index,
            element,
        } => {
            match db.lset(key, *index, element.clone()) {
                Ok(()) => {
                    record_change!(cmd);
                    out.extend_from_slice(b"+OK\r\n");
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Lrem {
            key,
            count,
            element,
        } => {
            match db.lrem(key, *count, element) {
                Ok(removed) => {
                    if removed > 0 {
                        record_change!(cmd);
                    }
                    write_resp_integer(out, removed as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Lpos {
            key,
            element,
            rank,
            count,
            maxlen,
        } => {
            match db.lpos(key, element, rank.unwrap_or(1), *count, *maxlen) {
                Ok(indices) => {
                    if count.is_none() {
                        if let Some(idx) = indices.first() {
                            write_resp_integer(out, *idx as i64);
                        } else {
                            out.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        out.extend_from_slice(format!("*{}\r\n", indices.len()).as_bytes());
                        for idx in indices {
                            write_resp_integer(out, idx as i64);
                        }
                    }
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Linsert {
            key,
            before,
            pivot,
            element,
        } => {
            match db.linsert(key.clone(), *before, pivot, element.clone()) {
                Ok(len) => {
                    if len > 0 {
                        record_change!(cmd);
                        notify_list_or_defer(db, key);
                    }
                    write_resp_integer(out, len);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Sort {
            key,
            desc,
            alpha,
            store,
            limit,
        } => {
            let t = db.type_of(key);
            let mut items: Vec<Bytes> = match t {
                "none" => Vec::new(),
                "list" => db.lrange(key, 0, -1).unwrap_or_default(),
                "set" => db.smembers(key).unwrap_or_default(),
                "zset" => db
                    .zrange(
                        key,
                        &crate::table::ZRangeOpts {
                            start: 0,
                            stop: -1,
                            ..Default::default()
                        },
                    )
                    .map(|pairs| pairs.into_iter().map(|(m, _)| m).collect())
                    .unwrap_or_default(),
                _ => {
                    write_resp_err(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                    return false;
                }
            };

            if *alpha {
                items.sort();
            } else {
                let mut float_items = Vec::with_capacity(items.len());
                for item in &items {
                    let s = match std::str::from_utf8(item) {
                        Ok(s) => s,
                        Err(_) => {
                            write_resp_err(
                                out,
                                "ERR One or more scores can't be converted into double",
                            );
                            return false;
                        }
                    };
                    let val: f64 = match s.parse() {
                        Ok(v) => v,
                        Err(_) => {
                            write_resp_err(
                                out,
                                "ERR One or more scores can't be converted into double",
                            );
                            return false;
                        }
                    };
                    float_items.push((val, item.clone()));
                }
                float_items
                    .sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                items = float_items.into_iter().map(|(_, item)| item).collect();
            }

            if *desc {
                items.reverse();
            }

            if let Some((offset, count)) = *limit {
                if offset < 0 || count <= 0 || (offset as usize) >= items.len() {
                    items.clear();
                } else {
                    let offset = offset as usize;
                    let count = count as usize;
                    items = items.into_iter().skip(offset).take(count).collect();
                }
            }

            if let Some(dest) = store {
                record_change!(cmd);
                db.del(dest);
                let count = items.len();
                if !items.is_empty() {
                    let _ = db.rpush(dest.clone(), items);
                }
                notify_list_or_defer(db, dest);
                write_resp_integer(out, count as i64);
            } else {
                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                for item in items {
                    write_resp_bulk(out, &item);
                }
            }
            false
        }
        Command::Lmove {
            source,
            destination,
            where_from,
            where_to,
        } => {
            match db.lmove(source, destination.clone(), *where_from, *where_to) {
                Ok(Some(val)) => {
                    record_change!(cmd);
                    notify_list_or_defer(db, destination);
                    write_resp_bulk(out, &val);
                }
                Ok(None) => {
                    write_resp_null(out);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Incrbyfloat { key, increment } => {
            match db.incrbyfloat(key.clone(), *increment) {
                Ok(val) => {
                    record_change!(cmd);
                    write_resp_bulk(out, val.to_string().as_bytes());
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Setrange { key, offset, value } => {
            match db.setrange(key.clone(), *offset, value) {
                Ok(len) => {
                    record_change!(cmd);
                    write_resp_integer(out, len as i64);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Getrange { key, start, end } => {
            match db.getrange(key, *start, *end) {
                Ok(slice) => {
                    write_resp_bulk(out, &slice);
                }
                Err(err) => {
                    if err.starts_with("ERR") || err.starts_with("WRONGTYPE") {
                        out.extend_from_slice(format!("-{}\r\n", err).as_bytes());
                    } else {
                        write_resp_err(out, err);
                    }
                }
            }
            false
        }
        Command::Time => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let secs = now.as_secs();
            let micros = now.subsec_micros();
            out.extend_from_slice(
                format!(
                    "*2\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                    secs.to_string().len(),
                    secs,
                    micros.to_string().len(),
                    micros
                )
                .as_bytes(),
            );
            false
        }
        Command::Echo(msg) => {
            write_resp_bulk(out, msg);
            false
        }
        // REDISJSON COMMANDS
        Command::JsonSet {
            key,
            path,
            json_val,
            nx,
            xx,
        } => {
            match db.json_store.json_set(key, path, json_val, *nx, *xx) {
                Ok(true) => {
                    if path == "$"
                        && let Ok(serde_json::Value::Object(map)) = serde_json::from_str(json_val)
                    {
                        let mut str_fields = std::collections::HashMap::new();
                        for (k, v) in map {
                            let val_str = match v {
                                serde_json::Value::String(s) => s,
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                other => other.to_string(),
                            };
                            str_fields.insert(k, val_str);
                        }
                        crate::search::index_document_hook(
                            &String::from_utf8_lossy(key),
                            str_fields,
                        );
                    }
                    out.extend_from_slice(b"+OK\r\n");
                }
                Ok(false) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::JsonGet { key, paths } => {
            let path_refs: Vec<&str> = paths.iter().map(|p| p.as_str()).collect();
            match db.json_store.json_get(key, &path_refs) {
                Some(res) => {
                    write_resp_bulk(out, res.as_bytes());
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::JsonDel { key, path } => {
            let count = db.json_store.json_del(key, path.as_deref());
            write_resp_integer(out, count as i64);
            false
        }
        Command::JsonType { key, path } => {
            match db.json_store.json_type(key, path.as_deref()) {
                Some(t) => {
                    out.extend_from_slice(format!("+{}\r\n", t).as_bytes());
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::JsonNumIncrBy { key, path, delta } => {
            match db.json_store.json_numincrby(key, path, *delta) {
                Ok(new_val) => {
                    write_resp_bulk(out, new_val.as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::JsonNumMultBy { key, path, factor } => {
            match db.json_store.json_numincrby(key, path, 0.0) {
                Ok(cur_str) => {
                    if let Ok(cur) = cur_str.parse::<f64>() {
                        let new_num = cur * factor;
                        let delta = new_num - cur;
                        let _ = db.json_store.json_numincrby(key, path, delta);
                        write_resp_bulk(out, new_num.to_string().as_bytes());
                    } else {
                        out.extend_from_slice(b"-ERR value at path is not a number\r\n");
                    }
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::JsonStrAppend { key, path, value } => {
            match db.json_store.json_strappend(key, path.as_deref(), value) {
                Ok(new_len) => {
                    write_resp_integer(out, new_len as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::JsonStrLen { key, path } => {
            match db.json_store.json_strlen(key, path.as_deref()) {
                Some(len) => {
                    write_resp_integer(out, len as i64);
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::JsonArrAppend { key, path, values } => {
            let val_refs: Vec<&str> = values.iter().map(|v| v.as_str()).collect();
            match db.json_store.json_arrappend(key, path, &val_refs) {
                Ok(new_len) => {
                    write_resp_integer(out, new_len as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::JsonArrLen { key, path } => {
            match db.json_store.json_arrlen(key, path.as_deref()) {
                Some(len) => {
                    write_resp_integer(out, len as i64);
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::JsonArrPop { key, path, index } => {
            match db.json_store.json_arrpop(key, path.as_deref(), *index) {
                Some(popped) => {
                    write_resp_bulk(out, popped.as_bytes());
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::JsonObjKeys { key, path } => {
            match db.json_store.json_objkeys(key, path.as_deref()) {
                Some(keys) => {
                    out.extend_from_slice(format!("*{}\r\n", keys.len()).as_bytes());
                    for k in keys {
                        write_resp_bulk(out, k.as_bytes());
                    }
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::JsonObjLen { key, path } => {
            match db.json_store.json_objlen(key, path.as_deref()) {
                Some(len) => {
                    write_resp_integer(out, len as i64);
                }
                None => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::JsonToggle { key, path } => {
            match db.json_store.json_toggle(key, path) {
                Ok(b) => {
                    write_resp_bulk(out, b.as_bytes());
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::JsonClear { key, path } => {
            let cleared = db.json_store.json_clear(key, path.as_deref());
            write_resp_integer(out, cleared as i64);
            false
        }
        // GEOSPATIAL COMMANDS
        Command::Geoadd {
            key,
            items,
            nx,
            xx,
            ch,
        } => {
            let mut elements = Vec::with_capacity(items.len());
            for (lon, lat, member) in items {
                match crate::geo::encode_geohash(*lon, *lat) {
                    Ok(hash) => {
                        elements.push((hash as f64, member.clone()));
                    }
                    Err(e) => {
                        out.extend_from_slice(format!("-{}\r\n", e).as_bytes());
                        return false;
                    }
                }
            }
            let flags = crate::table::ZAddFlags {
                nx: *nx,
                xx: *xx,
                ch: *ch,
                gt: false,
                lt: false,
                incr: false,
            };
            match db.zadd(key.clone(), elements, flags) {
                Ok((added, _)) => {
                    record_change!(cmd);
                    write_resp_integer(out, added as i64);
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Geodist { key, m1, m2, unit } => {
            let s1 = db.zscore(key, m1);
            let s2 = db.zscore(key, m2);
            match (s1, s2) {
                (Ok(Some(sc1)), Ok(Some(sc2))) => {
                    let (lon1, lat1) = crate::geo::decode_geohash(sc1 as u64);
                    let (lon2, lat2) = crate::geo::decode_geohash(sc2 as u64);
                    let mut dist = crate::geo::haversine_distance(lon1, lat1, lon2, lat2);
                    if let Some(u) = unit {
                        dist = u.from_meters(dist);
                    }
                    let s = format!("{:.4}", dist);
                    write_resp_bulk(out, s.as_bytes());
                }
                _ => {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            false
        }
        Command::Geopos { key, members } => {
            out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
            for m in members {
                match db.zscore(key, m) {
                    Ok(Some(score)) => {
                        let (lon, lat) = crate::geo::decode_geohash(score as u64);
                        out.extend_from_slice(b"*2\r\n");
                        let lon_str = format!("{:.6}", lon);
                        let lat_str = format!("{:.6}", lat);
                        write_resp_bulk(out, lon_str.as_bytes());
                        write_resp_bulk(out, lat_str.as_bytes());
                    }
                    _ => {
                        out.extend_from_slice(b"*-1\r\n");
                    }
                }
            }
            false
        }
        Command::Geohash { key, members } => {
            out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
            for m in members {
                match db.zscore(key, m) {
                    Ok(Some(score)) => {
                        let b32 = crate::geo::geohash_to_base32(score as u64);
                        write_resp_bulk(out, b32.as_bytes());
                    }
                    _ => {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
            }
            false
        }
        Command::Georadius {
            key,
            lon,
            lat,
            radius,
            unit,
            withcoord,
            withdist,
            withhash,
            count,
            asc,
        } => {
            let radius_meters = unit.to_meters(*radius);
            let mut results = Vec::new();
            let z_opts = crate::table::ZRangeOpts {
                start: 0,
                stop: -1,
                with_scores: true,
                ..Default::default()
            };
            if let Ok(pairs) = db.zrange(key, &z_opts) {
                for (member, score) in pairs {
                    let (m_lon, m_lat) = crate::geo::decode_geohash(score as u64);
                    let dist = crate::geo::haversine_distance(*lon, *lat, m_lon, m_lat);
                    if dist <= radius_meters {
                        results.push(crate::geo::GeoItemResult {
                            member,
                            dist: if *withdist {
                                Some(unit.from_meters(dist))
                            } else {
                                None
                            },
                            hash: if *withhash { Some(score as u64) } else { None },
                            coord: if *withcoord {
                                Some((m_lon, m_lat))
                            } else {
                                None
                            },
                        });
                    }
                }
            }
            if let Some(is_asc) = asc {
                if *is_asc {
                    results.sort_by(|a, b| {
                        a.dist
                            .unwrap_or(0.0)
                            .partial_cmp(&b.dist.unwrap_or(0.0))
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                } else {
                    results.sort_by(|a, b| {
                        b.dist
                            .unwrap_or(0.0)
                            .partial_cmp(&a.dist.unwrap_or(0.0))
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                }
            }
            if let Some(c) = count {
                results.truncate(*c);
            }
            let has_options = *withcoord || *withdist || *withhash;
            crate::geo::format_geo_results(out, &results, has_options);
            false
        }
        Command::Georadiusbymember {
            key,
            member,
            radius,
            unit,
            withcoord,
            withdist,
            withhash,
            count,
            asc,
        } => {
            match db.zscore(key, member) {
                Ok(Some(score)) => {
                    let (center_lon, center_lat) = crate::geo::decode_geohash(score as u64);
                    let radius_meters = unit.to_meters(*radius);
                    let mut results = Vec::new();
                    let z_opts = crate::table::ZRangeOpts {
                        start: 0,
                        stop: -1,
                        with_scores: true,
                        ..Default::default()
                    };
                    if let Ok(pairs) = db.zrange(key, &z_opts) {
                        for (m, sc) in pairs {
                            let (m_lon, m_lat) = crate::geo::decode_geohash(sc as u64);
                            let dist = crate::geo::haversine_distance(
                                center_lon, center_lat, m_lon, m_lat,
                            );
                            if dist <= radius_meters {
                                results.push(crate::geo::GeoItemResult {
                                    member: m,
                                    dist: if *withdist {
                                        Some(unit.from_meters(dist))
                                    } else {
                                        None
                                    },
                                    hash: if *withhash { Some(sc as u64) } else { None },
                                    coord: if *withcoord {
                                        Some((m_lon, m_lat))
                                    } else {
                                        None
                                    },
                                });
                            }
                        }
                    }
                    if let Some(is_asc) = asc {
                        if *is_asc {
                            results.sort_by(|a, b| {
                                a.dist
                                    .unwrap_or(0.0)
                                    .partial_cmp(&b.dist.unwrap_or(0.0))
                                    .unwrap_or(std::cmp::Ordering::Equal)
                            });
                        } else {
                            results.sort_by(|a, b| {
                                b.dist
                                    .unwrap_or(0.0)
                                    .partial_cmp(&a.dist.unwrap_or(0.0))
                                    .unwrap_or(std::cmp::Ordering::Equal)
                            });
                        }
                    }
                    if let Some(c) = count {
                        results.truncate(*c);
                    }
                    let has_options = *withcoord || *withdist || *withhash;
                    crate::geo::format_geo_results(out, &results, has_options);
                }
                _ => {
                    out.extend_from_slice(b"-ERR could not decode requested zset member\r\n");
                }
            }
            false
        }
        Command::Geosearch {
            key,
            from_member,
            from_lonlat,
            by_radius,
            by_box,
            asc,
            count,
            withcoord,
            withdist,
            withhash,
        } => {
            let center_opt = if let Some((lon, lat)) = from_lonlat {
                Some((*lon, *lat))
            } else if let Some(m) = from_member {
                match db.zscore(key, m) {
                    Ok(Some(score)) => Some(crate::geo::decode_geohash(score as u64)),
                    _ => None,
                }
            } else {
                None
            };

            let (center_lon, center_lat) = match center_opt {
                Some(c) => c,
                None => {
                    out.extend_from_slice(b"-ERR could not determine search origin\r\n");
                    return false;
                }
            };

            let mut results = Vec::new();
            let z_opts = crate::table::ZRangeOpts {
                start: 0,
                stop: -1,
                with_scores: true,
                ..Default::default()
            };
            if let Ok(pairs) = db.zrange(key, &z_opts) {
                for (member, score) in pairs {
                    let (m_lon, m_lat) = crate::geo::decode_geohash(score as u64);
                    let dist_m =
                        crate::geo::haversine_distance(center_lon, center_lat, m_lon, m_lat);

                    let inside = if let Some((rad, u)) = by_radius {
                        dist_m <= u.to_meters(*rad)
                    } else if let Some((w, h, u)) = by_box {
                        let w_m = u.to_meters(*w) / 2.0;
                        let h_m = u.to_meters(*h) / 2.0;
                        let dlat_m = (m_lat - center_lat).abs() * 111_320.0;
                        let dlon_m = (m_lon - center_lon).abs()
                            * 111_320.0
                            * (center_lat.to_radians().cos());
                        dlat_m <= h_m && dlon_m <= w_m
                    } else {
                        true
                    };

                    if inside {
                        let dist_unit = by_radius
                            .map(|(_, u)| u)
                            .or_else(|| by_box.map(|(_, _, u)| u))
                            .unwrap_or(crate::geo::GeoUnit::Meters);
                        results.push(crate::geo::GeoItemResult {
                            member,
                            dist: if *withdist {
                                Some(dist_unit.from_meters(dist_m))
                            } else {
                                None
                            },
                            hash: if *withhash { Some(score as u64) } else { None },
                            coord: if *withcoord {
                                Some((m_lon, m_lat))
                            } else {
                                None
                            },
                        });
                    }
                }
            }

            if let Some(is_asc) = asc {
                if *is_asc {
                    results.sort_by(|a, b| {
                        a.dist
                            .unwrap_or(0.0)
                            .partial_cmp(&b.dist.unwrap_or(0.0))
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                } else {
                    results.sort_by(|a, b| {
                        b.dist
                            .unwrap_or(0.0)
                            .partial_cmp(&a.dist.unwrap_or(0.0))
                            .unwrap_or(std::cmp::Ordering::Equal)
                    });
                }
            }
            if let Some(c) = count {
                results.truncate(*c);
            }
            let has_options = *withcoord || *withdist || *withhash;
            crate::geo::format_geo_results(out, &results, has_options);
            false
        }
        // PROBABILISTIC COMMANDS
        Command::BfReserve {
            key,
            error_rate,
            capacity,
        } => {
            if db.probabilistic_store.bloom_filters.contains_key(key) {
                out.extend_from_slice(b"-ERR item exists\r\n");
            } else {
                db.probabilistic_store.bloom_filters.insert(
                    key.clone(),
                    crate::probabilistic::BloomFilter::new(*capacity, *error_rate),
                );
                record_change!(cmd);
                out.extend_from_slice(b"+OK\r\n");
            }
            false
        }
        Command::BfAdd { key, item } => {
            let bf = db
                .probabilistic_store
                .bloom_filters
                .entry(key.clone())
                .or_insert_with(|| crate::probabilistic::BloomFilter::new(1000, 0.01));
            let added = bf.add(item);
            if added {
                record_change!(cmd);
                out.extend_from_slice(b":1\r\n");
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::BfMadd { key, items } => {
            let bf = db
                .probabilistic_store
                .bloom_filters
                .entry(key.clone())
                .or_insert_with(|| crate::probabilistic::BloomFilter::new(1000, 0.01));
            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
            let mut any_added = false;
            for it in items {
                let added = bf.add(it);
                if added {
                    any_added = true;
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
            }
            if any_added {
                record_change!(cmd);
            }
            false
        }
        Command::BfExists { key, item } => {
            if let Some(bf) = db.probabilistic_store.bloom_filters.get(key) {
                if bf.contains(item) {
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::BfMexists { key, items } => {
            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
            if let Some(bf) = db.probabilistic_store.bloom_filters.get(key) {
                for it in items {
                    if bf.contains(it) {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
            } else {
                for _ in items {
                    out.extend_from_slice(b":0\r\n");
                }
            }
            false
        }
        Command::BfInfo(key) => {
            if let Some(bf) = db.probabilistic_store.bloom_filters.get(key) {
                out.extend_from_slice(b"*8\r\n");
                write_resp_bulk(out, b"Capacity");
                write_resp_integer(out, bf.capacity as i64);
                write_resp_bulk(out, b"Size");
                write_resp_integer(out, (bf.bits.len() * 8) as i64);
                write_resp_bulk(out, b"Number of filters");
                write_resp_integer(out, 1);
                write_resp_bulk(out, b"Number of items inserted");
                write_resp_integer(out, bf.count as i64);
            } else {
                out.extend_from_slice(b"-ERR not found\r\n");
            }
            false
        }
        Command::CfReserve { key, capacity } => {
            if db.probabilistic_store.cuckoo_filters.contains_key(key) {
                out.extend_from_slice(b"-ERR item exists\r\n");
            } else {
                db.probabilistic_store.cuckoo_filters.insert(
                    key.clone(),
                    crate::probabilistic::CuckooFilter::new(*capacity),
                );
                record_change!(cmd);
                out.extend_from_slice(b"+OK\r\n");
            }
            false
        }
        Command::CfAdd { key, item } => {
            let cf = db
                .probabilistic_store
                .cuckoo_filters
                .entry(key.clone())
                .or_insert_with(|| crate::probabilistic::CuckooFilter::new(1000));
            match cf.add(item) {
                Ok(_) => {
                    record_change!(cmd);
                    out.extend_from_slice(b":1\r\n");
                }
                Err(e) => {
                    out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes());
                }
            }
            false
        }
        Command::CfAddnx { key, item } => {
            let cf = db
                .probabilistic_store
                .cuckoo_filters
                .entry(key.clone())
                .or_insert_with(|| crate::probabilistic::CuckooFilter::new(1000));
            if cf.contains(item) {
                out.extend_from_slice(b":0\r\n");
            } else {
                match cf.add(item) {
                    Ok(_) => {
                        record_change!(cmd);
                        out.extend_from_slice(b":1\r\n");
                    }
                    Err(e) => {
                        out.extend_from_slice(format!("-ERR {}\r\n", e).as_bytes());
                    }
                }
            }
            false
        }
        Command::CfExists { key, item } => {
            if let Some(cf) = db.probabilistic_store.cuckoo_filters.get(key) {
                if cf.contains(item) {
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::CfDel { key, item } => {
            if let Some(cf) = db.probabilistic_store.cuckoo_filters.get_mut(key) {
                if cf.delete(item) {
                    record_change!(cmd);
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::CfInfo(key) => {
            if let Some(cf) = db.probabilistic_store.cuckoo_filters.get(key) {
                out.extend_from_slice(b"*6\r\n");
                write_resp_bulk(out, b"Size");
                write_resp_integer(out, (cf.num_buckets * 4 * 2) as i64);
                write_resp_bulk(out, b"Number of buckets");
                write_resp_integer(out, cf.num_buckets as i64);
                write_resp_bulk(out, b"Number of items inserted");
                write_resp_integer(out, cf.count as i64);
            } else {
                out.extend_from_slice(b"-ERR not found\r\n");
            }
            false
        }
        Command::CmsInitbydim { key, width, depth } => {
            db.probabilistic_store.cms_sketches.insert(
                key.clone(),
                crate::probabilistic::CountMinSketch::new(*width, *depth),
            );
            record_change!(cmd);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::CmsInitbyprob {
            key,
            error,
            probability,
        } => {
            db.probabilistic_store.cms_sketches.insert(
                key.clone(),
                crate::probabilistic::CountMinSketch::from_prob(*error, *probability),
            );
            record_change!(cmd);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::CmsIncrby { key, pairs } => {
            let cms = db
                .probabilistic_store
                .cms_sketches
                .entry(key.clone())
                .or_insert_with(|| crate::probabilistic::CountMinSketch::new(2000, 5));
            out.extend_from_slice(format!("*{}\r\n", pairs.len()).as_bytes());
            for (item, delta) in pairs {
                let count = cms.incr_by(item, *delta);
                write_resp_integer(out, count as i64);
            }
            record_change!(cmd);
            false
        }
        Command::CmsQuery { key, items } => {
            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
            if let Some(cms) = db.probabilistic_store.cms_sketches.get(key) {
                for it in items {
                    let count = cms.query(it);
                    write_resp_integer(out, count as i64);
                }
            } else {
                for _ in items {
                    write_resp_integer(out, 0);
                }
            }
            false
        }
        Command::CmsInfo(key) => {
            if let Some(cms) = db.probabilistic_store.cms_sketches.get(key) {
                out.extend_from_slice(b"*6\r\n");
                write_resp_bulk(out, b"width");
                write_resp_integer(out, cms.width as i64);
                write_resp_bulk(out, b"depth");
                write_resp_integer(out, cms.depth as i64);
                write_resp_bulk(out, b"count");
                write_resp_integer(out, cms.total_count as i64);
            } else {
                out.extend_from_slice(b"-ERR not found\r\n");
            }
            false
        }
        Command::TopkReserve { key, topk } => {
            db.probabilistic_store
                .topk_trackers
                .insert(key.clone(), crate::probabilistic::TopK::new(*topk));
            record_change!(cmd);
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::TopkAdd { key, items } => {
            let tk = db
                .probabilistic_store
                .topk_trackers
                .entry(key.clone())
                .or_insert_with(|| crate::probabilistic::TopK::new(50));
            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
            for it in items {
                if let Some(evicted) = tk.add(it.clone(), 1) {
                    write_resp_bulk(out, &evicted);
                } else {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            record_change!(cmd);
            false
        }
        Command::TopkQuery { key, items } => {
            out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
            if let Some(tk) = db.probabilistic_store.topk_trackers.get(key) {
                for it in items {
                    if tk.query(it) {
                        out.extend_from_slice(b":1\r\n");
                    } else {
                        out.extend_from_slice(b":0\r\n");
                    }
                }
            } else {
                for _ in items {
                    out.extend_from_slice(b":0\r\n");
                }
            }
            false
        }
        Command::TopkList(key) => {
            if let Some(tk) = db.probabilistic_store.topk_trackers.get(key) {
                let items = tk.list();
                out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
                for (item, _) in items {
                    write_resp_bulk(out, &item);
                }
            } else {
                out.extend_from_slice(b"*0\r\n");
            }
            false
        }
        Command::TopkInfo(key) => {
            if let Some(tk) = db.probabilistic_store.topk_trackers.get(key) {
                out.extend_from_slice(b"*4\r\n");
                write_resp_bulk(out, b"k");
                write_resp_integer(out, tk.k as i64);
                write_resp_bulk(out, b"width");
                write_resp_integer(out, tk.items.len() as i64);
            } else {
                out.extend_from_slice(b"-ERR not found\r\n");
            }
            false
        }
        Command::Memory(sub) => {
            match sub {
                MemorySubcommand::Usage { key } => {
                    if let Some((val, _)) = db.get_entry(key) {
                        let size = 24 + key.len() + val.approx_bytes();
                        write_resp_integer(out, size as i64);
                    } else {
                        out.extend_from_slice(b"$-1\r\n");
                    }
                }
                MemorySubcommand::Stats => {
                    out.extend_from_slice(b"*2\r\n$10\r\npeak.alloc\r\n:1048576\r\n");
                }
                _ => {
                    out.extend_from_slice(b"+OK\r\n");
                }
            }
            false
        }
        Command::Debug(args) => {
            if let Some(sub) = args.first() {
                if sub.eq_ignore_ascii_case(b"object") {
                    if let Some(key) = args.get(1) {
                        if let Some(enc) = db.object_encoding(key) {
                            out.extend_from_slice(format!("+Value at:0x12345678 refcount:1 encoding:{} serializedlength:10 lru:0 lru_seconds_idle:0\r\n", enc).as_bytes());
                        } else {
                            out.extend_from_slice(b"-ERR no such key\r\n");
                        }
                        return false;
                    }
                } else if sub.eq_ignore_ascii_case(b"set-allow-access-expired") {
                    let flag = args.get(1).map(|v| v.as_ref() == b"1").unwrap_or(false);
                    ALLOW_ACCESS_EXPIRED.store(flag, std::sync::atomic::Ordering::Relaxed);
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"set-active-expire") {
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                } else if sub.eq_ignore_ascii_case(b"sleep") {
                    if let Some(arg) = args.get(1)
                        && let Ok(s) = std::str::from_utf8(arg)
                        && let Ok(secs) = s.parse::<f64>()
                    {
                        std::thread::sleep(std::time::Duration::from_secs_f64(secs));
                    }
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                }
            }
            out.extend_from_slice(b"+OK\r\n");
            false
        }
        Command::Digest(key) => {
            match db.table.get(key) {
                Ok(Some(bytes)) => {
                    let digest = crate::table::compute_digest(&bytes);
                    write_resp_bulk(out, digest.as_bytes());
                }
                Ok(None) => {
                    out.extend_from_slice(b"$-1\r\n");
                }
                Err(err) => {
                    write_resp_err(out, err);
                }
            }
            false
        }
        Command::Delex { key, condition } => {
            let should_del = match condition {
                None => db.exists(key),
                Some((op, expected)) => match db.table.get(key) {
                    Ok(Some(val)) => match op.to_uppercase().as_str() {
                        "IFEQ" => val == *expected,
                        "IFNE" => val != *expected,
                        "IFDEQ" => {
                            if expected.len() != 16
                                || !expected.iter().all(|b| b.is_ascii_hexdigit())
                            {
                                write_resp_err(
                                    out,
                                    "ERR digest must be exactly 16 hexadecimal characters",
                                );
                                return false;
                            }
                            let d = crate::table::compute_digest(&val);
                            d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected))
                        }
                        "IFDNE" => {
                            if expected.len() != 16
                                || !expected.iter().all(|b| b.is_ascii_hexdigit())
                            {
                                write_resp_err(
                                    out,
                                    "ERR digest must be exactly 16 hexadecimal characters",
                                );
                                return false;
                            }
                            let d = crate::table::compute_digest(&val);
                            !d.eq_ignore_ascii_case(&String::from_utf8_lossy(expected))
                        }
                        "IFGT" => val > *expected,
                        "IFLT" => val < *expected,
                        _ => false,
                    },
                    Ok(None) => false,
                    Err(_) => {
                        write_resp_err(
                            out,
                            "ERR WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                        return false;
                    }
                },
            };
            if should_del {
                if db.del(key) {
                    record_change!(cmd);
                    out.extend_from_slice(b":1\r\n");
                } else {
                    out.extend_from_slice(b":0\r\n");
                }
            } else {
                out.extend_from_slice(b":0\r\n");
            }
            false
        }
        Command::CrdtSet { key, val } => {
            let ts = db.crdt_set(key.clone(), val.clone());
            record_change!(cmd);
            let s = format!("+OK {}:{}:{}\r\n", ts.physical_ms, ts.logical, ts.node_id);
            out.extend_from_slice(s.as_bytes());
            false
        }
        Command::CrdtGet(key) => {
            if let Some(v) = db.crdt_get(key) {
                write_resp_bulk(out, &v);
            } else {
                out.extend_from_slice(b"$-1\r\n");
            }
            false
        }
        Command::CrdtDel(key) => {
            let removed = db.crdt_del(key);
            if removed {
                record_change!(cmd);
                write_resp_integer(out, 1);
            } else {
                write_resp_integer(out, 0);
            }
            false
        }
        Command::CrdtIncrby { key, delta } => {
            let val = db.crdt_incrby(key.clone(), *delta);
            record_change!(cmd);
            write_resp_integer(out, val);
            false
        }
        Command::CrdtSadd { key, member } => {
            let added = db.crdt_sadd(key.clone(), member.clone());
            record_change!(cmd);
            write_resp_integer(out, if added { 1 } else { 0 });
            false
        }
        Command::CrdtSmembers(key) => {
            let members = db.crdt_smembers(key);
            out.extend_from_slice(format!("*{}\r\n", members.len()).as_bytes());
            for m in members {
                write_resp_bulk(out, &m);
            }
            false
        }
        Command::CrdtSrem { key, member } => {
            let removed = db.crdt_srem(key, member);
            if removed {
                record_change!(cmd);
                write_resp_integer(out, 1);
            } else {
                write_resp_integer(out, 0);
            }
            false
        }
        Command::CrdtDump => {
            let payload = db.crdt_dump();
            write_resp_bulk(out, &payload);
            false
        }
        Command::CrdtMerge(payload) => {
            match db.crdt_merge(payload) {
                Ok(count) => {
                    record_change!(cmd);
                    write_resp_integer(out, count as i64);
                }
                Err(e) => {
                    let err_resp = format!("-ERR {}\r\n", e);
                    out.extend_from_slice(err_resp.as_bytes());
                }
            }
            false
        }
        Command::CrdtGc(ttl_ms) => {
            let (regs, set_tombstones) = db.crdt_gc(*ttl_ms);
            out.extend_from_slice(b"*4\r\n");
            write_resp_bulk(out, b"registers_pruned");
            write_resp_integer(out, regs as i64);
            write_resp_bulk(out, b"set_tombstones_pruned");
            write_resp_integer(out, set_tombstones as i64);
            false
        }
        Command::Quit => {
            out.extend_from_slice(b"+OK\r\n");
            true
        }
        _ => false,
    }
}

async fn execute_commands_squashed(
    commands: Vec<Command>,
    router: &Router,
    responders: &[ResponderChannel],
    remote_batches: &mut [Vec<(usize, Command)>],
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    out: &mut Vec<u8>,
    asking: &mut bool,
    authenticated: &mut bool,
    auth_user: &mut String,
) -> bool {
    let mut can_squash = *authenticated;
    if can_squash {
        for cmd in &commands {
            if matches!(
                cmd,
                Command::Blpop { .. }
                    | Command::Brpop { .. }
                    | Command::Blmove { .. }
                    | Command::Blmpop { .. }
                    | Command::Hello { .. }
                    | Command::Reset
                    | Command::Auth { .. }
                    | Command::Acl(_)
                    | Command::Tier(_)
                    | Command::ConfigGet(_)
                    | Command::ConfigSet(_, _)
                    | Command::Xread {
                        block_ms: Some(_),
                        ..
                    }
                    | Command::Xreadgroup {
                        block_ms: Some(_),
                        ..
                    }
                    | Command::DflyCluster(_)
                    | Command::DflyMigrate(_)
                    | Command::Stick(_)
                    | Command::Unstick(_)
                    | Command::MemcachedStats
                    | Command::MemcachedVersion
                    | Command::MemcachedQuit
                    | Command::Watch(_)
                    | Command::Unwatch
            ) {
                can_squash = false;
                break;
            }
            {
                let acl = crate::acl::get_acl_for_port(router.port);
                let acl_guard = acl.read().unwrap();
                if let Some(user) = acl_guard.get_user(auth_user) {
                    let cmd_name = get_cmd_name(cmd);
                    if !user.can_execute_command(cmd_name) {
                        can_squash = false;
                        break;
                    }
                    if let Some(k) = cmd_primary_key(cmd)
                        && !user.can_access_key(k.as_ref())
                    {
                        can_squash = false;
                        break;
                    }
                }
            }
            if let Some(k) = cmd_primary_key(cmd) {
                let slot = key_slot(k);
                if router.slot_states.borrow()[slot as usize] != crate::shard::SlotState::Stable {
                    can_squash = false;
                    break;
                }
                let hub = crate::cluster::get_cluster_hub(router.port);
                let my_slots = hub.my_slots.read().unwrap();
                let owns_slot = my_slots.iter().any(|&(s, e)| slot >= s && slot <= e);
                let nodes = hub.nodes.read().unwrap();
                if !owns_slot && !nodes.is_empty() {
                    can_squash = false;
                    break;
                }
            } else if !matches!(
                cmd,
                Command::Ping(_)
                    | Command::CommandDocs
                    | Command::Quit
                    | Command::Time
                    | Command::Echo(_)
            ) {
                can_squash = false;
                break;
            }
        }
    }

    if !can_squash {
        let mut should_close = false;
        for cmd in commands {
            if execute_command(
                cmd,
                router,
                client_id,
                client_registry,
                out,
                asking,
                authenticated,
                auth_user,
            )
            .await
            {
                should_close = true;
            }
        }
        return should_close;
    }

    *asking = false;

    let n = commands.len();
    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
        c.last_active = Instant::now();
        if let Some(last_cmd) = commands.last() {
            let cmd_name = get_cmd_name(last_cmd);
            c.last_cmd = cmd_name.to_lowercase();
        }
    }
    let mut responses: Vec<CompactResp> = vec![CompactResp::empty(); n];
    let mut local_buf = Vec::with_capacity(128);
    let mut should_close = false;

    for batch in remote_batches.iter_mut() {
        batch.clear();
    }

    // 1. Process local shard commands immediately; bucket remote commands by shard
    let mut has_local_writes = false;
    for (idx, cmd) in commands.into_iter().enumerate() {
        if let Some(target) = target_shard_of_cmd(&cmd, router.num_shards) {
            if target == router.shard_id {
                local_buf.clear();
                if let Command::Get(ref key) = cmd {
                    let val = router.local_db.borrow_mut().get(key);
                    if let Some(v) = val {
                        write_resp_bulk(&mut local_buf, &v);
                    } else if router.local_db.borrow_mut().table.is_tiered(key).is_some() {
                        if let Some(v) = router.stream_cold_read_local(key).await {
                            write_resp_bulk(&mut local_buf, &v);
                        } else {
                            local_buf.extend_from_slice(b"$-1\r\n");
                        }
                    } else {
                        local_buf.extend_from_slice(b"$-1\r\n");
                    }
                } else {
                    if matches!(
                        cmd,
                        Command::Set { .. } | Command::Del(_) | Command::IncrBy { .. }
                    ) {
                        has_local_writes = true;
                    }
                    if execute_local_command(
                        &cmd,
                        &mut router.local_db.borrow_mut(),
                        &mut local_buf,
                        router.aof.as_deref(),
                    ) {
                        should_close = true;
                    }
                }
                responses[idx] = CompactResp::from_slice(&local_buf);
            } else {
                remote_batches[target].push((idx, cmd));
            }
        } else {
            // Non-sharded simple commands (PING, QUIT, COMMAND DOCS) run locally
            local_buf.clear();
            if execute_local_command(
                &cmd,
                &mut router.local_db.borrow_mut(),
                &mut local_buf,
                None,
            ) {
                should_close = true;
            }
            responses[idx] = CompactResp::from_slice(&local_buf);
        }
    }

    if has_local_writes {
        let r = router.clone();
        monoio::spawn(async move {
            r.check_auto_tier().await;
        });
    }

    // 2. Dispatch batched hops to all remote shards in parallel using pre-allocated channels
    let mut pending = Vec::new();
    for (target_shard, items) in remote_batches.iter_mut().enumerate() {
        if !items.is_empty() {
            let (tx, rx) = &responders[target_shard];
            let is_resp3 = CURRENT_CLIENT_RESP3.get();
            let msg = ShardMessage::Batch {
                items: std::mem::take(items),
                responder: tx.clone(),
                is_resp3,
            };
            if router.senders[target_shard].send(msg).is_ok() {
                pending.push(rx);
            }
        }
    }

    // 3. Await parallel responses from all remote shards
    for rx in pending {
        if let Ok(results) = rx.recv_async().await {
            for (idx, resp) in results {
                responses[idx] = resp;
            }
        }
    }

    // 4. Append responses in exact FIFO pipeline order
    for resp in &responses {
        out.extend_from_slice(resp.as_slice());
    }

    should_close
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp::Command;
    use bytes::Bytes;

    #[test]
    fn test_crdt_primary_key_and_target_shard() {
        let num_shards = 4;
        let k = Bytes::from("crdt:test:key");
        let expected_shard = target_shard(&k, num_shards);

        let cmds = vec![
            Command::CrdtSet {
                key: k.clone(),
                val: Bytes::from("val"),
            },
            Command::CrdtGet(k.clone()),
            Command::CrdtDel(k.clone()),
            Command::CrdtIncrby {
                key: k.clone(),
                delta: 10,
            },
            Command::CrdtSadd {
                key: k.clone(),
                member: Bytes::from("m1"),
            },
            Command::CrdtSmembers(k.clone()),
            Command::CrdtSrem {
                key: k.clone(),
                member: Bytes::from("m1"),
            },
        ];

        for cmd in &cmds {
            assert_eq!(cmd_primary_key(cmd), Some(&k));
            assert_eq!(target_shard_of_cmd(cmd, num_shards), Some(expected_shard));
        }
    }

    #[test]
    fn test_cluster_key_slot_and_tag_extraction() {
        use crate::router::{extract_hash_tag, key_slot};

        let key1 = b"user1000";
        let slot1 = key_slot(key1);
        assert!(slot1 < 16384);

        // Keys with identical hash tags must hash to identical cluster slots
        let tagged1 = b"{user:123}:profile";
        let tagged2 = b"{user:123}:settings";
        assert_eq!(extract_hash_tag(tagged1), b"user:123");
        assert_eq!(extract_hash_tag(tagged2), b"user:123");
        assert_eq!(key_slot(tagged1), key_slot(tagged2));
    }
}

