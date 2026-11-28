use bytes::{Bytes, BytesMut};
use monoio::io::{AsyncReadRent, AsyncWriteRentExt, Splitable};
use monoio::net::TcpStream;
use smallvec::smallvec;
use std::cell::RefCell;
use std::net::SocketAddr;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::resp::{
    ClientSubcommand, ClusterSubcommand, Command, LatencySubcommand, MemorySubcommand,
    MsetexCondition, MsetexExpiry, SetSlotSubcommand, parse_command,
};
use crate::router::{Router, key_slot, target_shard};
use crate::shard::{CompactResp, ShardDb, ShardMessage};

pub(crate) const READ_BUFFER_SIZE: usize = 65536;

/// Limits on one parse/execute batch. While a frame is incomplete the read
/// loop drains more bytes straight from the socket (`read_ready`) instead of
/// going back to the event loop; past either limit it stops draining, runs
/// what it has and leaves the rest for the next read. Without a limit a client
/// that keeps the socket full (`--pipe`, a bulk loader) gets its whole stream
/// parsed into one batch: memory grows with the stream and the shard serves
/// nobody else until the batch is done.
const MAX_DRAIN_BATCH_CMDS: usize = 1024;
const MAX_DRAIN_BATCH_BYTES: usize = 1 << 20;

/// Pooled connection scratch keeps its buffers' capacity for the next client;
/// anything that grew past these is shrunk back first, so one burst does not
/// pin memory on the shard for the life of the process.
const SCRATCH_KEEP_ITEMS: usize = 1024;
const SCRATCH_KEEP_BYTES: usize = 1 << 20;

#[derive(Debug)]
pub struct ClientStats {
    pub connected_at: Instant,
    pub addr: SocketAddr,
    pub tot_net_in: std::sync::atomic::AtomicU64,
    pub tot_net_out: std::sync::atomic::AtomicU64,
    pub tot_cmds: std::sync::atomic::AtomicU64,
    pub read_events: std::sync::atomic::AtomicU64,
    /// When the client last sent anything, by [`crate::table::coarse_now_ms`]
    /// (INFO clients' `active_clients`).
    pub last_read_ms: std::sync::atomic::AtomicU64,
    pub pipeline_len_sum: std::sync::atomic::AtomicU64,
    pub pipeline_len_cnt: std::sync::atomic::AtomicU64,
    pub killed: std::sync::atomic::AtomicBool,
    /// The connection became a replica's replication link (PSYNC, SYNC or
    /// a DFLY FLOW): CLIENT LIST flag `S`, CLIENT KILL TYPE replica.
    pub is_replica: std::sync::atomic::AtomicBool,
    /// Commands queued in its MULTI (CLIENT LIST `multi`), -1 outside one.
    pub multi_count: std::sync::atomic::AtomicI64,
    /// Bytes of those commands (CLIENT LIST `multi-mem`).
    pub multi_mem: std::sync::atomic::AtomicUsize,
    /// CLIENT NO-EVICT ON: never evicted for `maxmemory-clients`.
    pub no_evict: std::sync::atomic::AtomicBool,
    /// Its `tot-mem` as its shard last measured it (every 100 ms while
    /// `maxmemory-clients` is set), for the evictor on another thread.
    pub tot_mem: std::sync::atomic::AtomicUsize,
}

impl ClientStats {
    pub fn new(connected_at: Instant, addr: SocketAddr) -> Self {
        Self {
            connected_at,
            addr,
            tot_net_in: std::sync::atomic::AtomicU64::new(0),
            tot_net_out: std::sync::atomic::AtomicU64::new(0),
            tot_cmds: std::sync::atomic::AtomicU64::new(0),
            read_events: std::sync::atomic::AtomicU64::new(0),
            last_read_ms: std::sync::atomic::AtomicU64::new(crate::table::coarse_now_ms()),
            pipeline_len_sum: std::sync::atomic::AtomicU64::new(0),
            pipeline_len_cnt: std::sync::atomic::AtomicU64::new(0),
            killed: std::sync::atomic::AtomicBool::new(false),
            is_replica: std::sync::atomic::AtomicBool::new(false),
            multi_count: std::sync::atomic::AtomicI64::new(-1),
            multi_mem: std::sync::atomic::AtomicUsize::new(0),
            no_evict: std::sync::atomic::AtomicBool::new(false),
            tot_mem: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientInfo {
    pub id: u64,
    pub addr: SocketAddr,
    pub name: Option<String>,
    pub lib_name: Option<String>,
    pub lib_ver: Option<String>,
    pub connected_at: Instant,
    pub last_active: Instant,
    pub last_cmd: &'static str,
    pub is_resp3: bool,
    pub db: usize,
    pub track_tx: Option<flume::Sender<Vec<u8>>>,
    pub raw_fd: std::os::unix::io::RawFd,
    pub omem: usize,
    pub qbuf_len: usize,
    pub qbuf_cap: usize,
    pub reply_mode: crate::resp::ClientReplyMode,
    pub is_monitor: bool,
    /// READONLY: reads for its master's slots are served on this cluster
    /// replica instead of redirected (until READWRITE).
    pub readonly: bool,
    pub stats: std::sync::Arc<ClientStats>,
    /// How other threads deliver MONITOR output to this client.
    pub push: crate::transport::PushTarget,
    /// HIMPORT PREPARE's fieldsets: field lists by name, this connection's
    /// own until DISCARD, RESET or disconnect.
    pub himport_fieldsets: hashbrown::HashMap<Bytes, Vec<Bytes>>,
    /// Once the connection's writes go through a queue (subscribers,
    /// MONITOR), its byte count.
    pub out_queue: Option<crate::pubsub::QueueGauge>,
}

impl ClientInfo {
    /// Output bytes waiting (CLIENT LIST `omem`).
    pub fn omem_now(&self) -> usize {
        self.out_queue.as_ref().map_or(self.omem, |q| q.queued())
    }

    pub fn effective_qbuf(&self, idle_secs: u64) -> (usize, usize) {
        if self.qbuf_cap == 0 && self.qbuf_len == 0 {
            (0, 0)
        } else if !PAUSE_CRON.load(std::sync::atomic::Ordering::Relaxed) && idle_secs >= 2 {
            (self.qbuf_len, 0)
        } else {
            (self.qbuf_len, self.qbuf_cap.saturating_sub(self.qbuf_len))
        }
    }
}

/// Where the data of the big argument (32 KB or more, Redis's
/// PROTO_MBULK_BIG_ARG) that the incomplete command in `buf` is still
/// reading starts. Redis moves such an argument to the start of the query
/// buffer, the arguments before it into argv, so CLIENT LIST's qbuf counts
/// only its bytes read so far.
fn pending_big_arg_start(buf: &[u8]) -> Option<usize> {
    fn line(buf: &[u8], from: usize, prefix: u8) -> Option<(usize, usize)> {
        if buf.get(from) != Some(&prefix) {
            return None;
        }
        let end = from + 1 + buf[from + 1..].windows(2).position(|w| w == b"\r\n")?;
        let n = std::str::from_utf8(&buf[from + 1..end])
            .ok()?
            .parse()
            .ok()?;
        Some((n, end + 2))
    }
    let (argc, mut i) = line(buf, 0, b'*')?;
    for _ in 0..argc {
        let (len, data) = line(buf, i, b'$')?;
        if data + len + 2 > buf.len() {
            return (len >= 32 * 1024).then_some(data);
        }
        i = data + len + 2;
    }
    None
}

pub fn estimate_incomplete_qbuf_cap(buf: &[u8]) -> usize {
    let mut max_bulk = 0usize;
    let mut i = 0;
    while i < buf.len() {
        if buf[i] == b'$' {
            let start = i + 1;
            let mut end = start;
            while end < buf.len() && buf[end].is_ascii_digit() {
                end += 1;
            }
            if end > start
                && end + 1 < buf.len()
                && buf[end] == b'\r'
                && buf[end + 1] == b'\n'
                && let Ok(s) = std::str::from_utf8(&buf[start..end])
                && let Ok(n) = s.parse::<usize>()
                && n <= 512 * 1024 * 1024
            {
                max_bulk = max_bulk.max(n);
            }
            i = end;
        } else {
            i += 1;
        }
    }
    // The buffer grows to hold the biggest argument (as Redis sizes its
    // query buffer for a big bulk), not that much on top of what came: a
    // bulk nearly all in counted twice, and maxmemory-clients evicted a
    // client at half the limit.
    buf.len().max(max_bulk + 64).max(16384)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferLimit {
    pub hard_limit: u64,
    pub soft_limit: u64,
    pub soft_seconds: u64,
}

impl BufferLimit {
    pub const fn new(hard: u64, soft: u64, secs: u64) -> Self {
        Self {
            hard_limit: hard,
            soft_limit: soft,
            soft_seconds: secs,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientClass {
    Normal,
    Replica,
    Pubsub,
}

pub static HAS_CUSTOM_BUFFER_LIMIT: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
pub static NORMAL_BUFFER_LIMIT: parking_lot::RwLock<BufferLimit> =
    parking_lot::RwLock::new(BufferLimit::new(0, 0, 0));
pub static SLAVE_BUFFER_LIMIT: parking_lot::RwLock<BufferLimit> =
    parking_lot::RwLock::new(BufferLimit::new(268435456, 67108864, 60));
pub static PUBSUB_BUFFER_LIMIT: parking_lot::RwLock<BufferLimit> =
    parking_lot::RwLock::new(BufferLimit::new(33554432, 8388608, 60));

#[inline(always)]
pub fn get_client_output_buffer_limit(class: ClientClass) -> BufferLimit {
    match class {
        ClientClass::Normal => {
            if !HAS_CUSTOM_BUFFER_LIMIT.load(std::sync::atomic::Ordering::Relaxed) {
                return BufferLimit::new(0, 0, 0);
            }
            *NORMAL_BUFFER_LIMIT.read()
        }
        ClientClass::Replica => *SLAVE_BUFFER_LIMIT.read(),
        ClientClass::Pubsub => *PUBSUB_BUFFER_LIMIT.read(),
    }
}

pub fn format_client_output_buffer_limit_config() -> String {
    let norm = *NORMAL_BUFFER_LIMIT.read();
    let slv = *SLAVE_BUFFER_LIMIT.read();
    let ps = *PUBSUB_BUFFER_LIMIT.read();
    format!(
        "normal {} {} {} slave {} {} {} pubsub {} {} {}",
        norm.hard_limit,
        norm.soft_limit,
        norm.soft_seconds,
        slv.hard_limit,
        slv.soft_limit,
        slv.soft_seconds,
        ps.hard_limit,
        ps.soft_limit,
        ps.soft_seconds
    )
}

pub fn parse_limit_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let lower = s.to_lowercase();
    let (num_str, multiplier) = if lower.ends_with("gb") {
        (&lower[..lower.len() - 2], 1024 * 1024 * 1024u64)
    } else if lower.ends_with("g") {
        (&lower[..lower.len() - 1], 1024 * 1024 * 1024u64)
    } else if lower.ends_with("mb") {
        (&lower[..lower.len() - 2], 1024 * 1024u64)
    } else if lower.ends_with("m") {
        (&lower[..lower.len() - 1], 1024 * 1024u64)
    } else if lower.ends_with("kb") {
        (&lower[..lower.len() - 2], 1024u64)
    } else if lower.ends_with("k") {
        (&lower[..lower.len() - 1], 1024u64)
    } else if lower.ends_with("b") {
        (&lower[..lower.len() - 1], 1u64)
    } else {
        (lower.as_str(), 1u64)
    };

    if let Ok(n) = num_str.trim().parse::<u64>() {
        Some(n.saturating_mul(multiplier))
    } else if num_str.trim().chars().all(|c| c.is_ascii_digit()) {
        Some(u64::MAX)
    } else {
        None
    }
}

pub fn set_client_output_buffer_limit_str(val: &str) -> Result<(), &'static str> {
    let tokens: Vec<&str> = val.split_whitespace().collect();
    if tokens.is_empty() || !tokens.len().is_multiple_of(4) {
        return Err("-ERR Wrong number of arguments for client-output-buffer-limit\r\n");
    }

    let mut updates = Vec::new();
    for chunk in tokens.chunks(4) {
        let class = match chunk[0].to_lowercase().as_str() {
            "normal" => ClientClass::Normal,
            "slave" | "replica" => ClientClass::Replica,
            "pubsub" => ClientClass::Pubsub,
            _ => return Err("-ERR Invalid client class for client-output-buffer-limit\r\n"),
        };
        let hard = parse_limit_bytes(chunk[1]).ok_or("-ERR Error parsing hard limit\r\n")?;
        let soft = parse_limit_bytes(chunk[2]).ok_or("-ERR Error parsing soft limit\r\n")?;
        let secs = chunk[3]
            .parse::<u64>()
            .map_err(|_| "-ERR Error parsing soft_seconds\r\n")?;
        updates.push((class, BufferLimit::new(hard, soft, secs)));
    }

    for (class, limit) in updates {
        match class {
            ClientClass::Normal => {
                *NORMAL_BUFFER_LIMIT.write() = limit;
                HAS_CUSTOM_BUFFER_LIMIT.store(
                    limit.hard_limit > 0 || limit.soft_limit > 0,
                    std::sync::atomic::Ordering::Relaxed,
                );
            }
            ClientClass::Replica => *SLAVE_BUFFER_LIMIT.write() = limit,
            ClientClass::Pubsub => *PUBSUB_BUFFER_LIMIT.write() = limit,
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct GlobalClientEntry {
    pub raw_fd: std::os::unix::io::RawFd,
    /// How other threads deliver tracking invalidations to this client.
    pub push: crate::transport::PushTarget,
    pub track_tx: flume::Sender<Vec<u8>>,
    pub is_resp3: bool,
    pub auth_user: String,
    pub is_pubsub: bool,
    pub stats: std::sync::Arc<ClientStats>,
}

#[derive(Clone, Debug)]
pub struct ClientTracker {
    pub port: u16,
    pub client_id: u64,
    pub redirect_id: u64,
    pub broken_redirect: bool,
    pub bcast: bool,
    pub prefixes: Vec<Bytes>,
    pub optin: bool,
    pub optout: bool,
    pub noloop: bool,
    pub caching: Option<bool>,
    pub tracked_keys: hashbrown::HashSet<Vec<u8>>,
    pub pending_bcast: Vec<(Bytes, Vec<Vec<u8>>)>,
}

thread_local! {
    pub static CURRENT_CLIENT_RESP3: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub static CURRENT_DB: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Set while this thread re-applies writes that already happened: an AOF
    /// replay (`aof::replay_aof_with`) or commands from this replica's
    /// master. Like Redis (nothing expires while loading; a replica waits
    /// for its master's DEL), expired keys count as live meanwhile, so a
    /// past `PEXPIREAT` followed by more writes to the key (APPEND, PERSIST,
    /// ...) applies the way it did originally. Expiring mid-replay dropped
    /// such keys, or recreated them from the later writes alone, without a
    /// TTL. See [`ReplayingWrites`].
    pub static REPLAYING_WRITES: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub static CURRENT_CLIENT_ID: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub static EXECUTING_CLIENT_ID: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    pub static DEFER_BCAST_FLUSH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub static PRE_CMD_TRACK_BUF: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
    /// `CLIENT NO-TOUCH` of the client whose command runs (per client, like
    /// Redis: reads by this client leave LRU/LFU data untouched).
    pub static CLIENT_NO_TOUCH: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    /// Clients of this shard thread whose EXEC is running. A transaction
    /// awaits cross-shard work mid-way, letting other connections' tasks run
    /// on this thread, so "in a transaction" has to be per client, not a
    /// thread-wide flag (that made a concurrent BLPOP return nil at once and
    /// let other clients skip cross-shard locks).
    static TX_CLIENTS: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) };
    pub static CURRENT_ROUTER: std::cell::RefCell<Option<std::rc::Rc<Router>>> = const { std::cell::RefCell::new(None) };
    /// This shard thread's connections, for commands run without a
    /// connection's state (CLIENT LIST from a script).
    pub static SHARD_CLIENTS: std::cell::RefCell<Option<Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>>> =
        const { std::cell::RefCell::new(None) };
}

/// CLIENT LIST run by a script: every shard's clients, as a bulk string.
#[inline(never)]
pub fn script_client_list(filter_ids: &[u64], ty: Option<&str>, out: &mut Vec<u8>) {
    let router = CURRENT_ROUTER.with(|r| r.borrow().clone());
    let clients = SHARD_CLIENTS.with(|c| c.borrow().clone());
    let list = match (router, clients) {
        (Some(router), Some(clients)) => router.client_list_blocking(&clients, filter_ids),
        _ => String::new(),
    };
    write_resp_bulk(out, filter_client_list_type(list, ty).as_bytes());
}

/// Keeps the CLIENT LIST lines of clients of type `ty` (`normal`, `master`,
/// `replica`/`slave` or `pubsub`), told apart by their `flags` and `resp`
/// fields as Redis's `getClientType` does: a monitor is a normal client,
/// and a RESP3 subscriber too.
pub fn filter_client_list_type(list: String, ty: Option<&str>) -> String {
    let Some(ty) = ty else { return list };
    let field = |line: &str, name: &str| -> String {
        line.split(' ')
            .find_map(|f| f.strip_prefix(name))
            .unwrap_or("")
            .to_string()
    };
    let mut out = String::with_capacity(list.len());
    for line in list.split_inclusive('\n') {
        let flags = field(line, "flags=");
        let class = if flags.contains('M') {
            "master"
        } else if flags.contains('S') && !flags.contains('O') {
            "replica"
        } else if flags.contains('P') && field(line, "resp=").trim_end() != "3" {
            "pubsub"
        } else {
            "normal"
        };
        let wanted = if ty == "slave" { "replica" } else { ty };
        if class == wanted {
            out.push_str(line);
        }
    }
    out
}

/// The client whose command this shard thread is running, from
/// thread-local state only (0 if none). Pass it along with work sent to
/// other shards; there is deliberately no process-wide "active client"
/// (every shard thread would overwrite it).
#[inline]
pub fn requesting_client_id() -> u64 {
    let c = CURRENT_CLIENT_ID.get();
    if c != 0 { c } else { EXECUTING_CLIENT_ID.get() }
}

/// Whether `client_id`'s EXEC is running (its queued commands execute
/// without blocking, pausing or taking cross-shard locks again).
#[inline]
pub fn in_tx(client_id: u64) -> bool {
    client_id != 0 && TX_CLIENTS.with(|t| t.borrow().contains(&client_id))
}

fn enter_tx(client_id: u64) {
    TX_CLIENTS.with(|t| t.borrow_mut().push(client_id));
}

fn exit_tx(client_id: u64) {
    TX_CLIENTS.with(|t| {
        let mut t = t.borrow_mut();
        if let Some(i) = t.iter().position(|&c| c == client_id) {
            t.swap_remove(i);
        }
    });
}

/// Undoes what a running EXEC set up: the in-transaction mark, deferred
/// tracking broadcasts, the cross-shard (VLL) locks and the pause of
/// blocking-client notifications. Runs on drop, so it also happens when the
/// EXEC future is dropped part-way: connection tasks run under
/// `catch_unwind`, and a panicking queued command used to leave the port's
/// blocked clients never woken again and the locked shards closed to every
/// later multi-shard transaction.
struct ExecCleanup<'a> {
    router: &'a Router,
    client_id: u64,
    locks: Option<(Vec<usize>, u64)>,
    /// Marks the transaction as a cross-shard write while it holds `locks`.
    xwrite: Option<crate::router::XShardWriteGuard>,
    hub: Option<std::sync::Arc<parking_lot::Mutex<crate::block::BlockHub>>>,
}

impl Drop for ExecCleanup<'_> {
    fn drop(&mut self) {
        DEFER_BCAST_FLUSH.set(false);
        exit_tx(self.client_id);
        flush_pending_bcast(self.router.port);
        EXECUTING_CLIENT_ID.set(0);
        // Before the locks go: a reader that then locks the shards must
        // not take the write for still running.
        self.xwrite.take();
        if let Some((shards, tx_id)) = self.locks.take() {
            self.router.release_tx_locks_now(&shards, tx_id);
        }
        if let Some(hub_arc) = self.hub.take() {
            resume_blocked_serving(self.router, &hub_arc);
        }
    }
}

/// Ends one pause of `hub_arc` (EXEC's, or a script's): once none is left,
/// serves the blocked clients whose keys changed meanwhile.
fn resume_blocked_serving(router: &Router, hub_arc: &parking_lot::Mutex<crate::block::BlockHub>) {
    let pending = hub_arc.lock().resume();
    let prev_cur_db = CURRENT_DB.get();
    for (db_id, k) in pending {
        let shard_id = router.target_shard(&k);
        // `try_borrow_mut`: when unwinding, a borrow may still be
        // live higher up; hand the key to the shard's own mailbox.
        let local = (shard_id == router.shard_id)
            .then(|| router.local_db.try_borrow_mut().ok())
            .flatten();
        if let Some(mut local_db) = local {
            let prev_local_db = local_db.current_db;
            CURRENT_DB.set(db_id);
            local_db.select_db(db_id);
            let mut hub = hub_arc.lock();
            hub.notify_stream(&mut local_db, &k);
            hub.notify_list(&mut local_db.table, &k);
            hub.notify_zset(&mut local_db.table, &k);
            drop(hub);
            local_db.select_db(prev_local_db);
        } else {
            let _ = router.senders[shard_id].send(ShardMessage::NotifyList {
                db: db_id,
                keys: vec![k],
            });
        }
    }
    CURRENT_DB.set(prev_cur_db);
}

/// Holds back serving blocked clients while a script runs, as Redis
/// serves them only once the script is done: a BLPOP waiting on a list
/// the script pushes to and then pushes to again must see the script's
/// final list, not pop its first push mid-run (which a suspended script
/// would also show other clients).
pub struct BlockedServingPause {
    router: Rc<Router>,
    hub: std::sync::Arc<parking_lot::Mutex<crate::block::BlockHub>>,
}

impl BlockedServingPause {
    /// `None` off a shard thread (AOF replay, unit tests): no clients.
    pub fn start() -> Option<Self> {
        let router = CURRENT_ROUTER.with(|r| r.borrow().clone())?;
        let hub = crate::block::get_block_hub_for_port(router.port);
        hub.lock().pause();
        Some(Self { router, hub })
    }
}

impl Drop for BlockedServingPause {
    fn drop(&mut self) {
        resume_blocked_serving(&self.router, &self.hub);
    }
}

thread_local! {
    /// While maxmemory eviction runs on behalf of a client (locally or for
    /// a remote shard's request), that client's id and the tracking
    /// invalidations owed to it, which must reach it before the reply to
    /// the command that triggered the eviction.
    static EVICT_CAPTURE: std::cell::RefCell<Option<(u64, Vec<u8>)>> =
        const { std::cell::RefCell::new(None) };
}

/// Runs `f` (maxmemory eviction) capturing the tracking invalidations
/// addressed to `client_id`; returns `f`'s result and those bytes, which the
/// caller writes ahead of the client's reply. Nested captures are kept
/// separate.
pub fn with_evict_capture<R>(client_id: u64, f: impl FnOnce() -> R) -> (R, Vec<u8>) {
    let prev = EVICT_CAPTURE.with(|c| c.replace(Some((client_id, Vec::new()))));
    let r = f();
    let captured = EVICT_CAPTURE.with(|c| c.replace(prev));
    (r, captured.map(|(_, buf)| buf).unwrap_or_default())
}

/// Shard id of the router installed on this thread (0 if none).
pub fn current_shard_id() -> usize {
    CURRENT_ROUTER.with(|cr| cr.borrow().as_ref().map_or(0, |r| r.shard_id))
}

pub fn set_current_router(router: std::rc::Rc<Router>) {
    CURRENT_ROUTER.with(|cr| {
        *cr.borrow_mut() = Some(router);
    });
}

/// Publishes `del` when a write removed the last element of `key`. The
/// existence check only runs when notifications are enabled.
#[inline]
fn notify_del_if_emptied(db: &mut ShardDb, key: &[u8]) {
    if NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) != 0 && !db.exists(key) {
        notify_keyspace_event(NOTIFY_GENERIC, "del", key);
    }
}

pub fn notify_keyspace_event(event_type: u32, event: &str, key: &[u8]) {
    CURRENT_ROUTER.with(|cr| {
        if let Some(router) = cr.borrow().as_ref() {
            notify_keyspace_event_sync(router, event_type, event, key);
        }
    });
}

pub fn notify_keyspace_event_in_db(db_id: usize, event_type: u32, event: &str, key: &[u8]) {
    CURRENT_ROUTER.with(|cr| {
        if let Some(router) = cr.borrow().as_ref() {
            notify_keyspace_event_sync_in_db(router, db_id, event_type, event, key);
        }
    });
}

/// A blocking command first checks its keys and, finding nothing to serve,
/// then registers as a waiter. A push that lands on a key's owner shard in
/// between (shards run concurrently, and the check itself may await remote
/// hops) finds no waiter and wakes nobody, leaving the client blocked until
/// its timeout. Called right after registering: asks each key's owner to run
/// the wakeup again, so a push from that window serves the new waiter.
pub fn recheck_blocking_keys<'a>(router: &Router, keys: impl IntoIterator<Item = &'a Bytes>) {
    let db_id = CURRENT_DB.get();
    let hub_arc = crate::block::get_block_hub_for_port(router.port);
    let mut remote: Vec<Vec<Bytes>> = Vec::new();
    for k in keys {
        let shard = router.target_shard(k);
        if shard == router.shard_id {
            let mut db = router.local_db.borrow_mut();
            db.select_db(db_id);
            let mut hub = hub_arc.lock();
            if hub.is_paused() {
                hub.add_pending_notify(k.clone());
                continue;
            }
            hub.notify_stream(&mut db, k);
            hub.notify_list(&mut db.table, k);
            hub.notify_zset(&mut db.table, k);
        } else {
            if remote.is_empty() {
                remote.resize(router.num_shards, Vec::new());
            }
            remote[shard].push(k.clone());
        }
    }
    for (shard, keys) in remote.into_iter().enumerate() {
        if keys.is_empty() {
            continue;
        }
        {
            // While paused, resume() routes pending keys to their owners.
            let mut hub = hub_arc.lock();
            if hub.is_paused() {
                for k in keys {
                    hub.add_pending_notify(k);
                }
                continue;
            }
        }
        let _ = router.senders[shard].send(ShardMessage::NotifyList { db: db_id, keys });
    }
}

#[inline]
pub fn notify_list_or_defer(db: &mut ShardDb, key: &Bytes) {
    touch_watched_key(db.port, key.as_ref());
    if !crate::block::has_blocked_waiters(db.port) {
        return;
    }
    let hub_arc = crate::block::get_block_hub_for_port(db.port);
    let mut hub = hub_arc.lock();
    if hub.is_paused() {
        hub.add_pending_notify(key.clone());
    } else {
        hub.notify_list(&mut db.table, key);
    }
}

#[inline]
pub fn notify_zset_or_defer(db: &mut ShardDb, key: &Bytes) {
    touch_watched_key(db.port, key.as_ref());
    if !crate::block::has_blocked_waiters(db.port) {
        return;
    }
    let hub_arc = crate::block::get_block_hub_for_port(db.port);
    let mut hub = hub_arc.lock();
    if hub.is_paused() {
        hub.add_pending_notify(key.clone());
    } else {
        hub.notify_zset(&mut db.table, key);
    }
}

#[inline]
pub fn notify_stream_or_defer(db: &mut ShardDb, key: &Bytes) {
    touch_watched_key(db.port, key.as_ref());
    if !crate::block::has_blocked_waiters(db.port) {
        return;
    }
    let hub_arc = crate::block::get_block_hub_for_port(db.port);
    let mut hub = hub_arc.lock();
    if hub.is_paused() {
        hub.add_pending_notify(key.clone());
    } else {
        hub.notify_stream(db, key);
    }
}

/// The idempotent XCLAIM Redis propagates for a PEL entry that XREADGROUP,
/// XCLAIM or XAUTOCLAIM created or changed (`streamPropagateXCLAIM`):
///
/// `XCLAIM <key> <group> <consumer> 0 <id> TIME <ms> RETRYCOUNT <count> FORCE JUSTID LASTID <last-id>`
///
/// On the replica it recreates the entry with its owner, delivery time and
/// count, or drops it if the stream entry no longer exists.
fn stream_xclaim_cmd(
    key: &Bytes,
    group: &Bytes,
    consumer: Bytes,
    id: crate::table::StreamId,
    pel: &crate::table::StreamPelEntry,
    last_id: crate::table::StreamId,
) -> Command {
    Command::Xclaim {
        key: key.clone(),
        group: group.clone(),
        consumer,
        min_idle_time: 0,
        ids: Box::new(vec![Bytes::from(id.to_string())]),
        idle: None,
        time: Some(pel.delivery_time_ms),
        retrycount: Some(pel.delivery_count),
        force: true,
        justid: true,
        lastid: Some(Box::new(last_id)),
    }
}

/// `XGROUP SETID <key> <group> <last-id> ENTRIESREAD <n>`, which Redis
/// propagates when a group's last-delivered-id moved without an XCLAIM
/// carrying it (`streamPropagateGroupID`). ENTRIESREAD is left out when
/// the counter is invalid, which SETID then also sets on the replica.
fn stream_setid_cmd(
    key: &Bytes,
    group: &Bytes,
    snap: &crate::table::StreamGroupSnapshot,
) -> Command {
    Command::XgroupSetId {
        key: key.clone(),
        group: group.clone(),
        id: snap.last_delivered_id.to_string(),
        entries_read: snap.entries_read,
    }
}

fn stream_createconsumer_cmd(key: &Bytes, group: &Bytes, consumer: &Bytes) -> Command {
    Command::XgroupCreateConsumer {
        key: key.clone(),
        group: group.clone(),
        consumer: consumer.clone(),
    }
}

/// The commands that propagate what an XREADGROUP did to `key`, in place of
/// the command itself (which would block or deliver differently on a
/// replica): consumer creation, an XCLAIM for every entry it now has
/// pending (new deliveries, CLAIMed entries and re-read history), and an
/// XGROUP SETID if the group's last-delivered-id moved.
///
/// Unlike Redis, the consumer creation is propagated with or without NOACK
/// and history reads propagate their new delivery count and time, so the
/// replica and the AOF match the master exactly.
pub(crate) fn stream_read_propagation(
    db: &mut ShardDb,
    key: &Bytes,
    group: &Bytes,
    consumer: &Bytes,
    consumer_created: bool,
    delivered: &[crate::table::StreamId],
    prev_last_id: crate::table::StreamId,
) -> Vec<Command> {
    let mut cmds = Vec::new();
    if consumer_created {
        cmds.push(stream_createconsumer_cmd(key, group, consumer));
    }
    let Some(snap) = db.table.stream_group_snapshot(key, group, delivered) else {
        return cmds;
    };
    for (id, pe) in &snap.pel {
        if pe.consumer == *consumer {
            cmds.push(stream_xclaim_cmd(
                key,
                group,
                consumer.clone(),
                *id,
                pe,
                snap.last_delivered_id,
            ));
        }
    }
    if snap.last_delivered_id != prev_last_id {
        cmds.push(stream_setid_cmd(key, group, &snap));
    }
    cmds
}

/// The commands that propagate an XCLAIM or XAUTOCLAIM, as Redis does: an
/// XCLAIM per claimed entry and per PEL entry dropped because its stream
/// entry is gone, and XGROUP SETID if XCLAIM's LASTID moved the group
/// without any XCLAIM carrying it. A consumer the command created is
/// propagated too, even when nothing was claimed.
pub(crate) fn stream_claim_propagation(
    db: &mut ShardDb,
    key: &Bytes,
    group: &Bytes,
    consumer: &Bytes,
    outcome: &crate::table::StreamClaimOutcome,
) -> Vec<Command> {
    let mut cmds = Vec::new();
    if outcome.consumer_created {
        cmds.push(stream_createconsumer_cmd(key, group, consumer));
    }
    let ids: Vec<crate::table::StreamId> = outcome.claimed.iter().map(|(id, _)| *id).collect();
    let Some(snap) = db.table.stream_group_snapshot(key, group, &ids) else {
        return cmds;
    };
    let mut claims = 0;
    for (id, pe) in &snap.pel {
        cmds.push(stream_xclaim_cmd(
            key,
            group,
            pe.consumer.clone(),
            *id,
            pe,
            snap.last_delivered_id,
        ));
        claims += 1;
    }
    for (id, pe) in &outcome.deleted {
        // XNACKed entries have no owner: name the claiming consumer, which
        // exists on the replica too.
        let owner = if pe.consumer.is_empty() {
            consumer.clone()
        } else {
            pe.consumer.clone()
        };
        cmds.push(stream_xclaim_cmd(
            key,
            group,
            owner,
            *id,
            pe,
            snap.last_delivered_id,
        ));
        claims += 1;
    }
    if outcome.last_id_advanced && claims == 0 {
        cmds.push(stream_setid_cmd(key, group, &snap));
    }
    cmds
}

#[inline]
pub fn tx_has_cross_slot(tx_queue: &[Command]) -> bool {
    let mut expected_slot: Option<u16> = None;
    for cmd in tx_queue {
        // A shard channel has a slot too, as in Redis.
        let channel = match cmd {
            Command::Spublish { channel, .. } => Some(channel.as_ref()),
            _ => None,
        };
        for k in cmd_keys(cmd).into_iter().chain(channel) {
            let slot = crate::router::key_slot(k);
            if let Some(s) = expected_slot {
                if s != slot {
                    return true;
                }
            } else {
                expected_slot = Some(slot);
            }
        }
    }
    false
}

#[inline]
pub fn write_resp_null(out: &mut Vec<u8>) {
    if CURRENT_CLIENT_RESP3.get() {
        out.extend_from_slice(b"_\r\n");
    } else {
        out.extend_from_slice(b"$-1\r\n");
    }
}

/// A map header of `pairs` entries: RESP3 `%`, or RESP2's flat array of
/// twice as many elements.
#[inline]
pub fn write_resp_map_len(out: &mut Vec<u8>, pairs: usize) {
    if CURRENT_CLIENT_RESP3.get() {
        out.extend_from_slice(format!("%{pairs}\r\n").as_bytes());
    } else {
        out.extend_from_slice(format!("*{}\r\n", pairs * 2).as_bytes());
    }
}

/// FUNCTION STATS `running_script`: null, or the running function's
/// `name`, `command` and `duration_ms`.
#[inline(never)]
fn write_function_stats_running(out: &mut Vec<u8>, port: u16) {
    let Some((name, argv, ms)) = crate::scripting::running_function(port) else {
        write_resp_null(out);
        return;
    };
    write_resp_map_len(out, 3);
    write_resp_bulk(out, b"name");
    write_resp_bulk(out, name.as_bytes());
    write_resp_bulk(out, b"command");
    write_resp_array_header(out, argv.len());
    for a in &argv {
        write_resp_bulk(out, a);
    }
    write_resp_bulk(out, b"duration_ms");
    write_resp_integer(out, ms.min(i64::MAX as u128) as i64);
}

/// A double already formatted as `text`: RESP3 `,`, or a RESP2 bulk
/// string (GEO coordinates, which Redis prints with their own precision).
#[inline]
pub fn write_resp_double_text(out: &mut Vec<u8>, text: &str) {
    if CURRENT_CLIENT_RESP3.get() {
        out.extend_from_slice(format!(",{text}\r\n").as_bytes());
    } else {
        write_resp_bulk(out, text.as_bytes());
    }
}

/// A simple-string reply line (`+text`), as Redis sends HELP lines.
#[inline]
pub fn write_resp_status(out: &mut Vec<u8>, text: &str) {
    out.push(b'+');
    out.extend_from_slice(text.as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// A set header of `len` members: RESP3 `~`, or a RESP2 array.
#[inline]
pub fn write_resp_set_len(out: &mut Vec<u8>, len: usize) {
    let tag = if CURRENT_CLIENT_RESP3.get() { '~' } else { '*' };
    out.extend_from_slice(format!("{tag}{len}\r\n").as_bytes());
}

#[inline]
pub fn write_resp_null_array(out: &mut Vec<u8>) {
    if CURRENT_CLIENT_RESP3.get() {
        out.extend_from_slice(b"_\r\n");
    } else {
        out.extend_from_slice(b"*-1\r\n");
    }
}

/// Digits of `val` when it is a whole number below 10^15 in magnitude, the
/// common case for scores, exactly as `%.17g` prints it: no `snprintf`.
#[inline]
fn integral_score_digits(val: f64, buf: &mut [u8; 20]) -> Option<usize> {
    if val.fract() != 0.0 || val.abs() >= 1e15 || val == 0.0 {
        return None;
    }
    let neg = val < 0.0;
    let mut n = val.abs() as u64;
    let mut i = buf.len();
    while n > 0 {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    if neg {
        i -= 1;
        buf[i] = b'-';
    }
    Some(i)
}

#[inline]
pub fn format_score(val: f64) -> String {
    let mut digits = [0u8; 20];
    if let Some(start) = integral_score_digits(val, &mut digits) {
        // ASCII digits and '-' only.
        return String::from_utf8_lossy(&digits[start..]).into_owned();
    }
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
        // SAFETY: `buf` is a 64-byte local array and snprintf writes at most
        // `buf.len()` bytes including the NUL; the format is a NUL-terminated C
        // literal whose single `%.17g` is matched by one f64 vararg.
        let len = unsafe {
            libc::snprintf(
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len(),
                c"%.17g".as_ptr(),
                val,
            )
        };
        if len > 0 && (len as usize) < buf.len() {
            String::from_utf8_lossy(&buf[..len as usize]).into_owned()
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
        let mut digits = [0u8; 20];
        if let Some(start) = integral_score_digits(val, &mut digits) {
            write_resp_bulk(out, &digits[start..]);
            return;
        }
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
        let mut hub = hub_arc.lock();
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
    // SAFETY: `pollfd` is a valid, exclusively borrowed local and nfds is 1;
    // timeout 0 never blocks. A stale fd only yields POLLNVAL.
    let ret = unsafe { libc::poll(&mut pollfd, 1, 0) };
    if ret > 0 {
        if pollfd.revents & (libc::POLLRDHUP | libc::POLLHUP | libc::POLLERR) != 0 {
            return true;
        }
        if pollfd.revents & libc::POLLIN != 0 {
            let mut buf = [0u8; 1];
            // SAFETY: `buf` is a 1-byte local array and the length passed is 1;
            // MSG_PEEK | MSG_DONTWAIT neither consumes data nor blocks.
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

/// Remaining milliseconds until `dl`, rounded up so a caller that waits in
/// whole-millisecond steps never times out before `dl` has passed.
#[inline]
pub(crate) fn remaining_wait_ms(dl: Instant, now: Instant) -> u64 {
    if now >= dl {
        0
    } else {
        (dl - now).as_nanos().div_ceil(1_000_000) as u64
    }
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
                let rem_ms = remaining_wait_ms(dl, now);
                if rem_ms == 0 {
                    return (None, false);
                }
                std::time::Duration::from_millis(rem_ms.min(20))
            }
            None => std::time::Duration::from_millis(20),
        };

        match monoio::time::timeout(check_dur, rx.recv_async()).await {
            Ok(Ok(res)) => return (Some(res), false),
            Ok(Err(_)) => return (None, false),
            Err(_) => {
                // While draining for shutdown the server has shut the read
                // side itself, which reads as a peer close: let the command
                // finish (the drain timeout bounds the wait) and reply.
                if let Some(fd) = raw_fd
                    && !crate::shutdown::is_shutting_down()
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
    rx: &flume::Receiver<crate::block::BlockedStreamResult>,
    timeout_ms: u64,
    raw_fd: Option<std::os::unix::io::RawFd>,
) -> (Option<crate::block::BlockedStreamResult>, bool) {
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
                let rem_ms = remaining_wait_ms(dl, now);
                if rem_ms == 0 {
                    return (None, false);
                }
                std::time::Duration::from_millis(rem_ms.min(20))
            }
            None => std::time::Duration::from_millis(20),
        };

        match monoio::time::timeout(check_dur, rx.recv_async()).await {
            Ok(Ok(res)) => return (Some(res), false),
            Ok(Err(_)) => return (None, false),
            Err(_) => {
                if let Some(fd) = raw_fd
                    && !crate::shutdown::is_shutting_down()
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

/// INFO clients' `watching_clients` (clients watching at least one key)
/// and `total_watched_keys` (keys watched, by database and name).
fn watch_counts(port: u16) -> (usize, usize) {
    if !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return (0, 0);
    }
    let map = WATCHED_KEYS.read();
    let Some(port_map) = map.get(&node_port(port)) else {
        return (0, 0);
    };
    let watchers: hashbrown::HashSet<u64> = port_map.values().flatten().copied().collect();
    let keys = port_map.values().filter(|c| !c.is_empty()).count();
    (watchers.len(), keys)
}

/// What every connection costs before it buffers anything: its client
/// state and reply buffer (Redis's idle `tot-mem` is about this).
pub const CLIENT_BASE_MEM: usize = 2048;
/// Per subscription, watched key or queued MULTI command, besides its bytes.
const CLIENT_ENTRY_OVERHEAD: usize = 64;

/// A client's memory, as Redis's `tot-mem` counts it: its query buffer,
/// output buffer, MULTI queue, subscriptions and watched keys, plus
/// [`CLIENT_BASE_MEM`]. `maxmemory-clients` evicts by it.
pub(crate) fn client_tot_mem(
    c: &ClientInfo,
    idle_secs: u64,
    port: u16,
    pubsub: &crate::pubsub::PubSubHub,
) -> usize {
    use std::sync::atomic::Ordering::Relaxed;
    let (qbuf, qbuf_free) = c.effective_qbuf(idle_secs);
    let (sub, psub, ssub, sub_bytes) = pubsub.client_sub_stats(c.id);
    let (watched, watch_bytes) = client_watch_mem(port, c.id);
    let (prefixes, prefix_bytes) = client_tracking_mem(c.id);
    let queued = c.stats.multi_count.load(Relaxed).max(0) as usize;
    CLIENT_BASE_MEM
        + qbuf
        + qbuf_free
        + c.omem_now()
        + c.stats.multi_mem.load(Relaxed)
        + sub_bytes
        + watch_bytes
        + prefix_bytes
        + (sub + psub + ssub + watched + queued + prefixes) * CLIENT_ENTRY_OVERHEAD
}

/// One CLIENT LIST / CLIENT INFO line (with its newline).
pub(crate) fn format_client_line(
    c: &ClientInfo,
    port: u16,
    pubsub: &crate::pubsub::PubSubHub,
    now: Instant,
) -> String {
    use std::sync::atomic::Ordering::Relaxed;
    let age = now.duration_since(c.connected_at).as_secs();
    let idle = now.duration_since(c.last_active).as_secs();
    let is_blocked = crate::block::get_block_hub_for_port(port)
        .lock()
        .is_blocked(c.id);
    let (sub, psub, ssub, _) = pubsub.client_sub_stats(c.id);
    let mut flags = String::new();
    if c.is_monitor {
        flags.push('O');
    } else if c.stats.is_replica.load(Relaxed) {
        flags.push('S');
    }
    if sub + psub + ssub > 0 {
        flags.push('P');
    }
    if is_blocked {
        flags.push('b');
    }
    let multi = c.stats.multi_count.load(Relaxed);
    if multi >= 0 {
        flags.push('x');
    }
    if c.stats.no_evict.load(Relaxed) {
        flags.push('e');
    }
    if flags.is_empty() {
        flags.push('N');
    }
    let (qbuf, qbuf_free) = c.effective_qbuf(idle);
    let s = &c.stats;
    format!(
        "id={} addr={} laddr=127.0.0.1:{} fd=8 name={} age={} idle={} flags={} db={} \
         sub={} psub={} ssub={} multi={} watch={} qbuf={} qbuf-free={} argv-mem=0 \
         multi-mem={} rbs=1024 rbp=0 obl=0 oll=0 omem={} omem-shared=0 omem-unshared=0 \
         tot-mem={} events=r cmd={} user=default redir=-1 resp={} lib-name={} lib-ver={} \
         io-thread=0 tot-net-in={} tot-net-out={} tot-cmds={} read-events={} \
         avg-pipeline-len-sum={} avg-pipeline-len-cnt={}\n",
        c.id,
        c.addr,
        port,
        c.name.as_deref().unwrap_or(""),
        age,
        idle,
        flags,
        c.db,
        sub,
        psub,
        ssub,
        multi,
        client_watch_count(port, c.id),
        qbuf,
        qbuf_free,
        s.multi_mem.load(Relaxed),
        c.omem_now(),
        client_tot_mem(c, idle, port, pubsub),
        c.last_cmd.to_lowercase(),
        if c.is_resp3 { 3 } else { 2 },
        c.lib_name.as_deref().unwrap_or(""),
        c.lib_ver.as_deref().unwrap_or(""),
        s.tot_net_in.load(Relaxed),
        s.tot_net_out.load(Relaxed),
        s.tot_cmds.load(Relaxed),
        s.read_events.load(Relaxed),
        s.pipeline_len_sum.load(Relaxed),
        s.pipeline_len_cnt.load(Relaxed),
    )
}

/// Client `id`'s CLIENT TRACKING prefixes: how many, and their bytes
/// (part of `tot-mem`, as in Redis).
pub(crate) fn client_tracking_mem(id: u64) -> (usize, usize) {
    if !HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
        return (0, 0);
    }
    let map = TRACKING_CLIENTS.read();
    map.iter()
        .find(|((_, cid), _)| *cid == id)
        .map_or((0, 0), |(_, t)| {
            (t.prefixes.len(), t.prefixes.iter().map(|p| p.len()).sum())
        })
}

/// The keys client `id` watches: how many, and their bytes.
pub(crate) fn client_watch_mem(port: u16, id: u64) -> (usize, usize) {
    if !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return (0, 0);
    }
    let map = WATCHED_KEYS.read();
    map.get(&node_port(port)).map_or((0, 0), |m| {
        m.iter()
            .filter(|(_, c)| c.contains(&id))
            .fold((0, 0), |(n, b), ((_, key), _)| (n + 1, b + key.len()))
    })
}

/// CLIENT LIST/INFO `watch=`: how many keys client `id` is watching.
pub(crate) fn client_watch_count(port: u16, id: u64) -> usize {
    if !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return 0;
    }
    let map = WATCHED_KEYS.read();
    map.get(&node_port(port))
        .map_or(0, |m| m.values().filter(|c| c.contains(&id)).count())
}

/// INFO clients' `active_clients`: the server's clients that sent
/// something in the last 512 ms.
fn active_clients(router: &Router) -> usize {
    let now = crate::table::coarse_now_ms();
    GLOBAL_CLIENTS
        .read()
        .iter()
        .filter(|((p, _), e)| {
            is_server_client_port(router, *p)
                && now.saturating_sub(
                    e.stats
                        .last_read_ms
                        .load(std::sync::atomic::Ordering::Relaxed),
                ) < 512
        })
        .count()
}

static WATCHED_KEYS: std::sync::LazyLock<
    parking_lot::RwLock<
        hashbrown::HashMap<u16, hashbrown::HashMap<(usize, Bytes), hashbrown::HashSet<u64>>>,
    >,
> = std::sync::LazyLock::new(|| parking_lot::RwLock::new(hashbrown::HashMap::new()));

static CLIENT_WATCH_TAINTED: std::sync::LazyLock<
    parking_lot::RwLock<hashbrown::HashMap<(u16, u64), bool>>,
> = std::sync::LazyLock::new(|| parking_lot::RwLock::new(hashbrown::HashMap::new()));

/// Number of latency histogram buckets; bucket `i` counts calls taking at
/// most 2^i microseconds (the last one also takes everything slower).
pub const LATENCY_BUCKETS: usize = 31;

/// Calls, total execution time and latency distribution of one command,
/// as reported by INFO commandstats and LATENCY HISTOGRAM.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct CmdStat {
    pub calls: u64,
    pub nanos: u64,
    pub hist: [u64; LATENCY_BUCKETS],
}

impl CmdStat {
    /// Adds `calls` calls taking `nanos` in total. All of them land in the
    /// bucket of their average latency.
    #[inline]
    fn add(&mut self, calls: u64, nanos: u64) {
        self.calls += calls;
        self.nanos += nanos;
        let usec = nanos / 1000 / calls.max(1);
        self.hist[latency_bucket(usec)] += calls;
    }

    fn merge(&mut self, other: &CmdStat) {
        self.calls += other.calls;
        self.nanos += other.nanos;
        for (a, b) in self.hist.iter_mut().zip(other.hist.iter()) {
            *a += b;
        }
    }
}

/// Index of the smallest power-of-two bucket holding `usec`.
#[inline]
fn latency_bucket(usec: u64) -> usize {
    if usec <= 1 {
        0
    } else {
        ((64 - (usec - 1).leading_zeros()) as usize).min(LATENCY_BUCKETS - 1)
    }
}

/// `cross-shard-strict`: refuse a multi-key command, or a transaction,
/// whose keys live on different shards, with Redis Cluster's CROSSSLOT
/// error. Off by default: such commands then run across shards, with the
/// weaker atomicity `docs/differences.md` describes.
static CROSS_SHARD_STRICT: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn set_cross_shard_strict(on: bool) {
    CROSS_SHARD_STRICT.store(on, std::sync::atomic::Ordering::Relaxed);
}

pub fn cross_shard_strict() -> bool {
    CROSS_SHARD_STRICT.load(std::sync::atomic::Ordering::Relaxed)
}

/// Whether `keys` live on more than one of `num_shards` shards.
pub fn keys_on_several_shards<'a>(
    keys: impl IntoIterator<Item = &'a [u8]>,
    num_shards: usize,
) -> bool {
    let mut first = None;
    for k in keys {
        let shard = target_shard(k, num_shards);
        match first {
            None => first = Some(shard),
            Some(f) if f != shard => return true,
            Some(_) => {}
        }
    }
    false
}

/// `latency-tracking`: whether `INFO latencystats` reports percentiles.
pub static LATENCY_TRACKING: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// `latency-tracking-info-percentiles` (Redis default `50 99 99.9`).
static LATENCY_PERCENTILES: parking_lot::RwLock<Vec<f64>> = parking_lot::RwLock::new(Vec::new());

fn latency_percentiles() -> Vec<f64> {
    let p = LATENCY_PERCENTILES.read();
    if p.is_empty() {
        vec![50.0, 99.0, 99.9]
    } else {
        p.clone()
    }
}

/// Formats a percentile like Redis does in `INFO latencystats` and
/// `CONFIG GET latency-tracking-info-percentiles` (`50`, `99.9`).
fn format_percentile(p: f64) -> String {
    let s = format!("{p:.3}");
    s.trim_end_matches('0').trim_end_matches('.').to_string()
}

/// The `p`th percentile (0-100) of a command's latency in microseconds,
/// interpolated inside the power-of-two histogram bucket it falls in.
fn latency_percentile_usec(hist: &[u64; LATENCY_BUCKETS], calls: u64, p: f64) -> f64 {
    if calls == 0 {
        return 0.0;
    }
    let target = ((p / 100.0) * calls as f64).ceil().max(1.0);
    let mut cum = 0u64;
    for (i, &n) in hist.iter().enumerate() {
        if n == 0 {
            continue;
        }
        if (cum + n) as f64 >= target {
            let upper = (1u64 << i) as f64;
            let lower = if i == 0 {
                0.0
            } else {
                (1u64 << (i - 1)) as f64
            };
            let frac = (target - cum as f64) / n as f64;
            return lower + (upper - lower) * frac;
        }
        cum += n;
    }
    (1u64 << (LATENCY_BUCKETS - 1)) as f64
}

pub static CMD_STATS: std::sync::LazyLock<
    parking_lot::RwLock<hashbrown::HashMap<String, CmdStat>>,
> = std::sync::LazyLock::new(|| parking_lot::RwLock::new(hashbrown::HashMap::new()));
pub static MIN_REPLICAS_TO_WRITE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);
/// `min-replicas-max-lag`: seconds since its last ack within which a
/// replica counts towards `min-replicas-to-write`.
pub static MIN_REPLICAS_MAX_LAG: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(10);

/// Whether `min-replicas-to-write` rejects writes on this server now: it
/// is set, this is a master, and fewer replicas than that acked within
/// `min-replicas-max-lag` seconds (Redis's NOREPLICAS).
pub fn not_enough_good_replicas(port: u16) -> bool {
    let min = MIN_REPLICAS_TO_WRITE.load(std::sync::atomic::Ordering::Relaxed);
    if min == 0 {
        return false;
    }
    let hub = crate::replication::cached_hub(port);
    if hub.is_slave() {
        return false;
    }
    let lag = MIN_REPLICAS_MAX_LAG.load(std::sync::atomic::Ordering::Relaxed);
    hub.good_replicas(lag) < min
}

thread_local! {
    static LOCAL_CMD_STATS: RefCell<hashbrown::HashMap<&'static str, CmdStat>> = RefCell::new(hashbrown::HashMap::new());
    static LOCAL_CMD_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub static CURRENT_AUTH_USER: RefCell<String> = const { RefCell::new(String::new()) };
}

#[inline]
pub fn record_cmd_stat(name: &'static str) {
    record_cmd_stats(name, 1, 0);
}

/// Adds `calls` calls of `name` taking `nanos` in total to this thread's
/// command statistics.
pub fn record_cmd_stats(name: &'static str, calls: u64, nanos: u64) {
    crate::server_stats::add(crate::server_stats::Stat::Commands, calls);
    LOCAL_CMD_STATS.with(|stats| {
        let mut map = stats.borrow_mut();
        map.entry(name).or_default().add(calls, nanos);
    });
    LOCAL_CMD_COUNT.with(|count| {
        let c = count.get() + calls as usize;
        if c >= 1024 {
            count.set(0);
            flush_local_cmd_stats();
        } else {
            count.set(c);
        }
    });
}

/// Commands whose run time includes waiting for data, replicas or fsync.
/// Like Redis, the wait is not counted as execution time.
fn may_block(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Blpop { .. }
            | Command::Brpop { .. }
            | Command::Blmpop { .. }
            | Command::Blmove { .. }
            | Command::Bzpopmin { .. }
            | Command::Bzpopmax { .. }
            | Command::Bzmpop { .. }
            | Command::Xread { .. }
            | Command::Xreadgroup { .. }
            | Command::Wait { .. }
            | Command::WaitAof { .. }
    )
}

/// Records one call of a command run by `execute_command`, timed from
/// `start` until the guard drops.
struct CmdStatGuard {
    name: &'static str,
    start: Option<std::time::Instant>,
    port: u16,
    client_id: u64,
    /// Refused before it ran (NOAUTH, NOPERM, OOM, READONLY, ...): a
    /// rejected call, as Redis's rejectCommand counts it, not a call.
    rejected: std::cell::Cell<bool>,
    /// The slot its CPU time goes to (CLUSTER SLOT-STATS cpu-usec).
    slot: Option<u16>,
    /// While a HOTKEYS session runs: the command's keys and request bytes.
    hot: Option<(Vec<Bytes>, u64)>,
}

impl Drop for CmdStatGuard {
    fn drop(&mut self) {
        // A blocked command refused by ACL when data arrived is a rejected
        // call, as in Redis (it never ran).
        if self.rejected.get() || take_blocked_rejected(self.port, self.client_id) {
            record_rejected_stat(self.name);
            return;
        }
        let nanos = self.start.map_or(0, |s| s.elapsed().as_nanos() as u64);
        record_cmd_stats(self.name, 1, nanos);
        if let Some((keys, net)) = self.hot.take() {
            crate::hotkeys::record(&keys, nanos, net);
        }
        if self.slot.is_some() && !crate::slot_stats::take_crossed() {
            crate::slot_stats::record_cpu(self.slot, nanos);
        }
    }
}

/// Command statistics for one batched pipeline. Calls are tallied per
/// command name; the time of each segment (the stretch between `restart`
/// and `flush`) is split across the commands counted in it in proportion
/// to their calls. That costs two clock reads per segment instead of two
/// per command.
struct CmdStatBatch {
    start: std::time::Instant,
    runs: smallvec::SmallVec<[(&'static str, u64); 8]>,
}

impl CmdStatBatch {
    fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
            runs: smallvec::SmallVec::new(),
        }
    }

    #[inline]
    fn note(&mut self, name: &'static str) {
        if let Some(last) = self.runs.last_mut()
            && std::ptr::eq(last.0, name)
        {
            last.1 += 1;
        } else if let Some(run) = self.runs.iter_mut().find(|r| std::ptr::eq(r.0, name)) {
            run.1 += 1;
        } else {
            self.runs.push((name, 1));
        }
    }

    /// Ends the current segment and records its commands.
    fn flush(&mut self) {
        if self.runs.is_empty() {
            return;
        }
        let elapsed = self.start.elapsed().as_nanos();
        let total: u64 = self.runs.iter().map(|r| r.1).sum();
        for (name, calls) in self.runs.drain(..) {
            let nanos = elapsed * u128::from(calls) / u128::from(total);
            record_cmd_stats(name, calls, nanos as u64);
        }
    }

    /// Starts a new segment, leaving out the time since the last `flush`.
    fn restart(&mut self) {
        self.start = std::time::Instant::now();
    }
}

pub fn flush_local_cmd_stats() {
    LOCAL_CMD_STATS.with(|stats| {
        let mut local_map = stats.borrow_mut();
        if local_map.is_empty() {
            return;
        }
        {
            let mut global_map = CMD_STATS.write();
            for (&cmd, stat) in local_map.iter() {
                global_map
                    .entry(cmd.to_lowercase())
                    .or_default()
                    .merge(stat);
            }
        }
        local_map.clear();
    });
}

pub fn reset_local_cmd_stats() {
    LOCAL_CMD_STATS.with(|stats| stats.borrow_mut().clear());
    LOCAL_CMD_COUNT.with(|count| count.set(0));
}

pub static LATENCY_MONITOR_THRESHOLD: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(0);
pub static LATENCY_EVENTS: std::sync::LazyLock<
    parking_lot::RwLock<std::collections::BTreeMap<String, Vec<(u64, u64)>>>,
> = std::sync::LazyLock::new(|| parking_lot::RwLock::new(std::collections::BTreeMap::new()));

/// Name a command is counted under in INFO commandstats and LATENCY
/// HISTOGRAM: [`get_cmd_name`], with CONFIG split into its subcommands
/// like Redis does.
pub fn stat_cmd_name(cmd: &Command) -> &'static str {
    use crate::resp::XinfoSubcommand as XI;
    match cmd {
        Command::XgroupCreate { .. } => "xgroup|create",
        Command::XgroupDestroy { .. } => "xgroup|destroy",
        Command::XgroupCreateConsumer { .. } => "xgroup|createconsumer",
        Command::XgroupDelConsumer { .. } => "xgroup|delconsumer",
        Command::XgroupSetId { .. } => "xgroup|setid",
        Command::XgroupHelp => "xgroup|help",
        Command::Xinfo(sub) => match sub {
            XI::Stream(_) | XI::StreamFull { .. } => "xinfo|stream",
            XI::Groups(_) => "xinfo|groups",
            XI::Consumers { .. } => "xinfo|consumers",
            XI::Help => "xinfo|help",
        },
        Command::ConfigGet(_) => "config|get",
        Command::ConfigSet(pairs) => match pairs.first() {
            Some((p, _)) if p.eq_ignore_ascii_case(b"resetstat") => "config|resetstat",
            Some((p, _)) if p.eq_ignore_ascii_case(b"rewrite") => "config|rewrite",
            _ => "config|set",
        },
        _ => get_cmd_name(cmd),
    }
}

pub fn record_latency_event(event: &str, latency_ms: u64) {
    let thresh = LATENCY_MONITOR_THRESHOLD.load(std::sync::atomic::Ordering::Relaxed);
    if thresh <= 0 || latency_ms < thresh as u64 {
        return;
    }
    let now_sec = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    {
        let mut map = LATENCY_EVENTS.write();
        let samples = map.entry(event.to_string()).or_default();
        if let Some(last) = samples.last_mut()
            && last.0 == now_sec
        {
            if latency_ms > last.1 {
                last.1 = latency_ms;
            }
            return;
        }
        samples.push((now_sec, latency_ms));
    }
}

pub fn write_latency_response(sub: &LatencySubcommand, out: &mut Vec<u8>) {
    match sub {
        LatencySubcommand::Latest => {
            let map = LATENCY_EVENTS.read();
            let active: Vec<_> = map.iter().filter(|(_, s)| !s.is_empty()).collect();
            write_resp_array_header(out, active.len());
            for (ev, samples) in active {
                let &(last_ts, last_lat) = samples.last().unwrap();
                let max_lat = samples.iter().map(|&(_, l)| l).max().unwrap_or(last_lat);
                write_resp_array_header(out, 4);
                write_resp_bulk(out, ev.as_bytes());
                write_resp_integer(out, last_ts as i64);
                write_resp_integer(out, last_lat as i64);
                write_resp_integer(out, max_lat as i64);
            }
        }
        LatencySubcommand::History(ev) => {
            if ev.is_empty() {
                out.extend_from_slice(
                    b"-ERR wrong number of arguments for 'latency|history' command\r\n",
                );
            } else {
                let map = LATENCY_EVENTS.read();
                if let Some(samples) = map.get(ev) {
                    write_resp_array_header(out, samples.len());
                    for &(ts, lat) in samples {
                        write_resp_array_header(out, 2);
                        write_resp_integer(out, ts as i64);
                        write_resp_integer(out, lat as i64);
                    }
                } else {
                    out.extend_from_slice(b"*0\r\n");
                }
            }
        }
        LatencySubcommand::Doctor => {
            out.extend_from_slice(b"+Dave, no latency spikes observed in your instance.\r\n");
        }
        LatencySubcommand::Reset(events) => {
            let mut map = LATENCY_EVENTS.write();
            let count = if events.is_empty() {
                let n = map.len() as i64;
                map.clear();
                n
            } else {
                let mut n = 0;
                for ev in events {
                    if map.remove(ev).is_some() {
                        n += 1;
                    }
                }
                n
            };
            write_resp_integer(out, count);
        }
        LatencySubcommand::Graph(ev) => {
            if ev.is_empty() {
                out.extend_from_slice(
                    b"-ERR wrong number of arguments for 'latency|graph' command\r\n",
                );
            } else {
                let map = LATENCY_EVENTS.read();
                if let Some(samples) = map.get(ev)
                    && !samples.is_empty()
                {
                    let high = samples.iter().map(|&(_, l)| l).max().unwrap_or(0);
                    let low = samples.iter().map(|&(_, l)| l).min().unwrap_or(0);
                    let graph = format!(
                        "{} - high {} ms, low {} ms (all time high {} ms)\n--------------------------------------------------------------------------------\n",
                        ev, high, low, high
                    );
                    write_resp_bulk(out, graph.as_bytes());
                } else {
                    let err = format!("-ERR No samples available for event '{}'\r\n", err_arg(ev));
                    out.extend_from_slice(err.as_bytes());
                }
            }
        }
        LatencySubcommand::Histogram(cmds) => {
            // Callers flush every shard's pending stats first.
            let map = CMD_STATS.read();
            let wanted: Vec<String> = cmds.iter().map(|c| c.to_lowercase()).collect();
            let mut matched: Vec<(&String, &CmdStat)> = map
                .iter()
                .filter(|&(k, stat)| {
                    stat.calls > 0
                        && (wanted.is_empty()
                            || wanted.iter().any(|c| {
                                k == c
                                    || (k.starts_with(c.as_str()) && k[c.len()..].starts_with('|'))
                            }))
                })
                .collect();
            matched.sort_by(|a, b| a.0.cmp(b.0));
            write_resp_array_header(out, matched.len() * 2);
            for (k, stat) in matched {
                write_resp_bulk(out, k.as_bytes());
                write_resp_array_header(out, 4);
                write_resp_bulk(out, b"calls");
                write_resp_integer(out, stat.calls as i64);
                write_resp_bulk(out, b"histogram_usec");
                // Like Redis: (bucket upper bound in usec, cumulative calls)
                // for every non-empty power-of-two bucket.
                let buckets = stat.hist.iter().filter(|&&n| n > 0).count();
                write_resp_array_header(out, buckets * 2);
                let mut cumulative = 0u64;
                for (i, &n) in stat.hist.iter().enumerate() {
                    if n > 0 {
                        cumulative += n;
                        write_resp_integer(out, 1i64 << i);
                        write_resp_integer(out, cumulative as i64);
                    }
                }
            }
        }
        LatencySubcommand::Help => {
            let help_lines = [
                "LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                "DOCTOR",
                "    Return a human readable latency analysis report.",
                "GRAPH <event>",
                "    Return an ASCII latency graph for the <event> class.",
                "HISTORY <event>",
                "    Return time-latency samples for the <event> class.",
                "LATEST",
                "    Return the latest latency samples for all events.",
                "RESET [<event> ...]",
                "    Reset latency data of one or more <event> classes.",
                "    (default: reset all data for all event classes)",
                "HISTOGRAM [<command> ...]",
                "    Return a cumulative distribution of latencies in the format of a histogram for the specified command names, or all commands if no command name is specified.",
                "HELP",
                "    Print this help.",
            ];
            out.extend_from_slice(format!("*{}\r\n", help_lines.len()).as_bytes());
            for line in help_lines {
                write_resp_status(out, line);
            }
        }
    }
}

/// `timeout`: close a client connection after this many idle seconds
/// (0, the default, means never).
pub static CLIENT_IDLE_TIMEOUT_SECS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// `tcp-keepalive`: SO_KEEPALIVE idle time in seconds for client sockets
/// (0 disables keepalive). 300 like Redis.
pub static TCP_KEEPALIVE_SECS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(300);

/// `replica-read-only`: whether a replica rejects writes from its clients.
/// `stop-writes-on-bgsave-error` (Redis default yes).
pub static STOP_WRITES_ON_BGSAVE_ERROR: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// Whether any command of a batch writes (by the same test as the
/// -READONLY check of `execute_command`).
pub(crate) fn batch_has_writes(commands: &[Command]) -> bool {
    commands
        .iter()
        .any(|c| c.is_write_command() || crate::aof::command_to_resp(c).is_some())
}

/// The `-MISCONF` reply for write commands while the server at `router`
/// cannot persist, as Redis refuses writes then: an AOF write is failing,
/// or the last RDB save failed with save points set and
/// `stop-writes-on-bgsave-error yes`. `None` when writes are allowed.
pub(crate) fn persistence_misconf(router: &Router) -> Option<String> {
    if let Some(e) = crate::aof::aof_write_misconf(router.port) {
        return Some(e);
    }
    if crate::snapshot::any_save_failing()
        && STOP_WRITES_ON_BGSAVE_ERROR.load(std::sync::atomic::Ordering::Relaxed)
        && !crate::snapshot::state(router.base_port).last_save_ok()
        && !crate::config::save_points(router.base_port).is_empty()
    {
        return Some(
            "-MISCONF Redis is configured to save RDB snapshots, but it's currently unable to persist to disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs for details about the RDB error.\r\n"
                .to_string(),
        );
    }
    None
}

/// `replica-serve-stale-data`: whether a replica whose master link is
/// down still serves its clients (yes, the Redis default); with no, only
/// commands flagged `stale` (see `command_info::serves_stale`) are, the
/// others get -MASTERDOWN.
pub static REPLICA_SERVE_STALE_DATA: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// The -MASTERDOWN reply (without the leading `-`) when this server, a
/// replica with `replica-serve-stale-data no` and its master link down,
/// must refuse `cmd`; None otherwise.
pub(crate) fn masterdown_refuses(router: &Router, cmd: &Command) -> Option<&'static [u8]> {
    if !serving_stale_data(router.base_port) {
        return None;
    }
    let served = match cmd {
        // A script runs on a stale replica only if it says it may
        // (Redis decides from the script's flags, not FCALL's).
        Command::Fcall { function, .. } => {
            crate::scripting::function_has_flag(function, "allow-stale")
        }
        Command::Eval { script, .. } => crate::scripting::shebang_has_flag(script, "allow-stale"),
        Command::Evalsha { sha, .. } => crate::scripting::get_script(&String::from_utf8_lossy(sha))
            .is_some_and(|src| crate::scripting::shebang_has_flag(src.as_bytes(), "allow-stale")),
        _ => crate::command_info::serves_stale(acl_cmd_name(cmd)),
    };
    if served {
        return None;
    }
    Some(b"MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n")
}

/// Whether the server on `base_port` is a replica whose master link is
/// down with `replica-serve-stale-data no`: only commands flagged `stale`
/// (and scripts flagged `allow-stale`) may run.
pub(crate) fn serving_stale_data(base_port: u16) -> bool {
    if REPLICA_SERVE_STALE_DATA.load(std::sync::atomic::Ordering::Relaxed)
        || !crate::replication::HAS_SLAVE_INSTANCE.load(std::sync::atomic::Ordering::Relaxed)
    {
        return false;
    }
    let hub = crate::replication::cached_hub(base_port);
    hub.is_slave() && !hub.can_serve_replicas()
}

/// Inside an `allow-stale` script on such a replica: the error for a
/// command not flagged `stale`, as Redis refuses it there.
pub(crate) fn script_stale_refuses(port: u16, cmd: &Command) -> bool {
    serving_stale_data(node_port(port)) && !crate::command_info::serves_stale(acl_cmd_name(cmd))
}

pub(crate) const SCRIPT_STALE_ERR: &str = "ERR Can not execute the command on a stale replica. Please use a non-stale replica or the master";

pub static REPLICA_READ_ONLY: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// `replica-ignore-maxmemory`: whether a replica leaves `maxmemory` to its
/// master (yes, the Redis default). A replica evicting on its own would
/// drop keys its master still has, and its keyspace would diverge.
pub static REPLICA_IGNORE_MAXMEMORY: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// Whether the server on `port` skips `maxmemory` enforcement because it is
/// a replica and `replica-ignore-maxmemory` is on.
#[inline]
pub fn maxmemory_ignored_as_replica(port: u16) -> bool {
    REPLICA_IGNORE_MAXMEMORY.load(std::sync::atomic::Ordering::Relaxed)
        && crate::replication::HAS_SLAVE_INSTANCE.load(std::sync::atomic::Ordering::Relaxed)
        && crate::replication::cached_hub(port).is_slave()
}

/// Applies `tcp-keepalive` to an accepted client socket, like Redis's
/// anetKeepAlive: probes start after the idle time and are sent every third
/// of it, three times, before the peer is considered dead.
pub fn apply_tcp_keepalive(fd: std::os::unix::io::RawFd) {
    let secs = TCP_KEEPALIVE_SECS.load(std::sync::atomic::Ordering::Relaxed);
    if secs == 0 {
        return;
    }
    // SAFETY: (for the closure's unsafe block) `val` is a live c_int on the stack and
    // optlen is its exact size; setsockopt only reads it. `fd` is the caller's
    // freshly accepted socket.
    let set = |level: libc::c_int, opt: libc::c_int, val: libc::c_int| unsafe {
        libc::setsockopt(
            fd,
            level,
            opt,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of_val(&val) as libc::socklen_t,
        );
    };
    let idle = secs.min(i32::MAX as u64) as libc::c_int;
    set(libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1);
    set(libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, idle);
    set(libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, (idle / 3).max(1));
    set(libc::IPPROTO_TCP, libc::TCP_KEEPCNT, 3);
}

/// How long the next read from this client may wait before the connection
/// is closed for being idle, or None if it must not time out. As in Redis,
/// MONITOR clients and Pub/Sub subscribers are exempt (replica links leave
/// this loop for the replication stream); blocked clients are not reading.
fn idle_read_limit(
    router: &Router,
    client_id: u64,
    registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
) -> Option<std::time::Duration> {
    let secs = CLIENT_IDLE_TIMEOUT_SECS.load(std::sync::atomic::Ordering::Relaxed);
    if secs == 0 {
        return None;
    }
    if router.pubsub.borrow().clients.contains_key(&client_id)
        || registry
            .borrow()
            .get(&client_id)
            .is_some_and(|c| c.is_monitor)
    {
        return None;
    }
    Some(std::time::Duration::from_secs(secs))
}

pub static PAUSE_DEADLINE_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static PAUSE_WRITE_ONLY: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
pub static PAUSED_CLIENTS: std::sync::LazyLock<
    parking_lot::Mutex<Vec<(u16, u64, flume::Sender<()>)>>,
> = std::sync::LazyLock::new(|| parking_lot::Mutex::new(Vec::new()));

#[inline]
pub fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn set_client_pause(timeout_ms: u64, write_only: bool) {
    if timeout_ms == 0 {
        let _ = unpause_clients();
        return;
    }
    let now = now_epoch_ms();
    let new_deadline = now.saturating_add(timeout_ms);
    let old_deadline = PAUSE_DEADLINE_MS.load(std::sync::atomic::Ordering::SeqCst);
    if old_deadline > now {
        let old_write_only = PAUSE_WRITE_ONLY.load(std::sync::atomic::Ordering::SeqCst);
        let merged_write_only = old_write_only && write_only;
        let merged_deadline = old_deadline.max(new_deadline);
        PAUSE_WRITE_ONLY.store(merged_write_only, std::sync::atomic::Ordering::SeqCst);
        PAUSE_DEADLINE_MS.store(merged_deadline, std::sync::atomic::Ordering::SeqCst);
    } else {
        PAUSE_WRITE_ONLY.store(write_only, std::sync::atomic::Ordering::SeqCst);
        PAUSE_DEADLINE_MS.store(new_deadline, std::sync::atomic::Ordering::SeqCst);
    }
}

pub fn unpause_clients() -> usize {
    PAUSE_DEADLINE_MS.store(0, std::sync::atomic::Ordering::SeqCst);
    let mut list = PAUSED_CLIENTS.lock();
    let woken = list.len();
    for (_, _, tx) in list.drain(..) {
        let _ = tx.send(());
    }
    woken
}

#[inline]
pub fn is_client_paused() -> Option<(u64, bool)> {
    let dl = PAUSE_DEADLINE_MS.load(std::sync::atomic::Ordering::Relaxed);
    if dl == 0 {
        return None;
    }
    let now = now_epoch_ms();
    // Times are whole milliseconds, so `now == dl` can still be up to 1ms
    // short of the full pause; only release once `dl` has passed.
    if now > dl {
        let _ = unpause_clients();
        return None;
    }
    let write_only = PAUSE_WRITE_ONLY.load(std::sync::atomic::Ordering::Relaxed);
    Some((dl - now + 1, write_only))
}

#[inline]
pub fn paused_clients_count() -> usize {
    if is_client_paused().is_none() {
        return 0;
    }
    PAUSED_CLIENTS.lock().len()
}

/// Commands Redis flags `allow-busy`: served while a script runs past
/// `busy-reply-threshold`, when every other command gets `-BUSY`.
fn allowed_while_busy(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Auth { .. }
            | Command::Hello { .. }
            | Command::Client(
                ClientSubcommand::Caching(_)
                    | ClientSubcommand::GetName
                    | ClientSubcommand::GetRedir
                    | ClientSubcommand::Id
                    | ClientSubcommand::NoEvict(_)
                    | ClientSubcommand::NoTouch(_)
                    | ClientSubcommand::Reply(_)
                    | ClientSubcommand::SetInfo { .. }
                    | ClientSubcommand::SetName(_)
            )
            | Command::Discard
            | Command::FunctionKill
            | Command::FunctionStats
            | Command::Multi
            | Command::Quit
            | Command::Replconf(_)
            | Command::Reset
            | Command::ScriptKill
            | Command::Select(_)
            | Command::Shutdown { .. }
            | Command::Unwatch
            | Command::Watch(_)
    )
}

pub fn should_pause_command(cmd: &Command, write_only: bool) -> bool {
    if matches!(
        cmd,
        Command::Client(ClientSubcommand::Unpause | ClientSubcommand::Pause(..)) | Command::Quit
    ) {
        return false;
    }
    if !write_only {
        return true;
    }
    match cmd {
        Command::Eval {
            script, read_only, ..
        } => !*read_only && !crate::scripting::is_script_read_only(script),
        Command::Evalsha { sha, read_only, .. } => {
            !*read_only && !crate::scripting::is_sha_read_only(sha)
        }
        Command::Fcall {
            function,
            read_only,
            ..
        } => !*read_only && !crate::scripting::is_function_read_only(function),
        Command::Publish { .. }
        | Command::Spublish { .. }
        | Command::Pfcount { .. }
        | Command::Blpop { .. }
        | Command::Brpop { .. }
        | Command::Blmove { .. }
        | Command::Blmovem { .. }
        | Command::Blmpop { .. }
        | Command::Bzpopmin { .. }
        | Command::Bzpopmax { .. }
        | Command::Bzmpop { .. }
        | Command::Xreadgroup { .. } => true,
        _ => cmd.is_write_command(),
    }
}

const WRITE_SLOTS: usize = 64;
/// Write commands (and squashed batches with writes) executing now, past
/// the CLIENT PAUSE gate; slotted per thread so the count isn't one
/// contended cache line. See [`writes_running`].
static WRITES_RUNNING: [crate::mailbox::CachePadded<std::sync::atomic::AtomicUsize>; WRITE_SLOTS] =
    [const { crate::mailbox::CachePadded(std::sync::atomic::AtomicUsize::new(0)) }; WRITE_SLOTS];
static NEXT_WRITE_SLOT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

thread_local! {
    static MY_WRITE_SLOT: usize =
        NEXT_WRITE_SLOT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % WRITE_SLOTS;
}

/// Counts a write as running (see [`writes_running`]) while it lives.
pub struct WriteRunning(Option<usize>);

impl WriteRunning {
    /// Counted before checking the pause (both SeqCst), so a pause and a
    /// write racing it see each other: either the write waits, or whoever
    /// paused sees it running.
    fn enter(counts: bool) -> Self {
        if !counts {
            return Self(None);
        }
        let slot = MY_WRITE_SLOT.with(|s| *s);
        WRITES_RUNNING[slot].fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Self(Some(slot))
    }
}

impl Drop for WriteRunning {
    fn drop(&mut self) {
        if let Some(slot) = self.0 {
            WRITES_RUNNING[slot].fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

/// Write commands that passed the pause gate and haven't finished. Right
/// after pausing writes, some may still be running; once this is 0 every
/// write is applied and replicated, so the replication offset is final
/// (FAILOVER, and a shutdown waiting for replicas, compare it).
pub fn writes_running() -> usize {
    WRITES_RUNNING
        .iter()
        .map(|c| c.0.load(std::sync::atomic::Ordering::SeqCst))
        .sum()
}

/// Whether `cmd` counts in [`writes_running`]: a write a pause holds, and
/// not one that may block (a blocked BLPOP changes nothing until a write
/// serves it).
fn counts_as_running_write(cmd: &Command) -> bool {
    should_pause_command(cmd, true) && !may_block(cmd)
}

/// A squashed pipeline batch with writes, counted while it runs; None
/// (nothing counted) if writes are paused now: then it must take the
/// per-command path, which waits at the pause gate.
pub fn enter_write_batch(commands: &[Command]) -> Option<WriteRunning> {
    let guard = WriteRunning::enter(commands.iter().any(counts_as_running_write));
    // Writes to slots being handed over wait on the per-command path.
    if guard.0.is_some() && crate::asm::any_write_pause() {
        return None;
    }
    // A congested replica (`repl-backpressure-threshold`) holds writes back
    // at the same gate.
    if guard.0.is_some()
        && (is_client_paused().is_some() || crate::replication::backpressure_active())
    {
        return None;
    }
    Some(guard)
}

struct PausedClientGuard {
    client_id: u64,
}

impl Drop for PausedClientGuard {
    fn drop(&mut self) {
        let mut list = PAUSED_CLIENTS.lock();
        list.retain(|&(_, cid, _)| cid != self.client_id);
    }
}

pub async fn wait_if_client_paused(port: u16, client_id: u64, cmd: &Command) -> WriteRunning {
    if in_tx(client_id) {
        return WriteRunning(None);
    }
    let counts = counts_as_running_write(cmd);
    let mut waited_for_replicas = false;
    loop {
        let running = WriteRunning::enter(counts);
        let Some((rem_ms, write_only)) = is_client_paused() else {
            // A replica falling behind holds the write back (once, bounded
            // by `repl-backpressure-max-delay`), then it checks the pause
            // again.
            if counts && !waited_for_replicas && crate::replication::backpressure_active() {
                drop(running);
                waited_for_replicas = true;
                crate::replication::backpressure_delay().await;
                continue;
            }
            return running;
        };
        if !should_pause_command(cmd, write_only) {
            return running;
        }
        drop(running);
        let (tx, rx) = flume::bounded(1);
        {
            let mut list = PAUSED_CLIENTS.lock();
            if !list.iter().any(|&(_, cid, _)| cid == client_id) {
                list.push((port, client_id, tx));
            }
        }
        let _guard = PausedClientGuard { client_id };
        let _ = monoio::time::timeout(
            std::time::Duration::from_millis(rem_ms.max(1)),
            rx.recv_async(),
        )
        .await;
        drop(_guard);
        if now_epoch_ms() > PAUSE_DEADLINE_MS.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = unpause_clients();
        }
    }
}

pub async fn wait_if_tx_paused(port: u16, client_id: u64, tx_queue: &[Command]) -> WriteRunning {
    let counts = tx_queue.iter().any(counts_as_running_write);
    let mut waited_for_replicas = false;
    loop {
        let running = WriteRunning::enter(counts);
        let Some((rem_ms, write_only)) = is_client_paused() else {
            // See wait_if_client_paused.
            if counts && !waited_for_replicas && crate::replication::backpressure_active() {
                drop(running);
                waited_for_replicas = true;
                crate::replication::backpressure_delay().await;
                continue;
            }
            return running;
        };
        let should_pause =
            !write_only || tx_queue.iter().any(|c| should_pause_command(c, write_only));
        if !should_pause {
            return running;
        }
        drop(running);
        let (tx, rx) = flume::bounded(1);
        {
            let mut list = PAUSED_CLIENTS.lock();
            if !list.iter().any(|&(_, cid, _)| cid == client_id) {
                list.push((port, client_id, tx));
            }
        }
        let _guard = PausedClientGuard { client_id };
        let _ = monoio::time::timeout(
            std::time::Duration::from_millis(rem_ms.max(1)),
            rx.recv_async(),
        )
        .await;
        drop(_guard);
        if now_epoch_ms() > PAUSE_DEADLINE_MS.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = unpause_clients();
        }
    }
}

pub static TOTAL_ERROR_REPLIES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static ERROR_STATS: std::sync::LazyLock<parking_lot::RwLock<hashbrown::HashMap<String, u64>>> =
    std::sync::LazyLock::new(|| parking_lot::RwLock::new(hashbrown::HashMap::new()));
pub static FAILED_CMD_STATS: std::sync::LazyLock<
    parking_lot::RwLock<hashbrown::HashMap<String, u64>>,
> = std::sync::LazyLock::new(|| parking_lot::RwLock::new(hashbrown::HashMap::new()));
pub static REJECTED_CMD_STATS: std::sync::LazyLock<
    parking_lot::RwLock<hashbrown::HashMap<String, u64>>,
> = std::sync::LazyLock::new(|| parking_lot::RwLock::new(hashbrown::HashMap::new()));

/// Increments `map[key]` (lowercased when `lower`), allocating the key only
/// on first insert. Error replies can be hot (e.g. every write rejected
/// under `noeviction`), so the common case stays allocation-free.
fn bump_stat(map: &mut hashbrown::HashMap<String, u64>, key: &str, lower: bool) {
    let mut buf = [0u8; 64];
    let key = if lower && key.bytes().any(|b| b.is_ascii_uppercase()) {
        if key.len() <= buf.len() && key.is_ascii() {
            buf[..key.len()].copy_from_slice(key.as_bytes());
            buf[..key.len()].make_ascii_lowercase();
            // ASCII in, ASCII out.
            std::str::from_utf8(&buf[..key.len()]).unwrap_or(key)
        } else {
            *map.entry(key.to_lowercase()).or_insert(0) += 1;
            return;
        }
    } else {
        key
    };
    *map.entry_ref(key).or_insert(0) += 1;
}

pub fn record_rejected_stat(cmd_name: &str) {
    {
        let mut map = REJECTED_CMD_STATS.write();
        bump_stat(&mut map, cmd_name, true);
    }
}

pub fn record_failed_stat(cmd_name: &str) {
    {
        let mut map = FAILED_CMD_STATS.write();
        bump_stat(&mut map, cmd_name, true);
    }
}

/// Distinct error codes INFO errorstats tracks, as in Redis. Scripts can
/// reply errors with any code (`redis.error_reply`), so without a cap the
/// table grows without bound.
const ERROR_STATS_NUMBER: usize = 128;
/// Cleared when the cap is hit; CONFIG RESETSTAT sets it again.
static ERRORSTATS_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

pub fn record_error_stat(prefix: &str, cmd_name: Option<&str>) {
    TOTAL_ERROR_REPLIES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if ERRORSTATS_ENABLED.load(std::sync::atomic::Ordering::Relaxed) {
        let mut map = ERROR_STATS.write();
        if !ERRORSTATS_ENABLED.load(std::sync::atomic::Ordering::Relaxed) {
            // Disabled by another thread while we waited for the lock.
        } else if map.len() >= ERROR_STATS_NUMBER && !map.contains_key(prefix) {
            let mut codes: Vec<&str> = map.keys().map(String::as_str).collect();
            codes.sort_unstable();
            crate::log_warning!(
                "Errorstats stopped adding new errors because the number of different errors reached {}; new errors will be reported as 'ERRORSTATS_DISABLED'. Use CONFIG RESETSTAT to reset errorstats.",
                ERROR_STATS_NUMBER
            );
            crate::log_warning!("Current errors code list: {}", codes.join(", "));
            map.clear();
            map.insert("ERRORSTATS_DISABLED".to_string(), 1);
            ERRORSTATS_ENABLED.store(false, std::sync::atomic::Ordering::Relaxed);
        } else {
            bump_stat(&mut map, prefix, false);
        }
    }
    if let Some(cmd) = cmd_name {
        bump_stat(&mut FAILED_CMD_STATS.write(), cmd, true);
    }
}

pub fn reset_error_stats() {
    TOTAL_ERROR_REPLIES.store(0, std::sync::atomic::Ordering::Relaxed);
    {
        let mut map = ERROR_STATS.write();
        map.clear();
        ERRORSTATS_ENABLED.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    {
        let mut map = FAILED_CMD_STATS.write();
        map.clear();
    }
    {
        let mut map = REJECTED_CMD_STATS.write();
        map.clear();
    }
}

pub static HAS_WATCHED_KEYS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn watch_keys(port: u16, client_id: u64, keys: &[Bytes]) {
    if keys.is_empty() {
        return;
    }
    // The node's keyspace is watched, whichever shard port a client (or a
    // FLUSHALL) came in on.
    let port = node_port(port);
    let db = CURRENT_DB.get();
    let mut map = WATCHED_KEYS.write();
    let port_map = map.entry(port).or_default();
    for k in keys {
        port_map
            .entry((db, k.clone()))
            .or_default()
            .insert(client_id);
    }
    CLIENT_WATCH_TAINTED
        .write()
        .entry((port, client_id))
        .or_insert(false);
    // Set under the WATCHED_KEYS lock, after the insert: `unwatch_keys`
    // clears the flag under the same lock when the map is empty, so setting
    // it before taking the lock let a concurrent UNWATCH clear it while this
    // key was watched, and later writes then skipped the WATCH taint check.
    HAS_WATCHED_KEYS.store(true, std::sync::atomic::Ordering::Relaxed);
}

pub fn unwatch_keys(port: u16, client_id: u64) {
    if !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let port = node_port(port);
    let mut map = WATCHED_KEYS.write();
    if let Some(port_map) = map.get_mut(&port) {
        // Drop keys nobody watches any more; they used to stay forever.
        port_map.retain(|_, set| {
            set.remove(&client_id);
            !set.is_empty()
        });
        if port_map.is_empty() {
            map.remove(&port);
        }
    }
    CLIENT_WATCH_TAINTED.write().remove(&(port, client_id));
    if map.values().all(|pm| pm.values().all(|s| s.is_empty())) {
        HAS_WATCHED_KEYS.store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

pub fn is_watch_tainted(port: u16, client_id: u64) -> bool {
    if !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return false;
    }
    let port = node_port(port);
    CLIENT_WATCH_TAINTED
        .read()
        .get(&(port, client_id))
        .copied()
        .unwrap_or(false)
}

#[inline(always)]
pub fn touch_watched_key(port: u16, key: &[u8]) {
    if !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    touch_watched_key_in_db(port, CURRENT_DB.get(), key);
}

pub fn touch_watched_key_in_db(port: u16, db: usize, key: &[u8]) {
    if !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let port = node_port(port);
    let map = WATCHED_KEYS.read();
    if let Some(port_map) = map.get(&port) {
        let mut tainted = None;
        for ((w_db, w_key), clients) in port_map.iter() {
            if *w_db == db && w_key.as_ref() == key {
                let t = tainted.get_or_insert_with(|| CLIENT_WATCH_TAINTED.write());
                for &cid in clients {
                    t.insert((port, cid), true);
                }
            }
        }
    }
}

/// The keys `client_id` watches, with their database.
pub fn watched_keys_of(port: u16, client_id: u64) -> Vec<(usize, Bytes)> {
    if !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return Vec::new();
    }
    let port = node_port(port);
    WATCHED_KEYS.read().get(&port).map_or_else(Vec::new, |pm| {
        pm.iter()
            .filter(|(_, clients)| clients.contains(&client_id))
            .map(|(k, _)| k.clone())
            .collect()
    })
}

/// Lazily expires `key` in database `db`, as any access does (EXISTS): a
/// key past its TTL is deleted now, which touches its watchers.
async fn expire_if_needed(router: &Router, db: usize, key: &Bytes) {
    let prev = CURRENT_DB.replace(db);
    if router.target_shard(key) == router.shard_id {
        let mut sdb = router.local_db.borrow_mut();
        let selected = sdb.current_db;
        sdb.select_db(db);
        sdb.exists(key);
        sdb.select_db(selected);
    } else {
        router.exists(key.clone()).await;
    }
    CURRENT_DB.set(prev);
}

/// SWAPDB `db1` `db2`, before it runs: as in Redis (and as FLUSHDB does),
/// a watched key of either database is touched only if one of the two
/// has it live, since swapping keys that don't exist or have expired
/// changes nothing the client watched. Checking expires stale keys.
async fn touch_live_watched_keys_on_swapdb(router: &Router, db1: usize, db2: usize) {
    if db1 == db2 || !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let keys: Vec<(usize, Bytes)> = WATCHED_KEYS
        .read()
        .get(&node_port(router.port))
        .map(|m| {
            m.keys()
                .filter(|(w_db, _)| *w_db == db1 || *w_db == db2)
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    for (w_db, key) in keys {
        let mut live = false;
        for d in [db1, db2] {
            let prev = CURRENT_DB.replace(d);
            if router.target_shard(&key) == router.shard_id {
                let mut sdb = router.local_db.borrow_mut();
                let selected = sdb.current_db;
                sdb.select_db(d);
                live |= sdb.exists(&key);
                sdb.select_db(selected);
            } else {
                live |= router.exists(key.clone()).await;
            }
            CURRENT_DB.set(prev);
        }
        if live {
            touch_watched_key_in_db(router.port, w_db, &key);
        }
    }
}

/// Whether `cmd`, a write that may grow memory, is refused for being over
/// `maxmemory` (evicting first under an eviction policy), as `execute_command`
/// checks it. For MULTI, which checks when queueing, as Redis does.
async fn oom_refuses(router: &Router, cmd: &Command, client_id: u64, out: &mut Vec<u8>) -> bool {
    let max_mem = router
        .tier_stats
        .max_memory
        .load(std::sync::atomic::Ordering::Relaxed);
    if max_mem == 0
        || maxmemory_ignored_as_replica(router.base_port)
        || !cmd.is_write_command()
        || cmd.allows_oom()
    {
        return false;
    }
    let policy = get_max_memory_policy();
    if policy != "noeviction" {
        !router
            .evict_until_under_maxmemory(max_mem as usize, &policy, client_id, out)
            .await
    } else {
        router.over_maxmemory_checked().await && {
            router.check_auto_tier().await;
            router.over_maxmemory()
        }
    }
}

/// WATCH: like Redis, a key already past its TTL counts as not existing:
/// it is expired first (so deleting or emptying it later changes nothing
/// the client watched), then watched.
async fn watch_command(router: &Router, client_id: u64, keys: &[Bytes]) {
    let db = CURRENT_DB.get();
    for k in keys {
        expire_if_needed(router, db, k).await;
    }
    watch_keys(router.port, client_id, keys);
}

/// Whether EXEC must fail for its WATCH: a watched key changed. A watched
/// key that expired since the WATCH (still there, past its TTL) is expired
/// first, which counts as a change, as in Redis.
async fn watch_failed(router: &Router, client_id: u64) -> bool {
    for (db, key) in watched_keys_of(router.port, client_id) {
        expire_if_needed(router, db, &key).await;
    }
    is_watch_tainted(router.port, client_id)
}

pub fn touch_watched_keys_on_swapdb(port: u16, db1: usize, db2: usize) {
    if db1 == db2 || !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let port = node_port(port);
    let map = WATCHED_KEYS.read();
    if let Some(port_map) = map.get(&port) {
        let mut tainted = CLIENT_WATCH_TAINTED.write();
        for ((w_db, _), clients) in port_map.iter() {
            if *w_db == db1 || *w_db == db2 {
                for &cid in clients {
                    tainted.insert((port, cid), true);
                }
            }
        }
    }
}

pub fn touch_watched_keys_on_flushdb(port: u16, db: usize) {
    if !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let port = node_port(port);
    let map = WATCHED_KEYS.read();
    if let Some(port_map) = map.get(&port) {
        let mut tainted = CLIENT_WATCH_TAINTED.write();
        for ((w_db, _), clients) in port_map.iter() {
            if *w_db == db {
                for &cid in clients {
                    tainted.insert((port, cid), true);
                }
            }
        }
    }
}

pub fn touch_watched_keys_on_flushall(port: u16) {
    if !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let port = node_port(port);
    let map = WATCHED_KEYS.read();
    if let Some(port_map) = map.get(&port) {
        let mut tainted = CLIENT_WATCH_TAINTED.write();
        for (_, clients) in port_map.iter() {
            for &cid in clients {
                tainted.insert((port, cid), true);
            }
        }
    }
}

#[inline(always)]
pub fn touch_watched_key_any_port(key: &[u8]) {
    if !HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let cur_db = CURRENT_DB.get();
    let map = WATCHED_KEYS.read();
    let mut tainted = CLIENT_WATCH_TAINTED.write();
    for (&port, port_map) in map.iter() {
        for ((w_db, w_key), clients) in port_map.iter() {
            if *w_db == cur_db && w_key.as_ref() == key {
                for &cid in clients {
                    tainted.insert((port, cid), true);
                }
            }
        }
    }
}

static GLOBAL_CLIENTS: std::sync::LazyLock<
    parking_lot::RwLock<hashbrown::HashMap<(u16, u64), GlobalClientEntry>>,
> = std::sync::LazyLock::new(|| parking_lot::RwLock::new(hashbrown::HashMap::new()));

/// The registry entry of client `cid`, whichever of the node's ports it
/// connected on (client ids are unique in the process). CLIENT KILL looked
/// it up under the killer's own port, so on a cluster-mode node a client on
/// another shard port was reported killed but stayed connected.
/// The registry entry of client `cid` of the server on `base_port`. Client
/// ids are per-server sequences, so several servers in one process (tests
/// run many) hand out the same ids: the port must match too. A cluster-mode
/// node's shard N registers its clients under `base_port + N`.
fn global_client_entry(
    map: &hashbrown::HashMap<(u16, u64), GlobalClientEntry>,
    base_port: u16,
    shard_ports: u16,
    cid: u64,
) -> Option<&GlobalClientEntry> {
    (0..shard_ports.max(1)).find_map(|s| map.get(&(base_port.wrapping_add(s), cid)))
}

/// The number of client ports of `router`'s server, from its base port.
fn client_port_span(router: &Router) -> u16 {
    if router.cluster_enabled {
        router.num_shards as u16
    } else {
        1
    }
}

/// Whether `port` is one of the client ports of `router`'s server: the base
/// port, or any shard port in cluster mode. ACL tables are shared across those
/// ports, so ACL-driven disconnects must cover all of them.
fn is_server_client_port(router: &Router, port: u16) -> bool {
    port_in_span(router.base_port, client_port_span(router), port)
}

fn port_in_span(base_port: u16, span: u16, port: u16) -> bool {
    port >= base_port && u32::from(port) < u32::from(base_port) + u32::from(span.max(1))
}

/// Counts a command a script ran towards its calling client's `tot_cmds`.
pub fn inc_active_client_tot_cmds(port: u16) {
    let cid = requesting_client_id();
    if cid > 0
        && let Some(entry) = GLOBAL_CLIENTS.read().get(&(port, cid))
    {
        entry
            .stats
            .tot_cmds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub static HAS_MONITOR_CLIENTS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

static MONITOR_CLIENTS: std::sync::LazyLock<
    parking_lot::RwLock<Vec<(u16, u64, crate::transport::PushTarget)>>,
> = std::sync::LazyLock::new(|| parking_lot::RwLock::new(Vec::new()));

#[inline(always)]
pub fn has_monitor_clients() -> bool {
    HAS_MONITOR_CLIENTS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Shard port -> the node's base port, for registries that are the node's
/// (MONITOR): in cluster mode every shard listens on its own port.
static NODE_PORTS: std::sync::LazyLock<parking_lot::RwLock<hashbrown::HashMap<u16, u16>>> =
    std::sync::LazyLock::new(|| parking_lot::RwLock::new(hashbrown::HashMap::new()));

/// Records that `shard_port` is a port of the node on `base_port`.
pub fn set_node_port(shard_port: u16, base_port: u16) {
    if shard_port != base_port {
        NODE_PORTS.write().insert(shard_port, base_port);
    }
}

/// The base port of the node `port` belongs to.
pub(crate) fn node_port(port: u16) -> u16 {
    NODE_PORTS.read().get(&port).copied().unwrap_or(port)
}

pub fn register_monitor_client(port: u16, client_id: u64, push: crate::transport::PushTarget) {
    // One node, one MONITOR feed: commands on every shard port reach every
    // monitor (keyed by the shard port, a monitor saw only its own port's).
    let port = node_port(port);
    let mut list = MONITOR_CLIENTS.write();
    if !list.iter().any(|(p, id, _)| *p == port && *id == client_id) {
        list.push((port, client_id, push));
    }
    HAS_MONITOR_CLIENTS.store(!list.is_empty(), std::sync::atomic::Ordering::Release);
}

pub fn unregister_monitor_client(port: u16, client_id: u64) {
    if !has_monitor_clients() {
        return;
    }
    let port = node_port(port);
    let mut list = MONITOR_CLIENTS.write();
    let prev_len = list.len();
    list.retain(|(p, id, _)| !(*p == port && *id == client_id));
    if list.len() != prev_len {
        HAS_MONITOR_CLIENTS.store(!list.is_empty(), std::sync::atomic::Ordering::Release);
    }
}

pub fn broadcast_monitor(port: u16, source_addr: &str, argv: &[Bytes]) {
    if !has_monitor_clients() || argv.is_empty() {
        return;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let mut line = format!(
        "+{}.{:06} [{} {}]",
        now.as_secs(),
        now.subsec_micros(),
        CURRENT_DB.get(),
        source_addr
    );
    for arg in argv {
        line.push_str(" \"");
        for &b in arg.iter() {
            match b {
                b'\\' => line.push_str("\\\\"),
                b'"' => line.push_str("\\\""),
                b'\n' => line.push_str("\\n"),
                b'\r' => line.push_str("\\r"),
                b'\t' => line.push_str("\\t"),
                0x20..=0x7e => line.push(b as char),
                _ => line.push_str(&format!("\\x{:02x}", b)),
            }
        }
        line.push('"');
    }
    line.push_str("\r\n");
    let bytes = line.as_bytes();
    let port = node_port(port);
    let list = MONITOR_CLIENTS.read();
    for (p, _, target) in list.iter() {
        if *p == port {
            target.push(bytes);
        }
    }
}

pub static RDB_KEY_SAVE_DELAY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static RDB_BGSAVE_IN_PROGRESS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
pub static RDB_BGSAVE_ABORTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn register_global_client(
    port: u16,
    client_id: u64,
    raw_fd: std::os::unix::io::RawFd,
    push: crate::transport::PushTarget,
    track_tx: flume::Sender<Vec<u8>>,
    is_resp3: bool,
    auth_user: String,
    is_pubsub: bool,
    stats: std::sync::Arc<ClientStats>,
) {
    GLOBAL_CLIENTS.write().insert(
        (port, client_id),
        GlobalClientEntry {
            raw_fd,
            push,
            track_tx,
            is_resp3,
            auth_user,
            is_pubsub,
            stats,
        },
    );
}

pub fn unregister_global_client(port: u16, client_id: u64) {
    GLOBAL_CLIENTS.write().remove(&(port, client_id));
}

pub fn update_global_client_resp3(port: u16, client_id: u64, is_resp3: bool) {
    if let Some(entry) = GLOBAL_CLIENTS.write().get_mut(&(port, client_id)) {
        entry.is_resp3 = is_resp3;
    }
}

pub fn update_global_client_auth(port: u16, client_id: u64, auth_user: &str) {
    if let Some(entry) = GLOBAL_CLIENTS.write().get_mut(&(port, client_id)) {
        entry.auth_user = auth_user.to_string();
    }
}

pub fn update_global_client_pubsub(port: u16, client_id: u64, is_pubsub: bool) {
    if let Some(entry) = GLOBAL_CLIENTS.write().get_mut(&(port, client_id)) {
        entry.is_pubsub = is_pubsub;
    }
}

static TRACKING_CLIENTS: std::sync::LazyLock<
    parking_lot::RwLock<hashbrown::HashMap<(u16, u64), ClientTracker>>,
> = std::sync::LazyLock::new(|| parking_lot::RwLock::new(hashbrown::HashMap::new()));

pub static HAS_TRACKING_CLIENTS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub static TRACKING_TABLE_MAX_KEYS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(1_000_000);

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
    let v_len = val.len();
    out.reserve(v_len + 16);
    out.push(b'$');
    match v_len {
        0 => out.extend_from_slice(b"0\r\n"),
        1..=9 => {
            out.push(b'0' + v_len as u8);
            out.extend_from_slice(b"\r\n");
        }
        10..=99 => {
            let p = v_len * 2;
            out.push(crate::shard::DIGIT_PAIRS[p]);
            out.push(crate::shard::DIGIT_PAIRS[p + 1]);
            out.extend_from_slice(b"\r\n");
        }
        100..=999 => {
            let h = (v_len / 100) as u8;
            let p = (v_len % 100) * 2;
            out.push(b'0' + h);
            out.push(crate::shard::DIGIT_PAIRS[p]);
            out.push(crate::shard::DIGIT_PAIRS[p + 1]);
            out.extend_from_slice(b"\r\n");
        }
        1000..=9999 => {
            let p1 = (v_len / 100) * 2;
            let p2 = (v_len % 100) * 2;
            out.push(crate::shard::DIGIT_PAIRS[p1]);
            out.push(crate::shard::DIGIT_PAIRS[p1 + 1]);
            out.push(crate::shard::DIGIT_PAIRS[p2]);
            out.push(crate::shard::DIGIT_PAIRS[p2 + 1]);
            out.extend_from_slice(b"\r\n");
        }
        _ => {
            let mut buf = [0u8; 20];
            let mut i = buf.len();
            let mut uval = v_len;
            while uval > 0 {
                i -= 1;
                buf[i] = b'0' + (uval % 10) as u8;
                uval /= 10;
            }
            out.extend_from_slice(&buf[i..]);
            out.extend_from_slice(b"\r\n");
        }
    }
    out.extend_from_slice(val);
    out.extend_from_slice(b"\r\n");
}

#[inline(always)]
pub fn write_resp_bulk_bytesmut(out: &mut BytesMut, val: &[u8]) {
    let v_len = val.len();
    out.reserve(v_len + 16);
    out.extend_from_slice(b"$");
    match v_len {
        0 => out.extend_from_slice(b"0\r\n"),
        1..=9 => {
            out.extend_from_slice(&[b'0' + v_len as u8, b'\r', b'\n']);
        }
        10..=99 => {
            let p = v_len * 2;
            out.extend_from_slice(&[
                crate::shard::DIGIT_PAIRS[p],
                crate::shard::DIGIT_PAIRS[p + 1],
                b'\r',
                b'\n',
            ]);
        }
        100..=999 => {
            let h = (v_len / 100) as u8;
            let p = (v_len % 100) * 2;
            out.extend_from_slice(&[
                b'0' + h,
                crate::shard::DIGIT_PAIRS[p],
                crate::shard::DIGIT_PAIRS[p + 1],
                b'\r',
                b'\n',
            ]);
        }
        1000..=9999 => {
            let p1 = (v_len / 100) * 2;
            let p2 = (v_len % 100) * 2;
            out.extend_from_slice(&[
                crate::shard::DIGIT_PAIRS[p1],
                crate::shard::DIGIT_PAIRS[p1 + 1],
                crate::shard::DIGIT_PAIRS[p2],
                crate::shard::DIGIT_PAIRS[p2 + 1],
                b'\r',
                b'\n',
            ]);
        }
        _ => {
            let mut buf = [0u8; 20];
            let mut i = buf.len();
            let mut uval = v_len;
            while uval > 0 {
                i -= 1;
                buf[i] = b'0' + (uval % 10) as u8;
                uval /= 10;
            }
            out.extend_from_slice(&buf[i..]);
            out.extend_from_slice(b"\r\n");
        }
    }
    out.extend_from_slice(val);
    out.extend_from_slice(b"\r\n");
}

#[inline(always)]
pub fn write_resp_array_header(out: &mut Vec<u8>, len: usize) {
    out.push(b'*');
    match len {
        0 => out.extend_from_slice(b"0\r\n"),
        1..=9 => {
            out.push(b'0' + len as u8);
            out.extend_from_slice(b"\r\n");
        }
        10..=99 => {
            let p = len * 2;
            out.push(crate::shard::DIGIT_PAIRS[p]);
            out.push(crate::shard::DIGIT_PAIRS[p + 1]);
            out.extend_from_slice(b"\r\n");
        }
        100..=999 => {
            let h = (len / 100) as u8;
            let p = (len % 100) * 2;
            out.push(b'0' + h);
            out.push(crate::shard::DIGIT_PAIRS[p]);
            out.push(crate::shard::DIGIT_PAIRS[p + 1]);
            out.extend_from_slice(b"\r\n");
        }
        _ => {
            let mut buf = [0u8; 20];
            let mut i = buf.len();
            let mut uval = len;
            while uval > 0 {
                i -= 1;
                buf[i] = b'0' + (uval % 10) as u8;
                uval /= 10;
            }
            out.extend_from_slice(&buf[i..]);
            out.extend_from_slice(b"\r\n");
        }
    }
}

#[inline(always)]
pub fn write_resp_array_header_bytesmut(out: &mut BytesMut, len: usize) {
    match len {
        0 => out.extend_from_slice(b"*0\r\n"),
        1..=9 => {
            out.extend_from_slice(&[b'*', b'0' + len as u8, b'\r', b'\n']);
        }
        10..=99 => {
            let p = len * 2;
            out.extend_from_slice(&[
                b'*',
                crate::shard::DIGIT_PAIRS[p],
                crate::shard::DIGIT_PAIRS[p + 1],
                b'\r',
                b'\n',
            ]);
        }
        100..=999 => {
            let h = (len / 100) as u8;
            let p = (len % 100) * 2;
            out.extend_from_slice(&[
                b'*',
                b'0' + h,
                crate::shard::DIGIT_PAIRS[p],
                crate::shard::DIGIT_PAIRS[p + 1],
                b'\r',
                b'\n',
            ]);
        }
        _ => {
            out.extend_from_slice(b"*");
            let mut buf = [0u8; 20];
            let mut i = buf.len();
            let mut uval = len;
            while uval > 0 {
                i -= 1;
                buf[i] = b'0' + (uval % 10) as u8;
                uval /= 10;
            }
            out.extend_from_slice(&buf[i..]);
            out.extend_from_slice(b"\r\n");
        }
    }
}

/// A client-supplied string made safe to echo inside an error reply: CR and
/// LF become spaces, as Redis does (`addReplyErrorFormat`). Left in, they end
/// the error line early and the rest is read as further replies, so an
/// argument like `"x\r\n+OK"` could forge a reply to the client's next
/// command.
pub fn err_arg(s: &str) -> std::borrow::Cow<'_, str> {
    if s.contains(['\r', '\n']) {
        std::borrow::Cow::Owned(s.replace(['\r', '\n'], " "))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

/// LCS builds an (m+1) x (n+1) table of u32. Like Redis, refuse inputs whose
/// table would exceed `proto-max-bulk-len` instead of trying to allocate it:
/// two 400 KB strings already need ~640 GB.
fn lcs_table_too_big(m: usize, n: usize) -> Option<&'static str> {
    if m >= u32::MAX as usize - 1 || n >= u32::MAX as usize - 1 {
        return Some("String too long for LCS");
    }
    let cells = (m as u128 + 1) * (n as u128 + 1);
    let bytes = cells * std::mem::size_of::<u32>() as u128;
    if bytes > crate::resp::get_proto_max_bulk_len() as u128 {
        return Some("Insufficient memory, transient memory for LCS exceeds proto-max-bulk-len");
    }
    None
}

/// Error stats for a command the parser refused (wrong arity, unknown
/// command, bad syntax), as Redis's rejectCommand keeps them: the error in
/// `errorstat_*` and `total_error_replies`, and a rejected call of the
/// command when it is a known one with the wrong number of arguments.
/// These were not counted at all.
fn record_parse_error_stats(err: &str) {
    let line = err.lines().next().unwrap_or(err);
    let mut out = Vec::new();
    write_resp_err(&mut out, line);
    let prefix = std::str::from_utf8(&out[1..])
        .ok()
        .and_then(|l| l.split([' ', '\r']).next())
        .unwrap_or("ERR");
    record_error_stat(prefix, None);
    let msg = line.strip_prefix("ERR ").unwrap_or(line);
    if let Some(rest) = msg.strip_prefix("wrong number of arguments for '")
        && let Some(name) = rest.split('\'').next()
    {
        record_rejected_stat(name);
    }
}

#[inline]
pub fn write_resp_err(out: &mut Vec<u8>, err: impl AsRef<str>) {
    let err = err.as_ref();
    let err_single_line = err.lines().next().unwrap_or(err);
    if err_single_line.starts_with("WRONGTYPE")
        || err_single_line.starts_with("CROSSSLOT")
        || err_single_line.starts_with("MOVED")
        || err_single_line.starts_with("ASK")
        || err_single_line.starts_with("NOSCRIPT")
        || err_single_line.starts_with("EXECABORT")
        || err_single_line.starts_with("BUSYGROUP")
        || err_single_line.starts_with("NOGROUP")
        || err_single_line.starts_with("INVALIDOBJ")
        || err_single_line.starts_with("NOREPLICAS")
        || err_single_line.starts_with("OOM")
        || err_single_line.starts_with("READONLY")
        || err_single_line.starts_with("NOPERM")
        || err_single_line.starts_with("ERR")
    {
        out.extend_from_slice(b"-");
        out.extend_from_slice(err_single_line.as_bytes());
        out.extend_from_slice(b"\r\n");
    } else {
        out.extend_from_slice(b"-ERR ");
        out.extend_from_slice(err_single_line.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
}

/// `list-max-listpack-size`: positive, the most entries a list reports
/// `listpack` encoding with; -1 to -5, the most bytes (4 KB to 64 KB), as
/// Redis's list encoding switch.
pub static LIST_MAX_LISTPACK_SIZE: std::sync::atomic::AtomicI64 =
    std::sync::atomic::AtomicI64::new(-2);

pub static HASH_MAX_ENTRIES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(512);
pub static HASH_MAX_VALUE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(64);
pub static ALLOW_ACCESS_EXPIRED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Whether expired keys must be treated as live: set process-wide by `DEBUG
/// SET-ALLOW-ACCESS-EXPIRED`, or for this thread while it re-applies writes
/// ([`REPLAYING_WRITES`]).
#[inline]
pub fn access_expired_allowed() -> bool {
    ALLOW_ACCESS_EXPIRED.load(std::sync::atomic::Ordering::Relaxed) || REPLAYING_WRITES.get()
}

/// Sets [`REPLAYING_WRITES`] for this thread until dropped.
pub struct ReplayingWrites(bool);

impl ReplayingWrites {
    #[inline]
    pub fn new() -> Self {
        ReplayingWrites(REPLAYING_WRITES.replace(true))
    }
}

impl Default for ReplayingWrites {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for ReplayingWrites {
    #[inline]
    fn drop(&mut self) {
        REPLAYING_WRITES.set(self.0);
    }
}
pub static PAUSE_CRON: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// `DEBUG CROSS-SHARD-MOVE-DELAY <microseconds>` (a test knob): how long a
/// cross-shard LMOVE / SMOVE waits between its pop and its push, so a test
/// can catch one half done.
pub static CROSS_SHARD_MOVE_DELAY_US: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// `client-default-resp` (Valkey 8): the protocol a new connection speaks
/// before any HELLO, and returns to on RESET. `true` = RESP3.
pub static CLIENT_DEFAULT_RESP3: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// `client-default-resp`'s value: `true` for 3, `false` for 2.
fn parse_client_default_resp(v: &str) -> Result<bool, String> {
    match v {
        "2" => Ok(false),
        "3" => Ok(true),
        _ => Err("client-default-resp must be 2 or 3".to_string()),
    }
}

#[inline]
fn client_default_resp3() -> bool {
    CLIENT_DEFAULT_RESP3.load(std::sync::atomic::Ordering::Relaxed)
}
/// `DEBUG CROSS-SHARD-PUSH-DELAY <microseconds>` (a test knob): how long
/// the destination shard of a cross-shard LMOVE / SMOVE stalls before it
/// claims the push, so a test can make the source give up waiting.
pub static CROSS_SHARD_PUSH_DELAY_US: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
/// `DEBUG CROSS-SHARD-MOVE-TIMEOUT <milliseconds>`: how long the source of
/// a cross-shard LMOVE / SMOVE waits for the destination to take its push
/// before withdrawing it and undoing the pop (default 10 s).
pub static CROSS_SHARD_MOVE_TIMEOUT_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(10_000);
pub static ACTIVE_CLIENTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
pub static MAX_CLIENTS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(10000);
pub static MAX_MEMORY_POLICY: parking_lot::RwLock<String> = parking_lot::RwLock::new(String::new());

static CONFIG_BACKUP_SEALED_TTL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(86400);
static CONFIG_MAXMEMORY_SAMPLES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(5);

/// A `maxmemory-samples` value: 1..=64, as in Redis.
fn maxmemory_samples_arg(val: &str) -> Result<u64, String> {
    match val.trim().parse::<u64>() {
        Ok(n) if (1..=64).contains(&n) => Ok(n),
        _ => Err("argument must be between 1 and 64 inclusive".to_string()),
    }
}

/// `maxmemory-samples`: how many keys eviction compares to pick its victim.
pub fn maxmemory_samples() -> usize {
    CONFIG_MAXMEMORY_SAMPLES
        .load(std::sync::atomic::Ordering::Relaxed)
        .clamp(1, 64) as usize
}
static CONFIG_CLIENT_QUERY_BUFFER_LIMIT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1073741824);
static CONFIG_KEY_LOAD_DELAY: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

/// Smallest `client-query-buffer-limit` accepted, as in Redis (1mb).
const MIN_CLIENT_QUERY_BUFFER_LIMIT: u64 = 1024 * 1024;

/// `client-query-buffer-limit` in bytes: the most input a client may have
/// buffered for one command (a partially received command, or a complete
/// one) before it is disconnected. Read once per batch of input.
#[inline]
fn client_query_buffer_limit() -> usize {
    usize::try_from(CONFIG_CLIENT_QUERY_BUFFER_LIMIT.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(usize::MAX)
}

/// Whether the incomplete command left in a client's query buffer after
/// parsing (`pending` bytes, all of it one partial frame) breaks `limit`.
#[inline(always)]
fn partial_query_over_limit(pending: usize, limit: usize) -> bool {
    pending > limit
}

/// Whether the complete command just parsed off the front of a query buffer
/// (which held `len_before` bytes and now holds `len_after`) breaks `limit`.
/// The first compare is almost always false, so a normal pipeline pays one
/// predictable branch per command.
#[inline(always)]
fn parsed_query_over_limit(len_before: usize, len_after: usize, limit: usize) -> bool {
    len_before > limit && len_before - len_after > limit
}

/// Counts and logs a client dropped for breaking client-query-buffer-limit
/// (Redis's wording; rate-limited, as many clients may break it at once).
fn client_query_limit_reached(client_id: u64, qbuf: usize) {
    static LOG_LIMIT: crate::log::RateLimit = crate::log::RateLimit::new();
    crate::server_stats::add(
        crate::server_stats::Stat::ClientQueryBufferLimitDisconnections,
        1,
    );
    if LOG_LIMIT.allow(std::time::Duration::from_secs(1)) {
        crate::log_warning!(
            "Closing client that reached max query buffer length: id={} qbuf={}",
            client_id,
            qbuf
        );
    }
}

/// `key-load-delay`: microseconds to pause after each key an RDB load
/// adds, like Redis (which uses it to make loads observable in tests).
/// Values <= 0 mean no pause.
pub fn key_load_delay_us() -> u64 {
    CONFIG_KEY_LOAD_DELAY
        .load(std::sync::atomic::Ordering::Relaxed)
        .max(0) as u64
}
static CONFIG_LUA_TIME_LIMIT: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(5000);

/// `busy-reply-threshold` (alias `lua-time-limit`) in milliseconds: how long
/// a script runs before other clients get `-BUSY` (0: never).
pub fn busy_reply_threshold_ms() -> u64 {
    CONFIG_LUA_TIME_LIMIT.load(std::sync::atomic::Ordering::Relaxed)
}
static CONFIG_OOM_SCORE_ADJ: std::sync::LazyLock<parking_lot::RwLock<String>> =
    std::sync::LazyLock::new(|| parking_lot::RwLock::new("no".to_string()));
static CONFIG_OOM_SCORE_ADJ_VALUES: std::sync::LazyLock<parking_lot::RwLock<String>> =
    std::sync::LazyLock::new(|| parking_lot::RwLock::new("0 200 800".to_string()));
static CONFIG_MAXMEMORY_CLIENTS: std::sync::LazyLock<parking_lot::RwLock<String>> =
    std::sync::LazyLock::new(|| parking_lot::RwLock::new("0".to_string()));
static CONFIG_BACKUPDIRNAME: std::sync::LazyLock<parking_lot::RwLock<String>> =
    std::sync::LazyLock::new(|| parking_lot::RwLock::new("backup".to_string()));

/// Whether a `maxmemory-clients` value asks for client eviction (anything
/// but 0 / 0%), which rudis does not implement.
/// `maxmemory-clients` in bytes (0: off, or given as a percentage).
static MAXMEMORY_CLIENTS_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
/// `maxmemory-clients` as a percentage of `maxmemory` (0: given in bytes).
static MAXMEMORY_CLIENTS_PCT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Parses `maxmemory-clients`: bytes (`1gb`) or a percentage of
/// maxmemory (`10%`, at most 100); 0 turns client eviction off.
fn parse_maxmemory_clients(val: &str) -> Result<(u64, u64), String> {
    let val = val.trim();
    match val.strip_suffix('%') {
        Some(pct) => match pct.trim().parse::<u64>() {
            Ok(p) if p <= 100 => Ok((0, p)),
            _ => Err("percentage argument must be less or equal to 100".to_string()),
        },
        None => crate::tiering::parse_memory_bytes(val)
            .map(|b| (b, 0))
            .ok_or_else(|| "invalid maxmemory-clients".to_string()),
    }
}

/// The client memory limit `maxmemory-clients` sets on the server at
/// `port`, in bytes; 0 when off (also a percentage while maxmemory is 0).
pub fn maxmemory_clients_limit(port: u16) -> u64 {
    match MAXMEMORY_CLIENTS_PCT.load(std::sync::atomic::Ordering::Relaxed) {
        0 => MAXMEMORY_CLIENTS_BYTES.load(std::sync::atomic::Ordering::Relaxed),
        pct => crate::tiering::get_max_memory(port).saturating_mul(pct) / 100,
    }
}

/// Measures every client of this shard (`registry`) for the evictor
/// (`ClientStats::tot_mem`): its shard holds what tot-mem counts.
pub fn measure_client_memory(
    registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    port: u16,
    pubsub: &crate::pubsub::PubSubHub,
) {
    let now = Instant::now();
    for c in registry.borrow().values() {
        let idle = now.duration_since(c.last_active).as_secs();
        c.stats.tot_mem.store(
            client_tot_mem(c, idle, port, pubsub),
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

/// INFO memory's `mem_clients_slaves` and `mem_clients_normal`: the
/// clients' memory as last measured (tot-mem, once a second), replicas
/// apart from the others.
pub fn mem_clients_info() -> String {
    use std::sync::atomic::Ordering::Relaxed;
    let (mut replicas, mut normal) = (0u64, 0u64);
    for e in GLOBAL_CLIENTS.read().values() {
        if e.stats.killed.load(Relaxed) {
            continue;
        }
        let mem = e.stats.tot_mem.load(Relaxed) as u64;
        if e.stats.is_replica.load(Relaxed) {
            replicas += mem;
        } else {
            normal += mem;
        }
    }
    format!("mem_clients_slaves:{replicas}\r\nmem_clients_normal:{normal}\r\n")
}

/// `maxmemory-clients`: while the clients subject to eviction (not
/// replicas, not CLIENT NO-EVICT) hold more than the limit, closes the
/// biggest, as Redis evicts clients largest first. Runs on one shard for
/// the whole node (`port`: its base port), from the shards' measurements.
pub fn evict_clients(port: u16, limit: u64) {
    use std::sync::atomic::Ordering::Relaxed;
    let mut candidates: Vec<(usize, u64)> = Vec::new();
    let mut total: u64 = 0;
    {
        let map = GLOBAL_CLIENTS.read();
        for (&(p, id), e) in map.iter() {
            if node_port(p) != port
                || e.stats.killed.load(Relaxed)
                || e.stats.is_replica.load(Relaxed)
                || e.stats.no_evict.load(Relaxed)
            {
                continue;
            }
            let mem = e.stats.tot_mem.load(Relaxed);
            total += mem as u64;
            candidates.push((mem, id));
        }
    }
    if total <= limit {
        return;
    }
    candidates.sort_unstable_by(|a, b| b.cmp(a));
    for (mem, id) in candidates {
        if total <= limit {
            break;
        }
        if evict_client(port, id, mem) {
            total = total.saturating_sub(mem as u64);
        }
    }
}

/// `maxmemory-clients`, checked as client `id` (holding `my_mem` now)
/// grows, as Redis evicts on reading input and queueing MULTI commands:
/// if the evictable clients' memory is over the limit and this client is
/// the biggest, it is evicted at once (true: close it without running
/// what it sent). The 100 ms check handles the rest.
pub(crate) fn client_evicted_for_memory(port: u16, id: u64, my_mem: usize) -> bool {
    use std::sync::atomic::Ordering::Relaxed;
    let limit = maxmemory_clients_limit(port);
    if limit == 0 {
        return false;
    }
    let mut total = my_mem as u64;
    let mut biggest_other = 0;
    {
        let map = GLOBAL_CLIENTS.read();
        for (&(p, cid), e) in map.iter() {
            if node_port(p) != port {
                continue;
            }
            let exempt = e.stats.killed.load(Relaxed)
                || e.stats.is_replica.load(Relaxed)
                || e.stats.no_evict.load(Relaxed);
            if cid == id {
                if exempt {
                    return false;
                }
                e.stats.tot_mem.fetch_max(my_mem, Relaxed);
                continue;
            }
            if !exempt {
                let mem = e.stats.tot_mem.load(Relaxed);
                total += mem as u64;
                biggest_other = biggest_other.max(mem);
            }
        }
    }
    total > limit && my_mem >= biggest_other && evict_client(port, id, my_mem)
}

/// Closes client `id` for `maxmemory-clients` (as CLIENT KILL does), logs
/// and counts it. Returns whether it was still open.
fn evict_client(port: u16, id: u64, mem: usize) -> bool {
    let map = GLOBAL_CLIENTS.read();
    let Some((_, entry)) = map
        .iter()
        .find(|((p, cid), _)| *cid == id && node_port(*p) == port)
    else {
        return false;
    };
    if entry
        .stats
        .killed
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        )
        .is_err()
    {
        return false;
    }
    ACTIVE_CLIENTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    crate::server_stats::add(crate::server_stats::Stat::EvictedClients, 1);
    crate::log_verbose!(
        "Evicting client: id={} addr={} tot-mem={} (maxmemory-clients)",
        id,
        entry.stats.addr,
        mem
    );
    // SAFETY: shutdown(2) takes no pointers; the entry is read under the
    // GLOBAL_CLIENTS read lock and a connection removes its entry before
    // closing its socket (see close_client_over_output_limit).
    unsafe {
        libc::shutdown(entry.raw_fd, libc::SHUT_RDWR);
    }
    true
}

/// The process's `oom_score_adj` before rudis changed it: what `relative`
/// values add to, and what `oom-score-adj no` restores.
static OOM_SCORE_ADJ_BASE: std::sync::OnceLock<i32> = std::sync::OnceLock::new();
const OOM_SCORE_ADJ_PATH: &str = "/proc/self/oom_score_adj";

/// Parses `oom-score-adj-values`: three integers (master, replica,
/// background child) in -2000..=2000, as in Redis.
fn parse_oom_score_adj_values(val: &str) -> Result<[i32; 3], String> {
    let nums: Vec<i32> = val
        .split_whitespace()
        .map(|v| v.parse::<i32>().ok().filter(|n| (-2000..=2000).contains(n)))
        .collect::<Option<_>>()
        .ok_or(
            "Invalid oom-score-adj-values: each value must be an integer between -2000 and 2000",
        )?;
    <[i32; 3]>::try_from(nums).map_err(|_| "oom-score-adj-values must have 3 values".to_string())
}

/// The `oom_score_adj` to set for `mode`/`values` (or None for `no`, which
/// keeps `base`): `absolute` uses the value as is, `yes`/`relative` adds it
/// to `base`; clamped to the kernel's -1000..=1000.
fn oom_score_adj_target(
    mode: &str,
    values: &[i32; 3],
    base: i32,
    is_replica: bool,
) -> Result<Option<i32>, String> {
    let v = values[usize::from(is_replica)];
    let target = match mode.to_ascii_lowercase().as_str() {
        "no" => return Ok(None),
        "yes" | "relative" => base + v,
        "absolute" => v,
        _ => {
            return Err(
                "argument(s) must be one of the following: no, yes, relative, absolute".into(),
            );
        }
    };
    Ok(Some(target.clamp(-1000, 1000)))
}

/// Applies `oom-score-adj`/`oom-score-adj-values` for the current role
/// (master or replica) to the process, like Redis's `setOOMScoreAdj`.
pub fn apply_oom_score_adj(is_replica: bool) -> Result<(), String> {
    let read = || -> Option<i32> {
        std::fs::read_to_string(OOM_SCORE_ADJ_PATH)
            .ok()?
            .trim()
            .parse()
            .ok()
    };
    let mode = CONFIG_OOM_SCORE_ADJ.read().clone();
    let values = parse_oom_score_adj_values(&CONFIG_OOM_SCORE_ADJ_VALUES.read())?;
    if mode.eq_ignore_ascii_case("no") && OOM_SCORE_ADJ_BASE.get().is_none() {
        return Ok(()); // never changed it
    }
    let Some(&base) = OOM_SCORE_ADJ_BASE.get().or_else(|| {
        let base = read()?;
        Some(OOM_SCORE_ADJ_BASE.get_or_init(|| base))
    }) else {
        return Err("Failed to read the current oom_score_adj".into());
    };
    let target = oom_score_adj_target(&mode, &values, base, is_replica)?.unwrap_or(base);
    if read() == Some(target) {
        return Ok(());
    }
    std::fs::write(OOM_SCORE_ADJ_PATH, target.to_string()).map_err(|e| {
        crate::log_warning!("Unable to write oom_score_adj {}: {}", target, e);
        "Failed to set current oom_score_adj. Check server logs.".to_string()
    })
}
static CONFIG_SLAVEOF: std::sync::LazyLock<parking_lot::RwLock<String>> =
    std::sync::LazyLock::new(|| parking_lot::RwLock::new(String::new()));

pub fn set_max_clients(limit: usize) {
    MAX_CLIENTS.store(limit, std::sync::atomic::Ordering::Relaxed);
}

/// When this process started, for INFO uptime.
static SERVER_START: std::sync::LazyLock<(std::time::Instant, String)> =
    std::sync::LazyLock::new(|| {
        // INFO run_id: 40 random hex characters, fixed for the process.
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let a = fxhash::hash64(&(seed, std::process::id()));
        let b = fxhash::hash64(&(a, seed.rotate_left(17)));
        let c = fxhash::hash64(&(b, a));
        let run_id = format!("{a:016x}{b:016x}{c:016x}")[..40].to_string();
        (std::time::Instant::now(), run_id)
    });

/// Called at startup so uptime counts from then, not from the first INFO.
pub fn mark_server_start() {
    std::sync::LazyLock::force(&SERVER_START);
}

/// The process fields of Redis's INFO server section (process_id, run_id,
/// uptime, executable, config_file, ...), which monitoring relies on.
fn server_info_process_fields() -> String {
    let (started, run_id) = &*SERVER_START;
    let uptime = started.elapsed().as_secs();
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    let executable = std::env::current_exe()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    let config_file = crate::config::ACTIVE_CONFIG_FILE
        .read()
        .as_ref()
        .map(|p| p.display().to_string())
        .unwrap_or_default();
    format!(
        "process_id:{}\r\nprocess_supervised:no\r\nrun_id:{}\r\nserver_time_usec:{}\r\nuptime_in_seconds:{}\r\nuptime_in_days:{}\r\nhz:10\r\nconfigured_hz:10\r\nlru_clock:{}\r\nexecutable:{}\r\nconfig_file:{}\r\nio_threads_active:0\r\nshutdown_in_milliseconds:{}\r\n",
        std::process::id(),
        run_id,
        now_us,
        uptime,
        uptime / 86400,
        (now_us / 1_000_000) as u64 & ((1 << 24) - 1),
        executable,
        config_file,
        crate::replication::shutdown_in_milliseconds(),
    )
}

/// INFO cpu from getrusage: the whole process (all shard threads).
fn info_cpu_section() -> String {
    let secs = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 / 1e6;
    let mut ru = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: getrusage writes one `rusage` into the pointer it is given;
    // `ru` is a valid, writable, properly aligned `rusage`. A zeroed
    // `rusage` is a valid value, so reading it after a failed call is fine.
    let ru = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, ru.as_mut_ptr());
        ru.assume_init()
    };
    let (sys, user) = (secs(ru.ru_stime), secs(ru.ru_utime));
    format!(
        "# CPU\r\nused_cpu_sys:{sys:.6}\r\nused_cpu_user:{user:.6}\r\nused_cpu_sys_children:0.000000\r\nused_cpu_user_children:0.000000\r\nused_cpu_sys_main_thread:{sys:.6}\r\nused_cpu_user_main_thread:{user:.6}\r\n"
    )
}

pub fn get_max_clients() -> usize {
    MAX_CLIENTS.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn get_active_clients() -> usize {
    ACTIVE_CLIENTS.load(std::sync::atomic::Ordering::Relaxed)
}

/// INFO `connected_clients`: connections other than replicas, as Redis
/// counts them (replicas are `connected_slaves`), so a master with
/// replicas does not look like it has clients that aren't there.
pub fn connected_clients(base_port: u16) -> usize {
    let replicas = crate::replication::cached_hub(base_port)
        .replicas
        .read()
        .len();
    connected_clients_of(get_active_clients(), replicas)
}

fn connected_clients_of(connections: usize, replicas: usize) -> usize {
    connections.saturating_sub(replicas)
}

pub fn set_max_memory_policy(policy: &str) {
    {
        let mut lock = MAX_MEMORY_POLICY.write();
        *lock = policy.to_lowercase();
        crate::table::LFU_ACTIVE.store(lock.contains("lfu"), std::sync::atomic::Ordering::Relaxed);
    }
    crate::tiering::bump_maxmemory_epoch();
}

/// The I/O driver the shards actually run on. monoio falls back to epoll
/// when io_uring is unavailable or `MONOIO_FORCE_LEGACY_DRIVER` is set, so
/// this must not be hard-coded.
pub fn io_driver_name() -> &'static str {
    if monoio::utils::detect_uring() {
        "io_uring"
    } else {
        "epoll"
    }
}

pub fn get_max_memory_policy() -> String {
    let lock = MAX_MEMORY_POLICY.read();
    if lock.is_empty() {
        "noeviction".to_string()
    } else {
        lock.clone()
    }
}

/// Serialises unit tests that mutate the process-global `maxmemory-policy`.
#[cfg(test)]
pub(crate) static MAX_MEMORY_POLICY_TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// Whether `maxmemory-policy` is `noeviction` (the default), without
/// allocating.
pub fn max_memory_policy_is_noeviction() -> bool {
    let lock = MAX_MEMORY_POLICY.read();
    lock.is_empty() || lock.eq_ignore_ascii_case("noeviction")
}

pub const NOTIFY_KEYSPACE: u32 = 1 << 0; // K
pub const NOTIFY_KEYEVENT: u32 = 1 << 1; // E
pub const NOTIFY_GENERIC: u32 = 1 << 2; // g
pub const NOTIFY_STRING: u32 = 1 << 3; // $
pub const NOTIFY_LIST: u32 = 1 << 4; // l
pub const NOTIFY_SET: u32 = 1 << 5; // s
pub const NOTIFY_HASH: u32 = 1 << 6; // h
pub const NOTIFY_ZSET: u32 = 1 << 7; // z
pub const NOTIFY_EXPIRED: u32 = 1 << 8; // x
pub const NOTIFY_EVICTED: u32 = 1 << 9; // e
pub const NOTIFY_STREAM: u32 = 1 << 10; // t
pub const NOTIFY_KEY_MISS: u32 = 1 << 11; // m
pub const NOTIFY_MODULE: u32 = 1 << 12; // d
pub const NOTIFY_NEW: u32 = 1 << 13; // n
pub const NOTIFY_OVERWRITTEN: u32 = 1 << 14; // o
pub const NOTIFY_TYPE_CHANGED: u32 = 1 << 15; // c
/// Subkey (hash field) notification channels. Like `K`/`E` they select
/// where events go; the type letters (`h`) still select which events.
pub const NOTIFY_SUBKEYSPACE: u32 = 1 << 16; // S: __subkeyspace@<db>__:<key>
pub const NOTIFY_SUBKEYEVENT: u32 = 1 << 17; // T: __subkeyevent@<db>__:<event>
pub const NOTIFY_SUBKEYSPACEITEM: u32 = 1 << 18; // I: __subkeyspaceitem@<db>__:<key>\n<field>
pub const NOTIFY_SUBKEYSPACEEVENT: u32 = 1 << 19; // V: __subkeyspaceevent@<db>__:<event>|<key>
pub const NOTIFY_SUBKEY_CHANNELS: u32 =
    NOTIFY_SUBKEYSPACE | NOTIFY_SUBKEYEVENT | NOTIFY_SUBKEYSPACEITEM | NOTIFY_SUBKEYSPACEEVENT;

pub const NOTIFY_ALL: u32 = NOTIFY_GENERIC
    | NOTIFY_STRING
    | NOTIFY_LIST
    | NOTIFY_SET
    | NOTIFY_HASH
    | NOTIFY_ZSET
    | NOTIFY_EXPIRED
    | NOTIFY_EVICTED
    | NOTIFY_STREAM
    | NOTIFY_MODULE; // A

pub static NOTIFY_KEYSPACE_FLAGS: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(0);
pub static NOTIFY_KEYSPACE_STR: parking_lot::RwLock<String> =
    parking_lot::RwLock::new(String::new());
/// Held by unit tests that change the process-wide notification flags.
#[cfg(test)]
pub(crate) static NOTIFY_FLAGS_TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

pub fn notify_keyspace_flags_to_string(flags: u32) -> String {
    let mut res = String::new();
    if (flags & NOTIFY_ALL) == NOTIFY_ALL {
        res.push('A');
    } else {
        if flags & NOTIFY_GENERIC != 0 {
            res.push('g');
        }
        if flags & NOTIFY_STRING != 0 {
            res.push('$');
        }
        if flags & NOTIFY_LIST != 0 {
            res.push('l');
        }
        if flags & NOTIFY_SET != 0 {
            res.push('s');
        }
        if flags & NOTIFY_HASH != 0 {
            res.push('h');
        }
        if flags & NOTIFY_ZSET != 0 {
            res.push('z');
        }
        if flags & NOTIFY_EXPIRED != 0 {
            res.push('x');
        }
        if flags & NOTIFY_EVICTED != 0 {
            res.push('e');
        }
        if flags & NOTIFY_STREAM != 0 {
            res.push('t');
        }
        if flags & NOTIFY_MODULE != 0 {
            res.push('d');
        }
        if flags & NOTIFY_NEW != 0 {
            res.push('n');
        }
    }
    if flags & NOTIFY_OVERWRITTEN != 0 {
        res.push('o');
    }
    if flags & NOTIFY_TYPE_CHANGED != 0 {
        res.push('c');
    }
    if flags & NOTIFY_KEYSPACE != 0 {
        res.push('K');
    }
    if flags & NOTIFY_KEYEVENT != 0 {
        res.push('E');
    }
    if flags & NOTIFY_SUBKEYSPACE != 0 {
        res.push('S');
    }
    if flags & NOTIFY_SUBKEYEVENT != 0 {
        res.push('T');
    }
    if flags & NOTIFY_SUBKEYSPACEITEM != 0 {
        res.push('I');
    }
    if flags & NOTIFY_SUBKEYSPACEEVENT != 0 {
        res.push('V');
    }
    if flags & NOTIFY_KEY_MISS != 0 {
        res.push('m');
    }
    res
}

pub fn get_notify_keyspace_events_str() -> String {
    NOTIFY_KEYSPACE_STR.read().clone()
}

pub fn set_notify_keyspace_events_str(s: &str) {
    let mut flags = 0u32;
    for c in s.chars() {
        match c {
            'K' => flags |= NOTIFY_KEYSPACE,
            'E' => flags |= NOTIFY_KEYEVENT,
            'g' => flags |= NOTIFY_GENERIC,
            '$' => flags |= NOTIFY_STRING,
            'l' => flags |= NOTIFY_LIST,
            's' => flags |= NOTIFY_SET,
            'h' => flags |= NOTIFY_HASH,
            'z' => flags |= NOTIFY_ZSET,
            'x' => flags |= NOTIFY_EXPIRED,
            'e' => flags |= NOTIFY_EVICTED,
            't' => flags |= NOTIFY_STREAM,
            'm' => flags |= NOTIFY_KEY_MISS,
            'd' => flags |= NOTIFY_MODULE,
            'n' => flags |= NOTIFY_NEW,
            'o' => flags |= NOTIFY_OVERWRITTEN,
            'c' => flags |= NOTIFY_TYPE_CHANGED,
            'S' => flags |= NOTIFY_SUBKEYSPACE,
            'T' => flags |= NOTIFY_SUBKEYEVENT,
            'I' => flags |= NOTIFY_SUBKEYSPACEITEM,
            'V' => flags |= NOTIFY_SUBKEYSPACEEVENT,
            'A' => flags |= NOTIFY_ALL,
            _ => {}
        }
    }
    NOTIFY_KEYSPACE_FLAGS.store(flags, std::sync::atomic::Ordering::Relaxed);
    {
        let mut lock = NOTIFY_KEYSPACE_STR.write();
        *lock = notify_keyspace_flags_to_string(flags);
    }
}

pub fn notify_keyspace_event_sync(router: &Router, event_type: u32, event: &str, key: &[u8]) {
    notify_keyspace_event_sync_in_db(router, CURRENT_DB.get(), event_type, event, key);
}

pub fn notify_keyspace_event_sync_in_db(
    router: &Router,
    db_id: usize,
    event_type: u32,
    event: &str,
    key: &[u8],
) {
    let flags = NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed);
    if flags == 0 || (flags & event_type == 0) {
        return;
    }
    if flags & NOTIFY_KEYSPACE != 0 {
        let channel = format!("__keyspace@{}__:{}", db_id, String::from_utf8_lossy(key));
        let _ = router
            .pubsub
            .borrow()
            .publish(channel.as_bytes(), event.as_bytes());
        for (sid, sender) in router.senders.iter().enumerate() {
            if sid != router.shard_id
                && router
                    .presence_table
                    .is_shard_interested(sid, channel.as_bytes())
            {
                let (tx, _rx) = flume::bounded(1);
                let _ = sender.send(ShardMessage::Publish {
                    channel: Bytes::from(channel.clone().into_bytes()),
                    message: Bytes::from(event.as_bytes().to_vec()),
                    responder: tx,
                });
            }
        }
    }
    if flags & NOTIFY_KEYEVENT != 0 {
        let channel = format!("__keyevent@{}__:{}", db_id, event);
        let _ = router.pubsub.borrow().publish(channel.as_bytes(), key);
        for (sid, sender) in router.senders.iter().enumerate() {
            if sid != router.shard_id
                && router
                    .presence_table
                    .is_shard_interested(sid, channel.as_bytes())
            {
                let (tx, _rx) = flume::bounded(1);
                let _ = sender.send(ShardMessage::Publish {
                    channel: Bytes::from(channel.clone().into_bytes()),
                    message: Bytes::copy_from_slice(key),
                    responder: tx,
                });
            }
        }
    }
}

/// Whether events of `event_type` go to any subkey channel (`S`, `T`, `I`,
/// `V`). One relaxed load: callers check it before collecting the fields a
/// command touched, so with subkey notifications off nothing is collected.
#[inline(always)]
pub fn subkey_notify_enabled(event_type: u32) -> bool {
    let flags = NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed);
    flags & NOTIFY_SUBKEY_CHANNELS != 0 && flags & event_type != 0
}

/// Publishes `message` on `channel` here and on every other shard that has
/// a subscriber (or pattern) for it, as keyspace events do.
fn publish_notification(router: &Router, channel: &[u8], message: &[u8]) {
    let _ = router.pubsub.borrow().publish(channel, message);
    for (sid, sender) in router.senders.iter().enumerate() {
        if sid != router.shard_id && router.presence_table.is_shard_interested(sid, channel) {
            let (tx, _rx) = flume::bounded(1);
            let _ = sender.send(ShardMessage::Publish {
                channel: Bytes::copy_from_slice(channel),
                message: Bytes::copy_from_slice(message),
                responder: tx,
            });
        }
    }
}

/// Appends `<len>:<bytes>`, the length-prefixed form subkey payloads use so
/// that names holding `,` or `|` still parse.
fn push_len_prefixed(buf: &mut Vec<u8>, s: &[u8]) {
    use std::io::Write as _;
    let _ = write!(buf, "{}:", s.len());
    buf.extend_from_slice(s);
}

/// Subkey events for this thread's current db: see
/// [`notify_subkey_event_sync_in_db`].
pub fn notify_subkey_event<F: AsRef<[u8]>>(event_type: u32, event: &str, key: &[u8], fields: &[F]) {
    if fields.is_empty() || !subkey_notify_enabled(event_type) {
        return;
    }
    CURRENT_ROUTER.with(|cr| {
        if let Some(router) = cr.borrow().as_ref() {
            notify_subkey_event_sync_in_db(
                router,
                CURRENT_DB.get(),
                event_type,
                event,
                key,
                fields,
            );
        }
    });
}

/// Subkey events (Redis 8.8): `event` changed `fields` of `key`. Each
/// enabled channel class gets one message, listing every field once in
/// first-seen order as `<len>:<field>` joined by `,`:
/// - `S`: `__subkeyspace@<db>__:<key>` <- `<event>|<fields>`
/// - `T`: `__subkeyevent@<db>__:<event>` <- `<keylen>:<key>|<fields>`
/// - `V`: `__subkeyspaceevent@<db>__:<event>|<key>` <- `<fields>`
/// - `I`: `__subkeyspaceitem@<db>__:<key>\n<field>` <- `<event>`, one per
///   field; skipped for keys holding a newline, which would make the channel
///   name ambiguous.
///
/// Nothing is published without fields.
#[inline(never)]
pub fn notify_subkey_event_sync_in_db<F: AsRef<[u8]>>(
    router: &Router,
    db_id: usize,
    event_type: u32,
    event: &str,
    key: &[u8],
    fields: &[F],
) {
    let flags = NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed);
    if flags & NOTIFY_SUBKEY_CHANNELS == 0 || flags & event_type == 0 || fields.is_empty() {
        return;
    }
    // Each field once (HDEL f f, HSET f a f b), keeping first-seen order.
    let mut uniq: smallvec::SmallVec<[&[u8]; 16]> = smallvec::SmallVec::with_capacity(fields.len());
    if fields.len() <= 16 {
        for f in fields {
            let f = f.as_ref();
            if !uniq.contains(&f) {
                uniq.push(f);
            }
        }
    } else {
        let mut seen: hashbrown::HashSet<&[u8]> = hashbrown::HashSet::with_capacity(fields.len());
        for f in fields {
            let f = f.as_ref();
            if seen.insert(f) {
                uniq.push(f);
            }
        }
    }
    let mut list = Vec::with_capacity(uniq.iter().map(|f| f.len() + 4).sum());
    for (i, f) in uniq.iter().enumerate() {
        if i > 0 {
            list.push(b',');
        }
        push_len_prefixed(&mut list, f);
    }
    let db = db_id.to_string();
    if flags & NOTIFY_SUBKEYSPACE != 0 {
        let mut channel = Vec::with_capacity(20 + key.len());
        channel.extend_from_slice(b"__subkeyspace@");
        channel.extend_from_slice(db.as_bytes());
        channel.extend_from_slice(b"__:");
        channel.extend_from_slice(key);
        let mut msg = Vec::with_capacity(event.len() + 1 + list.len());
        msg.extend_from_slice(event.as_bytes());
        msg.push(b'|');
        msg.extend_from_slice(&list);
        publish_notification(router, &channel, &msg);
    }
    if flags & NOTIFY_SUBKEYEVENT != 0 {
        let mut channel = Vec::with_capacity(20 + event.len());
        channel.extend_from_slice(b"__subkeyevent@");
        channel.extend_from_slice(db.as_bytes());
        channel.extend_from_slice(b"__:");
        channel.extend_from_slice(event.as_bytes());
        let mut msg = Vec::with_capacity(key.len() + 8 + list.len());
        push_len_prefixed(&mut msg, key);
        msg.push(b'|');
        msg.extend_from_slice(&list);
        publish_notification(router, &channel, &msg);
    }
    if flags & NOTIFY_SUBKEYSPACEEVENT != 0 {
        let mut channel = Vec::with_capacity(26 + event.len() + key.len());
        channel.extend_from_slice(b"__subkeyspaceevent@");
        channel.extend_from_slice(db.as_bytes());
        channel.extend_from_slice(b"__:");
        channel.extend_from_slice(event.as_bytes());
        channel.push(b'|');
        channel.extend_from_slice(key);
        publish_notification(router, &channel, &list);
    }
    if flags & NOTIFY_SUBKEYSPACEITEM != 0 && !key.contains(&b'\n') {
        let mut channel = Vec::with_capacity(26 + key.len());
        channel.extend_from_slice(b"__subkeyspaceitem@");
        channel.extend_from_slice(db.as_bytes());
        channel.extend_from_slice(b"__:");
        channel.extend_from_slice(key);
        channel.push(b'\n');
        let prefix = channel.len();
        for f in &uniq {
            channel.truncate(prefix);
            channel.extend_from_slice(f);
            publish_notification(router, &channel, event.as_bytes());
        }
    }
}

/// `event` on hash `key` that touched `fields`: the keyspace event, then
/// the subkey events. Callers pass no fields when
/// [`subkey_notify_enabled`] is off.
#[inline(never)]
pub fn notify_hash_fields_event<F: AsRef<[u8]>>(event: &str, key: &[u8], fields: &[F]) {
    notify_keyspace_event(NOTIFY_HASH, event, key);
    notify_subkey_event(NOTIFY_HASH, event, key, fields);
}

pub fn notify_set_key_events(router: &Router, prev_kind: &str, new_kind: &str, key: &[u8]) {
    if prev_kind == "none" {
        notify_keyspace_event_sync(router, NOTIFY_NEW, "new", key);
    } else {
        notify_keyspace_event_sync(router, NOTIFY_OVERWRITTEN, "overwritten", key);
        if prev_kind != new_kind {
            notify_keyspace_event_sync(router, NOTIFY_TYPE_CHANGED, "type_changed", key);
        }
    }
}

pub fn notify_set_key_events_local(prev_kind: &str, new_kind: &str, key: &[u8]) {
    if prev_kind == "none" {
        notify_keyspace_event(NOTIFY_NEW, "new", key);
    } else {
        notify_keyspace_event(NOTIFY_OVERWRITTEN, "overwritten", key);
        if prev_kind != new_kind {
            notify_keyspace_event(NOTIFY_TYPE_CHANGED, "type_changed", key);
        }
    }
}

/// How long `publish_sync` / `spublish_sync` wait for other shards to report
/// their receiver counts. They run inside synchronous code (Lua scripts,
/// batched commands) that holds the shard thread, so they must not block on
/// another shard indefinitely: two shards publishing to each other from
/// scripts at the same time used to deadlock the whole server. Delivery does
/// not depend on the wait (the message is already in the remote mailbox);
/// only the returned count can come up short when a receiving shard is
/// itself stuck in synchronous code for longer than this.
const SYNC_PUBLISH_COUNT_WAIT: std::time::Duration = std::time::Duration::from_millis(1);

/// Sums the receiver counts that arrive within [`SYNC_PUBLISH_COUNT_WAIT`].
fn collect_publish_counts(mut pending: Vec<flume::Receiver<usize>>) -> usize {
    let deadline = std::time::Instant::now() + SYNC_PUBLISH_COUNT_WAIT;
    let mut count = 0;
    while !pending.is_empty() {
        pending.retain(|rx| match rx.try_recv() {
            Ok(c) => {
                count += c;
                false
            }
            Err(flume::TryRecvError::Empty) => true,
            Err(flume::TryRecvError::Disconnected) => false,
        });
        if pending.is_empty() || std::time::Instant::now() >= deadline {
            break;
        }
        // The replying shard may share this core when threads > cores.
        std::thread::yield_now();
    }
    count
}

/// Error for a command issued by a script that touches a key this shard does
/// not own. Scripts run synchronously against the local shard's data only,
/// so such a command would silently read or write the wrong shard (a key
/// written there is invisible to normal commands). Like Redis Cluster, reject
/// it instead.
pub const SCRIPT_NON_LOCAL_KEY_ERR: &str = "ERR Script attempted to access a non local key: \
     pass every key the script uses in KEYS, and use hash tags so they all map to one shard";

pub fn script_touches_non_local_key(cmd: &Command) -> bool {
    CURRENT_ROUTER.with(|cr| {
        let Some(router) = cr.borrow().as_ref().cloned() else {
            return false;
        };
        if router.num_shards <= 1 {
            return false;
        }
        let mut non_local = false;
        for_each_cmd_key(cmd, |k| {
            non_local |= router.target_shard(k) != router.shard_id;
        });
        non_local
    })
}

/// The shard of the first key `cmd` touches that this shard does not own.
pub fn script_non_local_target(cmd: &Command) -> Option<usize> {
    CURRENT_ROUTER.with(|cr| {
        let router = cr.borrow().as_ref().cloned()?;
        if router.num_shards <= 1 {
            return None;
        }
        let mut target = None;
        for_each_cmd_key(cmd, |k| {
            let shard = router.target_shard(k);
            if target.is_none() && shard != router.shard_id {
                target = Some(shard);
            }
        });
        target
    })
}

/// Sends a PUBLISH / SPUBLISH (`name`) to this server's replicas, as Redis
/// does, so their subscribers get the message too. Not to the AOF: a
/// message is not data. A replica applying it from its master passes it on
/// to its own replicas the same way.
fn propagate_publish(name: &[u8], channel: &[u8], message: &[u8]) {
    CURRENT_ROUTER.with(|cr| {
        if let Some(router) = cr.borrow().as_ref()
            && crate::replication::has_connected_replicas(router.base_port)
        {
            let mut bytes = Vec::with_capacity(channel.len() + message.len() + 32);
            write_resp_command(&mut bytes, &[name, channel, message]);
            crate::replication::propagate_shard_bytes(router.base_port, router.shard_id, &bytes);
        }
    });
}

pub fn publish_sync(channel: &[u8], message: &[u8]) -> usize {
    CURRENT_ROUTER.with(|cr| {
        if let Some(router) = cr.borrow().as_ref() {
            let count = router.pubsub.borrow().publish(channel, message);
            let mut pending = Vec::new();
            for (sid, sender) in router.senders.iter().enumerate() {
                if sid != router.shard_id && router.presence_table.is_shard_interested(sid, channel)
                {
                    let (tx, rx) = flume::bounded(1);
                    if sender
                        .send(ShardMessage::Publish {
                            channel: Bytes::copy_from_slice(channel),
                            message: Bytes::copy_from_slice(message),
                            responder: tx,
                        })
                        .is_ok()
                    {
                        pending.push(rx);
                    }
                }
            }
            count + collect_publish_counts(pending)
        } else {
            0
        }
    })
}

pub fn spublish_sync(channel: &[u8], message: &[u8]) -> usize {
    CURRENT_ROUTER.with(|cr| {
        if let Some(router) = cr.borrow().as_ref() {
            let target_shard = router.target_shard(channel);
            if target_shard == router.shard_id {
                router.pubsub.borrow().spublish(channel, message)
            } else {
                let (tx, rx) = flume::bounded(1);
                if router.senders[target_shard]
                    .send(ShardMessage::Spublish {
                        channel: Bytes::copy_from_slice(channel),
                        message: Bytes::copy_from_slice(message),
                        responder: tx,
                    })
                    .is_ok()
                {
                    collect_publish_counts(vec![rx])
                } else {
                    0
                }
            }
        } else {
            0
        }
    })
}

/// Warns (at most once a minute) that writes are being refused with -OOM,
/// which Redis only reports to the client: operators should see it too.
#[cold]
fn log_oom_rejection(max_mem: u64, policy: &str) {
    static LOG_LIMIT: crate::log::RateLimit = crate::log::RateLimit::new();
    if LOG_LIMIT.allow(std::time::Duration::from_secs(60)) {
        crate::log_warning!(
            "WARNING: used memory is over 'maxmemory' ({} bytes) and maxmemory-policy '{}' cannot free enough: write commands are refused with -OOM",
            max_mem,
            policy
        );
    }
}

/// For SUBSCRIBE / PSUBSCRIBE / SSUBSCRIBE (`denyoom` in Redis): whether
/// used memory is over `maxmemory` after the policy had its chance to free
/// some, so the subscription is refused. Subscriptions take memory (the
/// channel names, the registry), and a client could otherwise grow it past
/// the limit without bound. `incoming` is the size of the names about to be
/// subscribed: Redis counts them in the client's query buffer, but rudis
/// may hold them in a pooled read buffer that was already allocated, so
/// used memory alone did not always show them (a 2 MB channel name under a
/// 2 MB limit was sometimes accepted).
async fn subscribe_over_maxmemory(router: &Router, client_id: u64, incoming: usize) -> bool {
    let max_mem = router
        .tier_stats
        .max_memory
        .load(std::sync::atomic::Ordering::Relaxed);
    if max_mem == 0 || maxmemory_ignored_as_replica(router.base_port) {
        return false;
    }
    let policy = get_max_memory_policy();
    let mut over = if policy != "noeviction" {
        let mut scratch = Vec::new();
        !router
            .evict_until_under_maxmemory(max_mem as usize, &policy, client_id, &mut scratch)
            .await
    } else {
        router.over_maxmemory_checked().await && {
            router.check_auto_tier().await;
            router.over_maxmemory()
        }
    };
    if !over && incoming > 0 {
        router.over_maxmemory();
        router.refresh_published_usage().await;
        over = router.tier_stats.real_used_total(None) + incoming as i64 > max_mem as i64;
    }
    if over {
        log_oom_rejection(max_mem, &policy);
    }
    over
}

pub static ISOLATED_PANICS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[inline]
pub fn inc_isolated_panics() {
    ISOLATED_PANICS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub fn get_isolated_panics() -> u64 {
    ISOLATED_PANICS.load(std::sync::atomic::Ordering::Relaxed)
}

/// The cluster redirect (or -CLUSTERDOWN / -ASK) `cmd`'s key gets on this
/// node, written to `out`: whether there was one. Run for every command,
/// and when a command is queued in MULTI (Redis answers -MOVED then, and
/// the EXEC aborts).
async fn cluster_redirects(
    cmd: &Command,
    router: &Router,
    is_asking: bool,
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    out: &mut Vec<u8>,
) -> bool {
    if let Some(key) = cmd_primary_key(cmd) {
        let slot = key_slot(key);
        let state = router.get_slot_state(slot);
        match state {
            crate::shard::SlotState::Moved(target) => {
                out.extend_from_slice(format!("-MOVED {} {}\r\n", slot, target).as_bytes());
                return true;
            }
            crate::shard::SlotState::Importing(source) => {
                if !is_asking {
                    out.extend_from_slice(format!("-MOVED {} {}\r\n", slot, source).as_bytes());
                    return true;
                }
            }
            crate::shard::SlotState::Migrating(target) => {
                let key_exists = router.exists(key.clone()).await;
                if !key_exists {
                    out.extend_from_slice(format!("-ASK {} {}\r\n", slot, target).as_bytes());
                    return true;
                }
            }
            crate::shard::SlotState::Stable => {
                if router.cluster_enabled || crate::cluster::has_active_cluster(router.base_port) {
                    let hub = crate::cluster::get_cluster_hub(router.base_port);
                    // A master cut off from the majority of masters serves
                    // no keys (Redis's minority rule; see
                    // `cluster::update_cluster_state`).
                    if hub.is_down()
                        && (cmd.is_write_command()
                            || !hub
                                .allow_reads_when_down
                                .load(std::sync::atomic::Ordering::Relaxed))
                    {
                        out.extend_from_slice(b"-CLUSTERDOWN The cluster is down\r\n");
                        return true;
                    }
                    // Never hold `my_slots` while taking `nodes` below: the
                    // cluster bus takes them the other way round.
                    // Like Redis, a READONLY client of a cluster replica gets
                    // reads for its master's slots served here.
                    let owns_slot = hub.owns_slot(slot)
                        || (!cmd.is_write_command()
                            && client_registry
                                .borrow()
                                .get(&client_id)
                                .is_some_and(|c| c.readonly)
                            && hub.replicates_slot(slot));
                    if owns_slot {
                        if router.cluster_enabled {
                            let target_shard = router.target_shard_for_slot(slot);
                            if target_shard != router.shard_id {
                                let target_port = hub.my_port() + target_shard as u16;
                                out.extend_from_slice(
                                    format!(
                                        "-MOVED {} {}:{}\r\n",
                                        slot,
                                        hub.endpoint(&hub.my_ip(), &hub.my_hostname()),
                                        target_port
                                    )
                                    .as_bytes(),
                                );
                                return true;
                            }
                        }
                    } else {
                        let nodes = hub.nodes.read();
                        if !nodes.is_empty()
                            && let Some(peer) = nodes.values().find(|n| {
                                n.is_master()
                                    && n.fail != crate::cluster::FailState::Fail
                                    && n.slots.iter().any(|&(s, e)| slot >= s && slot <= e)
                            })
                        {
                            out.extend_from_slice(
                                format!(
                                    "-MOVED {} {}:{}\r\n",
                                    slot,
                                    hub.endpoint(&peer.ip, &peer.hostname),
                                    peer.port_for_slot(slot)
                                )
                                .as_bytes(),
                            );
                            return true;
                        }
                        out.extend_from_slice(b"-CLUSTERDOWN Hash slot not served\r\n");
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Publishes a client's MULTI state for CLIENT LIST (`multi`,
/// `multi-mem`) and `maxmemory-clients` after a step that may have
/// changed it (`queued_before`: the queue length before the step).
fn publish_tx_state(
    port: u16,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    client_id: u64,
    in_multi: bool,
    tx_queue: &[Command],
    queued_before: usize,
) {
    use std::sync::atomic::Ordering::Relaxed;
    let reg = client_registry.borrow();
    let Some(c) = reg.get(&client_id) else {
        return;
    };
    if !in_multi {
        c.stats.multi_count.store(-1, Relaxed);
        c.stats.multi_mem.store(0, Relaxed);
        return;
    }
    if tx_queue.len() < queued_before || c.stats.multi_count.load(Relaxed) < 0 {
        // A new MULTI (or the queue shrank): recount it whole.
        let mem = tx_queue.iter().map(queued_command_mem).sum();
        c.stats.multi_mem.store(mem, Relaxed);
    } else {
        let added: usize = tx_queue[queued_before..]
            .iter()
            .map(queued_command_mem)
            .sum();
        c.stats.multi_mem.fetch_add(added, Relaxed);
    }
    c.stats.multi_count.store(tx_queue.len() as i64, Relaxed);
    // maxmemory-clients: a growing MULTI queue is checked as it grows, as
    // Redis evicts on queueing.
    if tx_queue.len() > queued_before && maxmemory_clients_limit(port) > 0 {
        let now = Instant::now();
        let idle = now.duration_since(c.last_active).as_secs();
        // Each part once (the last measurement already includes the
        // queue, so it isn't the base here).
        let (qbuf, qbuf_free) = c.effective_qbuf(idle);
        let my_mem =
            CLIENT_BASE_MEM + qbuf + qbuf_free + c.omem_now() + c.stats.multi_mem.load(Relaxed);
        drop(reg);
        client_evicted_for_memory(port, client_id, my_mem);
    }
}

/// The bytes a command queued in MULTI holds: its arguments.
fn queued_command_mem(cmd: &Command) -> usize {
    let (argv, _) = crate::slowlog::command_to_slowlog_argv(cmd);
    argv.iter().map(|a| a.len()).sum::<usize>() + 16 * argv.len()
}

#[allow(clippy::too_many_arguments)]
async fn execute_tx_step(
    cmd: Command,
    router: &Rc<Router>,
    client_id: u64,
    client_registry: &Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    out_buf: &mut Vec<u8>,
    asking: &mut bool,
    authenticated: &mut bool,
    auth_user: &mut String,
    in_multi: &mut bool,
    tx_queue: &mut Vec<Command>,
    tx_has_error: &mut bool,
) -> bool {
    let was_in_multi = *in_multi;
    let queued_before = tx_queue.len();
    let quit = execute_tx_step_inner(
        cmd,
        router,
        client_id,
        client_registry,
        out_buf,
        asking,
        authenticated,
        auth_user,
        in_multi,
        tx_queue,
        tx_has_error,
    )
    .await;
    if was_in_multi || *in_multi {
        publish_tx_state(
            router.base_port,
            client_registry,
            client_id,
            *in_multi,
            tx_queue,
            queued_before,
        );
    }
    quit
}

#[allow(clippy::too_many_arguments)]
async fn execute_tx_step_inner(
    cmd: Command,
    router: &Rc<Router>,
    client_id: u64,
    client_registry: &Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    out_buf: &mut Vec<u8>,
    asking: &mut bool,
    authenticated: &mut bool,
    auth_user: &mut String,
    in_multi: &mut bool,
    tx_queue: &mut Vec<Command>,
    tx_has_error: &mut bool,
) -> bool {
    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
        CURRENT_DB.set(c.db);
        if !in_tx(client_id) {
            c.last_active = Instant::now();
            c.last_cmd = get_cmd_name(&cmd);
        }
    }
    if *in_multi {
        match cmd {
            Command::Multi => {
                record_cmd_stat("MULTI");
                record_failed_stat("MULTI");
                record_error_stat("ERR", None);
                if let Some(c) = client_registry.borrow().get(&client_id) {
                    c.stats
                        .tot_cmds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                out_buf.extend_from_slice(b"-ERR MULTI calls can not be nested\r\n");
                false
            }
            Command::Watch(_) => {
                record_cmd_stat("WATCH");
                record_failed_stat("WATCH");
                record_error_stat("ERR", None);
                if let Some(c) = client_registry.borrow().get(&client_id) {
                    c.stats
                        .tot_cmds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                out_buf.extend_from_slice(b"-ERR WATCH inside MULTI is not allowed\r\n");
                false
            }
            Command::Unwatch => {
                record_cmd_stat("UNWATCH");
                if let Some(c) = client_registry.borrow().get(&client_id) {
                    c.stats
                        .tot_cmds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                out_buf.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::Discard => {
                record_cmd_stat("DISCARD");
                if let Some(c) = client_registry.borrow().get(&client_id) {
                    c.stats
                        .tot_cmds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                *in_multi = false;
                tx_queue.clear();
                *tx_has_error = false;
                unwatch_keys(router.port, client_id);
                out_buf.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::Reset => {
                record_cmd_stat("RESET");
                *in_multi = false;
                tx_queue.clear();
                *tx_has_error = false;
                unwatch_keys(router.port, client_id);
                if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                    c.name = None;
                    c.is_resp3 = client_default_resp3();
                    c.db = 0;
                    c.is_monitor = false;
                    c.stats
                        .tot_cmds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                CURRENT_DB.set(0);
                router.local_db.borrow_mut().select_db(0);
                CLIENT_NO_TOUCH.set(false);
                update_global_client_resp3(router.port, client_id, client_default_resp3());
                update_global_client_auth(router.port, client_id, "default");
                unregister_client_tracking(router.port, client_id);
                unregister_monitor_client(router.port, client_id);
                reset_client_pubsub(router, client_id);

                let acl = crate::acl::get_acl_for_port(router.port);
                let default_requires_auth = acl.read().is_auth_required_for_default();
                *authenticated = !default_requires_auth;
                *auth_user = "default".to_string();

                out_buf.extend_from_slice(b"+RESET\r\n");
                false
            }
            Command::Exec => {
                // Timed as a whole, as Redis does (its queued commands are
                // also counted on their own); CLUSTER SLOT-STATS gives the
                // transaction's slot the same time.
                struct ExecStat(Option<u16>, std::time::Instant);
                impl Drop for ExecStat {
                    fn drop(&mut self) {
                        let nanos = self.1.elapsed().as_nanos() as u64;
                        record_cmd_stats("EXEC", 1, nanos);
                        crate::slot_stats::record_cpu(self.0, nanos);
                    }
                }
                let _exec_stat = ExecStat(
                    crate::slot_stats::take_exec_slot(),
                    std::time::Instant::now(),
                );
                let monitor_addr = if has_monitor_clients() {
                    let addr_s = client_registry
                        .borrow()
                        .get(&client_id)
                        .map(|c| c.addr.to_string())
                        .unwrap_or_default();
                    broadcast_monitor(router.port, &addr_s, &[Bytes::from_static(b"multi")]);
                    Some(addr_s)
                } else {
                    None
                };
                if let Some(c) = client_registry.borrow().get(&client_id) {
                    c.stats
                        .tot_cmds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                *in_multi = false;
                // EXEC itself is refused while a script runs past its time
                // limit: like Redis, the transaction is discarded with the
                // reason.
                if let Some(busy) = crate::scripting::busy_script_reply(router.port) {
                    record_failed_stat("EXEC");
                    record_error_stat("EXECABORT", None);
                    tx_queue.clear();
                    *tx_has_error = false;
                    unwatch_keys(router.port, client_id);
                    out_buf.extend_from_slice(b"-EXECABORT Transaction discarded because of: ");
                    out_buf.extend_from_slice(&busy[1..]);
                    return false;
                }
                // So is a transaction with writes the server no longer takes
                // (as Redis checks an EXEC like its writes): a replica now, or
                // too few good replicas. Checked here, so none of it runs.
                let tx_writes = tx_queue
                    .iter()
                    .any(|c| c.is_write_command() || crate::aof::command_to_resp(c).is_some());
                let refused: Option<&[u8]> = if let Some(down) =
                    tx_queue.iter().find_map(|c| masterdown_refuses(router, c))
                {
                    Some(down)
                } else if !tx_writes {
                    None
                } else if crate::replication::HAS_SLAVE_INSTANCE
                    .load(std::sync::atomic::Ordering::Relaxed)
                    && REPLICA_READ_ONLY.load(std::sync::atomic::Ordering::Relaxed)
                    && crate::replication::cached_hub(router.base_port).is_slave()
                {
                    Some(b"READONLY You can't write against a read only replica.\r\n")
                } else if MIN_REPLICAS_TO_WRITE.load(std::sync::atomic::Ordering::Relaxed) > 0
                    && not_enough_good_replicas(router.base_port)
                {
                    Some(b"NOREPLICAS Not enough good replicas to write.\r\n")
                } else {
                    None
                };
                // And one with a write that uses memory, over maxmemory.
                let refused = match refused {
                    Some(r) => Some(r),
                    None => {
                        let mut oom = false;
                        for c in tx_queue.iter() {
                            if oom_refuses(router, c, client_id, out_buf).await {
                                oom = true;
                                break;
                            }
                        }
                        oom.then_some(
                            &b"OOM command not allowed when used memory > 'maxmemory'.\r\n"[..],
                        )
                    }
                };
                if let Some(reason) = refused {
                    record_failed_stat("EXEC");
                    record_error_stat("EXECABORT", None);
                    tx_queue.clear();
                    *tx_has_error = false;
                    unwatch_keys(router.port, client_id);
                    out_buf.extend_from_slice(b"-EXECABORT Transaction discarded because of: ");
                    out_buf.extend_from_slice(reason);
                    return false;
                }
                if *tx_has_error {
                    if let Some(ref addr_s) = monitor_addr {
                        broadcast_monitor(router.port, addr_s, &[Bytes::from_static(b"exec")]);
                    }
                    record_failed_stat("EXEC");
                    record_error_stat("EXECABORT", None);
                    tx_queue.clear();
                    *tx_has_error = false;
                    unwatch_keys(router.port, client_id);
                    out_buf.extend_from_slice(
                        b"-EXECABORT Transaction discarded because of previous errors.\r\n",
                    );
                    false
                } else if (router.cluster_enabled && tx_has_cross_slot(tx_queue))
                    || (cross_shard_strict()
                        && router.num_shards > 1
                        && keys_on_several_shards(
                            tx_queue.iter().flat_map(cmd_keys),
                            router.num_shards,
                        ))
                {
                    if let Some(ref addr_s) = monitor_addr {
                        broadcast_monitor(router.port, addr_s, &[Bytes::from_static(b"exec")]);
                    }
                    record_failed_stat("EXEC");
                    record_error_stat("CROSSSLOT", None);
                    tx_queue.clear();
                    *tx_has_error = false;
                    unwatch_keys(router.port, client_id);
                    out_buf.extend_from_slice(
                        b"-CROSSSLOT Keys in request don't hash to the same slot\r\n",
                    );
                    false
                } else if watch_failed(router, client_id).await {
                    if let Some(ref addr_s) = monitor_addr {
                        broadcast_monitor(router.port, addr_s, &[Bytes::from_static(b"exec")]);
                    }
                    tx_queue.clear();
                    *tx_has_error = false;
                    unwatch_keys(router.port, client_id);
                    write_resp_null_array(out_buf);
                    false
                } else {
                    if crate::asm::any_write_pause() {
                        for c in tx_queue.iter() {
                            crate::asm::wait_write_pause(router.base_port, c).await;
                        }
                    }
                    let _write_running = wait_if_tx_paused(router.port, client_id, tx_queue).await;
                    unwatch_keys(router.port, client_id);
                    let mut shards = hashbrown::HashSet::new();
                    for cmd in tx_queue.iter() {
                        for k in cmd_keys(cmd) {
                            shards.insert(router.target_shard(k));
                        }
                    }
                    let mut sorted_shards: Vec<usize> = shards.into_iter().collect();
                    sorted_shards.sort_unstable();

                    let use_vll = sorted_shards.len() > 1;
                    // Armed before anything it undoes, so a drop at any await
                    // below (a panicking queued command isolates the
                    // connection) still releases everything.
                    let mut cleanup = ExecCleanup {
                        router,
                        client_id,
                        locks: None,
                        xwrite: None,
                        hub: None,
                    };
                    if use_vll {
                        let id = next_tx_id();
                        cleanup.locks = Some((sorted_shards.clone(), id));
                        router.acquire_tx_locks(&sorted_shards, id).await;
                        cleanup.xwrite = Some(crate::router::XShardWriteGuard::locked());
                    }

                    let hub_arc = crate::block::get_block_hub_for_port(router.port);
                    hub_arc.lock().pause();
                    cleanup.hub = Some(hub_arc);

                    // Writes go to replicas as one MULTI ... EXEC block; a
                    // single one goes alone, as Redis sends it.
                    let tx_block = if tx_queue.iter().filter(|c| c.is_write_command()).count() > 1 {
                        crate::replication::TxStreamBlock::open(router.base_port, router.num_shards)
                            .await
                    } else {
                        None
                    };
                    let count = tx_queue.len();
                    out_buf.extend_from_slice(format!("*{}\r\n", count).as_bytes());
                    let queued = std::mem::take(tx_queue);
                    let mut should_quit = false;
                    enter_tx(client_id);
                    DEFER_BCAST_FLUSH.set(true);
                    EXECUTING_CLIENT_ID.set(client_id);
                    for q_cmd in queued {
                        let quit = execute_command(
                            q_cmd,
                            router,
                            client_id,
                            client_registry,
                            out_buf,
                            asking,
                            authenticated,
                            auth_user,
                        )
                        .await;
                        if quit {
                            should_quit = true;
                            break;
                        }
                    }
                    if let Some(block) = tx_block {
                        block.close().await;
                    }
                    if let Some(ref addr_s) = monitor_addr {
                        broadcast_monitor(router.port, addr_s, &[Bytes::from_static(b"exec")]);
                    }
                    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                        c.last_active = Instant::now();
                        c.last_cmd = "EXEC";
                    }
                    drop(cleanup);
                    should_quit
                }
            }
            Command::Quit => {
                out_buf.extend_from_slice(b"+OK\r\n");
                true
            }
            Command::Save | Command::Bgsave { .. } | Command::Shutdown { .. } => {
                *tx_has_error = true;
                out_buf.extend_from_slice(b"-ERR Command not allowed inside a transaction\r\n");
                false
            }
            Command::Debug(_)
                if protected_command_denied(&cmd, router.base_port, client_id, client_registry)
                    .is_some() =>
            {
                *tx_has_error = true;
                out_buf.extend_from_slice(crate::netsec::DEBUG_COMMAND_NOT_ALLOWED);
                false
            }
            Command::Unknown(cmd_name) => {
                *tx_has_error = true;
                out_buf.extend_from_slice(
                    format!("-ERR unknown command '{}'\r\n", err_arg(&cmd_name)).as_bytes(),
                );
                false
            }
            // Redis checks permissions when queueing too: a denied command
            // gets its NOPERM now and aborts the EXEC.
            ref c => {
                if let Some(err) = acl_denied_in_context(
                    router.port,
                    auth_user,
                    c,
                    Some(("multi", acl_cmd_name(c))),
                ) {
                    *tx_has_error = true;
                    out_buf.extend_from_slice(&err);
                    return false;
                }
                // And the cluster: a key served elsewhere gets its -MOVED
                // now, as in Redis, and the EXEC aborts.
                if router.cluster_enabled
                    && cluster_redirects(c, router, false, client_id, client_registry, out_buf)
                        .await
                {
                    *tx_has_error = true;
                    return false;
                }
                // And a script running past its time limit: the command
                // gets -BUSY now and the EXEC aborts.
                if !allowed_while_busy(c)
                    && let Some(busy) = crate::scripting::busy_script_reply(router.port)
                {
                    *tx_has_error = true;
                    out_buf.extend_from_slice(busy);
                    return false;
                }
                // And memory: a write refused for OOM now aborts the EXEC.
                if oom_refuses(router, c, client_id, out_buf).await {
                    *tx_has_error = true;
                    let c_name = stat_cmd_name(c);
                    record_rejected_stat(c_name);
                    record_error_stat("OOM", Some(c_name));
                    out_buf.extend_from_slice(
                        b"-OOM command not allowed when used memory > 'maxmemory'.\r\n",
                    );
                    return false;
                }
                if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                    enforce_tracking_max_keys(router.port);
                }
                tx_queue.push(cmd);
                out_buf.extend_from_slice(b"+QUEUED\r\n");
                false
            }
        }
    } else {
        match cmd {
            Command::Multi => {
                record_cmd_stat("MULTI");
                if let Some(c) = client_registry.borrow().get(&client_id) {
                    c.stats
                        .tot_cmds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                *in_multi = true;
                tx_queue.clear();
                *tx_has_error = false;
                out_buf.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::Discard => {
                record_cmd_stat("DISCARD");
                record_failed_stat("DISCARD");
                record_error_stat("ERR", None);
                if let Some(c) = client_registry.borrow().get(&client_id) {
                    c.stats
                        .tot_cmds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                out_buf.extend_from_slice(b"-ERR DISCARD without MULTI\r\n");
                false
            }
            Command::Exec => {
                record_cmd_stat("EXEC");
                record_failed_stat("EXEC");
                record_error_stat("ERR", None);
                if let Some(c) = client_registry.borrow().get(&client_id) {
                    c.stats
                        .tot_cmds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                out_buf.extend_from_slice(b"-ERR EXEC without MULTI\r\n");
                false
            }
            Command::Watch(keys) => {
                record_cmd_stat("WATCH");
                if let Some(c) = client_registry.borrow().get(&client_id) {
                    c.stats
                        .tot_cmds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                watch_command(router, client_id, &keys).await;
                out_buf.extend_from_slice(b"+OK\r\n");
                false
            }
            Command::Unwatch => {
                record_cmd_stat("UNWATCH");
                if let Some(c) = client_registry.borrow().get(&client_id) {
                    c.stats
                        .tot_cmds
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                unwatch_keys(router.port, client_id);
                out_buf.extend_from_slice(b"+OK\r\n");
                false
            }
            _ => {
                execute_command(
                    cmd,
                    router,
                    client_id,
                    client_registry,
                    out_buf,
                    asking,
                    authenticated,
                    auth_user,
                )
                .await
            }
        }
    }
}

/// Serves a plaintext TCP client.
pub async fn handle_connection(
    stream: TcpStream,
    client_addr: SocketAddr,
    client_id: u64,
    client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    router: Rc<Router>,
) {
    handle_client(
        crate::transport::PlainTransport::new(stream),
        client_addr,
        client_id,
        client_registry,
        router,
    )
    .await
}

/// Reply to a replication handshake on a transport that cannot carry the
/// replication stream (it takes over the raw TCP socket).
const REPLICATION_NEEDS_PLAINTEXT: &[u8] =
    b"-ERR replication links are only supported on the plaintext port\r\n";

/// Per-client values the command path keeps in thread-locals. Connection
/// tasks on a shard thread interleave at every await (a cross-shard hop, a
/// blocking wait), so each task swaps its own copy in for the duration of
/// each poll; otherwise a RESP3 client's protocol or one client's id would
/// leak into another client's replies and tracking, and a script could be
/// ACL-checked as another client's user.
#[derive(Default)]
struct ClientThreadLocals {
    resp3: bool,
    db: usize,
    client_id: u64,
    executing_client_id: u64,
    defer_bcast_flush: bool,
    no_touch: bool,
    /// ACL user scripts check their commands against.
    auth_user: String,
}

impl ClientThreadLocals {
    #[inline]
    fn swap_with_thread(&mut self) {
        self.resp3 = CURRENT_CLIENT_RESP3.replace(self.resp3);
        self.db = CURRENT_DB.replace(self.db);
        self.client_id = CURRENT_CLIENT_ID.replace(self.client_id);
        self.executing_client_id = EXECUTING_CLIENT_ID.replace(self.executing_client_id);
        self.defer_bcast_flush = DEFER_BCAST_FLUSH.replace(self.defer_bcast_flush);
        self.no_touch = CLIENT_NO_TOUCH.replace(self.no_touch);
        CURRENT_AUTH_USER.with(|u| std::mem::swap(&mut *u.borrow_mut(), &mut self.auth_user));
    }
}

/// Runs a client's future with its own [`ClientThreadLocals`] installed
/// while it is polled, and the thread's previous values restored after.
struct ClientScope<F> {
    inner: std::pin::Pin<Box<F>>,
    locals: ClientThreadLocals,
}

impl<F: std::future::Future> std::future::Future for ClientScope<F> {
    type Output = F::Output;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<F::Output> {
        let this = &mut *self;
        this.locals.swap_with_thread();
        let res = this.inner.as_mut().poll(cx);
        this.locals.swap_with_thread();
        res
    }
}

/// The client connection loop, shared by every transport (see
/// `crate::transport`): parsing, transactions, pipeline squashing, Pub/Sub
/// and replication mode switches, output-buffer limits and client stats.
pub(crate) async fn handle_client<T: crate::transport::ClientTransport>(
    transport: T,
    client_addr: SocketAddr,
    client_id: u64,
    client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    router: Rc<Router>,
) {
    ClientScope {
        inner: Box::pin(handle_client_loop(
            transport,
            client_addr,
            client_id,
            client_registry,
            router,
        )),
        locals: ClientThreadLocals::default(),
    }
    .await
}

async fn handle_client_loop<T: crate::transport::ClientTransport>(
    mut transport: T,
    client_addr: SocketAddr,
    client_id: u64,
    client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    router: Rc<Router>,
) {
    let current_clients = ACTIVE_CLIENTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    let max_c = MAX_CLIENTS.load(std::sync::atomic::Ordering::Relaxed);
    if max_c > 0 && current_clients > max_c {
        ACTIVE_CLIENTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        crate::server_stats::add(crate::server_stats::Stat::RejectedConnections, 1);
        let err_msg = b"-ERR max number of clients reached\r\n";
        let _ = transport.write_all(err_msg.to_vec()).await;
        return;
    }
    crate::server_stats::add(crate::server_stats::Stat::ConnectionsReceived, 1);

    let raw_fd = transport.raw_fd();
    let push = transport.push_target();
    let now = Instant::now();
    let stats = std::sync::Arc::new(ClientStats::new(now, client_addr));
    let (track_tx, track_rx) = flume::unbounded::<Vec<u8>>();
    let default_resp3 = client_default_resp3();
    CURRENT_CLIENT_RESP3.set(default_resp3);
    register_global_client(
        router.port,
        client_id,
        raw_fd,
        push.clone(),
        track_tx.clone(),
        default_resp3,
        "default".to_string(),
        false,
        stats.clone(),
    );
    client_registry.borrow_mut().insert(
        client_id,
        ClientInfo {
            id: client_id,
            addr: client_addr,
            name: None,
            lib_name: None,
            lib_ver: None,
            connected_at: now,
            last_active: now,
            last_cmd: "NONE",
            is_resp3: default_resp3,
            db: 0,
            track_tx: Some(track_tx),
            raw_fd,
            omem: 0,
            qbuf_len: 0,
            qbuf_cap: 0,
            reply_mode: crate::resp::ClientReplyMode::On,
            is_monitor: false,
            readonly: false,
            stats: stats.clone(),
            push,
            himport_fieldsets: hashbrown::HashMap::new(),
            out_queue: None,
        },
    );

    struct ClientCleanup {
        port: u16,
        client_id: u64,
        registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
        router: Rc<Router>,
        stats: std::sync::Arc<ClientStats>,
    }
    impl Drop for ClientCleanup {
        fn drop(&mut self) {
            if !self
                .stats
                .killed
                .swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                ACTIVE_CLIENTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
            flush_local_cmd_stats();
            self.registry.borrow_mut().remove(&self.client_id);
            unregister_global_client(self.port, self.client_id);
            unregister_monitor_client(self.port, self.client_id);
            if !self.router.pubsub.borrow().clients.is_empty() {
                self.router.pubsub.borrow_mut().remove_client_with_presence(
                    self.client_id,
                    Some((self.router.shard_id, &self.router.presence_table)),
                );
                for (sid, sender) in self.router.senders.iter().enumerate() {
                    if sid != self.router.shard_id {
                        let _ = sender.send(crate::shard::ShardMessage::RemoveClientPubSub {
                            client_id: self.client_id,
                        });
                    }
                }
            }
            unregister_client_tracking(self.port, self.client_id);
            unwatch_keys(self.port, self.client_id);
            if crate::block::has_blocked_waiters(self.port) {
                let hub_arc = crate::block::get_block_hub_for_port(self.port);
                let mut hub = hub_arc.lock();
                hub.unregister_blocked_client(self.client_id);
            }
        }
    }
    let _cleanup = ClientCleanup {
        port: router.port,
        client_id,
        registry: client_registry.clone(),
        router: router.clone(),
        stats: stats.clone(),
    };

    let ConnScratch {
        mut buf,
        mut out_buf,
        responders,
        mut remote_batches,
        mut items_pool,
        mut squashed_responses,
        mut commands,
    } = take_conn_scratch(router.num_shards);
    // The arguments each command in `commands` was sent with, and how many
    // bytes it took, in step with it (MONITOR and SLOWLOG show them,
    // CLUSTER SLOT-STATS counts them).
    let mut argvs: std::collections::VecDeque<(Option<crate::resp::RawArgv>, u64)> =
        std::collections::VecDeque::new();
    let mut slot_acct = crate::slot_stats::ClientAcct::default();

    let mut asking = false;
    let mut in_multi = false;
    let mut tx_queue: Vec<Command> = Vec::new();
    let mut tx_has_error = false;
    let mut authenticated = !crate::acl::get_acl_for_port(router.port)
        .read()
        .is_auth_required_for_default();
    let mut auth_user = "default".to_string();
    let mut soft_limit_start: Option<Instant> = None;
    let mut aof_appends_seen = crate::aof::AOF_APPENDS.load(std::sync::atomic::Ordering::Relaxed);
    // AOF write-through before replies (see `aof_reply_barrier`): only with
    // an AOF, and only for batches that write.
    let aof_on = router.aof().is_some();
    let aof_progress = if aof_on {
        crate::aof::aof_progress_all(router.base_port, router.num_shards)
    } else {
        Vec::new()
    };

    use crate::transport::MIN_READ_SPARE;
    // Set when the previous batch stopped at MAX_DRAIN_BATCH_CMDS with
    // complete frames still in `buf`: run them before reading more.
    let mut carry_over = false;
    loop {
        release_big_query_buf(&mut buf);
        if buf.capacity() - buf.len() < MIN_READ_SPARE {
            buf.reserve(READ_BUFFER_SIZE);
        }

        let res = if std::mem::take(&mut carry_over) {
            Ok(None)
        } else {
            let (res, returned_buf) = match idle_read_limit(&router, client_id, &client_registry) {
                None => transport.read(buf).await,
                Some(limit) => {
                    match monoio::time::timeout(limit, transport.read(buf)).await {
                        Ok(r) => r,
                        Err(_) => {
                            // Idle for `timeout` seconds: close, like Redis.
                            buf = BytesMut::new();
                            break;
                        }
                    }
                }
            };
            buf = returned_buf;
            res.map(Some)
        };

        match res {
            Ok(Some(0)) => {
                // Client disconnected
                break;
            }
            Ok(read) => {
                let n = read.unwrap_or(0);
                if read.is_some() {
                    stats
                        .read_events
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    stats.last_read_ms.store(
                        crate::table::coarse_now_ms(),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
                stats
                    .tot_net_in
                    .fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
                crate::server_stats::add(crate::server_stats::Stat::NetInputBytes, n as u64);

                let batch_bytes = buf.len();
                let pause_cron = PAUSE_CRON.load(std::sync::atomic::Ordering::Relaxed);
                if batch_bytes > 16384 {
                    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                        c.qbuf_cap = if pause_cron {
                            c.qbuf_cap.max(batch_bytes)
                        } else {
                            batch_bytes
                        };
                    }
                } else if !pause_cron
                    && let Some(c) = client_registry.borrow_mut().get_mut(&client_id)
                    && c.qbuf_cap > 32768
                {
                    c.qbuf_cap = 16384;
                }

                // 1. Parse all complete commands currently in the buffer
                commands.clear();
                argvs.clear();
                let mut should_quit = false;
                let mut has_special = in_multi;
                let mut batch_in_bytes = n;
                // client-query-buffer-limit: one relaxed load per batch. Only the
                // bytes of a single command count (complete commands are parsed and
                // run batch by batch), so a long pipeline drained normally never
                // trips it; a huge partial command or one oversized command does.
                let qbuf_limit = client_query_buffer_limit();
                let mut qbuf_overflow = false;
                while !buf.is_empty() {
                    let len_before = buf.len();
                    match crate::resp::parse_command_with_argv(&mut buf) {
                        Ok(Some((cmd, argv))) => {
                            if parsed_query_over_limit(len_before, buf.len(), qbuf_limit) {
                                client_query_limit_reached(client_id, len_before - buf.len());
                                qbuf_overflow = true;
                                break;
                            }
                            let is_quit = matches!(cmd, Command::Quit);
                            if !has_special && is_special_pipeline_cmd(&cmd) {
                                has_special = true;
                            }
                            commands.push(cmd);
                            argvs.push_back((argv, (len_before - buf.len()) as u64));
                            if is_quit {
                                buf.clear();
                                break;
                            }
                            // Bound the batch even when one read delivered far
                            // more: memory checks (maxmemory), replies and other
                            // clients all wait for the whole batch to execute.
                            if commands.len() >= MAX_DRAIN_BATCH_CMDS {
                                carry_over = !buf.is_empty();
                                break;
                            }
                        }
                        Ok(None) => {
                            // `buf` now holds just one incomplete command.
                            if partial_query_over_limit(buf.len(), qbuf_limit) {
                                client_query_limit_reached(client_id, buf.len());
                                qbuf_overflow = true;
                                break;
                            }
                            // Incomplete frame: check if remaining bytes just arrived in kernel buffer.
                            // Stop draining once the batch is full; the partial frame stays in `buf`
                            // and the next `read` completes it after this batch has run.
                            let batch_full = commands.len() >= MAX_DRAIN_BATCH_CMDS
                                || batch_in_bytes >= MAX_DRAIN_BATCH_BYTES;
                            let drain_n = if batch_full {
                                0
                            } else {
                                transport.read_ready(&mut buf)
                            };
                            if drain_n > 0 {
                                batch_in_bytes += drain_n;
                                // The buffer grew to hold the rest of the
                                // command (CLIENT LIST qbuf). Recorded only
                                // after a read before: a big command whose
                                // first read was small and whose rest was
                                // drained here finished as qbuf=0 (the Redis
                                // unit/querybuf suite failed on it, depending
                                // on how the first segment arrived).
                                if buf.len() > 16384
                                    && let Some(c) =
                                        client_registry.borrow_mut().get_mut(&client_id)
                                {
                                    c.qbuf_cap = c.qbuf_cap.max(buf.len());
                                }
                                stats.tot_net_in.fetch_add(
                                    drain_n as u64,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                                crate::server_stats::add(
                                    crate::server_stats::Stat::NetInputBytes,
                                    drain_n as u64,
                                );
                                continue;
                            }
                            if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                                c.last_active = Instant::now();
                                c.qbuf_len = buf.len() - pending_big_arg_start(&buf).unwrap_or(0);
                                c.qbuf_cap = c.qbuf_cap.max(estimate_incomplete_qbuf_cap(&buf));
                                // Fresh for maxmemory-clients (CONFIG SET
                                // evicts from these readings at once).
                                c.stats.tot_mem.store(
                                    client_tot_mem(c, 0, router.port, &router.pubsub.borrow()),
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                            break;
                        }
                        Err(err) => {
                            if err.starts_with("Protocol error:") {
                                write_resp_err(&mut out_buf, &err);
                                stats.tot_net_out.fetch_add(
                                    out_buf.len() as u64,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                                crate::server_stats::add(
                                    crate::server_stats::Stat::NetOutputBytes,
                                    out_buf.len() as u64,
                                );
                                let write_chunk = std::mem::take(&mut out_buf);
                                let _ = transport.write_all(write_chunk).await.0;
                                return;
                            }
                            if in_multi {
                                for cmd in commands.drain(..) {
                                    let in_b = argvs.pop_front().map_or(0, |a| a.1);
                                    let ss_before = out_buf.len();
                                    let ss_slot = slot_stats_before(
                                        &router,
                                        &mut slot_acct,
                                        &cmd,
                                        in_multi,
                                        in_b,
                                    );
                                    let quit = execute_tx_step(
                                        cmd,
                                        &router,
                                        client_id,
                                        &client_registry,
                                        &mut out_buf,
                                        &mut asking,
                                        &mut authenticated,
                                        &mut auth_user,
                                        &mut in_multi,
                                        &mut tx_queue,
                                        &mut tx_has_error,
                                    )
                                    .await;
                                    slot_acct.after(ss_slot, (out_buf.len() - ss_before) as u64);
                                    if quit {
                                        should_quit = true;
                                        break;
                                    }
                                }
                                tx_has_error = true;
                            } else {
                                for cmd in commands.drain(..) {
                                    let (argv, _) = argvs.pop_front().unwrap_or((None, 0));
                                    if let Some(c) =
                                        client_registry.borrow_mut().get_mut(&client_id)
                                    {
                                        c.last_active = Instant::now();
                                        c.last_cmd = get_cmd_name(&cmd);
                                    }
                                    set_exec_argv(argv);
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
                            record_parse_error_stats(&err);
                            write_resp_err(&mut out_buf, &err);
                            if should_quit {
                                break;
                            }
                        }
                    }
                }

                // maxmemory-clients: a big batch (a large argv, or a long
                // pipeline) is checked before it runs, as Redis evicts on
                // reading input.
                if !qbuf_overflow
                    && batch_in_bytes.max(batch_bytes) > 32768
                    && maxmemory_clients_limit(router.base_port) > 0
                {
                    let my_mem = client_registry.borrow().get(&client_id).map_or(0, |c| {
                        CLIENT_BASE_MEM
                            + batch_in_bytes.max(batch_bytes).max(c.qbuf_cap)
                            + c.omem_now()
                            + c.stats.multi_mem.load(std::sync::atomic::Ordering::Relaxed)
                    });
                    if client_evicted_for_memory(router.base_port, client_id, my_mem) {
                        qbuf_overflow = true;
                    }
                }
                if qbuf_overflow {
                    // Like Redis: close without running or answering what is
                    // buffered; the loop exit recycles the scratch buffers.
                    commands.clear();
                    argvs.clear();
                    buf.clear();
                    break;
                }

                // Hold the batch while a shutdown quiesces the server (see
                // `shutdown::quiesce`); close once the shutdown goes ahead.
                let mut batch_guard = crate::shutdown::enter_batch(router.shard_id);
                while batch_guard.is_none() && !crate::shutdown::is_shutting_down() {
                    monoio::time::sleep(std::time::Duration::from_millis(10)).await;
                    batch_guard = crate::shutdown::enter_batch(router.shard_id);
                }
                if batch_guard.is_none() {
                    commands.clear();
                    argvs.clear();
                    break;
                }
                let batch_writes = aof_on
                    && commands.iter().any(|c| {
                        c.is_write_command()
                            || matches!(
                                c,
                                Command::Exec
                                    | Command::Eval { .. }
                                    | Command::Evalsha { .. }
                                    | Command::Fcall { .. }
                            )
                    });
                if !commands.is_empty() {
                    stats
                        .pipeline_len_sum
                        .fetch_add(commands.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    stats
                        .pipeline_len_cnt
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }

                // 2. Commands that hand the connection to a dedicated loop (pub/sub,
                // replica stream, DFLY flow) never reach execute_command, so AUTH and
                // ACL must be enforced here before switching modes.
                while has_special && let Some(idx) = commands.iter().position(is_mode_switch_cmd) {
                    for c in commands.drain(..idx) {
                        set_exec_argv(argvs.pop_front().and_then(|a| a.0));
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
                    if let Some(err) =
                        mode_switch_denied(&commands[0], router.port, authenticated, &auth_user)
                    {
                        commands.remove(0);
                        argvs.pop_front();
                        out_buf.extend_from_slice(&err);
                        continue;
                    }
                    if !out_buf.is_empty() {
                        aof_reply_barrier(
                            &router,
                            &mut aof_appends_seen,
                            batch_writes,
                            &aof_progress,
                        )
                        .await;
                        stats
                            .tot_net_out
                            .fetch_add(out_buf.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        crate::server_stats::add(
                            crate::server_stats::Stat::NetOutputBytes,
                            out_buf.len() as u64,
                        );
                        let write_chunk = std::mem::take(&mut out_buf);
                        let _ = transport.write_all(write_chunk).await.0;
                    }
                    let switch_cmd = commands.remove(0);
                    let switch_in = argvs.pop_front().map_or(0, |a| a.1);
                    if let Command::Ssubscribe(_) = switch_cmd
                        && crate::slot_stats::net_accounting()
                        && let Some(slot) = command_slot(&switch_cmd)
                    {
                        crate::slot_stats::record(slot, 0, switch_in, 0);
                    }
                    if matches!(
                        switch_cmd,
                        Command::Psync { .. }
                            | Command::Sync
                            | Command::DflyFlow { .. }
                            | Command::SyncSlots(_)
                    ) {
                        // The link lives as long as the replica: it isn't a
                        // command batch a shutdown waits for (every SHUTDOWN
                        // on a master with replicas sat out its whole drain
                        // timeout).
                        batch_guard.take();
                        stats
                            .is_replica
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                        if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                            c.last_cmd = get_cmd_name(&switch_cmd);
                        }
                    }
                    match switch_cmd {
                        Command::Psync { .. } | Command::Sync => {
                            match transport.into_tcp_stream() {
                                Ok(stream) => {
                                    run_master_replica_stream(
                                        stream,
                                        client_id,
                                        client_registry,
                                        router,
                                        switch_cmd,
                                    )
                                    .await;
                                }
                                Err(mut transport) => {
                                    let _ = transport
                                        .write_all(REPLICATION_NEEDS_PLAINTEXT.to_vec())
                                        .await;
                                }
                            }
                        }
                        Command::SyncSlots(args) => match transport.into_tcp_stream() {
                            Ok(stream) => {
                                crate::asm::run_migrate_source(stream, client_id, router, args)
                                    .await;
                            }
                            Err(mut transport) => {
                                let _ = transport
                                    .write_all(REPLICATION_NEEDS_PLAINTEXT.to_vec())
                                    .await;
                            }
                        },
                        Command::DflyFlow { shard_id, lsn, .. } => {
                            match transport.into_tcp_stream() {
                                Ok(stream) => {
                                    run_shard_replication_flow(
                                        stream, client_id, router, shard_id, lsn,
                                    )
                                    .await;
                                }
                                Err(mut transport) => {
                                    let _ = transport
                                        .write_all(REPLICATION_NEEDS_PLAINTEXT.to_vec())
                                        .await;
                                }
                            }
                        }
                        initial_sub => {
                            update_global_client_pubsub(router.port, client_id, true);
                            std::mem::forget(_cleanup);
                            run_pubsub_loop(
                                transport,
                                client_id,
                                client_registry,
                                router,
                                initial_sub,
                                commands,
                                buf,
                                auth_user,
                            )
                            .await;
                        }
                    }
                    return;
                }

                // 3. Execute parsed commands with transaction support and pipeline squashing
                if !commands.is_empty() {
                    let has_tx = has_special
                        && (in_multi
                            || commands.iter().any(|c| {
                                matches!(
                                    c,
                                    Command::Multi
                                        | Command::Exec
                                        | Command::Discard
                                        | Command::Watch(_)
                                        | Command::Unwatch
                                )
                            }));

                    // A squashed batch's writes count as running (see
                    // `writes_running`); with writes paused it takes the
                    // per-command path, which waits at the pause gate.
                    let mut batch_running = None;
                    let mut batch_paused = false;
                    if !has_tx && commands.len() > 1 {
                        match enter_write_batch(&commands) {
                            Some(guard) => batch_running = Some(guard),
                            None => batch_paused = true,
                        }
                    }
                    if has_tx {
                        for cmd in commands.drain(..) {
                            let in_b = argvs.pop_front().map_or(0, |a| a.1);
                            let ss_before = out_buf.len();
                            let ss_slot =
                                slot_stats_before(&router, &mut slot_acct, &cmd, in_multi, in_b);
                            let quit = execute_tx_step(
                                cmd,
                                &router,
                                client_id,
                                &client_registry,
                                &mut out_buf,
                                &mut asking,
                                &mut authenticated,
                                &mut auth_user,
                                &mut in_multi,
                                &mut tx_queue,
                                &mut tx_has_error,
                            )
                            .await;
                            slot_acct.after(ss_slot, (out_buf.len() - ss_before) as u64);
                            if quit {
                                should_quit = true;
                                break;
                            }
                            if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                while let Ok(inval) = track_rx.try_recv() {
                                    out_buf.extend_from_slice(&inval);
                                }
                            }
                        }
                    } else if batch_paused
                        || is_client_paused().is_some()
                        || HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed)
                        || has_monitor_clients()
                        || crate::slot_stats::accounting(router.cluster_enabled)
                        || crate::hotkeys::active()
                        || crate::block::has_blocked_waiters(router.port)
                        // Over maxmemory: the squashed path has no per-command
                        // OOM/eviction gate, so take the per-command path.
                        || (router.shard_memory_budget().is_some() && router.over_maxmemory())
                        || client_registry
                            .borrow()
                            .get(&client_id)
                            .map(|c| c.reply_mode != crate::resp::ClientReplyMode::On)
                            .unwrap_or(false)
                        || (has_special
                            && commands.iter().any(|c| {
                                matches!(
                                    c,
                                    Command::Client(_)
                                        | Command::Blpop { .. }
                                        | Command::Brpop { .. }
                                        | Command::Blmove { .. }
                                        | Command::Blmovem { .. }
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
                            }))
                    {
                        // Each command counts itself at the pause gate.
                        drop(batch_running.take());
                        for cmd in commands.drain(..) {
                            let (argv, in_b) = argvs.pop_front().unwrap_or((None, 0));
                            if (is_client_paused().is_some()
                                || matches!(
                                    cmd,
                                    Command::Blpop { .. }
                                        | Command::Brpop { .. }
                                        | Command::Blmove { .. }
                                        | Command::Blmovem { .. }
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
                                ))
                                && !out_buf.is_empty()
                            {
                                aof_reply_barrier(
                                    &router,
                                    &mut aof_appends_seen,
                                    batch_writes,
                                    &aof_progress,
                                )
                                .await;
                                stats.tot_net_out.fetch_add(
                                    out_buf.len() as u64,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                                crate::server_stats::add(
                                    crate::server_stats::Stat::NetOutputBytes,
                                    out_buf.len() as u64,
                                );
                                let write_chunk = std::mem::take(&mut out_buf);
                                let (res, returned_buf) = transport.write_all(write_chunk).await;
                                out_buf = returned_buf;
                                out_buf.clear();
                                if res.is_err() {
                                    should_quit = true;
                                    break;
                                }
                            }
                            if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                                c.last_active = Instant::now();
                                c.last_cmd = get_cmd_name(&cmd);
                            }
                            set_exec_argv(argv);
                            let ss_before = out_buf.len();
                            let ss_slot =
                                slot_stats_before(&router, &mut slot_acct, &cmd, in_multi, in_b);
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
                            slot_acct
                                .after(ss_slot, out_buf.len().saturating_sub(ss_before) as u64);
                            if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                while let Ok(inval) = track_rx.try_recv() {
                                    out_buf.extend_from_slice(&inval);
                                }
                            }
                            if quit {
                                should_quit = true;
                                break;
                            }
                        }
                    } else if commands.len() == 1 {
                        let single_cmd = commands.pop().unwrap();
                        let (argv, in_b) = argvs.pop_back().unwrap_or((None, 0));
                        set_exec_argv(argv);
                        let ss_before = out_buf.len();
                        let ss_slot =
                            slot_stats_before(&router, &mut slot_acct, &single_cmd, in_multi, in_b);
                        if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                            c.last_active = Instant::now();
                            c.last_cmd = get_cmd_name(&single_cmd);
                        }
                        let quit = execute_command(
                            single_cmd,
                            &router,
                            client_id,
                            &client_registry,
                            &mut out_buf,
                            &mut asking,
                            &mut authenticated,
                            &mut auth_user,
                        )
                        .await;
                        slot_acct.after(ss_slot, out_buf.len().saturating_sub(ss_before) as u64);
                        if quit {
                            should_quit = true;
                        }
                    } else {
                        let cmd_cnt = commands.len() as u64;
                        let mut fallbacks = 0u64;
                        // With a normal-client output limit set, run the batch
                        // in chunks of 1, 2, 4, ... commands and stop once the
                        // replies pass the hard limit: replies are only checked
                        // after the batch, so up to 1024 of them (e.g. large
                        // LRANGEs) were built first, several times the limit.
                        // Doubling keeps the overshoot near the limit and the
                        // squashing; with no limit (the default) it is one call.
                        let hard =
                            if HAS_CUSTOM_BUFFER_LIMIT.load(std::sync::atomic::Ordering::Relaxed) {
                                get_client_output_buffer_limit(ClientClass::Normal).hard_limit
                            } else {
                                0
                            };
                        let mut rest = if hard > 0 {
                            std::mem::take(&mut commands)
                        } else {
                            Vec::new()
                        };
                        let mut chunk = 1usize;
                        loop {
                            if hard > 0 {
                                commands = take_doubling_chunk(&mut rest, &mut chunk);
                            }
                            let quit = execute_commands_squashed(
                                &mut commands,
                                has_special,
                                &router,
                                &responders,
                                &mut remote_batches,
                                &mut items_pool,
                                &mut squashed_responses,
                                client_id,
                                &client_registry,
                                &mut out_buf,
                                &mut asking,
                                &mut authenticated,
                                &mut auth_user,
                                &mut fallbacks,
                            )
                            .await;
                            if quit {
                                should_quit = true;
                            }
                            if quit || rest.is_empty() || out_buf.len() as u64 >= hard {
                                break;
                            }
                        }
                        // Applied and replicated: no longer running.
                        drop(batch_running.take());
                        // Those run through `execute_command` counted themselves.
                        stats.tot_cmds.fetch_add(
                            cmd_cnt.saturating_sub(fallbacks),
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    commands.clear();
                }
                if buf.is_empty() {
                    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id)
                        && c.qbuf_len > 0
                    {
                        c.qbuf_len = 0;
                    }
                    let _ = buf.try_reclaim(READ_BUFFER_SIZE);
                }

                // 4. Batch flush all accumulated responses: direct non-blocking send with io_uring fallback
                if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                    while let Ok(inval) = track_rx.try_recv() {
                        out_buf.extend_from_slice(&inval);
                    }
                }
                let has_custom_limit =
                    HAS_CUSTOM_BUFFER_LIMIT.load(std::sync::atomic::Ordering::Relaxed);
                if has_custom_limit {
                    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                        c.omem = out_buf.len();
                    }

                    // Check normal client output buffer limit
                    let norm_limits = get_client_output_buffer_limit(ClientClass::Normal);
                    if norm_limits.hard_limit > 0 && out_buf.len() as u64 >= norm_limits.hard_limit
                    {
                        client_output_limit_reached(client_id, "N", out_buf.len());
                        break;
                    }
                    if norm_limits.soft_limit > 0 && out_buf.len() as u64 >= norm_limits.soft_limit
                    {
                        let now = Instant::now();
                        if let Some(st) = soft_limit_start {
                            if now.duration_since(st).as_secs() >= norm_limits.soft_seconds {
                                client_output_limit_reached(client_id, "N", out_buf.len());
                                break;
                            }
                        } else {
                            soft_limit_start = Some(now);
                        }
                    } else {
                        soft_limit_start = None;
                    }
                }

                if !out_buf.is_empty() {
                    aof_reply_barrier(&router, &mut aof_appends_seen, batch_writes, &aof_progress)
                        .await;
                    let len = out_buf.len();
                    stats
                        .tot_net_out
                        .fetch_add(len as u64, std::sync::atomic::Ordering::Relaxed);
                    crate::server_stats::add(crate::server_stats::Stat::NetOutputBytes, len as u64);
                    let flushed = transport
                        .flush(&mut out_buf, |omem| {
                            if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                                c.omem = omem;
                            }
                        })
                        .await;
                    if flushed.is_err() {
                        break;
                    }
                }

                if has_custom_limit
                    && let Some(c) = client_registry.borrow_mut().get_mut(&client_id)
                {
                    c.omem = out_buf.len();
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
    recycle_conn_scratch(ConnScratch {
        buf,
        out_buf,
        responders,
        remote_batches,
        items_pool,
        squashed_responses,
        commands,
    });
}

struct ConnScratch {
    buf: BytesMut,
    out_buf: Vec<u8>,
    responders: Vec<std::sync::Arc<crate::mailbox::BatchResponder>>,
    remote_batches: Vec<Vec<(usize, u64, Command)>>,
    items_pool: Vec<Vec<(usize, u64, Command)>>,
    squashed_responses: Vec<CompactResp>,
    commands: Vec<Command>,
}

thread_local! {
    static CONN_SCRATCH_POOL: RefCell<Vec<PooledScratch>> = const { RefCell::new(Vec::new()) };
}

thread_local! {
    /// Bytes this thread's connection scratch pool holds for connections
    /// to come: a cache, left out of the shard's used_memory share (see
    /// `Router::publish_memory_state`) as Redis has nothing like it.
    /// Counted, a few clients connecting and going moved used_memory by
    /// megabytes, and a server near maxmemory evicted.
    static POOLED_SCRATCH_BYTES: std::cell::Cell<i64> = const { std::cell::Cell::new(0) };
}

/// [`POOLED_SCRATCH_BYTES`] of this thread.
pub fn pooled_scratch_bytes() -> i64 {
    POOLED_SCRATCH_BYTES.get()
}

/// A pooled [`ConnScratch`], counted in this thread's
/// [`POOLED_SCRATCH_BYTES`] while it is in the pool.
struct PooledScratch {
    scratch: Option<ConnScratch>,
    bytes: i64,
}

impl PooledScratch {
    fn new(scratch: ConnScratch) -> Self {
        let bytes = (scratch.buf.capacity() + scratch.out_buf.capacity()) as i64;
        POOLED_SCRATCH_BYTES.set(POOLED_SCRATCH_BYTES.get() + bytes);
        PooledScratch {
            scratch: Some(scratch),
            bytes,
        }
    }

    fn take(mut self) -> ConnScratch {
        self.scratch.take().expect("present until taken")
    }
}

impl std::ops::Deref for PooledScratch {
    type Target = ConnScratch;
    fn deref(&self) -> &ConnScratch {
        self.scratch.as_ref().expect("present until taken")
    }
}

impl Drop for PooledScratch {
    fn drop(&mut self) {
        POOLED_SCRATCH_BYTES.set(POOLED_SCRATCH_BYTES.get() - self.bytes);
    }
}

fn take_conn_scratch(num_shards: usize) -> ConnScratch {
    if let Some(mut s) = CONN_SCRATCH_POOL.with(|pool| {
        let mut p = pool.borrow_mut();
        p.iter()
            .rposition(|item| item.responders.len() == num_shards)
            .map(|idx| p.swap_remove(idx).take())
    }) {
        s.buf.clear();
        let _ = s.buf.try_reclaim(131072);
        s.out_buf.clear();
        s.squashed_responses.clear();
        s.commands.clear();
        for b in &mut s.remote_batches {
            b.clear();
        }
        return s;
    }
    ConnScratch {
        buf: BytesMut::with_capacity(131072),
        out_buf: Vec::with_capacity(65536),
        responders: (0..num_shards)
            .map(|_| std::sync::Arc::new(crate::mailbox::BatchResponder::new()))
            .collect(),
        remote_batches: (0..num_shards).map(|_| Vec::new()).collect(),
        items_pool: (0..num_shards).map(|_| Vec::new()).collect(),
        squashed_responses: Vec::with_capacity(64),
        commands: Vec::with_capacity(64),
    }
}

fn recycle_conn_scratch(mut s: ConnScratch) {
    // A remote shard holds an Arc clone of a BatchResponder for as long as its batch
    // is in flight, and writes the payload cross-thread through an UnsafeCell in
    // BatchResponder::finish(). If this connection tore down (client hang-up, write
    // error) while a batch was still outstanding, handing that responder to the next
    // connection would let the late finish() deliver another client's results, or
    // leave a stale ready=true that desyncs the new harvest loop. Only pool scratch
    // whose responders are provably unreferenced and idle; otherwise drop it and let
    // the straggling shard release the Arc normally.
    let reusable = s
        .responders
        .iter()
        .all(|r| std::sync::Arc::strong_count(r) == 1 && r.is_idle());
    if !reusable {
        return;
    }
    s.commands.clear();
    s.squashed_responses.clear();
    s.out_buf.clear();
    s.buf.clear();
    let _ = s.buf.try_reclaim(131072);
    for b in &mut s.remote_batches {
        b.clear();
    }
    for b in &mut s.items_pool {
        b.clear();
    }
    trim_conn_scratch(&mut s);
    CONN_SCRATCH_POOL.with(|pool| {
        let mut p = pool.borrow_mut();
        if p.len() < 32 {
            p.push(PooledScratch::new(s));
        }
    });
}

/// Shrinks (already cleared) scratch buffers that grew past what is worth
/// keeping for the next connection back to their initial sizes.
fn trim_conn_scratch(s: &mut ConnScratch) {
    fn trim<T>(v: &mut Vec<T>, keep: usize, initial: usize) {
        if v.capacity() > keep {
            v.shrink_to(initial);
        }
    }
    if s.buf.capacity() > SCRATCH_KEEP_BYTES {
        s.buf = BytesMut::with_capacity(131072);
    }
    trim(&mut s.out_buf, SCRATCH_KEEP_BYTES, 65536);
    trim(&mut s.commands, SCRATCH_KEEP_ITEMS, 64);
    trim(&mut s.squashed_responses, SCRATCH_KEEP_ITEMS, 64);
    for b in &mut s.remote_batches {
        trim(b, SCRATCH_KEEP_ITEMS, 64);
    }
    for b in &mut s.items_pool {
        trim(b, SCRATCH_KEEP_ITEMS, 64);
    }
}

pub fn reset_client_pubsub(router: &Router, client_id: u64) {
    router
        .pubsub
        .borrow_mut()
        .remove_client_with_presence(client_id, Some((router.shard_id, &router.presence_table)));
    for (sid, sender) in router.senders.iter().enumerate() {
        if sid != router.shard_id {
            let _ = sender.send(crate::shard::ShardMessage::RemoveClientPubSub { client_id });
        }
    }
}

async fn run_pubsub_loop<T: crate::transport::ClientTransport>(
    transport: T,
    client_id: u64,
    client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
    router: Rc<Router>,
    initial_sub: Command,
    pending_cmds: Vec<Command>,
    mut buf: BytesMut,
    auth_user: String,
) {
    struct PubsubCleanup {
        client_id: u64,
        client_registry: Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
        router: Rc<Router>,
    }
    impl Drop for PubsubCleanup {
        fn drop(&mut self) {
            let already_killed = self
                .client_registry
                .borrow()
                .get(&self.client_id)
                .map(|c| {
                    c.stats
                        .killed
                        .swap(true, std::sync::atomic::Ordering::Relaxed)
                })
                .unwrap_or(false);
            if !already_killed {
                ACTIVE_CLIENTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
            flush_local_cmd_stats();
            self.client_registry.borrow_mut().remove(&self.client_id);
            unregister_global_client(self.router.port, self.client_id);
            unregister_monitor_client(self.router.port, self.client_id);
            unregister_client_tracking(self.router.port, self.client_id);
            self.router.pubsub.borrow_mut().remove_client_with_presence(
                self.client_id,
                Some((self.router.shard_id, &self.router.presence_table)),
            );
            for (sid, sender) in self.router.senders.iter().enumerate() {
                if sid != self.router.shard_id {
                    let _ = sender.send(crate::shard::ShardMessage::RemoveClientPubSub {
                        client_id: self.client_id,
                    });
                }
            }
        }
    }
    use crate::transport::{TransportRead, TransportWrite};
    // Unbounded: `SubscriberTx` bounds it by bytes, per the pubsub output
    // buffer limit.
    let (raw_tx, write_rx) = flume::unbounded::<Bytes>();
    let write_tx = crate::pubsub::SubscriberTx::new(raw_tx, router.port, client_id);
    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
        c.out_queue = Some(write_tx.gauge());
    }
    let (mut reader, mut writer) = transport.into_split(write_tx.raw());
    // Declared after the split so it drops (and unregisters the client from
    // GLOBAL_CLIENTS) before `reader` closes the socket: other threads use the
    // registered fd for CLIENT KILL / MONITOR, and must never see a closed,
    // possibly reused, fd number.
    let _ps_cleanup = PubsubCleanup {
        client_id,
        client_registry: client_registry.clone(),
        router: router.clone(),
    };

    // When this loop ends (QUIT, a self-inflicted ACL kill, EOF), tells the
    // writer to send what is queued and stop: it holds a sender itself, so
    // its channel never closes, and it would keep the socket open forever.
    // Dropped before `_ps_cleanup`, so the replies already queued go out.
    struct StopWriter(crate::pubsub::SubscriberTx);
    impl Drop for StopWriter {
        fn drop(&mut self) {
            let _ = self.0.raw().send(Bytes::new());
        }
    }
    let _stop_writer = StopWriter(write_tx.clone());

    let reg_writer = client_registry.clone();
    let writer_tx = write_tx.clone();
    monoio::spawn(async move {
        // Limits are enforced where messages are queued
        // (`SubscriberTx::deliver`); this only writes and keeps the count.
        // Everything already queued (up to WRITE_BATCH bytes) goes out in one
        // write: one write per message could not keep up with several shards
        // publishing, so a subscriber reading as fast as it could still fell
        // behind and hit the output limit.
        const WRITE_BATCH: usize = 256 * 1024;
        let mut batch = bytes::BytesMut::new();
        // An empty message is `StopWriter`'s: nothing else queues one.
        let mut stop = false;
        while let Ok(first) = write_rx.recv_async().await {
            if first.is_empty() {
                break;
            }
            let data = if write_rx.is_empty() {
                first
            } else {
                batch.extend_from_slice(&first);
                while batch.len() < WRITE_BATCH
                    && let Ok(more) = write_rx.try_recv()
                {
                    if more.is_empty() {
                        stop = true;
                        break;
                    }
                    batch.extend_from_slice(&more);
                }
                batch.split().freeze()
            };
            let data_len = data.len();
            if let Some(c) = reg_writer.borrow_mut().get_mut(&client_id) {
                c.omem = writer_tx.queued();
            }
            crate::server_stats::add(crate::server_stats::Stat::NetOutputBytes, data_len as u64);
            if writer.write_bytes(data).await.is_err() {
                break;
            }
            writer_tx.written(data_len);
            if let Some(c) = reg_writer.borrow_mut().get_mut(&client_id) {
                c.omem = writer_tx.queued();
            }
            if stop {
                break;
            }
        }
    });

    let mut in_multi = false;
    let mut tx_queue: Vec<Command> = Vec::new();
    let mut asking = false;
    // Only reachable after mode_switch_denied() passed, i.e. already authenticated;
    // keep the real user so ACLs keep applying inside pub/sub mode.
    let mut authenticated = true;
    let mut auth_user = auth_user;

    /// [`handle_pubsub_cmd_inner`] under CLIENT REPLY OFF/SKIP, as in
    /// Redis: subscription confirmations are pushes and always go out, every
    /// other reply (PING's PONG, errors) is dropped. A subscribed client
    /// ignored its reply mode and still got PONG.
    #[allow(clippy::too_many_arguments)]
    async fn handle_pubsub_cmd(
        cmd: Command,
        router: &Router,
        client_id: u64,
        client_registry: &Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
        write_tx: &crate::pubsub::SubscriberTx,
        in_multi: &mut bool,
        tx_queue: &mut Vec<Command>,
        out: &mut Vec<u8>,
        asking: &mut bool,
        authenticated: &mut bool,
        auth_user: &mut String,
    ) -> bool {
        let mode = client_registry
            .borrow()
            .get(&client_id)
            .map_or(crate::resp::ClientReplyMode::On, |c| c.reply_mode);
        let sets_mode = matches!(cmd, Command::Client(ClientSubcommand::Reply(_)));
        let pushes = matches!(
            cmd,
            Command::Subscribe(_)
                | Command::Psubscribe(_)
                | Command::Ssubscribe(_)
                | Command::Unsubscribe(_)
                | Command::Punsubscribe(_)
                | Command::Sunsubscribe(_)
        );
        let start = out.len();
        let close = handle_pubsub_cmd_inner(
            cmd,
            router,
            client_id,
            client_registry,
            write_tx,
            in_multi,
            tx_queue,
            out,
            asking,
            authenticated,
            auth_user,
        )
        .await;
        if mode != crate::resp::ClientReplyMode::On && !sets_mode {
            if !pushes {
                out.truncate(start);
            }
            if mode == crate::resp::ClientReplyMode::Skip
                && let Some(c) = client_registry.borrow_mut().get_mut(&client_id)
            {
                c.reply_mode = crate::resp::ClientReplyMode::On;
            }
        }
        close
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_pubsub_cmd_inner(
        cmd: Command,
        router: &Router,
        client_id: u64,
        client_registry: &Rc<RefCell<hashbrown::HashMap<u64, ClientInfo>>>,
        write_tx: &crate::pubsub::SubscriberTx,
        in_multi: &mut bool,
        tx_queue: &mut Vec<Command>,
        out: &mut Vec<u8>,
        asking: &mut bool,
        authenticated: &mut bool,
        auth_user: &mut String,
    ) -> bool {
        let is_resp3 = client_registry
            .borrow()
            .get(&client_id)
            .map(|c| c.is_resp3)
            .unwrap_or(false);

        if !*in_multi
            && matches!(
                cmd,
                Command::Subscribe(_) | Command::Psubscribe(_) | Command::Ssubscribe(_)
            )
            && subscribe_over_maxmemory(
                router,
                client_id,
                match &cmd {
                    Command::Subscribe(names)
                    | Command::Psubscribe(names)
                    | Command::Ssubscribe(names) => names.iter().map(|n| n.len()).sum(),
                    _ => 0,
                },
            )
            .await
        {
            let c_name = stat_cmd_name(&cmd);
            record_rejected_stat(c_name);
            record_error_stat("OOM", Some(c_name));
            out.extend_from_slice(b"-OOM command not allowed when used memory > 'maxmemory'.\r\n");
            return false;
        }

        if *in_multi {
            match cmd {
                Command::Multi => {
                    out.extend_from_slice(b"-ERR MULTI calls can not be nested\r\n");
                    return false;
                }
                Command::Discard => {
                    *in_multi = false;
                    tx_queue.clear();
                    out.extend_from_slice(b"+OK\r\n");
                    return false;
                }
                Command::Reset => {
                    *in_multi = false;
                    tx_queue.clear();
                    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                        c.name = None;
                        c.is_resp3 = client_default_resp3();
                        c.db = 0;
                        c.is_monitor = false;
                    }
                    CURRENT_DB.set(0);
                    router.local_db.borrow_mut().select_db(0);
                    reset_client_pubsub(router, client_id);

                    let acl = crate::acl::get_acl_for_port(router.port);
                    let default_requires_auth = acl.read().is_auth_required_for_default();
                    *authenticated = !default_requires_auth;
                    *auth_user = "default".to_string();

                    CLIENT_NO_TOUCH.set(false);
                    out.extend_from_slice(b"+RESET\r\n");
                    return false;
                }
                Command::Exec => {
                    *in_multi = false;
                    let queue = std::mem::take(tx_queue);
                    let mut exec_results = Vec::new();
                    for qcmd in queue {
                        let mut item_out = Vec::new();
                        if let Some(err) =
                            mode_switch_denied(&qcmd, router.port, *authenticated, auth_user)
                        {
                            exec_results.push(err);
                            continue;
                        }
                        match qcmd {
                            Command::Subscribe(channels) => {
                                let mut hub = router.pubsub.borrow_mut();
                                let presence_info =
                                    Some((router.shard_id, &*router.presence_table));
                                for ch in channels {
                                    let count = hub.subscribe_with_presence(
                                        client_id,
                                        ch.clone(),
                                        write_tx.clone(),
                                        is_resp3,
                                        presence_info,
                                    );
                                    let mut sub_resp = Vec::new();
                                    sub_resp.extend_from_slice(b"*3\r\n$9\r\nsubscribe\r\n$");
                                    sub_resp.extend_from_slice(ch.len().to_string().as_bytes());
                                    sub_resp.extend_from_slice(b"\r\n");
                                    sub_resp.extend_from_slice(&ch);
                                    sub_resp.extend_from_slice(b"\r\n:");
                                    sub_resp.extend_from_slice(count.to_string().as_bytes());
                                    sub_resp.extend_from_slice(b"\r\n");
                                    exec_results.push(sub_resp);
                                }
                            }
                            Command::Unsubscribe(channels) => {
                                let mut hub = router.pubsub.borrow_mut();
                                let presence_info =
                                    Some((router.shard_id, &*router.presence_table));
                                if channels.is_empty() {
                                    let unsubs =
                                        hub.unsubscribe_all_with_presence(client_id, presence_info);
                                    if unsubs.is_empty() {
                                        let total = hub.total_subscriptions(client_id);
                                        exec_results.push(
                                            format!(
                                                "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:{}\r\n",
                                                total
                                            )
                                            .into_bytes(),
                                        );
                                    } else {
                                        for (ch, rem) in unsubs {
                                            exec_results.push(unsubscribe_reply(
                                                "unsubscribe",
                                                &ch,
                                                rem,
                                            ));
                                        }
                                    }
                                } else {
                                    for ch in channels {
                                        let rem = hub.unsubscribe_with_presence(
                                            client_id,
                                            &ch,
                                            presence_info,
                                        );
                                        let mut un_resp = Vec::new();
                                        un_resp.extend_from_slice(b"*3\r\n$11\r\nunsubscribe\r\n$");
                                        un_resp.extend_from_slice(ch.len().to_string().as_bytes());
                                        un_resp.extend_from_slice(b"\r\n");
                                        un_resp.extend_from_slice(&ch);
                                        un_resp.extend_from_slice(b"\r\n:");
                                        un_resp.extend_from_slice(rem.to_string().as_bytes());
                                        un_resp.extend_from_slice(b"\r\n");
                                        exec_results.push(un_resp);
                                    }
                                }
                            }
                            Command::Psubscribe(patterns) => {
                                let mut hub = router.pubsub.borrow_mut();
                                let presence_info =
                                    Some((router.shard_id, &*router.presence_table));
                                for pat in patterns {
                                    let count = hub.psubscribe_with_presence(
                                        client_id,
                                        pat.clone(),
                                        write_tx.clone(),
                                        is_resp3,
                                        presence_info,
                                    );
                                    let mut psub_resp = Vec::new();
                                    psub_resp.extend_from_slice(b"*3\r\n$10\r\npsubscribe\r\n$");
                                    psub_resp.extend_from_slice(pat.len().to_string().as_bytes());
                                    psub_resp.extend_from_slice(b"\r\n");
                                    psub_resp.extend_from_slice(&pat);
                                    psub_resp.extend_from_slice(b"\r\n:");
                                    psub_resp.extend_from_slice(count.to_string().as_bytes());
                                    psub_resp.extend_from_slice(b"\r\n");
                                    exec_results.push(psub_resp);
                                }
                            }
                            Command::Punsubscribe(patterns) => {
                                let mut hub = router.pubsub.borrow_mut();
                                let presence_info =
                                    Some((router.shard_id, &*router.presence_table));
                                if patterns.is_empty() {
                                    let unsubs = hub
                                        .punsubscribe_all_with_presence(client_id, presence_info);
                                    if unsubs.is_empty() {
                                        let total = hub.total_subscriptions(client_id);
                                        exec_results.push(
                                            format!(
                                                "*3\r\n$12\r\npunsubscribe\r\n$-1\r\n:{}\r\n",
                                                total
                                            )
                                            .into_bytes(),
                                        );
                                    } else {
                                        for (pat, rem) in unsubs {
                                            exec_results.push(unsubscribe_reply(
                                                "punsubscribe",
                                                &pat,
                                                rem,
                                            ));
                                        }
                                    }
                                } else {
                                    for pat in patterns {
                                        let rem = hub.punsubscribe_with_presence(
                                            client_id,
                                            &pat,
                                            presence_info,
                                        );
                                        let mut un_resp = Vec::new();
                                        un_resp
                                            .extend_from_slice(b"*3\r\n$12\r\npunsubscribe\r\n$");
                                        un_resp.extend_from_slice(pat.len().to_string().as_bytes());
                                        un_resp.extend_from_slice(b"\r\n");
                                        un_resp.extend_from_slice(&pat);
                                        un_resp.extend_from_slice(b"\r\n:");
                                        un_resp.extend_from_slice(rem.to_string().as_bytes());
                                        un_resp.extend_from_slice(b"\r\n");
                                        exec_results.push(un_resp);
                                    }
                                }
                            }
                            _ => {
                                execute_command(
                                    qcmd,
                                    router,
                                    client_id,
                                    client_registry,
                                    &mut item_out,
                                    asking,
                                    authenticated,
                                    auth_user,
                                )
                                .await;
                                exec_results.push(item_out);
                            }
                        }
                    }
                    out.extend_from_slice(format!("*{}\r\n", exec_results.len()).as_bytes());
                    for res in exec_results {
                        out.extend_from_slice(&res);
                    }
                    return false;
                }
                _ => {
                    tx_queue.push(cmd);
                    out.extend_from_slice(b"+QUEUED\r\n");
                    return false;
                }
            }
        }

        if matches!(cmd, Command::Multi) {
            *in_multi = true;
            tx_queue.clear();
            out.extend_from_slice(b"+OK\r\n");
            return false;
        }

        if let Some(err) = mode_switch_denied(&cmd, router.port, *authenticated, auth_user) {
            out.extend_from_slice(&err);
            return false;
        }

        let cmd_name = match &cmd {
            Command::Subscribe(_) => "SUBSCRIBE",
            Command::Unsubscribe(_) => "UNSUBSCRIBE",
            Command::Psubscribe(_) => "PSUBSCRIBE",
            Command::Punsubscribe(_) => "PUNSUBSCRIBE",
            Command::Ssubscribe(_) => "SSUBSCRIBE",
            Command::Sunsubscribe(_) => "SUNSUBSCRIBE",
            Command::Ping(_) => "PING",
            Command::Quit => "QUIT",
            _ => "OTHER",
        };
        if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
            c.last_active = Instant::now();
            c.last_cmd = cmd_name;
        }
        let prefix = if is_resp3 { b">3\r\n" } else { b"*3\r\n" };
        match cmd {
            Command::Subscribe(channels) => {
                let mut hub = router.pubsub.borrow_mut();
                let presence_info = Some((router.shard_id, &*router.presence_table));
                for ch in channels {
                    let count = hub.subscribe_with_presence(
                        client_id,
                        ch.clone(),
                        write_tx.clone(),
                        is_resp3,
                        presence_info,
                    );
                    out.extend_from_slice(prefix);
                    out.extend_from_slice(b"$9\r\nsubscribe\r\n$");
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
                let presence_info = Some((router.shard_id, &*router.presence_table));
                if channels.is_empty() {
                    let unsubs = hub.unsubscribe_all_with_presence(client_id, presence_info);
                    if unsubs.is_empty() {
                        let total = hub.total_subscriptions(client_id);
                        out.extend_from_slice(prefix);
                        out.extend_from_slice(
                            format!("$11\r\nunsubscribe\r\n$-1\r\n:{}\r\n", total).as_bytes(),
                        );
                    } else {
                        for (ch, remaining) in unsubs {
                            out.extend_from_slice(prefix);
                            out.extend_from_slice(b"$11\r\nunsubscribe\r\n$");
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
                        let remaining =
                            hub.unsubscribe_with_presence(client_id, &ch, presence_info);
                        out.extend_from_slice(prefix);
                        out.extend_from_slice(b"$11\r\nunsubscribe\r\n$");
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
                let presence_info = Some((router.shard_id, &*router.presence_table));
                for pat in patterns {
                    let count = hub.psubscribe_with_presence(
                        client_id,
                        pat.clone(),
                        write_tx.clone(),
                        is_resp3,
                        presence_info,
                    );
                    out.extend_from_slice(prefix);
                    out.extend_from_slice(b"$10\r\npsubscribe\r\n$");
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
                let presence_info = Some((router.shard_id, &*router.presence_table));
                if patterns.is_empty() {
                    let unsubs = hub.punsubscribe_all_with_presence(client_id, presence_info);
                    if unsubs.is_empty() {
                        let total = hub.total_subscriptions(client_id);
                        out.extend_from_slice(prefix);
                        out.extend_from_slice(
                            format!("$12\r\npunsubscribe\r\n$-1\r\n:{}\r\n", total).as_bytes(),
                        );
                    } else {
                        for (pat, remaining) in unsubs {
                            out.extend_from_slice(prefix);
                            out.extend_from_slice(b"$12\r\npunsubscribe\r\n$");
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
                        let remaining =
                            hub.punsubscribe_with_presence(client_id, &pat, presence_info);
                        out.extend_from_slice(prefix);
                        out.extend_from_slice(b"$12\r\npunsubscribe\r\n$");
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
            Command::Ssubscribe(channels) => {
                for ch in channels {
                    let count = router
                        .ssubscribe(client_id, ch.clone(), write_tx.clone(), is_resp3)
                        .await;
                    let reply_start = out.len();
                    out.extend_from_slice(prefix);
                    out.extend_from_slice(b"$10\r\nssubscribe\r\n$");
                    out.extend_from_slice(ch.len().to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    out.extend_from_slice(&ch);
                    out.extend_from_slice(b"\r\n:");
                    out.extend_from_slice(count.to_string().as_bytes());
                    out.extend_from_slice(b"\r\n");
                    if crate::slot_stats::net_accounting() {
                        let len = (out.len() - reply_start) as u64;
                        crate::slot_stats::record(crate::router::key_slot(&ch), 0, 0, len);
                    }
                }
                false
            }
            Command::Sunsubscribe(channels) => {
                if channels.is_empty() {
                    let unsubs = router.sunsubscribe_all(client_id).await;
                    if unsubs.is_empty() {
                        let total = router.pubsub.borrow().total_subscriptions(client_id);
                        out.extend_from_slice(prefix);
                        out.extend_from_slice(
                            format!("$12\r\nsunsubscribe\r\n$-1\r\n:{}\r\n", total).as_bytes(),
                        );
                    } else {
                        for (ch, remaining) in unsubs {
                            out.extend_from_slice(prefix);
                            out.extend_from_slice(b"$12\r\nsunsubscribe\r\n$");
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
                        let remaining = router.sunsubscribe(client_id, ch.clone()).await;
                        out.extend_from_slice(prefix);
                        out.extend_from_slice(b"$12\r\nsunsubscribe\r\n$");
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
            Command::Ping(msg) => {
                let total_subs = router.pubsub.borrow().total_subscriptions(client_id);
                if is_resp3 || total_subs == 0 {
                    match msg {
                        Some(m) => {
                            write_resp_bulk(out, &m);
                        }
                        None => {
                            out.extend_from_slice(b"+PONG\r\n");
                        }
                    }
                } else {
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
                }
                false
            }
            Command::Client(ClientSubcommand::Reply(mode)) => {
                if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                    c.reply_mode = mode;
                }
                if mode == crate::resp::ClientReplyMode::On {
                    out.extend_from_slice(b"+OK\r\n");
                }
                false
            }
            Command::Quit => {
                out.extend_from_slice(b"+OK\r\n");
                true
            }
            Command::Reset => {
                *in_multi = false;
                tx_queue.clear();
                if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
                    c.name = None;
                    c.is_resp3 = client_default_resp3();
                    c.db = 0;
                    c.is_monitor = false;
                }
                CURRENT_DB.set(0);
                router.local_db.borrow_mut().select_db(0);
                reset_client_pubsub(router, client_id);

                let acl = crate::acl::get_acl_for_port(router.port);
                let default_requires_auth = acl.read().is_auth_required_for_default();
                *authenticated = !default_requires_auth;
                *auth_user = "default".to_string();

                CLIENT_NO_TOUCH.set(false);
                out.extend_from_slice(b"+RESET\r\n");
                false
            }
            other => {
                let total_subs = router.pubsub.borrow().total_subscriptions(client_id);
                if is_resp3 || total_subs == 0 {
                    execute_command(
                        other,
                        router,
                        client_id,
                        client_registry,
                        out,
                        asking,
                        authenticated,
                        auth_user,
                    )
                    .await
                } else {
                    let name = get_cmd_name(&other);
                    out.extend_from_slice(
                        format!(
                            "-ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n",
                            err_arg(name)
                        )
                        .as_bytes(),
                    );
                    false
                }
            }
        }
    }

    let mut out = Vec::new();
    crate::pubsub::CURRENT_COMMAND_CLIENT_ID.set(Some(client_id));
    let q = handle_pubsub_cmd(
        initial_sub,
        &router,
        client_id,
        &client_registry,
        &write_tx,
        &mut in_multi,
        &mut tx_queue,
        &mut out,
        &mut asking,
        &mut authenticated,
        &mut auth_user,
    )
    .await;
    crate::pubsub::CURRENT_COMMAND_CLIENT_ID.set(None);
    if !out.is_empty() {
        let _ = write_tx.send(Bytes::from(out));
    }
    let deferred =
        crate::pubsub::DEFERRED_SELF_PUBSUB.with(|d| std::mem::take(&mut *d.borrow_mut()));
    for msg in deferred {
        let _ = write_tx.send(msg);
    }
    if q {
        return;
    }

    for cmd in pending_cmds {
        let mut out = Vec::new();
        crate::pubsub::CURRENT_COMMAND_CLIENT_ID.set(Some(client_id));
        let q = handle_pubsub_cmd(
            cmd,
            &router,
            client_id,
            &client_registry,
            &write_tx,
            &mut in_multi,
            &mut tx_queue,
            &mut out,
            &mut asking,
            &mut authenticated,
            &mut auth_user,
        )
        .await;
        crate::pubsub::CURRENT_COMMAND_CLIENT_ID.set(None);
        if !out.is_empty() {
            let _ = write_tx.send(Bytes::from(out));
        }
        let deferred =
            crate::pubsub::DEFERRED_SELF_PUBSUB.with(|d| std::mem::take(&mut *d.borrow_mut()));
        for msg in deferred {
            let _ = write_tx.send(msg);
        }
        if q {
            return;
        }
    }

    while !buf.is_empty() {
        match parse_command(&mut buf) {
            Ok(Some(cmd)) => {
                let mut out = Vec::new();
                crate::pubsub::CURRENT_COMMAND_CLIENT_ID.set(Some(client_id));
                let q = handle_pubsub_cmd(
                    cmd,
                    &router,
                    client_id,
                    &client_registry,
                    &write_tx,
                    &mut in_multi,
                    &mut tx_queue,
                    &mut out,
                    &mut asking,
                    &mut authenticated,
                    &mut auth_user,
                )
                .await;
                crate::pubsub::CURRENT_COMMAND_CLIENT_ID.set(None);
                if !out.is_empty() {
                    let _ = write_tx.send(Bytes::from(out));
                }
                let deferred = crate::pubsub::DEFERRED_SELF_PUBSUB
                    .with(|d| std::mem::take(&mut *d.borrow_mut()));
                for msg in deferred {
                    let _ = write_tx.send(msg);
                }
                if q {
                    return;
                }
            }
            Ok(None) => break,
            Err(e) => {
                let mut out = Vec::new();
                write_resp_err(&mut out, &e);
                let _ = write_tx.send(Bytes::from(out));
                if e.starts_with("Protocol error:") {
                    return;
                }
            }
        }
    }

    loop {
        release_big_query_buf(&mut buf);
        match reader.read_append(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                crate::server_stats::add(crate::server_stats::Stat::NetInputBytes, n as u64);
                let qbuf_limit = client_query_buffer_limit();
                while !buf.is_empty() {
                    let len_before = buf.len();
                    match parse_command(&mut buf) {
                        Ok(Some(cmd)) => {
                            if parsed_query_over_limit(len_before, buf.len(), qbuf_limit) {
                                client_query_limit_reached(client_id, len_before - buf.len());
                                return;
                            }
                            let mut out = Vec::new();
                            crate::pubsub::CURRENT_COMMAND_CLIENT_ID.set(Some(client_id));
                            let q = handle_pubsub_cmd(
                                cmd,
                                &router,
                                client_id,
                                &client_registry,
                                &write_tx,
                                &mut in_multi,
                                &mut tx_queue,
                                &mut out,
                                &mut asking,
                                &mut authenticated,
                                &mut auth_user,
                            )
                            .await;
                            crate::pubsub::CURRENT_COMMAND_CLIENT_ID.set(None);
                            if !out.is_empty() {
                                let _ = write_tx.send(Bytes::from(out));
                            }
                            let deferred = crate::pubsub::DEFERRED_SELF_PUBSUB
                                .with(|d| std::mem::take(&mut *d.borrow_mut()));
                            for msg in deferred {
                                let _ = write_tx.send(msg);
                            }
                            if q {
                                return;
                            }
                        }
                        Ok(None) => {
                            if partial_query_over_limit(buf.len(), qbuf_limit) {
                                client_query_limit_reached(client_id, buf.len());
                                return;
                            }
                            break;
                        }
                        Err(e) => {
                            let mut out = Vec::new();
                            write_resp_err(&mut out, &e);
                            let _ = write_tx.send(Bytes::from(out));
                            if e.starts_with("Protocol error:") {
                                return;
                            }
                        }
                    }
                }
            }
            Err(_) => break,
        }
    }
}

/// A query buffer at least this large is dropped once drained.
const QUERY_BUF_KEEP_BYTES: usize = 1 << 20;

/// Swaps a drained query buffer that grew past [`QUERY_BUF_KEEP_BYTES`]
/// (one big command: a 5 MB argument grew it to ~8 MB) for a fresh one.
/// Kept, it stayed allocated for the client's lifetime and counted in
/// used_memory, so later commands of every client hit -OOM under a
/// maxmemory the big one was already refused for (Redis hands a big
/// argument's buffer to the command and starts a new one).
fn release_big_query_buf(buf: &mut BytesMut) {
    if buf.is_empty() && buf.capacity() >= QUERY_BUF_KEEP_BYTES {
        *buf = BytesMut::with_capacity(READ_BUFFER_SIZE);
    }
}

/// Counts and logs a client dropped for breaking its output buffer limit
/// (Redis's wording; rate-limited, as many clients may break it at once).
/// Takes the next `*chunk` items off the front of `rest` (fewer at its end)
/// and doubles `*chunk`: a batch run 1, 2, 4, ... commands at a time, so
/// output checked between chunks overshoots a limit by at most about what
/// was already built.
fn take_doubling_chunk<T>(rest: &mut Vec<T>, chunk: &mut usize) -> Vec<T> {
    let tail = rest.split_off((*chunk).min(rest.len()));
    *chunk = chunk.saturating_mul(2);
    std::mem::replace(rest, tail)
}

/// `[kind, name, count]`, the reply to (P|S)UNSUBSCRIBE for one channel or
/// pattern, binary-safe. The name was formatted lossily as UTF-8 under its
/// raw byte length, so a non-UTF-8 channel name made the reply malformed
/// (the bulk body longer than its length) and desynced the client.
fn unsubscribe_reply(kind: &str, name: &[u8], count: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + name.len());
    out.extend_from_slice(
        format!("*3\r\n${}\r\n{}\r\n${}\r\n", kind.len(), kind, name.len()).as_bytes(),
    );
    out.extend_from_slice(name);
    out.extend_from_slice(format!("\r\n:{}\r\n", count).as_bytes());
    out
}

fn client_output_limit_reached(client_id: u64, flags: &str, omem: usize) {
    static LOG_LIMIT: crate::log::RateLimit = crate::log::RateLimit::new();
    crate::server_stats::add(
        crate::server_stats::Stat::ClientOutputBufferLimitDisconnections,
        1,
    );
    if LOG_LIMIT.allow(std::time::Duration::from_secs(1)) {
        crate::log_warning!(
            "Client id={} flags={} omem={} scheduled to be closed ASAP for overcoming of output buffer limits.",
            client_id,
            flags,
            omem
        );
    }
}

/// Disconnects pub/sub client `client_id` on `port` for going over its
/// output buffer limit, from any thread (the publishing shard): logs and
/// counts it like Redis, then shuts its socket down the way CLIENT KILL
/// does, so its writer's pending write fails and the connection closes.
pub fn close_client_over_output_limit(port: u16, client_id: u64, omem: usize) {
    client_output_limit_reached(client_id, "P", omem);
    let map = GLOBAL_CLIENTS.read();
    if let Some(entry) = map.get(&(port, client_id)) {
        if entry
            .stats
            .killed
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_ok()
        {
            ACTIVE_CLIENTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        // SAFETY: shutdown(2) takes no pointers. The entry is read under the
        // GLOBAL_CLIENTS read lock and the connection removes its registry
        // entry (PubsubCleanup) before its socket is closed, so the fd is not
        // a reused number.
        unsafe {
            libc::shutdown(entry.raw_fd, libc::SHUT_RDWR);
        }
    }
}

fn replica_output_limit_reached(client_id: u64, name: &str) {
    crate::server_stats::add(
        crate::server_stats::Stat::ClientOutputBufferLimitDisconnections,
        1,
    );
    crate::log_warning!(
        "Client id={} addr={} flags=S scheduled to be closed ASAP for overcoming of output buffer limits.",
        client_id,
        name
    );
}

fn parse_ack_offset(arg: &[u8]) -> Option<u64> {
    std::str::from_utf8(arg).ok()?.parse().ok()
}

/// The `FACK <aofoffset>` after `REPLCONF ACK <offset>` (args from `ACK`
/// on), sent by a replica with AOF on.
fn parse_fack(args: &[bytes::Bytes]) -> Option<u64> {
    match args.get(2..4) {
        Some([k, v]) if k.eq_ignore_ascii_case(b"fack") => parse_ack_offset(v),
        _ => None,
    }
}

/// A command's full lowercase name as Redis logs it: `container|sub` for a
/// subcommand (`slowlog|get`), from the arguments as sent when there are
/// any.
fn full_cmd_name(cmd: &Command, argv: Option<&crate::resp::RawArgv>) -> String {
    if let Some(args) = argv.map(|a| a.to_vec())
        && let Some(first) = args.first()
    {
        let name = String::from_utf8_lossy(first).to_ascii_lowercase();
        if let Some(sub) = args.get(1) {
            let full = format!(
                "{name}|{}",
                String::from_utf8_lossy(sub).to_ascii_lowercase()
            );
            if crate::acl::command_index(&full).is_some() {
                return full;
            }
        }
        return name;
    }
    stat_cmd_name(cmd).to_ascii_lowercase()
}

/// Reads what a replica sends back, which is only `REPLCONF ACK <offset>`,
/// until the link ends. Malformed input or a partial command longer than
/// any ack drops the link instead of being skipped or buffered without
/// bound.
async fn read_replica_acks<R: AsyncReadRent>(
    reader: &mut R,
    mut on_ack: impl FnMut(u64, Option<u64>),
) {
    let mut read_buf = vec![0u8; READ_BUFFER_SIZE];
    let mut buf = BytesMut::with_capacity(32768);
    loop {
        let (res, returned_buf) = reader.read(read_buf).await;
        read_buf = returned_buf;
        match res {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&read_buf[..n]),
        }
        loop {
            match crate::resp::parse_command_with_argv(&mut buf) {
                Ok(Some((Command::Replconf(args), _)))
                    if args.len() >= 2 && args[0].eq_ignore_ascii_case(b"ack") =>
                {
                    if let Some(ack_off) = parse_ack_offset(&args[1]) {
                        on_ack(ack_off, parse_fack(&args));
                    }
                }
                Ok(Some((Command::Replconf(_), _))) => {}
                // A replica's link carries no replies: a command that
                // would have one (anything but REPLCONF) drops it, as Redis
                // does rather than corrupt the stream it sends.
                Ok(Some((cmd, argv))) => {
                    let name = full_cmd_name(&cmd, argv.as_ref());
                    // Redis refuses a replica's keyspace command with an
                    // error, which it logs as a master erroring at its
                    // replica, before that reply drops the link.
                    if cmd.is_write_command() || !cmd_keys(&cmd).is_empty() {
                        crate::log_warning!(
                            "== CRITICAL == This master is sending an error to its replica: 'Replica can't interact with the keyspace' after processing the command '{}'",
                            name
                        );
                    }
                    crate::log_warning!(
                        "Replica generated a reply to command '{}', disconnecting it: replica link",
                        name
                    );
                    return;
                }
                Ok(None) => break,
                Err(_) => return,
            }
        }
        if buf.len() > crate::replication::MAX_REPL_LINE {
            return;
        }
    }
}

/// Removes a connection from the cross-thread registries (GLOBAL_CLIENTS,
/// MONITOR) on drop. Declared after a stream is split so it runs before the
/// socket closes: other threads `shutdown`/`send` on the registered raw fd and
/// must never act on a closed, possibly reused, fd number. Idempotent with the
/// connection's own cleanup.
struct UnregisterFdOnDrop {
    port: u16,
    client_id: u64,
}

impl Drop for UnregisterFdOnDrop {
    fn drop(&mut self) {
        unregister_global_client(self.port, self.client_id);
        unregister_monitor_client(self.port, self.client_id);
    }
}

/// Unregisters a replica (and forgets its fd) however its master-side task
/// ends, including when the future is dropped part-way (connections run
/// under `catch_unwind`): otherwise the hub keeps buffering for a dead
/// replica and other threads may `send`/`shutdown` a reused fd number.
/// Unregistering is idempotent with the explicit calls.
struct ReplicaLink {
    hub: std::sync::Arc<crate::replication::ReplicationHub>,
    client_id: u64,
    repl: Option<std::sync::Arc<crate::replication::ConnectedReplica>>,
    /// The replica's `ip:port`, for "Connection with replica ... lost.".
    name: String,
    /// False for a dual-channel sync's RDB channel: the replica it
    /// registered lives on with its main connection.
    active: bool,
}

impl Drop for ReplicaLink {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Some(r) = self.repl.take() {
            // The writer task still holds the socket's write half: shut it
            // down so the replica sees the link end now (a link dropped
            // for a command with a reply would otherwise stay open).
            r.drop_link();
            r.detach_fd();
        }
        self.hub.unregister_replica(self.client_id);
        crate::log_notice!("Connection with replica {} lost.", self.name);
    }
}

/// [`ReplicaLink`] for a per-shard replication flow.
struct ShardFlowLink {
    hub: std::sync::Arc<crate::replication::ReplicationHub>,
    shard_id: usize,
    client_id: u64,
    flow: std::sync::Arc<crate::replication::ShardReplicaFlow>,
}

impl Drop for ShardFlowLink {
    fn drop(&mut self) {
        self.flow.detach_fd();
        self.hub
            .unregister_shard_flow(self.shard_id, self.client_id);
    }
}

pub fn cmd_primary_key(cmd: &Command) -> Option<&bytes::Bytes> {
    match cmd {
        Command::Exists(keys) => keys.first(),
        Command::Del(keys) | Command::Unlink(keys) => keys.first(),
        Command::Sismember { key, .. } => Some(key),
        Command::Sadd { key, .. } => Some(key),
        Command::Hset { key, .. } => Some(key),
        Command::Hget { key, .. } => Some(key),
        Command::Lpop { key, .. } => Some(key),
        Command::Lpush { key, .. } => Some(key),
        Command::Zadd { key, .. } => Some(key),
        Command::Get(key)
        | Command::Getex { key, .. }
        | Command::Set { key, .. }
        | Command::IncrBy(key, _, _)
        | Command::Expire { key, .. }
        | Command::Persist(key)
        | Command::Ttl(key, _)
        | Command::Hsetnx { key, .. }
        | Command::HimportSet { key, .. }
        | Command::Himport(crate::resp::HimportSubcommand::Set { key, .. })
        | Command::Hmset { key, .. }
        | Command::Hmget { key, .. }
        | Command::Hdel { key, .. }
        | Command::Hexists { key, .. }
        | Command::Hlen(key)
        | Command::Hgetall(key)
        | Command::Hkeys(key)
        | Command::Hvals(key)
        | Command::Hstrlen { key, .. }
        | Command::Hgetdel { key, .. }
        | Command::Rpush { key, .. }
        | Command::Lpushx { key, .. }
        | Command::Rpushx { key, .. }
        | Command::Rpop { key, .. }
        | Command::Lrange { key, .. }
        | Command::Llen(key)
        | Command::Lindex { key, .. }
        | Command::Srem { key, .. }
        | Command::Smembers(key)
        | Command::Scard(key)
        | Command::Spop { key, .. }
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
        | Command::Copy { source: key, .. }
        | Command::Move { key, .. }
        | Command::Setbit { key, .. }
        | Command::Getbit { key, .. }
        | Command::Bitcount { key, .. }
        | Command::Bitpos { key, .. }
        | Command::Bitfield { key, .. }
        | Command::Pfadd { key, .. }
        | Command::Dump(key)
        | Command::Restore { key, .. }
        | Command::Xadd { key, .. }
        | Command::Xcfgset { key, .. }
        | Command::Xsetid { key, .. }
        | Command::Xdelex { key, .. }
        | Command::Xackdel { key, .. }
        | Command::Xidmprecord { key, .. }
        | Command::Xlen(key)
        | Command::Xrange { key, .. }
        | Command::Xrevrange { key, .. }
        | Command::Xdel { key, .. }
        | Command::Xtrim { key, .. }
        | Command::XgroupCreate { key, .. }
        | Command::XgroupDestroy { key, .. }
        | Command::XgroupCreateConsumer { key, .. }
        | Command::XgroupDelConsumer { key, .. }
        | Command::XgroupSetId { key, .. }
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
        | Command::Increx { key, .. }
        | Command::Setrange { key, .. }
        | Command::Getrange { key, .. }
        | Command::Vadd { key, .. }
        | Command::Vquery { key, .. }
        | Command::Vsim { key, .. }
        | Command::Vdist { key, .. }
        | Command::Vdel { key, .. }
        | Command::Vinfo(key)
        | Command::Vcard(key)
        | Command::Vdim(key)
        | Command::Vemb { key, .. }
        | Command::Vlinks { key, .. }
        | Command::Vrandmember { key, .. }
        | Command::Vsetattr { key, .. }
        | Command::Vgetattr { key, .. }
        | Command::Vismember { key, .. }
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
        | Command::Geosearchstore { key, .. }
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
        | Command::ProbRestore { key, .. }
        | Command::CrdtSet { key, .. }
        | Command::CrdtGet(key)
        | Command::CrdtDel(key)
        | Command::CrdtIncrby { key, .. }
        | Command::CrdtSadd { key, .. }
        | Command::CrdtSmembers(key)
        | Command::CrdtSrem { key, .. }
        | Command::SemanticSet { namespace: key, .. }
        | Command::SemanticGet { namespace: key, .. }
        | Command::SemanticDel { namespace: key, .. }
        | Command::SemanticFlush(key)
        | Command::SemanticInfo(key)
        | Command::AgentMemAdd { session: key, .. }
        | Command::AgentMemContext { session: key, .. }
        | Command::AgentMemCompact { session: key, .. }
        | Command::AgentMemInfo(key)
        | Command::AgentMemClear(key)
        | Command::LlmQuotaReserve { key, .. }
        | Command::LlmQuotaSettle { key, .. }
        | Command::LlmQuotaInfo(key)
        | Command::AgentCheckpointPut { key, .. }
        | Command::AgentCheckpointGet { key, .. }
        | Command::AgentCheckpointHistory { key, .. }
        | Command::AgentToolClaim { key, .. }
        | Command::AgentToolComplete { key, .. } => Some(key),

        Command::Smove { source, .. }
        | Command::Lmove { source, .. }
        | Command::Blmove { source, .. }
        | Command::Lmovem { source, .. }
        | Command::Blmovem { source, .. } => Some(source),
        Command::Touch(keys) => keys.first(),
        Command::Mget(keys) => keys.first(),
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
        Command::Object(crate::resp::ObjectSubcommand::Encoding(key))
        | Command::Object(crate::resp::ObjectSubcommand::Freq(key))
        | Command::Object(crate::resp::ObjectSubcommand::Idletime(key))
        | Command::Object(crate::resp::ObjectSubcommand::Refcount(key))
        | Command::Xinfo(crate::resp::XinfoSubcommand::Stream(key))
        | Command::Xinfo(crate::resp::XinfoSubcommand::StreamFull { key, .. })
        | Command::Xinfo(crate::resp::XinfoSubcommand::Groups(key))
        | Command::Xinfo(crate::resp::XinfoSubcommand::Consumers { key, .. })
        | Command::Hexpire { key, .. }
        | Command::Httl { key, .. }
        | Command::Hpersist { key, .. }
        | Command::Hgetex { key, .. }
        | Command::Hsetex { key, .. }
        | Command::Xclaim { key, .. }
        | Command::Xautoclaim { key, .. }
        | Command::PfdebugGetreg(key)
        | Command::PfdebugEncoding(key)
        | Command::PfdebugTodense(key) => Some(key),
        _ => None,
    }
}

pub fn for_each_cmd_key<'a, F: FnMut(&'a [u8])>(cmd: &'a Command, mut f: F) {
    match cmd {
        Command::Get(k)
        | Command::IncrBy(k, _, _)
        | Command::Expire { key: k, .. }
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
        | Command::Xlen(k) => f(k.as_ref()),

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
        | Command::Zpopmin { key, .. }
        | Command::Zpopmax { key, .. }
        | Command::Setnx { key, .. }
        | Command::Getset { key, .. }
        | Command::Append { key, .. }
        | Command::Hset { key, .. }
        | Command::Hsetnx { key, .. }
        | Command::HimportSet { key, .. }
        | Command::Himport(crate::resp::HimportSubcommand::Set { key, .. })
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
        | Command::Bitfield { key, .. }
        | Command::Pfadd { key, .. }
        | Command::Restore { key, .. }
        | Command::Xadd { key, .. }
        | Command::Xcfgset { key, .. }
        | Command::Xsetid { key, .. }
        | Command::Xdelex { key, .. }
        | Command::Xidmprecord { key, .. }
        | Command::Xrange { key, .. }
        | Command::Xrevrange { key, .. }
        | Command::Xdel { key, .. }
        | Command::Xtrim { key, .. }
        | Command::XgroupCreate { key, .. }
        | Command::XgroupDestroy { key, .. }
        | Command::XgroupCreateConsumer { key, .. }
        | Command::XgroupDelConsumer { key, .. }
        | Command::XgroupSetId { key, .. }
        | Command::Xack { key, .. }
        | Command::Xackdel { key, .. }
        | Command::Xnack { key, .. }
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
        | Command::Increx { key, .. }
        | Command::Setrange { key, .. }
        | Command::Getrange { key, .. }
        | Command::Vadd { key, .. }
        | Command::Vquery { key, .. }
        | Command::Vsim { key, .. }
        | Command::Vdist { key, .. }
        | Command::Vdel { key, .. }
        | Command::Vinfo(key)
        | Command::Vcard(key)
        | Command::Vdim(key)
        | Command::Vemb { key, .. }
        | Command::Vlinks { key, .. }
        | Command::Vrandmember { key, .. }
        | Command::Vsetattr { key, .. }
        | Command::Vgetattr { key, .. }
        | Command::Vismember { key, .. }
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
        | Command::Geosearch { key, .. } => f(key.as_ref()),
        Command::Geosearchstore { dest, key, .. } => {
            f(dest.as_ref());
            f(key.as_ref());
        }
        Command::Georadius {
            key,
            store,
            storedist,
            ..
        }
        | Command::Georadiusbymember {
            key,
            store,
            storedist,
            ..
        } => {
            f(key.as_ref());
            if let Some(s) = store {
                f(s.as_ref());
            }
            if let Some(s) = storedist {
                f(s.as_ref());
            }
        }
        Command::BfReserve { key, .. }
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
        | Command::ProbRestore { key, .. } => f(key.as_ref()),
        // The keys it merges: WATCH, tracking, ACL key patterns and scripts'
        // shard checks all see them.
        Command::CrdtMerge(payload) => crate::crdt::for_each_payload_key(payload, f),

        Command::Smove {
            source,
            destination,
            ..
        }
        | Command::Zrangestore {
            dst: destination,
            src: source,
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
        }
        | Command::Lmovem {
            source,
            destination,
            ..
        }
        | Command::Blmovem {
            source,
            destination,
            ..
        } => {
            f(source.as_ref());
            f(destination.as_ref());
        }
        Command::Sort { key, store, .. } => {
            f(key.as_ref());
            if let Some(dest) = store {
                f(dest.as_ref());
            }
        }

        Command::Mget(keys) => {
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Del(keys)
        | Command::Unlink(keys)
        | Command::Exists(keys)
        | Command::Touch(keys) => {
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Pfcount { keys } => {
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Xread { keys, .. } | Command::Xreadgroup { keys, .. } => {
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Blpop { keys, .. }
        | Command::Brpop { keys, .. }
        | Command::Bzpopmin { keys, .. }
        | Command::Bzpopmax { keys, .. }
        | Command::Zmpop { keys, .. }
        | Command::Bzmpop { keys, .. } => {
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Sinter(keys)
        | Command::Sunion(keys)
        | Command::Sdiff(keys)
        | Command::Sintercard { keys, .. }
        | Command::Sunioncard { keys, .. }
        | Command::Sdiffcard { keys, .. } => {
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Sinterstore { destination, keys }
        | Command::Sunionstore { destination, keys }
        | Command::Sdiffstore { destination, keys } => {
            f(destination.as_ref());
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Zunionstore {
            destination, keys, ..
        }
        | Command::Zinterstore {
            destination, keys, ..
        }
        | Command::Zdiffstore { destination, keys } => {
            f(destination.as_ref());
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Zdiff { keys, .. }
        | Command::Zinter { keys, .. }
        | Command::Zunion { keys, .. }
        | Command::Zintercard { keys, .. } => {
            for k in keys {
                f(k.as_ref());
            }
        }

        Command::Mset(pairs) | Command::Msetnx(pairs) | Command::Msetex { pairs, .. } => {
            for (k, _) in pairs {
                f(k.as_ref());
            }
        }

        Command::Lcs { key1, key2, .. } => {
            f(key1.as_ref());
            f(key2.as_ref());
        }

        Command::Digest(key) => {
            f(key.as_ref());
        }

        Command::Rename { key, newkey, .. } => {
            f(key.as_ref());
            f(newkey.as_ref());
        }

        Command::Copy {
            source,
            destination,
            ..
        } => {
            f(source.as_ref());
            f(destination.as_ref());
        }

        Command::Move { key, .. } => {
            f(key.as_ref());
        }

        Command::Bitop {
            destkey, srckeys, ..
        }
        | Command::Pfmerge { destkey, srckeys } => {
            f(destkey.as_ref());
            for k in srckeys {
                f(k.as_ref());
            }
        }

        Command::Hmget { key, .. } | Command::Hdel { key, .. } => f(key.as_ref()),
        Command::PfdebugGetreg(k) | Command::PfdebugEncoding(k) | Command::PfdebugTodense(k) => {
            f(k.as_ref())
        }
        Command::Object(crate::resp::ObjectSubcommand::Encoding(k))
        | Command::Object(crate::resp::ObjectSubcommand::Freq(k))
        | Command::Object(crate::resp::ObjectSubcommand::Idletime(k))
        | Command::Object(crate::resp::ObjectSubcommand::Refcount(k))
        | Command::Memory(crate::resp::MemorySubcommand::Usage { key: k })
        | Command::Xinfo(crate::resp::XinfoSubcommand::Stream(k))
        | Command::Xinfo(crate::resp::XinfoSubcommand::StreamFull { key: k, .. })
        | Command::Xinfo(crate::resp::XinfoSubcommand::Groups(k))
        | Command::Xinfo(crate::resp::XinfoSubcommand::Consumers { key: k, .. })
        | Command::Getex { key: k, .. }
        | Command::Delex { key: k, .. }
        | Command::Sticky(k)
        | Command::MemcachedSet { key: k, .. }
        | Command::MemcachedAdd { key: k, .. }
        | Command::MemcachedReplace { key: k, .. }
        | Command::MemcachedDelete { key: k, .. }
        | Command::MemcachedIncr { key: k, .. }
        | Command::MemcachedDecr { key: k, .. }
        | Command::CrdtSet { key: k, .. }
        | Command::CrdtGet(k)
        | Command::CrdtDel(k)
        | Command::CrdtIncrby { key: k, .. }
        | Command::CrdtSadd { key: k, .. }
        | Command::CrdtSmembers(k)
        | Command::CrdtSrem { key: k, .. }
        | Command::SemanticSet { namespace: k, .. }
        | Command::SemanticGet { namespace: k, .. }
        | Command::SemanticDel { namespace: k, .. }
        | Command::SemanticFlush(k)
        | Command::SemanticInfo(k)
        | Command::AgentMemAdd { session: k, .. }
        | Command::AgentMemContext { session: k, .. }
        | Command::AgentMemCompact { session: k, .. }
        | Command::AgentMemInfo(k)
        | Command::AgentMemClear(k)
        | Command::LlmQuotaReserve { key: k, .. }
        | Command::LlmQuotaSettle { key: k, .. }
        | Command::LlmQuotaInfo(k)
        | Command::AgentCheckpointPut { key: k, .. }
        | Command::AgentCheckpointGet { key: k, .. }
        | Command::AgentCheckpointHistory { key: k, .. }
        | Command::AgentToolClaim { key: k, .. }
        | Command::AgentToolComplete { key: k, .. }
        | Command::Hexpire { key: k, .. }
        | Command::Httl { key: k, .. }
        | Command::Hpersist { key: k, .. }
        | Command::Hgetex { key: k, .. }
        | Command::Hsetex { key: k, .. }
        | Command::Xclaim { key: k, .. }
        | Command::Xautoclaim { key: k, .. } => f(k.as_ref()),
        Command::Eval { keys, .. }
        | Command::Evalsha { keys, .. }
        | Command::Fcall { keys, .. }
        | Command::Lmpop { keys, .. }
        | Command::Blmpop { keys, .. }
        | Command::JsonMget { keys, .. }
        | Command::Stick(keys)
        | Command::Unstick(keys)
        | Command::MemcachedGet { keys } => {
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Migrate { key, keys, .. } => {
            if let Some(k) = key {
                f(k.as_ref());
            }
            for k in keys {
                f(k.as_ref());
            }
        }

        _ => {}
    }
}

/// CLUSTER SLOT-STATS accounting before a command from the read loop runs
/// (nothing unless it is on): counts its `in_bytes`, hands its slot to the
/// command's CPU statistics, and returns the slot its reply goes to.
#[inline]
fn slot_stats_before(
    router: &Router,
    acct: &mut crate::slot_stats::ClientAcct,
    cmd: &Command,
    in_multi: bool,
    in_bytes: u64,
) -> Option<u16> {
    if !crate::slot_stats::accounting(router.cluster_enabled) {
        return None;
    }
    use crate::slot_stats::TxRole;
    let role = match cmd {
        Command::Multi if !in_multi => TxRole::Multi,
        Command::Exec if in_multi => TxRole::Exec,
        Command::Discard if in_multi => TxRole::Discard,
        _ if in_multi => TxRole::Queued,
        _ => TxRole::None,
    };
    let slot = acct.before(role, command_slot(cmd), in_bytes);
    crate::slot_stats::hand_over(slot);
    slot
}

/// A command's keys and request size for HOTKEYS (its encoding, or near
/// enough for commands the AOF never logs).
fn hotkeys_command_info(cmd: &Command) -> (Vec<Bytes>, u64) {
    let keys: Vec<Bytes> = cmd_keys(cmd)
        .iter()
        .map(|k| Bytes::copy_from_slice(k))
        .collect();
    let net = crate::aof::command_to_resp(cmd).map_or_else(
        || keys.iter().map(|k| k.len() as u64 + 16).sum::<u64>() + 16,
        |b| b.len() as u64,
    );
    (keys, net)
}

/// The cluster slot a command is accounted to in CLUSTER SLOT-STATS: its
/// keys' (or sharded channels') slot when they all share one; `None` for
/// commands without keys or across slots.
pub fn command_slot(cmd: &Command) -> Option<u16> {
    let mut slot = None;
    let mut same = true;
    let mut see = |k: &[u8]| {
        let s = crate::router::key_slot(k);
        match slot {
            None => slot = Some(s),
            Some(p) if p != s => same = false,
            _ => {}
        }
    };
    match cmd {
        Command::Spublish { channel, .. } => see(channel),
        Command::Ssubscribe(chs) | Command::Sunsubscribe(chs) => chs.iter().for_each(|c| see(c)),
        _ => for_each_cmd_key(cmd, see),
    }
    if same { slot } else { None }
}

pub fn cmd_keys(cmd: &Command) -> Vec<&[u8]> {
    let mut v = Vec::new();
    for_each_cmd_key(cmd, |k| v.push(k));
    v
}

/// How [`migrate_keys_to_node`] moves keys.
#[derive(Clone)]
struct MigrateOpts {
    /// The target database (MIGRATE's destination-db), selected first;
    /// `None` for slot migration (a cluster node has database 0 only).
    db: Option<u32>,
    /// Credentials for the target (MIGRATE AUTH / AUTH2).
    auth: Option<(Option<Bytes>, Bytes)>,
    /// Keep the source keys (MIGRATE COPY).
    copy: bool,
    /// Overwrite keys that already exist on the target.
    replace: bool,
    /// Limit for connecting to the target and for each read of its replies.
    timeout: std::time::Duration,
}

/// Timeout for moving one batch of keys during slot migration.
const SLOT_MIGRATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// Appends `args` to `out` as a RESP command (an array of bulk strings).
pub fn write_resp_command(out: &mut Vec<u8>, args: &[&[u8]]) {
    write_resp_array_header(out, args.len());
    for arg in args {
        write_resp_bulk(out, arg);
    }
}

/// Splits complete simple-string / integer / error replies off the front
/// of `buf` into `replies`: Ok for `+...` and `:...`, Err(message) for
/// `-...` and for any other reply type. Returns how many bytes were
/// consumed.
fn take_simple_replies(buf: &[u8], replies: &mut Vec<Result<(), String>>) -> usize {
    let mut pos = 0;
    while let Some(nl) = buf[pos..].windows(2).position(|w| w == b"\r\n") {
        let line = &buf[pos..pos + nl];
        replies.push(match line.first() {
            Some(b'+' | b':') => Ok(()),
            Some(b'-') => Err(String::from_utf8_lossy(&line[1..]).into_owned()),
            _ => Err(format!(
                "unexpected reply {:?}",
                String::from_utf8_lossy(line)
            )),
        });
        pos += nl + 2;
    }
    pos
}

/// Like Redis (`flushAllDataAndResetRDB`), with save points configured
/// FLUSHALL saves the now-empty dataset right away: otherwise the snapshot
/// on disk still held the flushed keys, and a crash or kill before the
/// next save brought them all back on restart. A BGSAVE that FLUSHALL just
/// aborted may still be winding down, so this waits for it briefly.
async fn save_empty_rdb_after_flushall(router: &Router) {
    if crate::config::save_points(router.base_port).is_empty() {
        return;
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match router.save_rdb().await {
            Ok(()) => return,
            Err(e) if e.contains("in progress") && std::time::Instant::now() < deadline => {
                monoio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            Err(e) => {
                crate::log_warning!("FLUSHALL: saving the empty dataset failed: {}", e);
                return;
            }
        }
    }
}

// Keyspace events of the hash commands. Kept out of line: in debug builds
// `execute_local_command` reserves stack for every arm's locals, and
// inlining these pushed deep call chains over the test threads' stack.

/// HEXPIRE & co: one event, `hdel` if a past time deleted fields (then
/// `del` if the hash went), else `hexpire` if any deadline was set (Redis).
/// The subkey events list the fields with that result.
#[inline(never)]
fn notify_hexpire_result(db: &mut ShardDb, key: &[u8], fields: &[Bytes], res: &[i64]) {
    let subkeys = subkey_notify_enabled(NOTIFY_HASH);
    let with_code = |code: i64| -> smallvec::SmallVec<[&[u8]; 16]> {
        if !subkeys {
            return smallvec::SmallVec::new();
        }
        fields
            .iter()
            .zip(res)
            .filter(|(_, c)| **c == code)
            .map(|(f, _)| f.as_ref())
            .collect()
    };
    if res.contains(&2) {
        notify_hash_event_then_del(db, key, "hdel", &with_code(2));
    } else if res.contains(&1) {
        notify_hash_fields_event("hexpire", key, &with_code(1));
    }
}

/// `event` on hash `key` (subkey events for `fields`), then `del` if it
/// was an `hdel` or `hexpired` that emptied it.
#[inline(never)]
fn notify_hash_event_then_del<F: AsRef<[u8]>>(
    db: &mut ShardDb,
    key: &[u8],
    event: &str,
    fields: &[F],
) {
    notify_hash_fields_event(event, key, fields);
    if event == "hdel" || event == "hexpired" {
        notify_del_if_gone(db, key);
    }
}

/// What the HDEL of `fields` about to run reports: its event, and with
/// subkey notifications on, the fields it deletes. A read-only replica
/// reports `hexpired` for its master's HDEL of fields already past their
/// deadline: that is how their expiry reaches it.
#[inline(never)]
fn hdel_event_and_fields(
    db: &mut ShardDb,
    key: &[u8],
    fields: &[Bytes],
) -> (&'static str, Vec<Bytes>) {
    if NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0 {
        return ("hdel", Vec::new());
    }
    let event = if !db.table.hash_field_expires.is_empty()
        && crate::replication::hide_expired_keys()
        && db.table.hash_fields_all_past_deadline(key, fields)
    {
        "hexpired"
    } else {
        "hdel"
    };
    let present = if subkey_notify_enabled(NOTIFY_HASH) {
        db.table.hash_fields_present(key, fields)
    } else {
        Vec::new()
    };
    (event, present)
}

/// `del` for `key` if it no longer exists.
#[inline(never)]
fn notify_del_if_gone(db: &mut ShardDb, key: &[u8]) {
    if !db.exists(key) {
        notify_keyspace_event(NOTIFY_GENERIC, "del", key);
    }
}

/// HSETEX that set its fields: `hset`, then the deadline's event, `hexpire`,
/// or for a time already passed `hdel` (+ `del`): the fields are gone. The
/// subkey events list all the fields set.
#[inline(never)]
fn notify_hsetex_applied(
    db: &mut ShardDb,
    key: &[u8],
    expire: &crate::resp::HFieldExpireOpt,
    pairs: &[(Bytes, Bytes)],
) {
    let fields: smallvec::SmallVec<[&[u8]; 16]> = if subkey_notify_enabled(NOTIFY_HASH) {
        pairs.iter().map(|(f, _)| f.as_ref()).collect()
    } else {
        smallvec::SmallVec::new()
    };
    notify_hash_fields_event("hset", key, &fields);
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let past = match expire {
        crate::resp::HFieldExpireOpt::ExMs(ms) => Some(*ms <= 0),
        crate::resp::HFieldExpireOpt::ExAtMs(ms) => Some(*ms <= now_ms),
        _ => None,
    };
    match past {
        Some(true) => notify_hash_event_then_del(db, key, "hdel", &fields),
        Some(false) => notify_hash_fields_event("hexpire", key, &fields),
        None => {}
    }
}

/// HPERSIST that removed deadlines: `hpersist`, the subkey events listing
/// the fields that lost theirs.
#[inline(never)]
fn notify_hpersist_result(key: &[u8], fields: &[Bytes], res: &[i64]) {
    let persisted: smallvec::SmallVec<[&[u8]; 16]> = if subkey_notify_enabled(NOTIFY_HASH) {
        fields
            .iter()
            .zip(res)
            .filter(|(_, c)| **c == 1)
            .map(|(f, _)| f.as_ref())
            .collect()
    } else {
        smallvec::SmallVec::new()
    };
    notify_hash_fields_event("hpersist", key, &persisted);
}

/// HSET / HMSET: `hset`, the subkey events listing every field given.
#[inline(never)]
fn notify_hset_fields(key: &[u8], pairs: &[(Bytes, Bytes)]) {
    if subkey_notify_enabled(NOTIFY_HASH) {
        let fields: smallvec::SmallVec<[&[u8]; 16]> =
            pairs.iter().map(|(f, _)| f.as_ref()).collect();
        notify_hash_fields_event("hset", key, &fields);
    } else {
        notify_keyspace_event(NOTIFY_HASH, "hset", key);
    }
}

/// How a deletion the server makes on its own (SET with a past expiry,
/// RESTORE of an expired key, SPOP of a whole set) is replicated: DEL, or
/// UNLINK under `lazyfree-lazy-server-del`, as in Redis.
fn server_del_cmd(key: &Bytes) -> Command {
    let keys = smallvec![key.clone()];
    if crate::table::LAZYFREE_LAZY_SERVER_DEL.load(std::sync::atomic::Ordering::Relaxed) {
        Command::Unlink(keys)
    } else {
        Command::Del(keys)
    }
}

/// Seconds a cached MIGRATE connection may stay unused (Redis
/// `MIGRATE_SOCKET_CACHE_TTL`).
const MIGRATE_SOCKET_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(10);
/// Cached MIGRATE connections, server-wide (Redis `MIGRATE_SOCKET_CACHE_ITEMS`).
const MIGRATE_SOCKET_CACHE_ITEMS: usize = 64;
/// INFO `migrate_cached_sockets`.
static MIGRATE_CACHED_SOCKETS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

thread_local! {
    /// This thread's idle connections to MIGRATE targets, by "host:port"
    /// and credentials, with when each was last used. Like Redis, MIGRATE reuses them so a
    /// resharding tool calling it batch after batch doesn't reconnect
    /// every time.
    static MIGRATE_CACHE: RefCell<hashbrown::HashMap<Vec<u8>, (TcpStream, std::time::Instant)>> =
        RefCell::new(hashbrown::HashMap::new());
}

/// Takes the cached connection to `target`, if any and not idle too long.
fn migrate_cache_take(target: &[u8]) -> Option<TcpStream> {
    let (stream, last) = MIGRATE_CACHE.with(|c| c.borrow_mut().remove(target))?;
    MIGRATE_CACHED_SOCKETS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    (last.elapsed() < MIGRATE_SOCKET_CACHE_TTL).then_some(stream)
}

/// Caches `stream` to `target` for the next MIGRATE, unless the cache is
/// full.
fn migrate_cache_put(target: Vec<u8>, stream: TcpStream) {
    if MIGRATE_CACHED_SOCKETS.load(std::sync::atomic::Ordering::Relaxed)
        >= MIGRATE_SOCKET_CACHE_ITEMS
    {
        return;
    }
    let old = MIGRATE_CACHE.with(|c| {
        c.borrow_mut()
            .insert(target, (stream, std::time::Instant::now()))
    });
    if old.is_none() {
        MIGRATE_CACHED_SOCKETS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Closes this thread's MIGRATE connections unused for
/// `MIGRATE_SOCKET_CACHE_TTL` (from the shard cron).
pub fn migrate_cache_prune() {
    MIGRATE_CACHE.with(|c| {
        let mut c = c.borrow_mut();
        let before = c.len();
        c.retain(|_, (_, last)| last.elapsed() < MIGRATE_SOCKET_CACHE_TTL);
        MIGRATE_CACHED_SOCKETS.fetch_sub(before - c.len(), std::sync::atomic::Ordering::Relaxed);
    });
}

/// INFO `migrate_cached_sockets`.
pub fn migrate_cached_sockets() -> usize {
    MIGRATE_CACHED_SOCKETS.load(std::sync::atomic::Ordering::Relaxed)
}

/// Moves `keys` to the node at `host:port` the way Redis MIGRATE does:
/// each key goes as its DUMP payload plus absolute expiry and is RESTOREd
/// there. Unless `copy`, a source key is deleted only after the target
/// replied +OK for it, and only if it is unchanged since it was dumped, so
/// a rejected, failed or interrupted transfer never loses a key. Missing
/// keys are skipped. Returns how many keys the target stored, or the first
/// error as a RESP error line without the leading '-'.
///
/// `conn` carries the connection to the target across calls (a slot moves
/// in many batches): it is opened when `None`, and dropped after an error
/// so the next call starts on a fresh one.
/// Sends `tx_buf` to the target and reads `expected` simple replies, in
/// order. On a write, read or timeout error, the replies read so far and
/// the error.
async fn exchange_with_target(
    stream: &mut TcpStream,
    tx_buf: Vec<u8>,
    expected: usize,
    timeout: std::time::Duration,
) -> (Vec<Result<(), String>>, Option<String>) {
    let mut replies = Vec::with_capacity(expected);
    if let Err(e) = stream.write_all(tx_buf).await.0 {
        return (
            replies,
            Some(format!("IOERR error sending to destination: {}", e)),
        );
    }
    let mut failure = None;
    let mut pending = Vec::new();
    let mut buf = vec![0u8; 4096];
    while replies.len() < expected {
        let (res, b) = match monoio::time::timeout(timeout, stream.read(buf)).await {
            Ok(r) => r,
            Err(_) => {
                failure = Some("IOERR timeout reading from destination".to_string());
                break;
            }
        };
        buf = b;
        match res {
            Ok(0) => {
                failure = Some("IOERR destination closed the connection".to_string());
                break;
            }
            Ok(n) => {
                pending.extend_from_slice(&buf[..n]);
                let used = take_simple_replies(&pending, &mut replies);
                pending.drain(..used);
            }
            Err(e) => {
                failure = Some(format!("IOERR error reading from destination: {}", e));
                break;
            }
        }
    }
    (replies, failure)
}

/// How many times a migrated key that clients keep changing is re-sent
/// before MIGRATE gives up on it.
const MIGRATE_SETTLE_ATTEMPTS: usize = 64;

/// Deletes a key just restored on the target from here, so it ends up on
/// the target only and with its latest value. Redis's MIGRATE blocks the
/// server from DUMP to DEL; here clients keep running, and one that
/// changed the key in between left the target an old copy: deleting the
/// source lost the write, keeping it left the key on both nodes (the
/// next `redis-cli --cluster reshard` of the slot failed "Target key
/// exists" and left it open). So a changed key is sent again with
/// REPLACE, and a key deleted meanwhile is deleted on the target too,
/// until the delete here finds the value the target has.
async fn settle_migrated_key(
    router: &Router,
    stream: &mut TcpStream,
    key: Bytes,
    mut payload: Bytes,
    opts: &MigrateOpts,
) -> Result<(), String> {
    let shard = router.target_shard(&key);
    for _ in 0..MIGRATE_SETTLE_ATTEMPTS {
        if router.del_if_unchanged(key.clone(), payload.clone()).await {
            return Ok(());
        }
        let dump = exec_on_shard(router, shard, Command::Dump(key.clone())).await;
        let mut tx_buf = Vec::new();
        let new_payload = resp_bulk_payload(&dump);
        match &new_payload {
            Some(p) => {
                let expire_at = resp_integer(
                    &exec_on_shard(router, shard, Command::Expiretime(key.clone(), true)).await,
                )
                .unwrap_or(-1);
                let ttl = expire_at.max(0).to_string();
                let mut args: Vec<&[u8]> =
                    vec![b"RESTORE-ASKING", &key, ttl.as_bytes(), p, b"REPLACE"];
                if expire_at > 0 {
                    args.push(b"ABSTTL");
                }
                write_resp_command(&mut tx_buf, &args);
            }
            None => {
                if let Some(err) = dump.strip_prefix(b"-") {
                    return Err(String::from_utf8_lossy(err).trim_end().to_string());
                }
                write_resp_command(&mut tx_buf, &[b"ASKING"]);
                write_resp_command(&mut tx_buf, &[b"DEL", &key]);
            }
        }
        let expected = if new_payload.is_some() { 1 } else { 2 };
        let (replies, failure) = exchange_with_target(stream, tx_buf, expected, opts.timeout).await;
        if let Some(e) = failure {
            return Err(e);
        }
        if let Some(Err(e)) = replies.iter().find(|r| r.is_err()) {
            return Err(format!("ERR Target instance replied with error: {}", e));
        }
        match new_payload {
            Some(p) => payload = p,
            None => return Ok(()),
        }
    }
    Err(format!(
        "ERR key {} kept changing while it was migrated",
        String::from_utf8_lossy(&key)
    ))
}

async fn migrate_keys_to_node(
    router: &Router,
    keys: &[Bytes],
    host: &str,
    port: u16,
    opts: MigrateOpts,
    conn: &mut Option<TcpStream>,
) -> Result<usize, String> {
    let mut dumps = Vec::with_capacity(keys.len());
    for k in keys {
        let shard = router.target_shard(k);
        let dump = exec_on_shard(router, shard, Command::Dump(k.clone())).await;
        let Some(payload) = resp_bulk_payload(&dump) else {
            if let Some(err) = dump.strip_prefix(b"-") {
                return Err(String::from_utf8_lossy(err).trim_end().to_string());
            }
            continue;
        };
        let expire_at =
            resp_integer(&exec_on_shard(router, shard, Command::Expiretime(k.clone(), true)).await)
                .unwrap_or(-1);
        dumps.push((k.clone(), payload, expire_at));
    }
    if dumps.is_empty() {
        return Ok(0);
    }

    let mut tx_buf = Vec::new();
    // Replies to these come before the RESTOREs'.
    let mut prefix = 0;
    match &opts.auth {
        Some((None, pass)) => write_resp_command(&mut tx_buf, &[b"AUTH", pass]),
        Some((Some(user), pass)) => write_resp_command(&mut tx_buf, &[b"AUTH", user, pass]),
        None => {}
    }
    prefix += usize::from(opts.auth.is_some());
    let db_s = opts.db.map(|d| d.to_string());
    if let Some(db) = &db_s {
        write_resp_command(&mut tx_buf, &[b"SELECT", db.as_bytes()]);
        prefix += 1;
    }
    // RESTORE-ASKING, which a target importing the slot accepts like a
    // command after ASKING (Redis has it with cluster mode off too). A
    // single ASKING up front covered only the first RESTORE (ASKING lasts
    // one command): with several keys (redis-cli --cluster reshard
    // migrates them 10 at a time) the target answered the rest -MOVED, the
    // reshard failed and left the slot open on both nodes.
    let restore: &[u8] = b"RESTORE-ASKING";
    for (k, payload, expire_at) in &dumps {
        let ttl = (*expire_at).max(0).to_string();
        let mut args: Vec<&[u8]> = vec![restore, k, ttl.as_bytes(), payload];
        if opts.replace {
            args.push(b"REPLACE");
        }
        if *expire_at > 0 {
            args.push(b"ABSTTL");
        }
        write_resp_command(&mut tx_buf, &args);
    }

    let mut stream = match conn.take() {
        Some(s) => s,
        None => {
            let target_addr = format!("{}:{}", host, port);
            let socket_addr = match std::net::ToSocketAddrs::to_socket_addrs(&target_addr) {
                Ok(mut iter) => match iter.next() {
                    Some(a) => a,
                    None => return Err("ERR cannot resolve destination host".to_string()),
                },
                Err(e) => return Err(format!("ERR invalid destination address: {}", e)),
            };
            match monoio::time::timeout(opts.timeout, TcpStream::connect(socket_addr)).await {
                Ok(Ok(s)) => s,
                Ok(Err(e)) => return Err(format!("IOERR error connecting to destination: {}", e)),
                Err(_) => return Err("IOERR timeout connecting to destination".to_string()),
            }
        }
    };
    // One reply per prefix command, then one per RESTORE, in order.
    let (replies, mut failure) =
        exchange_with_target(&mut stream, tx_buf, dumps.len() + prefix, opts.timeout).await;

    // A refused AUTH or SELECT fails the whole MIGRATE (the RESTOREs then
    // failed too, or went to the wrong database).
    if let Some(Err(e)) = replies[..prefix.min(replies.len())]
        .iter()
        .find(|r| r.is_err())
    {
        failure.get_or_insert_with(|| format!("ERR Target instance replied with error: {}", e));
        return Err(failure.unwrap_or_default());
    }
    let mut stored = 0;
    let mut stream_ok = failure.is_none();
    for (i, (k, payload, _)) in dumps.into_iter().enumerate() {
        match replies.get(i + prefix) {
            Some(Ok(())) => {
                stored += 1;
                if opts.copy {
                    // MIGRATE COPY keeps the source key.
                } else if stream_ok {
                    if let Err(e) =
                        settle_migrated_key(router, &mut stream, k, payload, &opts).await
                    {
                        // The stream may be mid-reply: only plain deletes
                        // for the rest.
                        stream_ok = false;
                        failure.get_or_insert(e);
                    }
                } else {
                    router.del_if_unchanged(k, payload).await;
                }
            }
            Some(Err(e)) => {
                failure.get_or_insert_with(|| {
                    format!("ERR Target instance replied with error: {}", e)
                });
            }
            None => {}
        }
    }
    match failure {
        Some(e) => Err(e),
        None => {
            // Every reply was read: the connection is clean for the next batch.
            *conn = Some(stream);
            Ok(stored)
        }
    }
}

/// Whether a reply to `CLUSTER SETSLOT` is `+OK`; otherwise the reply (or
/// what was read of it) as the error.
fn setslot_reply_ok(reply: &[u8]) -> Result<(), String> {
    if reply.starts_with(b"+OK\r\n") {
        return Ok(());
    }
    let line = reply.split(|&b| b == b'\r').next().unwrap_or_default();
    Err(
        match String::from_utf8_lossy(line).trim_start_matches(['-', '+']) {
            "" => "no reply".to_string(),
            l => l.to_string(),
        },
    )
}

/// Sends `CLUSTER SETSLOT <slot> <args...>` to the node at `host:port` and
/// waits for its `+OK` (1 s to connect, 1 s for the reply).
async fn send_setslot(host: &str, port: u16, slot: u16, args: &[&str]) -> Result<(), String> {
    let addr = std::net::ToSocketAddrs::to_socket_addrs(&(host, port))
        .ok()
        .and_then(|mut a| a.next())
        .ok_or_else(|| format!("cannot resolve {host}:{port}"))?;
    let mut stream = match monoio::time::timeout(
        Duration::from_millis(1000),
        monoio::net::TcpStream::connect(addr),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(format!("cannot connect to {host}:{port}: {e}")),
        Err(_) => return Err(format!("timed out connecting to {host}:{port}")),
    };
    let slot = slot.to_string();
    let mut parts = vec!["CLUSTER", "SETSLOT", slot.as_str()];
    parts.extend_from_slice(args);
    let mut cmd = format!("*{}\r\n", parts.len());
    for p in &parts {
        cmd.push_str(&format!("${}\r\n{}\r\n", p.len(), p));
    }
    let (res, _) = stream.write_all(cmd.into_bytes()).await;
    res.map_err(|e| format!("cannot write to {host}:{port}: {e}"))?;
    let mut reply = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_millis(1000);
    while !reply.windows(2).any(|w| w == b"\r\n") {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        match monoio::time::timeout(left, stream.read(Vec::with_capacity(256))).await {
            Ok((Ok(n), buf)) if n > 0 => reply.extend_from_slice(&buf[..n]),
            _ => break,
        }
    }
    setslot_reply_ok(&reply)
        .map_err(|e| format!("{host}:{port} refused SETSLOT {}: {e}", args.join(" ")))
}

/// Moves one slot of ours to the node at `host:port`, as `redis-cli
/// --cluster reshard` does: the target is set importing first (nothing
/// changes here if it refuses), then the slot migrating here, then the
/// keys move in batches, and only once every key moved does the target
/// take the slot (`SETSLOT NODE`). On a failure past the first step the slot
/// stays migrating: keys still here are served here, the moved ones via
/// `-ASK` at the target, and nothing is lost. Run the move again (or
/// `SETSLOT STABLE`) to settle it.
async fn migrate_slot_to(
    router: &Router,
    slot: u16,
    host: &str,
    port: u16,
    target_id: Option<&str>,
    batch_size: usize,
) -> Result<(), String> {
    let target_addr = format!("{host}:{port}");
    send_setslot(host, port, slot, &["IMPORTING", &router.my_id()]).await?;
    if let Some(id) = target_id {
        crate::cluster::get_cluster_hub(router.base_port)
            .slot_states
            .write()
            .insert(slot, ("migrating".to_string(), id.to_string()));
    }
    router.set_slot_state(
        slot,
        crate::shard::SlotState::Migrating(target_addr.clone()),
    );
    router.sync_slot_tables().await;

    let mut conn = None;
    loop {
        let keys = router.get_keys_in_slot(slot, batch_size).await;
        if keys.is_empty() {
            break;
        }
        migrate_keys_to_node(
            router,
            &keys,
            host,
            port,
            MigrateOpts {
                db: None,
                auth: None,
                copy: false,
                replace: true,
                timeout: SLOT_MIGRATION_TIMEOUT,
            },
            &mut conn,
        )
        .await?;
    }
    drop(conn);

    send_setslot(host, port, slot, &["NODE", "myself"]).await?;
    // The slot leaves our slot set before the MOVED goes in: a MOVED for
    // a slot we still own is ignored as stale (see
    // `Router::get_slot_state`).
    if let Some(id) = target_id {
        crate::cluster::get_cluster_hub(router.base_port).assign_slot(slot, id);
    }
    router.set_slot_state(slot, crate::shard::SlotState::Moved(target_addr));
    router.sync_slot_tables().await;
    Ok(())
}

/// Runs a rebalance or reshard plan. Returns how many plans were applied, or
/// stops at the first slot that could not be moved: that slot is left as
/// `migrate_slot_to` leaves it, and the rest of the plan is not run.
async fn execute_rebalance_plans(
    router: &Router,
    hub: &std::sync::Arc<crate::cluster::ClusterHub>,
    plans: &[crate::cluster::SlotMigrationPlan],
    pipeline: usize,
) -> Result<usize, String> {
    let mut moved = 0;
    for p in plans {
        let (host, port_str) = match p.target_addr.split_once(':') {
            Some((h, prt)) => (h.to_string(), prt),
            None => ("127.0.0.1".to_string(), "6379"),
        };
        let target_port: u16 = port_str.parse().unwrap_or(6379);

        if p.source_node_id == hub.my_id() {
            migrate_slot_to(
                router,
                p.slot,
                &host,
                target_port,
                Some(&p.target_node_id),
                pipeline.clamp(1, 1000),
            )
            .await
            .map_err(|e| {
                format!(
                    "slot {} not moved ({} slots moved before it): {}",
                    p.slot, moved, e
                )
            })?;
            hub.assign_slot(p.slot, &p.target_node_id);
            hub.slot_states.write().remove(&p.slot);
            // No epoch bump for giving the slot away: see CLUSTER SETSLOT
            // NODE. The target bumped its epoch when it took the slot;
            // learning that epoch now makes any older announcement from it
            // (still without the slot) stale.
            hub.refresh_peer(&p.target_node_id).await;
            moved += 1;
        } else if p.target_node_id == hub.my_id() {
            hub.slot_states
                .write()
                .insert(p.slot, ("importing".to_string(), p.source_node_id.clone()));
            router.set_slot_state(
                p.slot,
                crate::shard::SlotState::Importing(p.source_addr.clone()),
            );
            router.sync_slot_tables().await;

            hub.take_slot(p.slot);
            hub.slot_states.write().remove(&p.slot);
            // Ours now: the importing redirect goes.
            router.set_slot_state(p.slot, crate::shard::SlotState::Stable);
            router.sync_slot_tables().await;
            moved += 1;
        }
    }
    Ok(moved)
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
        Command::Unlink(_) => "UNLINK",
        Command::Exists(_) => "EXISTS",
        Command::IncrBy(_, _, name) => name.as_str(),
        Command::Expire { .. } => "EXPIRE",
        Command::Persist(_) => "PERSIST",
        Command::Ttl(_, _) => "TTL",
        Command::Cluster(_) => "CLUSTER",
        Command::Client(sub) => match sub {
            ClientSubcommand::List(..) => "client|list",
            ClientSubcommand::Info => "client|info",
            ClientSubcommand::SetName(_) => "client|setname",
            ClientSubcommand::GetName => "client|getname",
            ClientSubcommand::Id => "client|id",
            ClientSubcommand::Tracking { .. } => "client|tracking",
            ClientSubcommand::Caching(_) => "client|caching",
            ClientSubcommand::GetRedir => "client|getredir",
            ClientSubcommand::TrackingInfo => "client|trackinginfo",
            ClientSubcommand::Kill(_) => "client|kill",
            ClientSubcommand::Unblock { .. } => "client|unblock",
            ClientSubcommand::Pause(_, _) => "client|pause",
            ClientSubcommand::Unpause => "client|unpause",
            ClientSubcommand::NoTouch(_) => "client|no-touch",
            ClientSubcommand::NoEvict(_) => "client|no-evict",
            ClientSubcommand::SetInfo { .. } => "client|setinfo",
            ClientSubcommand::Reply(_) => "client|reply",
        },
        Command::Asking => "ASKING",
        Command::Migrate { .. } => "MIGRATE",
        Command::Hset { .. } => "HSET",
        Command::Hsetnx { .. } => "HSETNX",
        Command::Himport(_) | Command::HimportSet { .. } => "HIMPORT",
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
        Command::Sort { readonly, .. } => {
            if *readonly {
                "SORT_RO"
            } else {
                "SORT"
            }
        }
        Command::Lmove { is_rpoplpush, .. } => {
            if *is_rpoplpush {
                "RPOPLPUSH"
            } else {
                "LMOVE"
            }
        }
        Command::Blmove { is_rpoplpush, .. } => {
            if *is_rpoplpush {
                "BRPOPLPUSH"
            } else {
                "BLMOVE"
            }
        }
        Command::Lmovem { .. } => "LMOVEM",
        Command::Blmovem { .. } => "BLMOVEM",
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
        Command::Increx { .. } => "INCREX",
        Command::Setrange { .. } => "SETRANGE",
        Command::Getrange { .. } => "GETRANGE",
        Command::Hello { .. } => "HELLO",
        Command::Reset => "RESET",
        Command::Time => "TIME",
        Command::Echo(_) => "ECHO",
        Command::Type(_) => "TYPE",
        Command::Dbsize => "DBSIZE",
        Command::KeyspaceStats => "KEYSPACESTATS",
        Command::Select(_) => "SELECT",
        Command::Slowlog(_) => "SLOWLOG",
        Command::Commandlog(_) => "COMMANDLOG",
        Command::Lolwut(_) => "LOLWUT",
        Command::Sentinel(_) => "SENTINEL",
        Command::Flushdb => "FLUSHDB",
        Command::Flushall => "FLUSHALL",
        Command::Swapdb { .. } => "SWAPDB",
        Command::Touch(_) => "TOUCH",
        Command::Rename { nx: false, .. } => "RENAME",
        Command::Rename { nx: true, .. } => "RENAMENX",
        Command::Copy { .. } => "COPY",
        Command::Move { .. } => "MOVE",
        Command::Setnx { .. } => "SETNX",
        Command::Getset { .. } => "GETSET",
        Command::Getdel(_) => "GETDEL",
        Command::Append { .. } => "APPEND",
        Command::Strlen(_) => "STRLEN",
        Command::Msetnx(_) => "MSETNX",
        Command::Save => "SAVE",
        Command::Bgsave { .. } => "BGSAVE",
        Command::Bgrewriteaof => "BGREWRITEAOF",
        Command::Lastsave => "LASTSAVE",
        Command::Ping(_) => "PING",
        Command::Monitor => "MONITOR",
        Command::CommandDocs(_) => "COMMAND",
        Command::Info(_) => "INFO",
        Command::Replicaof { .. } => "REPLICAOF",
        Command::Failover { .. } => "FAILOVER",
        Command::Psync { .. } => "PSYNC",
        Command::Sync => "SYNC",
        Command::Trimslots(_) => "TRIMSLOTS",
        Command::SyncSlots(_) => "CLUSTER",
        Command::Hotkeys(_) => "HOTKEYS",
        Command::Replconf(_) => "REPLCONF",
        Command::Role => "ROLE",
        Command::Tier(_) => "TIER",
        Command::ConfigGet(_) | Command::ConfigSet(_) => "CONFIG",
        Command::Shutdown { .. } => "SHUTDOWN",
        Command::Quit => "QUIT",
        Command::Subscribe(_) => "SUBSCRIBE",
        Command::Unsubscribe(_) => "UNSUBSCRIBE",
        Command::Psubscribe(_) => "PSUBSCRIBE",
        Command::Punsubscribe(_) => "PUNSUBSCRIBE",
        Command::Ssubscribe(_) => "SSUBSCRIBE",
        Command::Sunsubscribe(_) => "SUNSUBSCRIBE",
        Command::Publish { .. } => "PUBLISH",
        Command::Spublish { .. } => "SPUBLISH",
        Command::PubsubChannels(_) => "PUBSUB CHANNELS",
        Command::PubsubShardchannels(_) => "PUBSUB SHARDCHANNELS",
        Command::PubsubNumsub(_) => "PUBSUB NUMSUB",
        Command::PubsubShardnumsub(_) => "PUBSUB SHARDNUMSUB",
        Command::PubsubNumpat => "PUBSUB NUMPAT",
        Command::PubsubHelp => "PUBSUB HELP",
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
        Command::Bitfield { readonly, .. } => {
            if *readonly {
                "BITFIELD_RO"
            } else {
                "BITFIELD"
            }
        }
        Command::Bitop { .. } => "BITOP",
        Command::Pfadd { .. } => "PFADD",
        Command::Pfcount { .. } => "PFCOUNT",
        Command::Pfmerge { .. } => "PFMERGE",
        Command::PfdebugGetreg(_)
        | Command::PfdebugEncoding(_)
        | Command::PfdebugTodense(_)
        | Command::PfdebugSimd(_) => "PFDEBUG",
        Command::Pfselftest => "PFSELFTEST",
        Command::Dump(_) => "DUMP",
        Command::Restore { asking: true, .. } => "RESTORE-ASKING",
        Command::Restore { asking: false, .. } => "RESTORE",
        Command::Xadd { .. } => "XADD",
        Command::Xlen(_) => "XLEN",
        Command::Xrange { .. } => "XRANGE",
        Command::Xrevrange { .. } => "XREVRANGE",
        Command::Xread { .. } => "XREAD",
        Command::Xdel { .. } => "XDEL",
        Command::Xtrim { .. } => "XTRIM",
        Command::Xcfgset { .. } => "XCFGSET",
        Command::Xsetid { .. } => "XSETID",
        Command::Xdelex { .. } => "XDELEX",
        Command::Xidmprecord { .. } => "XIDMPRECORD",
        Command::XgroupCreate { .. }
        | Command::XgroupDestroy { .. }
        | Command::XgroupCreateConsumer { .. }
        | Command::XgroupDelConsumer { .. }
        | Command::XgroupSetId { .. }
        | Command::XgroupHelp => "XGROUP",
        Command::Xreadgroup { .. } => "XREADGROUP",
        Command::Xack { .. } => "XACK",
        Command::Xpending { .. } => "XPENDING",
        Command::Eval { read_only, .. } => {
            if *read_only {
                "EVAL_RO"
            } else {
                "EVAL"
            }
        }
        Command::Evalsha { read_only, .. } => {
            if *read_only {
                "EVALSHA_RO"
            } else {
                "EVALSHA"
            }
        }
        Command::ScriptLoad(_)
        | Command::ScriptExists(_)
        | Command::ScriptFlush
        | Command::ScriptKill => "SCRIPT",
        Command::Vadd { .. } => "VADD",
        Command::Vquery { .. } => "VQUERY",
        Command::Vsim { .. } | Command::Vdist { .. } => "VSIM",
        Command::Vdel { .. } => "VREM",
        Command::Vinfo(_) => "VINFO",
        Command::Vcard(_) => "VCARD",
        Command::Vdim(_) => "VDIM",
        Command::Vemb { .. } => "VEMB",
        Command::Vlinks { .. } => "VLINKS",
        Command::Vrandmember { .. } => "VRANDMEMBER",
        Command::Vsetattr { .. } => "VSETATTR",
        Command::Vgetattr { .. } => "VGETATTR",
        Command::Vismember { .. } => "VISMEMBER",
        Command::SemanticSet { .. }
        | Command::SemanticGet { .. }
        | Command::SemanticDel { .. }
        | Command::SemanticFlush(_)
        | Command::SemanticInfo(_) => "SEMANTIC",
        Command::AgentMemAdd { .. }
        | Command::AgentMemContext { .. }
        | Command::AgentMemCompact { .. }
        | Command::AgentMemInfo(_)
        | Command::AgentMemClear(_)
        | Command::AgentCheckpointPut { .. }
        | Command::AgentCheckpointGet { .. }
        | Command::AgentCheckpointHistory { .. }
        | Command::AgentToolClaim { .. }
        | Command::AgentToolComplete { .. } => "AGENT",
        Command::LlmQuotaReserve { .. }
        | Command::LlmQuotaSettle { .. }
        | Command::LlmQuotaInfo(_) => "LLM",
        Command::McpTools | Command::McpCall { .. } | Command::McpRpc(_) => "MCP",
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
        | Command::FunctionList { .. }
        | Command::FunctionDelete(_)
        | Command::FunctionFlush
        | Command::FunctionStats
        | Command::FunctionDump
        | Command::FunctionRestore { .. }
        | Command::FunctionKill => "FUNCTION",
        Command::Fcall { read_only, .. } => {
            if *read_only {
                "FCALL_RO"
            } else {
                "FCALL"
            }
        }
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
        Command::Geoadd { .. } => "GEOADD",
        Command::Geodist { .. } => "GEODIST",
        Command::Geopos { .. } => "GEOPOS",
        Command::Geohash { .. } => "GEOHASH",
        // The parser does not keep the _RO spelling; a store-less query is
        // what GEORADIUS_RO accepts, and is read-only either way.
        Command::Georadius {
            store: None,
            storedist: None,
            ..
        } => "GEORADIUS_RO",
        Command::Georadius { .. } => "GEORADIUS",
        Command::Georadiusbymember {
            store: None,
            storedist: None,
            ..
        } => "GEORADIUSBYMEMBER_RO",
        Command::Georadiusbymember { .. } => "GEORADIUSBYMEMBER",
        Command::Geosearch { .. } => "GEOSEARCH",
        Command::Geosearchstore { .. } => "GEOSEARCHSTORE",
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
        Command::ProbRestore { kind, .. } => match kind {
            crate::probabilistic::ProbKind::Bloom => "BF",
            crate::probabilistic::ProbKind::Cuckoo => "CF",
            crate::probabilistic::ProbKind::Cms => "CMS",
            crate::probabilistic::ProbKind::TopK => "TOPK",
        },
        Command::FtCreate { .. }
        | Command::FtSearch { .. }
        | Command::FtAggregate { .. }
        | Command::FtInfo(_)
        | Command::FtDropIndex { .. }
        | Command::FtExplain { .. }
        | Command::FtAdd { .. }
        | Command::FtList
        | Command::FtAlter { .. }
        | Command::FtProfile { .. } => "FT",
        Command::XdpInfo
        | Command::XdpRuleAdd { .. }
        | Command::XdpRuleDel(_)
        | Command::XdpRuleList
        | Command::XdpStats
        | Command::XdpSocket(_)
        | Command::XdpInject { .. }
        | Command::XdpPacket(_) => "XDP",
        Command::DflyCluster(_) => "DFLYCLUSTER",
        Command::DflyMigrate(_) => "DFLYMIGRATE",
        Command::DflyFlow { .. } => "DFLY FLOW",
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
        Command::Latency(_) => "LATENCY",
        Command::Debug(_) => "DEBUG",
        Command::Readonly => "READONLY",
        Command::Readwrite => "READWRITE",
        Command::Wait { .. } => "WAIT",
        Command::WaitAof { .. } => "WAITAOF",
        Command::Object(_) => "OBJECT",
        Command::Xinfo(_) => "XINFO",
        Command::Hexpire { .. } => "HEXPIRE",
        Command::Httl { .. } => "HTTL",
        Command::Hpersist { .. } => "HPERSIST",
        Command::Hgetex { .. } => "HGETEX",
        Command::Hsetex { .. } => "HSETEX",
        Command::Xclaim { .. } => "XCLAIM",
        Command::Xautoclaim { .. } => "XAUTOCLAIM",
        Command::Xackdel { .. } => "XACKDEL",
        Command::Xnack { .. } => "XNACK",
        Command::CommandCount
        | Command::CommandList
        | Command::CommandListFiltered { .. }
        | Command::CommandGetkeys(_)
        | Command::CommandGetkeysAndFlags(_)
        | Command::CommandInfo(_) => "COMMAND",
        Command::Unknown(_) => "UNKNOWN",
    }
}

/// Key permissions (`crate::acl::KEY_READ` / `KEY_WRITE`) `cmd` needs on each
/// of its keys, from the command table's key specs. SET without GET only
/// overwrites (Redis setGetKeys). Unknown commands need both.
pub fn acl_key_perm(cmd: &Command) -> u8 {
    use crate::acl::{KEY_READ, KEY_WRITE};
    if let Command::Set { get: false, .. } = cmd {
        return KEY_WRITE;
    }
    crate::acl::command_index(acl_cmd_name(cmd))
        .map(|i| crate::acl_categories::COMMANDS[i].key_flags)
        .unwrap_or(KEY_READ | KEY_WRITE)
}

/// Name used for ACL checks: like [`get_cmd_name`], but resolves the
/// subcommand (`container|sub`) for containers whose subcommands have
/// different ACL categories. Unresolved containers return the bare container
/// name, which the ACL layer only permits if every subcommand is permitted.
pub fn acl_cmd_name(cmd: &Command) -> &'static str {
    use crate::resp::{AclSubcommand as A, ClusterSubcommand as C, MemorySubcommand as M};
    match cmd {
        Command::Acl(sub) => match sub {
            A::List => "acl|list",
            A::Users => "acl|users",
            A::GetUser(_) => "acl|getuser",
            A::SetUser { .. } => "acl|setuser",
            A::DelUser(_) => "acl|deluser",
            A::WhoAmI => "acl|whoami",
            A::Cat(_) => "acl|cat",
            A::GenPass(_) => "acl|genpass",
            A::DryRun { .. } => "acl|dryrun",
            A::Log(_) | A::LogReset => "acl|log",
            A::Save => "acl|save",
            A::Load => "acl|load",
            A::Help => "acl|help",
        },
        Command::Cluster(sub) => match sub {
            C::KeySlot(_) => "cluster|keyslot",
            C::CountKeysInSlot(_) => "cluster|countkeysinslot",
            C::GetKeysInSlot(..) => "cluster|getkeysinslot",
            C::Slots => "cluster|slots",
            C::Shards => "cluster|shards",
            C::Links => "cluster|links",
            C::Nodes => "cluster|nodes",
            C::Info => "cluster|info",
            C::MyId => "cluster|myid",
            C::Migration(_) => "cluster|migration",
            _ => "cluster",
        },
        Command::ConfigGet(_) => "config|get",
        Command::ConfigSet(_) => "config|set",
        Command::FunctionLoad { .. } => "function|load",
        Command::FunctionDump => "function|dump",
        Command::FunctionRestore { .. } => "function|restore",
        Command::FunctionList { .. } => "function|list",
        Command::FunctionFlush => "function|flush",
        Command::FunctionStats => "function|stats",
        Command::FunctionKill => "function|kill",
        Command::FunctionDelete(_) => "function|delete",
        Command::ScriptLoad(_) => "script|load",
        Command::ScriptExists(_) => "script|exists",
        Command::ScriptFlush => "script|flush",
        Command::ScriptKill => "script|kill",
        Command::Memory(sub) => match sub {
            M::Usage { .. } => "memory|usage",
            M::Stats => "memory|stats",
            M::Purge => "memory|purge",
            M::Doctor => "memory|doctor",
            M::Defrag => "memory",
            M::MallocStats => "memory|malloc-stats",
        },
        Command::Commandlog(args) => {
            match args.first().map(|a| a.to_ascii_lowercase()).as_deref() {
                Some(b"get") => "commandlog|get",
                Some(b"len") => "commandlog|len",
                Some(b"reset") => "commandlog|reset",
                Some(b"help") => "commandlog|help",
                _ => "commandlog",
            }
        }
        Command::Lolwut(_) => "lolwut",
        Command::Sentinel(_) => "sentinel",
        Command::Unknown(s) => match s.as_str() {
            "SCRIPT HELP" => "script|help",
            "CONFIG HELP" => "config|help",
            "CLIENT HELP" => "client|help",
            "CLUSTER HELP" => "cluster|help",
            "COMMAND HELP" => "command|help",
            "FUNCTION HELP" => "function|help",
            "LATENCY HELP" => "latency|help",
            "MEMORY HELP" => "memory|help",
            "MODULE HELP" => "module|help",
            "OBJECT HELP" => "object|help",
            "PUBSUB HELP" => "pubsub|help",
            "SLOWLOG HELP" => "slowlog|help",
            "XGROUP HELP" => "xgroup|help",
            "XINFO HELP" => "xinfo|help",
            _ => "UNKNOWN",
        },
        _ => get_cmd_name(cmd),
    }
}

pub fn write_command_getkeys(out: &mut Vec<u8>, cmd_args: &[Bytes]) {
    if cmd_args.is_empty() {
        out.extend_from_slice(b"-ERR Invalid number of arguments specified for command\r\n");
        return;
    }
    match crate::resp::build_command(cmd_args.to_vec()) {
        Ok(Some(Command::Unknown(_))) | Ok(None) => {
            out.extend_from_slice(b"-ERR Invalid command specified\r\n");
        }
        Err(_) => {
            out.extend_from_slice(b"-ERR Invalid arguments specified for command\r\n");
        }
        Ok(Some(inner_cmd)) => {
            let mut keys = Vec::new();
            for_each_cmd_key(&inner_cmd, |k| keys.push(Bytes::copy_from_slice(k)));
            write_resp_array_header(out, keys.len());
            for k in keys {
                write_resp_bulk(out, &k);
            }
        }
    }
}

pub fn write_command_getkeys_and_flags(out: &mut Vec<u8>, cmd_args: &[Bytes]) {
    if cmd_args.is_empty() {
        out.extend_from_slice(b"-ERR Invalid number of arguments specified for command\r\n");
        return;
    }
    match crate::resp::build_command(cmd_args.to_vec()) {
        Ok(Some(Command::Unknown(_))) | Ok(None) => {
            out.extend_from_slice(b"-ERR Invalid command specified\r\n");
        }
        Err(_) => {
            out.extend_from_slice(b"-ERR Invalid arguments specified for command\r\n");
        }
        Ok(Some(inner_cmd)) => {
            let mut items: Vec<(Bytes, &'static [&'static str])> = Vec::new();
            match &inner_cmd {
                Command::Set {
                    key,
                    condition,
                    get,
                    ..
                } => {
                    let flags: &'static [&'static str] = if *get {
                        &["RW", "access", "update"]
                    } else if !matches!(condition, crate::resp::SetCondition::None) {
                        &["RW", "update"]
                    } else {
                        &["OW", "update"]
                    };
                    items.push((key.clone(), flags));
                }
                Command::Mset(pairs) | Command::Msetex { pairs, .. } => {
                    for (k, _) in pairs {
                        items.push((k.clone(), &["OW", "update"]));
                    }
                }
                Command::Lmove {
                    source,
                    destination,
                    ..
                }
                | Command::Blmove {
                    source,
                    destination,
                    ..
                } => {
                    items.push((source.clone(), &["RW", "access", "delete"]));
                    items.push((destination.clone(), &["RW", "insert"]));
                }
                Command::Sort { key, store, .. } => {
                    items.push((key.clone(), &["RO", "access"]));
                    if let Some(dst) = store {
                        items.push((dst.clone(), &["OW", "update"]));
                    }
                }
                Command::Delex { key, condition } => {
                    let flags: &'static [&'static str] = if condition.is_some() {
                        &["RW", "delete"]
                    } else {
                        &["RM", "delete"]
                    };
                    items.push((key.clone(), flags));
                }
                _ => {
                    let default_flags: &'static [&'static str] = if inner_cmd.is_write_command() {
                        &["RW", "access", "update"]
                    } else {
                        &["RO", "access"]
                    };
                    for_each_cmd_key(&inner_cmd, |k| {
                        items.push((Bytes::copy_from_slice(k), default_flags))
                    });
                }
            }
            write_resp_array_header(out, items.len());
            for (k, flags) in items {
                write_resp_array_header(out, 2);
                write_resp_bulk(out, &k);
                write_resp_array_header(out, flags.len());
                for f in flags {
                    write_resp_bulk(out, f.as_bytes());
                }
            }
        }
    }
}

pub fn pattern_subst_and_field(pattern: &[u8], item: &[u8]) -> (Bytes, Option<Bytes>) {
    let mut arrow_pos = None;
    if let Some(star_pos) = pattern.iter().position(|&b| b == b'*') {
        if let Some(arrow_rel) = pattern[star_pos..].windows(2).position(|w| w == b"->") {
            let actual_arrow = star_pos + arrow_rel;
            if actual_arrow + 2 < pattern.len() {
                arrow_pos = Some(actual_arrow);
            }
        }
    } else if let Some(arrow) = pattern.windows(2).position(|w| w == b"->")
        && arrow + 2 < pattern.len()
    {
        arrow_pos = Some(arrow);
    }

    let (key_pat, field) = match arrow_pos {
        Some(pos) => (
            &pattern[..pos],
            Some(Bytes::copy_from_slice(&pattern[pos + 2..])),
        ),
        None => (pattern, None),
    };

    if let Some(star_idx) = key_pat.iter().position(|&b| b == b'*') {
        let mut key_buf = Vec::with_capacity(key_pat.len() + item.len());
        key_buf.extend_from_slice(&key_pat[..star_idx]);
        key_buf.extend_from_slice(item);
        key_buf.extend_from_slice(&key_pat[star_idx + 1..]);
        (Bytes::from(key_buf), field)
    } else {
        (Bytes::copy_from_slice(key_pat), field)
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
                            if let Some(aof_w) = &router.aof() {
                                aof_w.borrow_mut().append(&bytes);
                            }
                            if crate::replication::has_connected_replicas(router.base_port) {
                                crate::replication::propagate_shard_bytes(
                                    router.base_port,
                                    router.shard_id,
                                    &bytes,
                                );
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

    if in_tx(client_id) {
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
        let mut hub = hub_arc.lock();
        hub.register_blocked_zset_client(client_id, tx.clone());
        for k in keys.iter() {
            hub.register_zset_waiter(client_id, k.clone(), pop_type, 1, false, tx.clone());
        }
    }
    recheck_blocking_keys(router, keys.iter());

    let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
    let (recv_res, client_disconnected) = wait_for_blocked_result(&rx, timeout, raw_fd).await;
    if client_disconnected {
        return true;
    }

    match recv_res {
        Some(crate::block::BlockedZSetResult::Popped { key, mut items, .. }) => {
            // The block hub logged and replicated the pop on the key's shard.
            if let Some((m, s)) = items.pop() {
                format_bzpop_response(out, &key, &m, s);
            } else {
                write_resp_null_array(out);
            }
        }
        Some(crate::block::BlockedZSetResult::Unblocked(
            crate::block::ClientUnblockType::NoPerm,
        )) => {
            out.extend_from_slice(BLOCKED_NOPERM_REPLY);
        }
        Some(crate::block::BlockedZSetResult::Unblocked(
            crate::block::ClientUnblockType::Error,
        )) => {
            out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
        }
        Some(crate::block::BlockedZSetResult::Unblocked(
            crate::block::ClientUnblockType::Moved(slot),
        )) => {
            blocked_moved_reply(router, slot, out);
        }
        _ => {
            write_resp_null_array(out);
        }
    }
    false
}

async fn fetch_remote_set(
    router: &Router,
    key: &Bytes,
) -> Result<hashbrown::HashSet<Bytes>, Vec<u8>> {
    let target = router.target_shard(key);
    let cmd = Command::Smembers(key.clone());
    let raw = exec_on_shard_resp2(router, target, cmd).await;
    if raw.starts_with(b"-") {
        return Err(raw);
    }
    let mut set = hashbrown::HashSet::new();
    let mut pos = 0usize;
    if pos < raw.len()
        && raw[pos] == b'*'
        && let Some(nl) = raw[pos..].iter().position(|&b| b == b'\n')
    {
        pos += nl + 1;
        while pos < raw.len() {
            if raw[pos] != b'$' {
                break;
            }
            let Some(len_nl) = raw[pos..].iter().position(|&b| b == b'\n') else {
                break;
            };
            let len_str = std::str::from_utf8(&raw[pos + 1..pos + len_nl - 1]).unwrap_or("0");
            let bulk_len: usize = len_str.parse().unwrap_or(0);
            pos += len_nl + 1;
            if pos + bulk_len + 2 > raw.len() {
                break;
            }
            let val = Bytes::copy_from_slice(&raw[pos..pos + bulk_len]);
            pos += bulk_len + 2;
            set.insert(val);
        }
    }
    Ok(set)
}

/// Runs `cmd` on `shard` with its reply in RESP2, for callers that parse
/// it (a RESP3 client would otherwise get maps, sets and doubles there).
async fn exec_on_shard_resp2(router: &Router, shard: usize, cmd: Command) -> Vec<u8> {
    if shard == router.shard_id {
        let prev = CURRENT_CLIENT_RESP3.replace(false);
        let mut out = Vec::new();
        execute_local_command(&cmd, &mut router.local_db.borrow_mut(), &mut out, None);
        CURRENT_CLIENT_RESP3.set(prev);
        out
    } else {
        router.execute_remote_proto(shard, cmd, false).await
    }
}

async fn fetch_key_type(router: &Router, key: &Bytes) -> String {
    let target = router.target_shard(key);
    let type_cmd = Command::Type(key.clone());
    let t_raw = exec_on_shard_resp2(router, target, type_cmd).await;
    if t_raw.starts_with(b"+") {
        String::from_utf8_lossy(&t_raw[1..t_raw.len().saturating_sub(2)]).to_string()
    } else {
        "none".to_string()
    }
}

async fn fetch_remote_zset(
    router: &Router,
    key: &Bytes,
) -> Result<hashbrown::HashMap<Bytes, f64>, Vec<u8>> {
    let target = router.target_shard(key);
    let type_cmd = Command::Type(key.clone());
    let t_raw = exec_on_shard_resp2(router, target, type_cmd).await;
    if t_raw.as_slice() == b"+none\r\n" {
        return Ok(hashbrown::HashMap::new());
    }
    if t_raw.as_slice() == b"+set\r\n" {
        let set = fetch_remote_set(router, key).await?;
        return Ok(set.into_iter().map(|m| (m, 1.0)).collect());
    }
    if t_raw.as_slice() != b"+zset\r\n" {
        return Err(
            b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n".to_vec(),
        );
    }
    let zrange_cmd = Command::Zrange {
        key: key.clone(),
        opts: crate::table::ZRangeOpts {
            start: 0,
            stop: -1,
            with_scores: true,
            ..crate::table::ZRangeOpts::default()
        },
    };
    let raw = exec_on_shard_resp2(router, target, zrange_cmd).await;
    if raw.starts_with(b"-") {
        return Err(raw);
    }
    let mut items = Vec::new();
    let mut pos = 0usize;
    if pos < raw.len()
        && raw[pos] == b'*'
        && let Some(nl) = raw[pos..].iter().position(|&b| b == b'\n')
    {
        pos += nl + 1;
        while pos < raw.len() {
            if raw[pos] != b'$' {
                break;
            }
            let Some(len_nl) = raw[pos..].iter().position(|&b| b == b'\n') else {
                break;
            };
            let len_str = std::str::from_utf8(&raw[pos + 1..pos + len_nl - 1]).unwrap_or("0");
            let bulk_len: usize = len_str.parse().unwrap_or(0);
            pos += len_nl + 1;
            if pos + bulk_len + 2 > raw.len() {
                break;
            }
            let val = Bytes::copy_from_slice(&raw[pos..pos + bulk_len]);
            pos += bulk_len + 2;
            items.push(val);
        }
    }
    let mut map = hashbrown::HashMap::with_capacity(items.len() / 2);
    for pair in items.as_chunks::<2>().0 {
        let score: f64 = std::str::from_utf8(&pair[1])
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.0);
        map.insert(pair[0].clone(), score);
    }
    Ok(map)
}

fn extract_stream_blocks(raw: &[u8]) -> Vec<&[u8]> {
    let mut blocks = Vec::new();
    if raw.is_empty()
        || raw.starts_with(b"$-1")
        || raw.starts_with(b"_\r\n")
        || raw.starts_with(b"*0\r\n")
        || raw.starts_with(b"-")
    {
        return blocks;
    }
    if raw[0] != b'*' {
        return blocks;
    }
    let Some(first_nl) = raw.iter().position(|&b| b == b'\n') else {
        return blocks;
    };
    let mut pos = first_nl + 1;
    while pos < raw.len() {
        if raw[pos] != b'*' {
            break;
        }
        let block_start = pos;
        let Some(nl1) = raw[pos..].iter().position(|&b| b == b'\n') else {
            break;
        };
        pos += nl1 + 1;
        if pos >= raw.len() || raw[pos] != b'$' {
            break;
        }
        let Some(nl2) = raw[pos..].iter().position(|&b| b == b'\n') else {
            break;
        };
        let klen: usize = std::str::from_utf8(&raw[pos + 1..pos + nl2 - 1])
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);
        pos += nl2 + 1 + klen + 2;
        if pos >= raw.len() || raw[pos] != b'*' {
            break;
        }
        let Some(nl3) = raw[pos..].iter().position(|&b| b == b'\n') else {
            break;
        };
        let num_entries: usize = std::str::from_utf8(&raw[pos + 1..pos + nl3 - 1])
            .unwrap_or("0")
            .parse()
            .unwrap_or(0);
        pos += nl3 + 1;
        let mut ok = true;
        for _ in 0..num_entries {
            if pos >= raw.len() || raw[pos] != b'*' {
                ok = false;
                break;
            }
            let Some(enl1) = raw[pos..].iter().position(|&b| b == b'\n') else {
                ok = false;
                break;
            };
            pos += enl1 + 1;
            if pos >= raw.len() || raw[pos] != b'$' {
                ok = false;
                break;
            }
            let Some(enl2) = raw[pos..].iter().position(|&b| b == b'\n') else {
                ok = false;
                break;
            };
            let idlen: usize = std::str::from_utf8(&raw[pos + 1..pos + enl2 - 1])
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            pos += enl2 + 1 + idlen + 2;
            if pos >= raw.len() || raw[pos] != b'*' {
                ok = false;
                break;
            }
            let Some(enl3) = raw[pos..].iter().position(|&b| b == b'\n') else {
                ok = false;
                break;
            };
            let num_fields_kv: usize = std::str::from_utf8(&raw[pos + 1..pos + enl3 - 1])
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            pos += enl3 + 1;
            for _ in 0..num_fields_kv {
                if pos >= raw.len() || raw[pos] != b'$' {
                    ok = false;
                    break;
                }
                let Some(fnl) = raw[pos..].iter().position(|&b| b == b'\n') else {
                    ok = false;
                    break;
                };
                let flen: usize = std::str::from_utf8(&raw[pos + 1..pos + fnl - 1])
                    .unwrap_or("0")
                    .parse()
                    .unwrap_or(0);
                pos += fnl + 1 + flen + 2;
            }
            if !ok {
                break;
            }
        }
        if !ok {
            break;
        }
        let block_end = pos;
        blocks.push(&raw[block_start..block_end]);
    }
    blocks
}

async fn query_cross_shard_streams(
    router: &Router,
    cmd: &Command,
) -> Result<Vec<Vec<u8>>, Vec<u8>> {
    let (keys, ids) = match cmd {
        Command::Xread { keys, ids, .. } => (keys, ids),
        Command::Xreadgroup { keys, ids, .. } => (keys, ids),
        _ => return Ok(Vec::new()),
    };
    let mut shard_keys: hashbrown::HashMap<usize, (Vec<Bytes>, Vec<String>)> =
        hashbrown::HashMap::new();
    for (k, id) in keys.iter().zip(ids.iter()) {
        let s = router.target_shard(k);
        let entry = shard_keys.entry(s).or_default();
        entry.0.push(k.clone());
        entry.1.push(id.clone());
    }

    let mut shard_ids: Vec<usize> = shard_keys.keys().copied().collect();
    shard_ids.sort_unstable();

    let mut all_stream_blocks = Vec::new();
    for s in shard_ids {
        let (sub_keys, sub_ids) = shard_keys.remove(&s).unwrap();
        let sub_cmd = match cmd {
            Command::Xread {
                count,
                maxcount,
                maxsize,
                ..
            } => Command::Xread {
                count: *count,
                maxcount: *maxcount,
                maxsize: *maxsize,
                block_ms: None,
                keys: sub_keys,
                ids: sub_ids,
            },
            Command::Xreadgroup {
                group,
                consumer,
                count,
                maxcount,
                maxsize,
                noack,
                claim,
                ..
            } => Command::Xreadgroup {
                group: group.clone(),
                consumer: consumer.clone(),
                count: *count,
                maxcount: *maxcount,
                maxsize: maxsize.clone(),
                block_ms: None,
                noack: *noack,
                claim: claim.clone(),
                keys: sub_keys,
                ids: sub_ids,
            },
            _ => continue,
        };

        let raw = if s == router.shard_id {
            let mut tmp = Vec::new();
            execute_local_command(
                &sub_cmd,
                &mut router.local_db.borrow_mut(),
                &mut tmp,
                router.aof().as_deref(),
            );
            tmp
        } else {
            router.execute_remote(s, sub_cmd).await
        };

        if raw.starts_with(b"-") {
            return Err(raw);
        }
        let blocks = extract_stream_blocks(&raw);
        for b in blocks {
            all_stream_blocks.push(b.to_vec());
        }
    }
    Ok(all_stream_blocks)
}

fn extract_xinfo_last_id(raw: &[u8]) -> Option<String> {
    const NEEDLE: &[u8] = b"last-generated-id\r\n$";
    let pos = raw.windows(NEEDLE.len()).position(|w| w == NEEDLE)?;
    let rem = &raw[pos + NEEDLE.len()..];
    let nl = rem.iter().position(|&b| b == b'\n')?;
    let len_str = std::str::from_utf8(&rem[..nl.saturating_sub(1)]).ok()?;
    let val_len: usize = len_str.parse().ok()?;
    let val_start = nl + 1;
    if val_start + val_len <= rem.len() {
        let id_str = std::str::from_utf8(&rem[val_start..val_start + val_len]).ok()?;
        Some(id_str.to_string())
    } else {
        None
    }
}

async fn resolve_stream_last_ids(router: &Router, keys: &[Bytes], ids: &[String]) -> Vec<String> {
    let mut resolved = Vec::with_capacity(ids.len());
    for (k, id) in keys.iter().zip(ids.iter()) {
        if id != "$" && id != "+" {
            resolved.push(id.clone());
            continue;
        }
        let target = router.target_shard(k);
        let last_id = if target == router.shard_id {
            router
                .local_db
                .borrow_mut()
                .table
                .stream_last_id(k)
                .map(|sid| sid.to_string())
        } else {
            let res = router
                .execute_remote(
                    target,
                    Command::Xinfo(crate::resp::XinfoSubcommand::Stream(k.clone())),
                )
                .await;
            extract_xinfo_last_id(&res)
        };
        resolved.push(last_id.unwrap_or_else(|| "0-0".to_string()));
    }
    resolved
}

/// Commands that move a connection out of the normal request loop.
fn is_mode_switch_cmd(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Subscribe(_)
            | Command::Psubscribe(_)
            | Command::Ssubscribe(_)
            | Command::Psync { .. }
            | Command::Sync
            | Command::DflyFlow { .. }
    ) || is_syncslots_start(cmd)
}

/// `CLUSTER SYNCSLOTS START ...`: a node importing slots from this one
/// (see `crate::asm::run_migrate_source`).
fn is_syncslots_start(cmd: &Command) -> bool {
    matches!(cmd, Command::SyncSlots(args) if args.first().is_some_and(|a| a.eq_ignore_ascii_case(b"START")))
}

const NOPERM_CHANNEL: &[u8] =
    b"-NOPERM this user has no permissions to access one of the channels used as arguments\r\n";

/// Extracts the first argument for commands that support first-arg ACL rules
/// (`+select|0`, `+debug|object`).
pub fn acl_first_arg(cmd: &Command) -> Option<Vec<u8>> {
    match cmd {
        Command::Select(db) => Some(db.to_string().into_bytes()),
        Command::Debug(args) => args.first().map(|a| a.to_vec()),
        _ => None,
    }
}

/// Checks whether `user` is permitted to execute `cmd` without recording
/// metrics or `ACL LOG` entries (used by `ACL DRYRUN` and pipeline squashing).
pub fn acl_check_user_cmd(user: &crate::acl::AclUser, cmd: &Command) -> Option<Vec<u8>> {
    let acl_name = acl_cmd_name(cmd);
    let first_arg = acl_first_arg(cmd);
    if !user.can_execute_command_with_first_arg(acl_name, first_arg.as_deref()) {
        return Some(
            format!(
                "-NOPERM this user has no permissions to run the '{}' command\r\n",
                err_arg(&acl_name.to_lowercase())
            )
            .into_bytes(),
        );
    }
    let need = acl_key_perm(cmd);
    for key in cmd_keys(cmd) {
        if !user.can_access_key(key, need) {
            return Some(
                b"-NOPERM this user has no permissions to access one of the keys used as arguments\r\n"
                    .to_vec(),
            );
        }
    }
    if acl_first_denied_channel(user, cmd).is_some() {
        return Some(NOPERM_CHANNEL.to_vec());
    }
    None
}

fn format_acl_log_client_info(auth_user: &str, cmd_for_info: &str) -> String {
    let cid = requesting_client_id();
    format!(
        "id={} addr=127.0.0.1:0 laddr=127.0.0.1:6379 fd=0 name= age=0 idle=0 flags=N db={} sub=0 psub=0 ssub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 multi-mem=0 rbs=0 rbp=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd={} user={}",
        cid,
        CURRENT_DB.get(),
        cmd_for_info.to_ascii_lowercase(),
        auth_user
    )
}

pub fn record_acl_denial(
    port: u16,
    reason: crate::acl::AclDeniedReason,
    context: &str,
    object: &str,
    username: &str,
    cmd_for_info: &str,
) {
    use crate::acl::AclDeniedReason;
    use crate::server_stats::{Stat, add};
    match reason {
        AclDeniedReason::Auth => add(Stat::AclAccessDeniedAuth, 1),
        AclDeniedReason::Command => add(Stat::AclAccessDeniedCmd, 1),
        AclDeniedReason::Key => add(Stat::AclAccessDeniedKey, 1),
        AclDeniedReason::Channel => add(Stat::AclAccessDeniedChannel, 1),
    }
    let client_info = format_acl_log_client_info(username, cmd_for_info);
    let acl = crate::acl::get_acl_for_port(port);
    acl.write()
        .add_log_entry(reason, context, object, username, client_info);
}

/// The ACL check every command goes through (Redis `ACLCheckAllPerm`):
/// command, key and channel permissions of `auth_user`. Returns the NOPERM
/// reply if denied. Free when no custom ACL exists and the user is default.
pub fn acl_denied(port: u16, auth_user: &str, cmd: &Command) -> Option<Vec<u8>> {
    acl_denied_in_context(port, auth_user, cmd, None)
}

pub fn acl_denied_in_context(
    port: u16,
    auth_user: &str,
    cmd: &Command,
    explicit_ctx: Option<(&str, &str)>,
) -> Option<Vec<u8>> {
    if !crate::acl::HAS_CUSTOM_ACL.load(std::sync::atomic::Ordering::Relaxed)
        && auth_user == "default"
    {
        return None;
    }
    let acl_name = acl_cmd_name(cmd);
    let is_exec = explicit_ctx.is_none() && in_tx(requesting_client_id());
    let (ctx, info_cmd) = explicit_ctx.unwrap_or(if is_exec {
        ("multi", "exec")
    } else {
        ("toplevel", acl_name)
    });
    let denial = {
        let acl = crate::acl::get_acl_for_port(port);
        let guard = acl.read();
        let user = guard.users.get(auth_user)?;
        let first_arg = acl_first_arg(cmd);
        if !user.can_execute_command_with_first_arg(acl_name, first_arg.as_deref()) {
            let lower = acl_name.to_lowercase();
            let reply = if is_exec {
                b"-NOPERM ACLs rules changed between the moment the transaction was accumulated and the EXEC call. This command is no longer allowed for the following reason: no permission to execute the command or subcommand\r\n".to_vec()
            } else {
                format!(
                    "-NOPERM this user has no permissions to run the '{}' command\r\n",
                    err_arg(&lower)
                )
                .into_bytes()
            };
            Some((crate::acl::AclDeniedReason::Command, lower, reply))
        } else {
            let need = acl_key_perm(cmd);
            let mut denied_key: Option<String> = None;
            for key in cmd_keys(cmd) {
                if !user.can_access_key(key, need) {
                    denied_key = Some(String::from_utf8_lossy(key).into_owned());
                    break;
                }
            }
            if let Some(k) = denied_key {
                let reply = if is_exec {
                    b"-NOPERM ACLs rules changed between the moment the transaction was accumulated and the EXEC call. This command is no longer allowed for the following reason: no permission to touch the specified keys\r\n".to_vec()
                } else {
                    b"-NOPERM this user has no permissions to access one of the keys used as arguments\r\n".to_vec()
                };
                Some((crate::acl::AclDeniedReason::Key, k, reply))
            } else {
                acl_first_denied_channel(user, cmd).map(|ch| {
                    let reply = if is_exec {
                        b"-NOPERM ACLs rules changed between the moment the transaction was accumulated and the EXEC call. This command is no longer allowed for the following reason: no permission to access one of the channels used as arguments\r\n".to_vec()
                    } else {
                        NOPERM_CHANNEL.to_vec()
                    };
                    (
                        crate::acl::AclDeniedReason::Channel,
                        String::from_utf8_lossy(ch).into_owned(),
                        reply,
                    )
                })
            }
        }
    };
    if let Some((reason, obj, reply)) = denial {
        record_acl_denial(port, reason, ctx, &obj, auth_user, info_cmd);
        Some(reply)
    } else {
        None
    }
}

/// First Pub/Sub channel (or PSUBSCRIBE pattern) in `cmd` that `user` may not
/// access, or `None` if all are permitted.
pub fn acl_first_denied_channel<'a>(
    user: &crate::acl::AclUser,
    cmd: &'a Command,
) -> Option<&'a [u8]> {
    if user.all_channels {
        return None;
    }
    let (channels, is_pattern): (&[Bytes], bool) = match cmd {
        Command::Subscribe(c) | Command::Ssubscribe(c) => (c, false),
        Command::Psubscribe(p) => (p, true),
        Command::Publish { channel, .. } | Command::Spublish { channel, .. } => {
            (std::slice::from_ref(channel), false)
        }
        _ => return None,
    };
    channels
        .iter()
        .find(|c| !user.can_access_channel(c, is_pattern))
        .map(|c| c.as_ref())
}

/// True if `cmd` names a Pub/Sub channel (or PSUBSCRIBE pattern) the user's
/// `&` permissions do not cover.
pub fn acl_channels_denied(user: &crate::acl::AclUser, cmd: &Command) -> bool {
    acl_first_denied_channel(user, cmd).is_some()
}

/// Disconnects another client of the server on `port` (ACL DELUSER, ACL
/// LOAD, a revoked subscription): marks it killed, shuts its socket down so
/// its task ends, and drops any blocked-command wait it has.
fn kill_registered_client(port: u16, cid: u64, entry: &GlobalClientEntry) {
    if entry
        .stats
        .killed
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        )
        .is_ok()
    {
        ACTIVE_CLIENTS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
    // SAFETY: shutdown(2) takes no pointers; the caller holds the
    // GLOBAL_CLIENTS lock, under which raw_fd is the client's open socket.
    unsafe {
        libc::shutdown(entry.raw_fd, libc::SHUT_RDWR);
    }
    crate::block::get_block_hub_for_port(port)
        .lock()
        .remove_waiters_for_client(cid);
}

/// The clients in `hub` (one shard's Pub/Sub registry) subscribed to a
/// channel, pattern or shard channel their user may no longer access, or
/// whose user is gone.
fn revoked_subscribers(port: u16, hub: &crate::pubsub::PubSubHub) -> Vec<u64> {
    let acl = crate::acl::get_acl_for_port(port);
    let acl = acl.read();
    let clients = GLOBAL_CLIENTS.read();
    let denied = |subs: &hashbrown::HashMap<u64, hashbrown::HashSet<Bytes>>,
                  cid: u64,
                  user: &crate::acl::AclUser,
                  is_pattern: bool| {
        subs.get(&cid)
            .is_some_and(|s| s.iter().any(|c| !user.can_access_channel(c, is_pattern)))
    };
    let mut ids: Vec<u64> = hub
        .client_channels
        .keys()
        .chain(hub.client_patterns.keys())
        .chain(hub.client_shard_channels.keys())
        .copied()
        .collect();
    ids.sort_unstable();
    ids.dedup();
    ids.retain(|&cid| {
        let Some(entry) = clients.get(&(port, cid)) else {
            return false;
        };
        match acl.users.get(&entry.auth_user) {
            None => true,
            Some(user) if user.all_channels => false,
            Some(user) => {
                denied(&hub.client_channels, cid, user, false)
                    || denied(&hub.client_shard_channels, cid, user, false)
                    || denied(&hub.client_patterns, cid, user, true)
            }
        }
    });
    ids
}

/// Redis ACLKillPubsubClientsIfNeeded, for one shard: after ACL SETUSER or
/// ACL LOAD, disconnects the clients in `hub` subscribed to anything their
/// user may no longer access, so a revoked user stops receiving messages.
/// Their subscriptions go at once, as Redis frees such a client before the
/// ACL command replies: left to each connection's own cleanup, a PUBLISH
/// right after the +OK still counted (and tried) the revoked subscribers.
/// Returns whether `self_id`, the client that ran the ACL command, is one of
/// them: it gets its reply first, then its caller closes it.
pub fn kill_revoked_subscribers(
    port: u16,
    hub: &mut crate::pubsub::PubSubHub,
    presence: Option<(usize, &crate::pubsub::ShardedPresenceTable)>,
    self_id: u64,
) -> bool {
    if hub.client_channels.is_empty()
        && hub.client_patterns.is_empty()
        && hub.client_shard_channels.is_empty()
    {
        return false;
    }
    let revoked = revoked_subscribers(port, hub);
    let mut kill_self = false;
    {
        let clients = GLOBAL_CLIENTS.read();
        for &cid in &revoked {
            if cid == self_id {
                kill_self = true;
            } else if let Some(entry) = clients.get(&(port, cid)) {
                kill_registered_client(port, cid, entry);
            }
        }
    }
    for cid in revoked {
        hub.remove_client_with_presence(cid, presence);
    }
    kill_self
}

/// Clients whose blocked command was refused when data arrived: their
/// command counts as a rejected call, not a call (`CmdStatGuard`).
static BLOCKED_REJECTED: std::sync::LazyLock<parking_lot::Mutex<Vec<(u16, u64)>>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(Vec::new()));
static ANY_BLOCKED_REJECTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Notes that client `cid`'s blocked command was refused (see
/// [`BLOCKED_REJECTED`]).
fn note_blocked_rejected(port: u16, cid: u64) {
    BLOCKED_REJECTED.lock().push((port, cid));
    ANY_BLOCKED_REJECTED.store(true, std::sync::atomic::Ordering::Release);
}

/// Whether client `cid`'s blocked command was refused (the note stays).
fn blocked_rejected_pending(port: u16, cid: u64) -> bool {
    ANY_BLOCKED_REJECTED.load(std::sync::atomic::Ordering::Acquire)
        && BLOCKED_REJECTED.lock().contains(&(port, cid))
}

/// Whether client `cid`'s blocked command was refused; clears the note.
fn take_blocked_rejected(port: u16, cid: u64) -> bool {
    if !ANY_BLOCKED_REJECTED.load(std::sync::atomic::Ordering::Acquire) {
        return false;
    }
    let mut v = BLOCKED_REJECTED.lock();
    let Some(i) = v.iter().position(|&e| e == (port, cid)) else {
        return false;
    };
    v.swap_remove(i);
    if v.is_empty() {
        ANY_BLOCKED_REJECTED.store(false, std::sync::atomic::Ordering::Release);
    }
    true
}

/// The ACL user client `cid` of the server on `port` is authenticated as,
/// for the block hub's checks: `None` when no ACL beyond the default
/// all-access user exists (nothing to check).
fn blocked_client_user(port: u16, cid: u64) -> Option<String> {
    if !crate::acl::HAS_CUSTOM_ACL.load(std::sync::atomic::Ordering::Relaxed) {
        return None;
    }
    GLOBAL_CLIENTS
        .read()
        .get(&(port, cid))
        .map(|e| e.auth_user.clone())
}

/// For the block hub, about to serve a client blocked on list or sorted
/// set `keys`: whether its user lost read/write access to one of them since
/// it blocked (an ACL SETUSER in between). Redis reprocesses the command
/// when data arrives, ACL check included; the client is refused with
/// NOPERM instead of being served. (Only keys are checked: the waiter
/// doesn't record which blocking command it runs.)
pub fn blocked_client_denied<'a>(
    port: u16,
    cid: u64,
    mut keys: impl Iterator<Item = &'a [u8]>,
) -> bool {
    let Some(user) = blocked_client_user(port, cid) else {
        return false;
    };
    let acl = crate::acl::get_acl_for_port(port);
    let acl = acl.read();
    let denied = match acl.users.get(&user) {
        None => true,
        Some(u) => keys.any(|k| !u.can_access_key(k, crate::acl::KEY_READ | crate::acl::KEY_WRITE)),
    };
    if denied {
        note_blocked_rejected(port, cid);
    }
    denied
}

/// [`blocked_client_denied`] for a blocked XREAD / XREADGROUP, whose full
/// command the waiter keeps: the NOPERM reply if its user may no longer
/// run it.
pub fn blocked_stream_client_denied(port: u16, cid: u64, cmd: &Command) -> Option<Vec<u8>> {
    let user = blocked_client_user(port, cid)?;
    let err = acl_denied_in_context(port, &user, cmd, Some(("toplevel", acl_cmd_name(cmd))))?;
    note_blocked_rejected(port, cid);
    Some(err)
}

/// The reply of a blocked command its user lost key access for.
pub const BLOCKED_NOPERM_REPLY: &[u8] = b"-NOPERM No permissions to access a key\r\n";

/// The reply to a client unblocked because its key's `slot` moved to
/// another node (`ClientUnblockType::Moved`): -MOVED to the slot's owner
/// as this node knows it, as Redis answers when it reprocesses the
/// command, or -CLUSTERDOWN if no node serves it.
pub(crate) fn blocked_moved_reply(router: &Router, slot: u16, out: &mut Vec<u8>) {
    let hub = crate::cluster::get_cluster_hub(router.base_port);
    // Unblocked because the cluster went down (see
    // `ClusterHub::unblock_all_for_cluster_down`), as Redis answers then.
    if hub.is_down() {
        out.extend_from_slice(b"-CLUSTERDOWN The cluster is down\r\n");
        return;
    }
    let nodes = hub.nodes.read();
    match nodes.values().find(|n| {
        n.is_master()
            && n.fail != crate::cluster::FailState::Fail
            && n.slots.iter().any(|&(s, e)| slot >= s && slot <= e)
    }) {
        Some(peer) => out.extend_from_slice(
            format!(
                "-MOVED {} {}:{}\r\n",
                slot,
                hub.endpoint(&peer.ip, &peer.hostname),
                peer.port_for_slot(slot)
            )
            .as_bytes(),
        ),
        None => out.extend_from_slice(b"-CLUSTERDOWN Hash slot not served\r\n"),
    }
}

/// `appendfsync always`: if any AOF was appended to since this connection
/// last checked, makes every shard's AOF durable before its replies go out.
/// One relaxed load when no server in the process runs `always`.
#[inline]
/// Run before sending a batch's replies: under `appendfsync always` makes
/// its writes durable ([`aof_always_barrier`]); otherwise, if the batch had
/// writes and the AOF is on, gets them into the kernel first, as Redis does.
async fn aof_reply_barrier(
    router: &Router,
    seen: &mut u64,
    batch_writes: bool,
    progress: &[std::sync::Arc<crate::aof::AofProgress>],
) {
    aof_always_barrier(router, seen).await;
    if batch_writes && !crate::aof::appendfsync_is_always(router.base_port) {
        router.write_aof_through(progress).await;
    }
    // Shards hold replication bytes for a mailbox burst: an acknowledged
    // write must be in the stream (WAIT, a replica promoted after it).
    if batch_writes && crate::replication::has_connected_replicas(router.base_port) {
        crate::replication::wait_streams_flushed(router.base_port, router.num_shards).await;
    }
}

async fn aof_always_barrier(router: &Router, seen: &mut u64) {
    if !crate::aof::APPENDFSYNC_ALWAYS_ANY.load(std::sync::atomic::Ordering::Relaxed) {
        return;
    }
    let now = crate::aof::AOF_APPENDS.load(std::sync::atomic::Ordering::Relaxed);
    if now != *seen {
        *seen = now;
        if crate::aof::appendfsync_is_always(router.base_port) {
            router.sync_aof().await;
        }
    }
}

/// AUTH/ACL gate for mode-switching commands, which bypass execute_command.
/// Returns the error reply if the client may not run `cmd`; `None` otherwise
/// (and always `None` for commands that are not mode switches).
fn mode_switch_denied(
    cmd: &Command,
    port: u16,
    authenticated: bool,
    auth_user: &str,
) -> Option<Vec<u8>> {
    if !is_mode_switch_cmd(cmd) {
        return None;
    }
    if !authenticated {
        return Some(b"-NOAUTH Authentication required.\r\n".to_vec());
    }
    if !crate::acl::HAS_CUSTOM_ACL.load(std::sync::atomic::Ordering::Relaxed)
        && auth_user == "default"
    {
        return None;
    }
    let name = acl_cmd_name(cmd);
    let denial = {
        let acl = crate::acl::get_acl_for_port(port);
        let guard = acl.read();
        let user = guard.users.get(auth_user)?;
        let first_arg = acl_first_arg(cmd);
        if user.can_execute_command_with_first_arg(name, first_arg.as_deref()) {
            acl_first_denied_channel(user, cmd).map(|ch| {
                (
                    crate::acl::AclDeniedReason::Channel,
                    String::from_utf8_lossy(ch).into_owned(),
                    NOPERM_CHANNEL.to_vec(),
                )
            })
        } else {
            let lower = name.to_lowercase();
            let reply = format!(
                "-NOPERM this user has no permissions to run the '{}' command\r\n",
                err_arg(&lower)
            )
            .into_bytes();
            Some((crate::acl::AclDeniedReason::Command, lower, reply))
        }
    };
    if let Some((reason, obj, reply)) = denial {
        let ctx = if in_tx(requesting_client_id()) {
            "multi"
        } else {
            "toplevel"
        };
        record_acl_denial(port, reason, ctx, &obj, auth_user, name);
        Some(reply)
    } else {
        None
    }
}

/// Redis' `enable-debug-command` / `enable-module-command` gate
/// (`allowProtectedAction`): the error reply if the client may not run
/// `cmd`, `None` otherwise. Anything but DEBUG/MODULE costs one tag compare.
fn protected_command_denied(
    cmd: &Command,
    base_port: u16,
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
) -> Option<&'static [u8]> {
    let (setting, reply) = match cmd {
        Command::Debug(_) => (
            crate::netsec::enable_debug_command(base_port),
            crate::netsec::DEBUG_COMMAND_NOT_ALLOWED,
        ),
        // Like Redis, only loading and unloading modules is protected.
        Command::Unknown(name)
            if name.starts_with("MODULE ")
                && !matches!(name.as_str(), "MODULE LIST" | "MODULE HELP") =>
        {
            (
                crate::netsec::enable_module_command(base_port),
                crate::netsec::MODULE_COMMAND_NOT_ALLOWED,
            )
        }
        _ => return None,
    };
    let peer = client_registry
        .borrow()
        .get(&client_id)
        .map(|c| c.addr.ip());
    (!setting.allows(peer)).then_some(reply)
}

/// Commands Redis flags `loading`: what a client may run while a replica
/// loads its master's dataset. Everything else gets `-LOADING`.
fn allowed_while_loading(cmd: &Command) -> bool {
    let name = get_cmd_name(cmd);
    name.starts_with("client|")
        || name.starts_with("PUBSUB ")
        || matches!(
            name,
            "ACL"
                | "AUTH"
                | "COMMAND"
                | "CONFIG"
                | "DEBUG"
                | "HELLO"
                | "INFO"
                | "LATENCY"
                | "MONITOR"
                | "PSUBSCRIBE"
                | "PUBLISH"
                | "PUNSUBSCRIBE"
                | "QUIT"
                | "REPLCONF"
                | "REPLICAOF"
                | "RESET"
                | "ROLE"
                | "SELECT"
                | "SHUTDOWN"
                | "SLOWLOG"
                | "COMMANDLOG"
                | "LOLWUT"
                | "SPUBLISH"
                | "SSUBSCRIBE"
                | "SUBSCRIBE"
                | "SUNSUBSCRIBE"
                | "TIME"
                | "UNSUBSCRIBE"
        )
}

/// Commands allowed while loading ([`allowed_while_loading`]) that are
/// still refused during the startup load, because they would act on a
/// dataset only part loaded or replace it under the load: replication
/// (a full sync would be overwritten by the load), saving or reloading
/// (`DEBUG RELOAD`, ...), and turning AOF on (its rewrite would write the
/// part loaded so far).
fn refused_during_startup_load(cmd: &Command) -> bool {
    match cmd {
        Command::Replicaof { .. } => true,
        Command::Debug(args) => !args.first().is_some_and(|sub| {
            [b"log".as_slice(), b"sleep", b"help"]
                .iter()
                .any(|ok| sub.eq_ignore_ascii_case(ok))
        }),
        Command::ConfigSet(pairs) => pairs
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case(b"appendonly")),
        _ => false,
    }
}

/// Commands Redis flags `no-async-loading`: refused with `-LOADING` while
/// a replica loads its master's dataset asynchronously (the old dataset is
/// served meanwhile), because they would act on the replication link, save
/// the dataset about to be replaced, or change the cluster under the load.
/// Turning AOF on is refused too, as during any load.
fn refused_during_async_load(cmd: &Command) -> bool {
    use crate::resp::ClusterSubcommand as C;
    match cmd {
        Command::Replicaof { .. }
        | Command::Psync { .. }
        | Command::Sync
        | Command::Save
        | Command::Bgsave { .. }
        | Command::Bgrewriteaof => true,
        Command::Cluster(sub) => matches!(
            sub,
            C::AddSlots(_)
                | C::AddSlotsRange(_)
                | C::DelSlots(_)
                | C::DelSlotsRange(_)
                | C::SetSlot(..)
                | C::Meet { .. }
                | C::Failover { .. }
                | C::Forget(_)
                | C::Replicate(_)
                | C::SaveConfig
                | C::BumpEpoch
                | C::SetConfigEpoch(_)
                | C::FlushSlots
        ),
        Command::ConfigSet(pairs) => pairs
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case(b"appendonly")),
        _ => false,
    }
}

pub struct WithCurrentDb<'a, F> {
    pub router: &'a Router,
    pub db: usize,
    pub fut: F,
}

impl<'a, F: std::future::Future> std::future::Future for WithCurrentDb<'a, F> {
    type Output = (usize, F::Output);

    #[inline(always)]
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // SAFETY: `fut` is structurally pinned and never moved; `router` and `db` are Unpin.
        let this = unsafe { self.get_unchecked_mut() };
        CURRENT_DB.set(this.db);
        if let Ok(mut local_db) = this.router.local_db.try_borrow_mut() {
            local_db.select_db(this.db);
        }
        // SAFETY: `this.fut` is pinned for the lifetime of `Self`.
        let res = unsafe { std::pin::Pin::new_unchecked(&mut this.fut) }.poll(cx);
        this.db = CURRENT_DB.get();
        if let Ok(mut local_db) = this.router.local_db.try_borrow_mut() {
            local_db.select_db(this.db);
        }
        match res {
            std::task::Poll::Ready(out) => std::task::Poll::Ready((this.db, out)),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

thread_local! {
    /// The raw arguments of the command the read loop is about to run,
    /// handed to [`execute_command`] (which takes them before its first
    /// await, so no other task on the thread can see them).
    static EXEC_ARGV: RefCell<Option<crate::resp::RawArgv>> = const { RefCell::new(None) };
}

/// Hands `argv` to the next [`execute_command`] on this thread.
#[inline]
fn set_exec_argv(argv: Option<crate::resp::RawArgv>) {
    if argv.is_some() {
        EXEC_ARGV.with(|a| *a.borrow_mut() = argv);
    }
}

/// `argv` if it is `cmd`'s and may be shown as is: commands with arguments
/// to redact (AUTH, HELLO, MIGRATE, ACL, CONFIG, SENTINEL, DEBUG) keep the
/// reconstruction that redacts them.
fn shown_argv(argv: Option<&crate::resp::RawArgv>, cmd: &Command) -> Option<Vec<Bytes>> {
    let argv = argv?;
    if matches!(
        cmd,
        Command::Auth { .. }
            | Command::Hello { .. }
            | Command::Migrate { .. }
            | Command::Acl(_)
            | Command::ConfigSet(_)
            | Command::ConfigGet(_)
            | Command::Sentinel(_)
            | Command::Debug(_)
    ) || !argv
        .name()
        .eq_ignore_ascii_case(get_cmd_name(cmd).as_bytes())
    {
        return None;
    }
    Some(argv.to_vec())
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
    // First, before any await: the arguments and the accounting slot the
    // read loop handed over.
    let raw_argv = EXEC_ARGV.with(|a| a.borrow_mut().take());
    let stat_slot = crate::slot_stats::take_exec_slot();
    let initial_db = client_registry
        .borrow()
        .get(&client_id)
        .map(|c| c.db)
        .unwrap_or_else(|| CURRENT_DB.get());
    let replica_keys = writable_replica_write_keys(router, &cmd);
    // Replies assembled in RESP2 that RESP3 clients get as maps or
    // verbatim strings (see `crate::resp3`).
    let reshape = if CURRENT_CLIENT_RESP3.get() {
        crate::resp3::reshape_for(&cmd)
    } else {
        None
    };
    let reply_start = out.len();
    let fut = execute_command_inner(
        cmd,
        raw_argv,
        stat_slot,
        router,
        client_id,
        client_registry,
        out,
        asking,
        authenticated,
        auth_user,
    );
    #[cfg(debug_assertions)]
    let fut = Box::pin(fut);
    let quit = WithCurrentDb {
        router,
        db: initial_db,
        fut,
    }
    .await
    .1;
    if let Some(how) = reshape {
        crate::resp3::apply(how, out, reply_start);
    }
    if let Some(keys) = replica_keys {
        remember_replica_keys(router, initial_db, keys);
    }
    quit
}

/// The keys of a write a client sends to this server while it is a
/// writable replica (`replica-read-only no`), which the replica must
/// expire itself; `None` otherwise (the usual case: one atomic load).
fn writable_replica_write_keys(router: &Router, cmd: &Command) -> Option<Vec<Bytes>> {
    if !crate::replication::HAS_SLAVE_INSTANCE.load(std::sync::atomic::Ordering::Relaxed)
        || REPLICA_READ_ONLY.load(std::sync::atomic::Ordering::Relaxed)
        || !cmd.is_write_command()
        || !crate::replication::cached_hub(router.base_port).is_slave()
    {
        return None;
    }
    let mut keys = Vec::new();
    for_each_cmd_key(cmd, |k| keys.push(Bytes::copy_from_slice(k)));
    (!keys.is_empty()).then_some(keys)
}

/// Hands `keys` (written by a client on this writable replica) to the
/// shards that own them, to expire them (`ShardDb::replica_local_keys`).
fn remember_replica_keys(router: &Router, db: usize, keys: Vec<Bytes>) {
    let mut by_shard: Vec<Vec<Bytes>> = vec![Vec::new(); router.num_shards];
    for k in keys {
        by_shard[router.target_shard(&k)].push(k);
    }
    for (shard, keys) in by_shard.into_iter().enumerate() {
        if keys.is_empty() {
            continue;
        }
        if shard == router.shard_id {
            router
                .local_db
                .borrow_mut()
                .replica_local_keys
                .extend(keys.into_iter().map(|k| (db, k)));
        } else {
            let _ = router.senders[shard]
                .send(crate::shard::ShardMessage::RememberReplicaKeys { db, keys });
        }
    }
}

/// [`aof_image_before_command`] for a shard applying `cmd` to its own keys
/// outside the command path (a batch's fast paths write to the table
/// directly).
#[cold]
pub(crate) fn images_before_command(db: &ShardDb, cmd: &Command) {
    aof_image_before_command(db, cmd, |_| true);
}

/// While an AOF rewrite builds its image of this shard (see
/// [`crate::shard::AofImage`]), keeps it as of the rewrite's start: the
/// keys `cmd` may change (those `local` accepts) are written to it first.
#[cold]
fn aof_image_before_command(db: &ShardDb, cmd: &Command, local: impl Fn(&[u8]) -> bool) {
    if !cmd.is_write_command() {
        return;
    }
    if matches!(cmd, Command::Swapdb { .. }) {
        // Each shard's images follow the swap (`shard::images_swap_dbs`).
        return;
    }
    for_each_cmd_key(cmd, |k| {
        if local(k) {
            crate::shard::before_write(db, k);
        }
    });
    // Writes to another DB than the current one.
    let target = match cmd {
        Command::Move { key, db: to } => Some((key, *to as usize)),
        Command::Copy {
            destination,
            destination_db: Some(to),
            ..
        } => Some((destination, *to as usize)),
        _ => None,
    };
    if let Some((key, to)) = target
        && local(key)
    {
        crate::shard::before_write_in(db, to, key);
    }
}

/// Counts an error reply (`-PREFIX ...`) written to `out[start_len..]` in
/// INFO errorstats / commandstats.
fn record_reply_error_stat(out: &[u8], start_len: usize, cmd_name: &'static str) {
    if out.len() <= start_len {
        return;
    }
    let reply = &out[start_len..];
    if let Some(err_line) = reply.strip_prefix(b"-") {
        let end_idx = err_line
            .iter()
            .position(|&b| b == b' ' || b == b'\r' || b == b'\n')
            .unwrap_or(err_line.len());
        if let Ok(prefix) = std::str::from_utf8(&err_line[..end_idx]) {
            let is_script_cmd = matches!(
                cmd_name,
                "EVAL" | "EVALSHA" | "EVAL_RO" | "EVALSHA_RO" | "FCALL" | "FCALL_RO"
            );
            if is_script_cmd {
                record_failed_stat(cmd_name);
                // Set by this command's script only: a script command that
                // failed before running one (EVALSHA NOSCRIPT, ...) must
                // not see a flag left by an earlier script.
                if !crate::scripting::SCRIPT_RECORDED_ERROR.replace(false) {
                    record_error_stat(prefix, None);
                }
            } else {
                record_error_stat(prefix, Some(cmd_name));
            }
        }
    }
}

/// Runs the part of a HIMPORT subcommand that works on the session's
/// `fieldsets` (a connection's, or a script run's). For SET, returns the
/// resolved [`Command::HimportSet`] for the key's shard to run; anything
/// else is answered in `out`.
#[inline(never)]
pub fn himport_session(
    fieldsets: &mut hashbrown::HashMap<Bytes, Vec<Bytes>>,
    sub: &crate::resp::HimportSubcommand,
    out: &mut Vec<u8>,
) -> Option<Command> {
    use crate::resp::HimportSubcommand as H;
    match sub {
        H::Prepare { name, fields } => {
            fieldsets.insert(name.clone(), fields.clone());
            out.extend_from_slice(b"+OK\r\n");
            None
        }
        H::Discard(name) => {
            write_resp_integer(out, fieldsets.remove(name).is_some() as i64);
            None
        }
        H::DiscardAll => {
            let n = fieldsets.len();
            fieldsets.clear();
            write_resp_integer(out, n as i64);
            None
        }
        H::Set {
            key,
            fieldset,
            values,
        } => {
            let Some(fields) = fieldsets.get(fieldset) else {
                write_resp_err(
                    out,
                    format!(
                        "ERR no such fieldset '{}'",
                        String::from_utf8_lossy(fieldset)
                    ),
                );
                return None;
            };
            if fields.len() != values.len() {
                write_resp_err(
                    out,
                    format!(
                        "ERR value count does not match the fieldset ({} fields, {} values)",
                        fields.len(),
                        values.len()
                    ),
                );
                return None;
            }
            // In Redis' template order (shorter field names first, then
            // bytewise), so HGETALL lists them as Redis does.
            let mut pairs: smallvec::SmallVec<[(Bytes, Bytes); 1]> =
                fields.iter().cloned().zip(values.iter().cloned()).collect();
            pairs.sort_by(|(a, _), (b, _)| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
            Some(Command::HimportSet {
                key: key.clone(),
                fields: pairs,
            })
        }
    }
}

thread_local! {
    /// HIMPORT fieldsets of the script running on this thread: each script
    /// run starts with none, as each EVAL / FCALL is its own session.
    pub static SCRIPT_FIELDSETS: RefCell<hashbrown::HashMap<Bytes, Vec<Bytes>>> =
        RefCell::new(hashbrown::HashMap::new());
}

pub fn target_shard_of_cmd(cmd: &Command, num_shards: usize) -> Option<usize> {
    match cmd {
        Command::Exists(keys) if keys.len() == 1 => Some(target_shard(&keys[0], num_shards)),
        Command::Del(keys) | Command::Unlink(keys) if keys.len() == 1 => {
            Some(target_shard(&keys[0], num_shards))
        }
        Command::Sismember { key, .. } => Some(target_shard(key, num_shards)),
        Command::Sadd { key, .. } => Some(target_shard(key, num_shards)),
        Command::Hset { key, .. } => Some(target_shard(key, num_shards)),
        Command::Hget { key, .. } => Some(target_shard(key, num_shards)),
        Command::Lpop { key, .. } => Some(target_shard(key, num_shards)),
        Command::Lpush { key, .. } => Some(target_shard(key, num_shards)),
        Command::Zadd { key, .. } => Some(target_shard(key, num_shards)),
        Command::Get(key)
        | Command::Getex { key, .. }
        | Command::Set { key, .. }
        | Command::Digest(key)
        | Command::IncrBy(key, _, _)
        | Command::Expire { key, .. }
        | Command::Persist(key)
        | Command::Ttl(key, _)
        | Command::Hsetnx { key, .. }
        | Command::HimportSet { key, .. }
        | Command::Himport(crate::resp::HimportSubcommand::Set { key, .. })
        | Command::Hmset { key, .. }
        | Command::Hmget { key, .. }
        | Command::Hdel { key, .. }
        | Command::Hexists { key, .. }
        | Command::Hlen(key)
        | Command::Hgetall(key)
        | Command::Hkeys(key)
        | Command::Hvals(key)
        | Command::Hstrlen { key, .. }
        | Command::Hgetdel { key, .. }
        | Command::Rpush { key, .. }
        | Command::Lpushx { key, .. }
        | Command::Rpushx { key, .. }
        | Command::Rpop { key, .. }
        | Command::Lrange { key, .. }
        | Command::Llen(key)
        | Command::Lindex { key, .. }
        | Command::Srem { key, .. }
        | Command::Smembers(key)
        | Command::Scard(key)
        | Command::Spop { key, .. }
        | Command::Zrem { key, .. }
        | Command::Zscore { key, .. }
        | Command::Zcard(key)
        | Command::Zrank { key, .. }
        | Command::Zrevrank { key, .. }
        | Command::Zcount { key, .. }
        | Command::Zincrby { key, .. }
        | Command::Zrange { key, .. }
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
        | Command::Bitfield { key, .. }
        | Command::Pfadd { key, .. }
        | Command::PfdebugGetreg(key)
        | Command::PfdebugEncoding(key)
        | Command::PfdebugTodense(key)
        | Command::Dump(key)
        | Command::Restore { key, .. }
        | Command::Xadd { key, .. }
        | Command::Xcfgset { key, .. }
        | Command::Xsetid { key, .. }
        | Command::Xdelex { key, .. }
        | Command::Xidmprecord { key, .. }
        | Command::Xlen(key)
        | Command::Xrange { key, .. }
        | Command::Xrevrange { key, .. }
        | Command::Xdel { key, .. }
        | Command::Xtrim { key, .. }
        | Command::XgroupCreate { key, .. }
        | Command::XgroupDestroy { key, .. }
        | Command::XgroupCreateConsumer { key, .. }
        | Command::XgroupDelConsumer { key, .. }
        | Command::XgroupSetId { key, .. }
        | Command::Xack { key, .. }
        | Command::Xackdel { key, .. }
        | Command::Xnack { key, .. }
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
        | Command::Increx { key, .. }
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
        | Command::Geosearchstore { key, .. }
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
        | Command::ProbRestore { key, .. }
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
        | Command::CrdtSrem { key, .. }
        | Command::SemanticSet { namespace: key, .. }
        | Command::SemanticGet { namespace: key, .. }
        | Command::SemanticDel { namespace: key, .. }
        | Command::SemanticFlush(key)
        | Command::SemanticInfo(key)
        | Command::AgentMemAdd { session: key, .. }
        | Command::AgentMemContext { session: key, .. }
        | Command::AgentMemCompact { session: key, .. }
        | Command::AgentMemInfo(key)
        | Command::AgentMemClear(key)
        | Command::LlmQuotaReserve { key, .. }
        | Command::LlmQuotaSettle { key, .. }
        | Command::LlmQuotaInfo(key)
        | Command::AgentCheckpointPut { key, .. }
        | Command::AgentCheckpointGet { key, .. }
        | Command::AgentCheckpointHistory { key, .. }
        | Command::AgentToolClaim { key, .. }
        | Command::AgentToolComplete { key, .. }
        | Command::Vadd { key, .. }
        | Command::Vquery { key, .. }
        | Command::Vsim { key, .. }
        | Command::Vdist { key, .. }
        | Command::Vdel { key, .. }
        | Command::Vinfo(key)
        | Command::Vcard(key)
        | Command::Vdim(key)
        | Command::Vemb { key, .. }
        | Command::Vlinks { key, .. }
        | Command::Vrandmember { key, .. }
        | Command::Vsetattr { key, .. }
        | Command::Vgetattr { key, .. }
        | Command::Vismember { key, .. }
        | Command::Object(crate::resp::ObjectSubcommand::Encoding(key))
        | Command::Object(crate::resp::ObjectSubcommand::Freq(key))
        | Command::Object(crate::resp::ObjectSubcommand::Idletime(key))
        | Command::Object(crate::resp::ObjectSubcommand::Refcount(key))
        | Command::Xinfo(crate::resp::XinfoSubcommand::Stream(key))
        | Command::Xinfo(crate::resp::XinfoSubcommand::StreamFull { key, .. })
        | Command::Xinfo(crate::resp::XinfoSubcommand::Groups(key))
        | Command::Xinfo(crate::resp::XinfoSubcommand::Consumers { key, .. })
        | Command::Hexpire { key, .. }
        | Command::Httl { key, .. }
        | Command::Hpersist { key, .. }
        | Command::Hgetex { key, .. }
        | Command::Hsetex { key, .. }
        | Command::Xclaim { key, .. }
        | Command::Xautoclaim { key, .. }
        | Command::Move { key, .. } => Some(target_shard(key, num_shards)),
        Command::Zrangestore {
            dst: destination,
            src: source,
            ..
        }
        | Command::Smove {
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
        }
        | Command::Lmovem {
            source,
            destination,
            ..
        }
        | Command::Blmovem {
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
        Command::Copy {
            source,
            destination,
            ..
        } => {
            let s1 = target_shard(source, num_shards);
            let s2 = target_shard(destination, num_shards);
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
        Command::Touch(keys) if keys.len() == 1 => Some(target_shard(&keys[0], num_shards)),
        Command::Mget(keys)
            if !keys.is_empty()
                && keys
                    .iter()
                    .all(|k| target_shard(k, num_shards) == target_shard(&keys[0], num_shards)) =>
        {
            Some(target_shard(&keys[0], num_shards))
        }
        Command::Mset(pairs)
            if !pairs.is_empty()
                && pairs.iter().all(|(k, _)| {
                    target_shard(k, num_shards) == target_shard(&pairs[0].0, num_shards)
                }) =>
        {
            Some(target_shard(&pairs[0].0, num_shards))
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

/// Which container a HDEL/SREM/ZREM fast path removes from.
#[derive(Clone, Copy)]
pub(crate) enum RemKind {
    Hash,
    Set,
    ZSet,
}

#[inline(always)]
pub fn target_shard_and_hash_of_cmd(cmd: &Command, num_shards: usize) -> Option<(usize, u64)> {
    match cmd {
        Command::Exists(keys) if keys.len() == 1 => {
            Some(crate::router::target_shard_and_hash(&keys[0], num_shards))
        }
        Command::Touch(keys) if keys.len() == 1 => {
            Some(crate::router::target_shard_and_hash(&keys[0], num_shards))
        }
        Command::Del(keys) | Command::Unlink(keys) if keys.len() == 1 => {
            Some(crate::router::target_shard_and_hash(&keys[0], num_shards))
        }
        Command::Sismember { key, .. }
        | Command::Smismember { key, .. }
        | Command::Sadd { key, .. }
        | Command::Hset { key, .. }
        | Command::Hget { key, .. }
        | Command::Hmget { key, .. }
        | Command::Lpop { key, .. }
        | Command::Lpush { key, .. }
        | Command::Rpush { key, .. }
        | Command::Spop { key, .. }
        | Command::Zpopmin { key, .. }
        | Command::Hdel { key, .. }
        | Command::Srem { key, .. }
        | Command::Zrem { key, .. }
        | Command::Zadd { key, .. }
        | Command::Zmscore { key, .. }
        | Command::Get(key)
        | Command::Set { key, .. }
        | Command::IncrBy(key, _, _)
        | Command::Lrange { key, .. }
        | Command::Zrange { key, .. } => {
            Some(crate::router::target_shard_and_hash(key, num_shards))
        }
        _ => target_shard_of_cmd(cmd, num_shards).map(|s| {
            let h = cmd_primary_key(cmd)
                .map(|k| crate::table::hash_key(k))
                .unwrap_or(0);
            (s, h)
        }),
    }
}

/// Re-indexes the full, binary-safe content of HASH `key` into local and global search indexes.
fn reindex_hash_for_search(db: &mut ShardDb, key: &[u8]) {
    let raw = db.reindex_hash_local(key);
    let key_str = String::from_utf8_lossy(key);
    if raw.is_empty() {
        crate::search::delete_document_hook(&key_str);
    } else {
        crate::search::index_document_hook(&key_str, &raw);
    }
}

/// Calls `f` with each key a read command reads, for client tracking and
/// keyspace hit/miss statistics.
fn for_each_read_key(cmd: &Command, mut f: impl FnMut(&[u8])) {
    match cmd {
        Command::Get(k)
        | Command::Ttl(k, _)
        | Command::Expiretime(k, _)
        | Command::Hlen(k)
        | Command::Hgetall(k)
        | Command::Hkeys(k)
        | Command::Hvals(k)
        | Command::Llen(k)
        | Command::Smembers(k)
        | Command::Scard(k)
        | Command::Zcard(k)
        | Command::Type(k)
        | Command::Strlen(k)
        | Command::Dump(k)
        | Command::Xlen(k) => f(k.as_ref()),
        Command::Hget { key, .. }
        | Command::Hmget { key, .. }
        | Command::Hexists { key, .. }
        | Command::Hstrlen { key, .. }
        | Command::Hrandfield { key, .. }
        | Command::Hscan { key, .. }
        | Command::Lrange { key, .. }
        | Command::Lindex { key, .. }
        | Command::Lpos { key, .. }
        | Command::Sismember { key, .. }
        | Command::Smismember { key, .. }
        | Command::Srandmember { key, .. }
        | Command::Sscan { key, .. }
        | Command::Zscore { key, .. }
        | Command::Zmscore { key, .. }
        | Command::Zrank { key, .. }
        | Command::Zrevrank { key, .. }
        | Command::Zcount { key, .. }
        | Command::Zlexcount { key, .. }
        | Command::Zrange { key, .. }
        | Command::Zrandmember { key, .. }
        | Command::Zscan { key, .. }
        | Command::Getbit { key, .. }
        | Command::Bitcount { key, .. }
        | Command::Bitpos { key, .. }
        | Command::Getrange { key, .. }
        | Command::Xrange { key, .. }
        | Command::Xrevrange { key, .. }
        | Command::Xpending { key, .. } => f(key.as_ref()),
        Command::Exists(keys) => {
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Mget(keys)
        | Command::Pfcount { keys }
        | Command::Sinter(keys)
        | Command::Sunion(keys)
        | Command::Sdiff(keys) => {
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Sintercard { keys, .. }
        | Command::Sunioncard { keys, .. }
        | Command::Sdiffcard { keys, .. }
        | Command::Zdiff { keys, .. }
        | Command::Zinter { keys, .. }
        | Command::Zunion { keys, .. } => {
            for k in keys {
                f(k.as_ref());
            }
        }
        Command::Sort {
            key, store: None, ..
        } => f(key.as_ref()),
        _ => {}
    }
}

/// Write commands that, like in Redis, count toward
/// `rdb_changes_since_last_save` only for what they actually change, when
/// they execute. Other write commands count once per call up front.
#[inline]
pub fn dirty_counted_on_change(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Setbit { .. }
            | Command::Bitfield { .. }
            | Command::Xreadgroup { .. }
            | Command::Xdel { .. }
            | Command::Xdelex { .. }
            | Command::Xtrim { .. }
    )
}

/// Applies a command received from this replica's master on the local
/// shard. Like Redis, it counts in INFO commandstats, and an error reply
/// (which goes nowhere: the master never reads replies) is logged, since it
/// means the replica's data may now differ from the master's.
pub fn apply_replicated_command(
    cmd: &Command,
    db: &mut ShardDb,
    out: &mut Vec<u8>,
    aof: Option<&RefCell<crate::aof::AofWriter>>,
) {
    out.clear();
    if crate::slot_stats::net_accounting()
        && let Some(slot) = command_slot(cmd)
        && let Some(bytes) = crate::aof::command_to_resp(cmd)
    {
        // What the master sent for it, near enough: its encoding.
        crate::slot_stats::record(slot, 0, bytes.len() as u64, 0);
    }
    let start = std::time::Instant::now();
    {
        let _master_write = ReplayingWrites::new();
        execute_local_command(cmd, db, out, aof);
    }
    let name = stat_cmd_name(cmd);
    let nanos = start.elapsed().as_nanos() as u64;
    record_cmd_stats(name, 1, nanos);
    if crate::slot_stats::accounting(
        crate::cluster::HAS_ACTIVE_CLUSTER.load(std::sync::atomic::Ordering::Relaxed),
    ) {
        crate::slot_stats::record_cpu(command_slot(cmd), nanos);
    }
    if out.first() == Some(&b'-') {
        let end = out.iter().position(|&b| b == b'\r').unwrap_or(out.len());
        crate::log_warning!(
            "== CRITICAL == This replica is sending an error to its master: '{}' after processing the command '{}'",
            String::from_utf8_lossy(&out[1..end]),
            name.to_ascii_lowercase(),
        );
    }
}

thread_local! {
    /// Where the running script's writes propagate (redis.set_repl): bit 1
    /// the AOF, bit 2 the replicas. 3 (both) outside scripts.
    pub static SCRIPT_REPL_TARGETS: std::cell::Cell<u8> = const { std::cell::Cell::new(3) };
}

/// Appends a change to the AOF and replicates it (see `record_change!` in
/// [`execute_local_command`]). Out of line: expanded at every write site,
/// this grew that function's stack frame enough to overflow test threads.
#[inline(never)]
fn log_change(
    changed: &Command,
    aof: Option<&RefCell<crate::aof::AofWriter>>,
    port: u16,
    shard_id: usize,
    warn: bool,
) {
    // A script's redis.set_repl() may leave out the AOF, the replicas or
    // both (bit 1: AOF, bit 2: replicas; both outside scripts).
    let targets = SCRIPT_REPL_TARGETS.get();
    let need_rep = targets & 2 != 0 && crate::replication::has_connected_replicas(port);
    let aof = if targets & 1 != 0 { aof } else { None };
    if let Some(aof_w) = aof {
        // Encoded straight into the AOF buffer; replicas get the same bytes
        // from there.
        let mut w = aof_w.borrow_mut();
        match w.append_command(changed) {
            Some(start) if need_rep => {
                crate::replication::propagate_shard_bytes(port, shard_id, &w.buffer()[start..])
            }
            Some(_) => {}
            None if warn => crate::aof::note_unpropagated(changed),
            None => {}
        }
    } else if need_rep {
        if let Some(bytes) = crate::aof::command_to_resp(changed) {
            crate::replication::propagate_shard_bytes(port, shard_id, &bytes);
        } else if warn {
            crate::aof::note_unpropagated(changed);
        }
    }
}

#[inline(always)]
fn is_special_pipeline_cmd(cmd: &Command) -> bool {
    matches!(
        cmd,
        Command::Subscribe(_)
            | Command::Psubscribe(_)
            | Command::Ssubscribe(_)
            | Command::Psync { .. }
            | Command::Sync
            | Command::DflyFlow { .. }
            | Command::SyncSlots(_)
            | Command::Multi
            | Command::Exec
            | Command::Discard
            | Command::Watch(_)
            | Command::Unwatch
            | Command::Blpop { .. }
            | Command::Brpop { .. }
            | Command::Blmove { .. }
            | Command::Blmovem { .. }
            | Command::Blmpop { .. }
            | Command::Bzpopmin { .. }
            | Command::Bzpopmax { .. }
            | Command::Bzmpop { .. }
            | Command::Hello { .. }
            | Command::Reset
            | Command::Auth { .. }
            | Command::Acl(_)
            | Command::Tier(_)
            | Command::ConfigGet(_)
            | Command::ConfigSet(_)
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
            | Command::Quit
            | Command::Wait { .. }
            | Command::WaitAof { .. }
            // Changes whether the commands after it reply; the squashed path
            // only applies the reply mode in effect when the batch started.
            | Command::Client(ClientSubcommand::Reply(_))
            | Command::Select(_)
            | Command::Move { .. }
            | Command::Swapdb { .. }
            | Command::Flushdb
            | Command::Flushall
            | Command::Copy {
                destination_db: Some(_),
                ..
            }
    )
}

#[allow(clippy::await_holding_refcell_ref)]
async fn execute_commands_squashed(
    commands: &mut Vec<Command>,
    has_special: bool,
    router: &Router,
    responders: &[std::sync::Arc<crate::mailbox::BatchResponder>],
    remote_batches: &mut [Vec<(usize, u64, Command)>],
    items_pool: &mut Vec<Vec<(usize, u64, Command)>>,
    responses: &mut Vec<CompactResp>,
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    out: &mut Vec<u8>,
    asking: &mut bool,
    authenticated: &mut bool,
    auth_user: &mut String,
    fallbacks: &mut u64,
) -> bool {
    let initial_db = client_registry
        .borrow()
        .get(&client_id)
        .map(|c| c.db)
        .unwrap_or_else(|| CURRENT_DB.get());
    let fut = execute_commands_squashed_inner(
        commands,
        has_special,
        router,
        responders,
        remote_batches,
        items_pool,
        responses,
        client_id,
        client_registry,
        out,
        asking,
        authenticated,
        auth_user,
        fallbacks,
    );
    #[cfg(debug_assertions)]
    let fut = Box::pin(fut);
    WithCurrentDb {
        router,
        db: initial_db,
        fut,
    }
    .await
    .1
}

#[allow(clippy::await_holding_refcell_ref)]
async fn execute_commands_squashed_inner(
    commands: &mut Vec<Command>,
    has_special: bool,
    router: &Router,
    responders: &[std::sync::Arc<crate::mailbox::BatchResponder>],
    remote_batches: &mut [Vec<(usize, u64, Command)>],
    items_pool: &mut Vec<Vec<(usize, u64, Command)>>,
    responses: &mut Vec<CompactResp>,
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    out: &mut Vec<u8>,
    asking: &mut bool,
    authenticated: &mut bool,
    auth_user: &mut String,
    // Commands handed to `execute_command`, which counts them itself.
    fallbacks: &mut u64,
) -> bool {
    if let Some(c) = client_registry.borrow_mut().get_mut(&client_id) {
        CURRENT_CLIENT_RESP3.set(c.is_resp3);
        CURRENT_DB.set(c.db);
        c.last_active = Instant::now();
        if let Some(last_cmd) = commands.last() {
            c.last_cmd = get_cmd_name(last_cmd);
        }
    }
    let mut can_squash = *authenticated;
    // During a full-sync load, `execute_command` answers -LOADING.
    if can_squash && crate::replication::is_loading(router.base_port) {
        can_squash = false;
    }
    // On a read-only replica `execute_command` answers writes -READONLY;
    // batched, pipelined writes were applied (and the replica diverged).
    if can_squash
        && crate::replication::HAS_SLAVE_INSTANCE.load(std::sync::atomic::Ordering::Relaxed)
        && REPLICA_READ_ONLY.load(std::sync::atomic::Ordering::Relaxed)
        && crate::replication::cached_hub(router.base_port).is_slave()
        && batch_has_writes(commands)
    {
        can_squash = false;
    }
    // On a writable replica, `execute_command` notes the keys clients
    // write so the replica can expire them (batched writes would skip it).
    if can_squash
        && crate::replication::HAS_SLAVE_INSTANCE.load(std::sync::atomic::Ordering::Relaxed)
        && !REPLICA_READ_ONLY.load(std::sync::atomic::Ordering::Relaxed)
        && crate::replication::cached_hub(router.base_port).is_slave()
        && batch_has_writes(commands)
    {
        can_squash = false;
    }
    // While persistence fails, `execute_command` answers writes -MISCONF.
    if can_squash && persistence_misconf(router).is_some() {
        can_squash = false;
    }
    // While a script runs past its time limit, `execute_command` answers
    // -BUSY; batched, the commands would also run on a shard whose script
    // is only suspended, breaking its atomicity.
    if can_squash && crate::scripting::script_busy(router.port) {
        can_squash = false;
    }
    // With keyspace notifications on, a pipeline is batched only if every
    // command emits its events the same way when batched.
    if can_squash && NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) != 0 {
        can_squash = commands.iter().all(notifies_same_when_batched);
    }
    if can_squash {
        let acl = if crate::acl::HAS_CUSTOM_ACL.load(std::sync::atomic::Ordering::Relaxed)
            || auth_user != "default"
        {
            Some(crate::acl::get_acl_for_port(router.port))
        } else {
            None
        };
        let acl_guard = acl.as_ref().map(|a| a.read());
        let user = acl_guard.as_ref().and_then(|g| g.users.get(auth_user));

        let hub = if crate::cluster::has_active_cluster(router.base_port) {
            let h = crate::cluster::get_cluster_hub(router.base_port);
            if !h.nodes.read().is_empty() {
                Some(h)
            } else {
                None
            }
        } else {
            None
        };
        let my_slots_guard = hub.as_ref().map(|h| h.my_slots.read());
        let is_cluster = router.cluster_enabled || my_slots_guard.is_some();
        let has_dynamic_owners = !router.slot_owners.borrow().is_empty();
        if has_special || user.is_some() || is_cluster || has_dynamic_owners {
            for cmd in commands.iter() {
                if is_special_pipeline_cmd(cmd) {
                    can_squash = false;
                    break;
                }
                if let Some(user) = &user
                    && acl_check_user_cmd(user, cmd).is_some()
                {
                    can_squash = false;
                    break;
                }
                if is_cluster || has_dynamic_owners {
                    let mut first_slot: Option<u16> = None;
                    let mut first_shard: Option<usize> = None;
                    let mut invalid_slot = false;
                    let mut has_keys = false;
                    for_each_cmd_key(cmd, |k| {
                        has_keys = true;
                        let slot = key_slot(k);
                        if let Some(fs) = first_slot {
                            if is_cluster && fs != slot {
                                invalid_slot = true;
                            }
                        } else {
                            first_slot = Some(slot);
                        }
                        if has_dynamic_owners {
                            let shard = router.target_shard_for_slot(slot);
                            if let Some(fsh) = first_shard {
                                if fsh != shard
                                    && !matches!(
                                        cmd,
                                        Command::Mget(_)
                                            | Command::Mset(_)
                                            | Command::Del(_)
                                            | Command::Unlink(_)
                                            | Command::Exists(_)
                                            | Command::Touch(_)
                                    )
                                {
                                    invalid_slot = true;
                                }
                            } else {
                                first_shard = Some(shard);
                            }
                        }
                        if is_cluster {
                            if router.get_slot_state(slot) != crate::shard::SlotState::Stable {
                                invalid_slot = true;
                            }
                            if let Some(my_slots) = &my_slots_guard {
                                let owns_slot =
                                    my_slots.iter().any(|&(s, e)| slot >= s && slot <= e);
                                if !owns_slot {
                                    invalid_slot = true;
                                }
                            }
                        }
                    });
                    if invalid_slot {
                        can_squash = false;
                        break;
                    }
                    if is_cluster
                        && !has_keys
                        && !matches!(
                            cmd,
                            Command::Ping(_)
                                | Command::Quit
                                | Command::Time
                                | Command::Echo(_)
                                | Command::Readonly
                                | Command::Readwrite
                                | Command::CommandCount
                                | Command::CommandList
                                | Command::Latency(_)
                                | Command::PubsubHelp
                                | Command::FunctionStats
                                | Command::FunctionKill
                        )
                    {
                        can_squash = false;
                        break;
                    }
                }
            }
        }
    }

    // Cross-shard MSET / MGET are atomic (see `router::XShardWriteGuard`).
    // A batch with a cross-shard MSET is marked as one cross-shard write
    // while it runs (or runs command by command while an MGET reads under
    // the shards' locks). A batch of nothing but MGETs is folded as usual,
    // its reads checked against cross-shard writes once all replied and
    // read again one by one if one overlapped; mixed with anything else,
    // rereading an MGET could reorder it after the batch's later commands,
    // so such a batch runs command by command.
    let mut mget_check: Option<(u64, Vec<Command>)> = None;
    let mut _batch_write: Option<crate::router::XShardWriteGuard> = None;
    if can_squash && router.num_shards > 1 {
        let (mut cross_mget, mut cross_mset, mut only_mgets) = (false, false, true);
        for cmd in commands.iter() {
            match cmd {
                Command::Mget(keys) => {
                    cross_mget |= keys_span_shards(keys, |k| router.target_shard(k));
                }
                Command::Mset(pairs) => {
                    only_mgets = false;
                    let first = pairs.first().map(|(k, _)| router.target_shard(k));
                    cross_mset |= pairs
                        .iter()
                        .any(|(k, _)| Some(router.target_shard(k)) != first);
                }
                _ => only_mgets = false,
            }
        }
        if cross_mget && !only_mgets {
            can_squash = false;
        } else if cross_mset {
            _batch_write = crate::router::XShardWriteGuard::lock_free();
            can_squash = _batch_write.is_some();
        } else if cross_mget {
            match crate::router::xshard_read_begin() {
                Some(epoch) => mget_check = Some((epoch, commands.clone())),
                None => can_squash = false,
            }
        }
    }

    if !can_squash {
        let mut should_close = false;
        for cmd in commands.drain(..) {
            *fallbacks += 1;
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
    responses.clear();
    responses.resize(n, CompactResp::empty());
    let mut local_buf = Vec::new();
    let mut should_close = false;
    // Cross-shard MSETs are dispatched without awaiting so that every command in
    // the pipeline is in flight before we pay a single round-trip stall. These hold
    // the handles until the gather phase below.
    // MGETs and multi-key DEL/EXISTS are folded into the per-shard batches: (pipeline idx, first slot, keys).
    let mut folded_mgets: Vec<(usize, usize, usize)> = Vec::new();
    let mut folded_int_sums: Vec<(usize, usize, usize)> = Vec::new();
    let mut inflight_msets: Vec<(usize, crate::router::MsetInFlight)> = Vec::new();
    let mut local_cold_gets: smallvec::SmallVec<[(usize, bytes::Bytes); 8]> =
        smallvec::SmallVec::new();

    for batch in remote_batches.iter_mut() {
        batch.clear();
    }

    // 1. Process local shard commands immediately in-place; bucket remote commands by shard
    let mut has_local_writes = false;
    let is_resp3 = CURRENT_CLIENT_RESP3.get();
    // Write fast paths skip AOF, replication and keyspace notifications; when
    // any of those is needed, writes go through `execute_local_command`.
    let write_fast_path = router.aof().is_none()
        && !crate::replication::has_connected_replicas(router.base_port)
        && NOTIFY_KEYSPACE_FLAGS.load(std::sync::atomic::Ordering::Relaxed) == 0;
    let mut local_db = router.local_db.borrow_mut();
    local_db.select_db(CURRENT_DB.get());
    // Commands handed to `execute_command` below record their own stats; the
    // time spent awaiting other shards is left out of the batch segments.
    let mut stat_run = CmdStatBatch::new();
    // Commands run here rather than through `execute_command` are timed one
    // by one for SLOWLOG (and LATENCY's "command" event) the same way.
    let slow_threshold = batch_slow_threshold();
    for (idx, cmd) in commands.drain(..).enumerate() {
        if cmd.is_write_command() && !dirty_counted_on_change(&cmd) {
            crate::snapshot::note_write();
        }
        // A reply a RESP3 client gets reshaped (see `crate::resp3`) takes the
        // per-command path below, through `execute_command`, which does it.
        let reshaped = is_resp3 && crate::resp3::reshape_for(&cmd).is_some();
        if let Some((mut target, key_hash)) = target_shard_and_hash_of_cmd(&cmd, router.num_shards)
            && !reshaped
        {
            if !router.slot_owners.borrow().is_empty()
                && let Some(k) = cmd_primary_key(&cmd)
            {
                target = router.target_shard(k);
            }
            stat_run.note(stat_cmd_name(&cmd));
            if target == router.shard_id {
                let slow_t0 = slow_threshold.map(|_| crate::cycleclock::now());
                'local: {
                    if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                        for_each_read_key(&cmd, |k| record_client_read(router.port, client_id, k));
                    }
                    // The fast paths below write straight to the table: a
                    // snapshot image being built takes the keys' state first.
                    if crate::shard::aof_image_active() {
                        images_before_command(&local_db, &cmd);
                    }
                    local_buf.clear();
                    local_db.hydrate_cmd_keys(&cmd);
                    if let Command::Get(ref key) = cmd {
                        let compact_res =
                            local_db.table.get_compact_with_hash(key.as_ref(), key_hash);
                        match compact_res {
                            Ok(Some(resp)) => {
                                responses[idx] = resp;
                                break 'local;
                            }
                            Ok(None) => {
                                let is_tiered = local_db.tier_manager.is_some()
                                    && local_db.table.is_tiered(key).is_some();
                                if is_tiered {
                                    local_cold_gets.push((idx, key.clone()));
                                } else {
                                    responses[idx] = crate::shard::CompactResp::null(is_resp3);
                                }
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if write_fast_path
                        && let Command::Set {
                            ref key,
                            ref value,
                            expire_in,
                            condition: crate::resp::SetCondition::None,
                            get: false,
                            keepttl: false,
                            past_expired: false,
                        } = cmd
                    {
                        has_local_writes = true;
                        if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                            touch_watched_key(router.port, key.as_ref());
                        }
                        if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                            notify_key_invalidation(router.port, key.as_ref(), client_id);
                        }
                        // Borrowed, not moved: the command stays whole for the
                        // slowlog check after the block.
                        local_db.table.set_with_hash(
                            key.clone(),
                            key_hash,
                            value.clone(),
                            expire_in,
                        );
                        responses[idx] = crate::shard::CompactResp::OK;
                        break 'local;
                    } else if write_fast_path && let Command::IncrBy(ref key, delta, _) = cmd {
                        has_local_writes = true;
                        match local_db.table.incr_by_slice_with_hash(key, key_hash, delta) {
                            Ok(val) => {
                                if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                    touch_watched_key(router.port, key.as_ref());
                                }
                                if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                                }
                                if val == 1 {
                                    responses[idx] = crate::shard::CompactResp::INT_1;
                                } else if val == 0 {
                                    responses[idx] = crate::shard::CompactResp::INT_0;
                                } else {
                                    responses[idx] = crate::shard::CompactResp::from_integer(val);
                                }
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if let Command::Exists(ref keys) = cmd
                        && keys.len() == 1
                    {
                        let exists = local_db.table.exists_with_hash(keys[0].as_ref(), key_hash);
                        crate::server_stats::note_key_lookup(exists);
                        responses[idx] = if exists {
                            crate::shard::CompactResp::INT_1
                        } else {
                            crate::shard::CompactResp::INT_0
                        };
                        break 'local;
                    } else if let Command::Touch(ref keys) = cmd
                        && keys.len() == 1
                    {
                        let touched = local_db.table.touch_with_hash(keys[0].as_ref(), key_hash);
                        responses[idx] = if touched {
                            crate::shard::CompactResp::INT_1
                        } else {
                            crate::shard::CompactResp::INT_0
                        };
                        break 'local;
                    } else if write_fast_path
                        && !crate::search::has_active_search_indices()
                        && let Command::Del(ref keys) | Command::Unlink(ref keys) = cmd
                        && keys.len() == 1
                    {
                        let is_unlink = matches!(cmd, Command::Unlink(_));
                        let lazy = is_unlink && local_db.table.is_lazyfree_worthy(keys[0].as_ref());
                        let deleted = local_db.del_with_hash(keys[0].as_ref(), key_hash);
                        if deleted {
                            if lazy {
                                crate::table::add_lazyfreed_objects(1);
                            }
                            has_local_writes = true;
                            if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                touch_watched_key(router.port, keys[0].as_ref());
                            }
                            if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                notify_key_invalidation(router.port, keys[0].as_ref(), client_id);
                            }
                            responses[idx] = crate::shard::CompactResp::INT_1;
                        } else {
                            responses[idx] = crate::shard::CompactResp::INT_0;
                        }
                        break 'local;
                    } else if let Command::Hget { ref key, ref field } = cmd {
                        match local_db.table.hget_compact_with_hash(
                            key.as_ref(),
                            key_hash,
                            field.as_ref(),
                        ) {
                            Ok(resp) => {
                                responses[idx] = resp.for_proto(CURRENT_CLIENT_RESP3.get());
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if let Command::Hmget {
                        ref key,
                        ref fields,
                    } = cmd
                    {
                        match local_db.table.hmget_compact_with_hash(
                            key.as_ref(),
                            key_hash,
                            fields,
                            &mut local_buf,
                        ) {
                            Ok(resp) => {
                                responses[idx] = resp.for_proto(CURRENT_CLIENT_RESP3.get());
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if write_fast_path
                        && !crate::search::has_active_search_indices()
                        && let Command::Hset {
                            ref key,
                            ref fields,
                        } = cmd
                    {
                        has_local_writes = true;
                        let res = if fields.len() == 1 {
                            let (ref f, ref v) = fields[0];
                            local_db
                                .table
                                .hset_single_field_with_hash(key, key_hash, f, v)
                        } else {
                            local_db.table.hset_slice_with_hash(key, key_hash, fields)
                        };
                        match res {
                            Ok(count) => {
                                if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                    touch_watched_key(router.port, key.as_ref());
                                }
                                if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                                }
                                if count == 1 {
                                    responses[idx] = crate::shard::CompactResp::INT_1;
                                } else if count == 0 {
                                    responses[idx] = crate::shard::CompactResp::INT_0;
                                } else {
                                    responses[idx] =
                                        crate::shard::CompactResp::from_integer(count as i64);
                                }
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if let Command::Sismember {
                        ref key,
                        ref member,
                    } = cmd
                    {
                        match local_db.table.sismember_compact_with_hash(
                            key.as_ref(),
                            key_hash,
                            member.as_ref(),
                        ) {
                            Ok(resp) => {
                                responses[idx] = resp.for_proto(CURRENT_CLIENT_RESP3.get());
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if let Command::Smismember {
                        ref key,
                        ref members,
                    } = cmd
                    {
                        match local_db.table.smismember_compact_with_hash(
                            key.as_ref(),
                            key_hash,
                            members,
                            &mut local_buf,
                        ) {
                            Ok(resp) => {
                                responses[idx] = resp.for_proto(CURRENT_CLIENT_RESP3.get());
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if let Command::Zmscore {
                        ref key,
                        ref members,
                    } = cmd
                    {
                        match local_db.table.zmscore_compact_with_hash(
                            key.as_ref(),
                            key_hash,
                            members,
                            &mut local_buf,
                        ) {
                            Ok(resp) => {
                                responses[idx] = resp.for_proto(CURRENT_CLIENT_RESP3.get());
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if write_fast_path
                        && let Command::Sadd {
                            ref key,
                            ref members,
                        } = cmd
                    {
                        has_local_writes = true;
                        let res = if members.len() == 1 {
                            local_db
                                .table
                                .sadd_single_member_with_hash(key, key_hash, &members[0])
                        } else {
                            local_db.table.sadd_slice_with_hash(key, key_hash, members)
                        };
                        match res {
                            Ok(count) => {
                                if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                    touch_watched_key(router.port, key.as_ref());
                                }
                                if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                                }
                                if count == 1 {
                                    responses[idx] = crate::shard::CompactResp::INT_1;
                                } else if count == 0 {
                                    responses[idx] = crate::shard::CompactResp::INT_0;
                                } else {
                                    responses[idx] =
                                        crate::shard::CompactResp::from_integer(count as i64);
                                }
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if write_fast_path
                        && !crate::block::has_blocked_waiters(router.port)
                        && let Command::Zadd {
                            ref key,
                            ref elements,
                            flags,
                        } = cmd
                    {
                        has_local_writes = true;
                        match local_db
                            .table
                            .zadd_slice_with_hash(key, key_hash, elements, flags)
                        {
                            Ok((count, incr_score)) => {
                                if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                    touch_watched_key(router.port, key.as_ref());
                                }
                                if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                                }
                                if flags.incr {
                                    local_buf.clear();
                                    if let Some(score) = incr_score {
                                        write_resp_score(&mut local_buf, score);
                                    } else {
                                        write_resp_null(&mut local_buf);
                                    }
                                    responses[idx] = CompactResp::from_slice(&local_buf);
                                } else if count == 1 {
                                    responses[idx] = crate::shard::CompactResp::INT_1;
                                } else if count == 0 {
                                    responses[idx] = crate::shard::CompactResp::INT_0;
                                } else {
                                    responses[idx] =
                                        crate::shard::CompactResp::from_integer(count as i64);
                                }
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if write_fast_path
                        && !crate::block::has_blocked_waiters(router.port)
                        && let Command::Lpush {
                            ref key,
                            ref values,
                        } = cmd
                    {
                        has_local_writes = true;
                        match local_db.table.lpush_slice_with_hash(key, key_hash, values) {
                            Ok(len) => {
                                if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                    touch_watched_key(router.port, key.as_ref());
                                }
                                if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                                }
                                if len == 1 {
                                    responses[idx] = crate::shard::CompactResp::INT_1;
                                } else if len == 0 {
                                    responses[idx] = crate::shard::CompactResp::INT_0;
                                } else {
                                    responses[idx] =
                                        crate::shard::CompactResp::from_integer(len as i64);
                                }
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if write_fast_path
                        && !crate::block::has_blocked_waiters(router.port)
                        && let Command::Rpush {
                            ref key,
                            ref values,
                        } = cmd
                    {
                        has_local_writes = true;
                        match local_db.table.rpush_slice_with_hash(key, key_hash, values) {
                            Ok(len) => {
                                if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                    touch_watched_key(router.port, key.as_ref());
                                }
                                if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                                }
                                responses[idx] =
                                    crate::shard::CompactResp::from_integer(len as i64);
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if write_fast_path
                        && let Command::Spop {
                            ref key,
                            count: None,
                        } = cmd
                    {
                        match local_db.table.spop_one_with_hash(key.as_ref(), key_hash) {
                            Ok(Some(v)) => {
                                has_local_writes = true;
                                if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                    touch_watched_key(router.port, key.as_ref());
                                }
                                if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                                }
                                responses[idx] = CompactResp::from_owned_bulk(v);
                                break 'local;
                            }
                            Ok(None) => {
                                responses[idx] =
                                    crate::shard::CompactResp::null(CURRENT_CLIENT_RESP3.get());
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if write_fast_path
                        && let Command::Zpopmin {
                            ref key,
                            count: None,
                        } = cmd
                    {
                        match local_db.table.zpopmin_with_hash(key.as_ref(), key_hash, 1) {
                            Ok(popped) => {
                                if let Some((m, s)) = popped.into_iter().next() {
                                    has_local_writes = true;
                                    if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                        touch_watched_key(router.port, key.as_ref());
                                    }
                                    if HAS_TRACKING_CLIENTS
                                        .load(std::sync::atomic::Ordering::Relaxed)
                                    {
                                        notify_key_invalidation(
                                            router.port,
                                            key.as_ref(),
                                            client_id,
                                        );
                                    }
                                    local_buf.extend_from_slice(b"*2\r\n");
                                    write_resp_bulk(&mut local_buf, &m);
                                    write_resp_score(&mut local_buf, s);
                                    responses[idx] =
                                        CompactResp::from_vec(std::mem::take(&mut local_buf));
                                } else {
                                    responses[idx] = crate::shard::CompactResp::EMPTY_ARRAY;
                                }
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if write_fast_path
                        && let Some((key, items, kind)) = match &cmd {
                            // HDEL stays on the slow path while search indexes
                            // exist, like HSET.
                            Command::Hdel { key, fields }
                                if !crate::search::has_active_search_indices() =>
                            {
                                Some((key, fields, RemKind::Hash))
                            }
                            Command::Srem { key, members } => Some((key, members, RemKind::Set)),
                            Command::Zrem { key, members } => Some((key, members, RemKind::ZSet)),
                            _ => None,
                        }
                    {
                        let res = match kind {
                            RemKind::Hash => local_db.table.hdel_with_hash(key, key_hash, items),
                            RemKind::Set => local_db.table.srem_with_hash(key, key_hash, items),
                            RemKind::ZSet => local_db.table.zrem_with_hash(key, key_hash, items),
                        };
                        match res {
                            Ok(count) => {
                                if count > 0 {
                                    has_local_writes = true;
                                    if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                        touch_watched_key(router.port, key.as_ref());
                                    }
                                    if HAS_TRACKING_CLIENTS
                                        .load(std::sync::atomic::Ordering::Relaxed)
                                    {
                                        notify_key_invalidation(
                                            router.port,
                                            key.as_ref(),
                                            client_id,
                                        );
                                    }
                                }
                                responses[idx] =
                                    crate::shard::CompactResp::from_integer(count as i64);
                                break 'local;
                            }
                            Err(err) => {
                                write_resp_err(&mut local_buf, err);
                            }
                        }
                    } else if write_fast_path && let Command::Lpop { ref key, count } = cmd {
                        if count.is_none() {
                            match local_db.table.lpop_one_with_hash(key.as_ref(), key_hash) {
                                Ok(Some(v)) => {
                                    has_local_writes = true;
                                    if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                        touch_watched_key(router.port, key.as_ref());
                                    }
                                    if HAS_TRACKING_CLIENTS
                                        .load(std::sync::atomic::Ordering::Relaxed)
                                    {
                                        notify_key_invalidation(
                                            router.port,
                                            key.as_ref(),
                                            client_id,
                                        );
                                    }
                                    responses[idx] = CompactResp::from_owned_bulk(v);
                                    break 'local;
                                }
                                Ok(None) => {
                                    responses[idx] = crate::shard::CompactResp::null(is_resp3);
                                    break 'local;
                                }
                                Err(err) => {
                                    write_resp_err(&mut local_buf, err);
                                }
                            }
                        } else {
                            match local_db.write_lpop_resp_with_hash(
                                key.as_ref(),
                                key_hash,
                                count,
                                &mut local_buf,
                            ) {
                                Ok(has_pop) => {
                                    if has_pop {
                                        has_local_writes = true;
                                        if HAS_WATCHED_KEYS
                                            .load(std::sync::atomic::Ordering::Relaxed)
                                        {
                                            touch_watched_key(router.port, key.as_ref());
                                        }
                                        if HAS_TRACKING_CLIENTS
                                            .load(std::sync::atomic::Ordering::Relaxed)
                                        {
                                            notify_key_invalidation(
                                                router.port,
                                                key.as_ref(),
                                                client_id,
                                            );
                                        }
                                    }
                                }
                                Err(err) => {
                                    write_resp_err(&mut local_buf, err);
                                }
                            }
                        }
                    } else if let Command::Lrange {
                        ref key,
                        start,
                        stop,
                    } = cmd
                    {
                        match local_db.table.lrange_compact_with_hash(
                            key.as_ref(),
                            key_hash,
                            start,
                            stop,
                            &mut local_buf,
                        ) {
                            Ok(resp) => {
                                responses[idx] = resp.for_proto(CURRENT_CLIENT_RESP3.get());
                                break 'local;
                            }
                            Err(err) => write_resp_err(&mut local_buf, err),
                        }
                    } else if let Command::Zrange { ref key, ref opts } = cmd {
                        match local_db.table.zrange_compact_with_hash(
                            key.as_ref(),
                            key_hash,
                            opts,
                            is_resp3,
                            &mut local_buf,
                        ) {
                            Ok(resp) => {
                                responses[idx] = resp.for_proto(CURRENT_CLIENT_RESP3.get());
                                break 'local;
                            }
                            Err(err) => write_resp_err(&mut local_buf, err),
                        }
                    } else if write_fast_path && let Command::Rpop { ref key, count } = cmd {
                        if count.is_none() {
                            match local_db.table.rpop_one_with_hash(key.as_ref(), key_hash) {
                                Ok(Some(v)) => {
                                    has_local_writes = true;
                                    if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                                        touch_watched_key(router.port, key.as_ref());
                                    }
                                    if HAS_TRACKING_CLIENTS
                                        .load(std::sync::atomic::Ordering::Relaxed)
                                    {
                                        notify_key_invalidation(
                                            router.port,
                                            key.as_ref(),
                                            client_id,
                                        );
                                    }
                                    responses[idx] = CompactResp::from_owned_bulk(v);
                                    break 'local;
                                }
                                Ok(None) => {
                                    responses[idx] = crate::shard::CompactResp::null(is_resp3);
                                    break 'local;
                                }
                                Err(err) => {
                                    write_resp_err(&mut local_buf, err);
                                }
                            }
                        } else {
                            match local_db.write_rpop_resp_with_hash(
                                key.as_ref(),
                                key_hash,
                                count,
                                &mut local_buf,
                            ) {
                                Ok(has_pop) => {
                                    if has_pop {
                                        has_local_writes = true;
                                        if HAS_WATCHED_KEYS
                                            .load(std::sync::atomic::Ordering::Relaxed)
                                        {
                                            touch_watched_key(router.port, key.as_ref());
                                        }
                                        if HAS_TRACKING_CLIENTS
                                            .load(std::sync::atomic::Ordering::Relaxed)
                                        {
                                            notify_key_invalidation(
                                                router.port,
                                                key.as_ref(),
                                                client_id,
                                            );
                                        }
                                    }
                                }
                                Err(err) => {
                                    write_resp_err(&mut local_buf, err);
                                }
                            }
                        }
                    } else {
                        if matches!(
                            cmd,
                            Command::Set { .. }
                                | Command::Del(_)
                                | Command::Unlink(_)
                                | Command::IncrBy { .. }
                        ) {
                            has_local_writes = true;
                        }
                        if execute_local_command(
                            &cmd,
                            &mut local_db,
                            &mut local_buf,
                            router.aof().as_deref(),
                        ) {
                            should_close = true;
                        }
                    }
                    responses[idx] = CompactResp::from_vec(std::mem::take(&mut local_buf));
                }
                if let (Some(t0), Some(thr)) = (slow_t0, slow_threshold) {
                    let us = crate::cycleclock::micros_since(t0);
                    if us >= thr {
                        log_batched_slow(&cmd, us, client_id, client_registry);
                    }
                }
            } else {
                remote_batches[target].push((idx, key_hash, cmd));
            }
        } else if let Command::Mget(keys) = cmd {
            stat_run.note("MGET");
            // Fold into the per-shard batches as one GET per key, each with its
            // own slot after the pipeline's slots; the array is assembled once
            // every batch has replied. Per-shard FIFO keeps earlier writes of
            // this pipeline visible, so no flush is needed first.
            if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                for key in &keys {
                    record_client_read(router.port, client_id, key.as_ref());
                }
            }
            let start = responses.len();
            for key in keys {
                let slot = responses.len();
                responses.push(CompactResp::empty());
                let (target, key_hash) = router.target_shard_and_hash(key.as_ref());
                if target == router.shard_id {
                    match local_db.table.get_compact_with_hash(key.as_ref(), key_hash) {
                        Ok(Some(resp)) => responses[slot] = resp,
                        Ok(None)
                            if local_db.tier_manager.is_some()
                                && local_db.table.is_tiered(&key).is_some() =>
                        {
                            local_cold_gets.push((slot, key));
                        }
                        _ => responses[slot] = CompactResp::null(is_resp3),
                    }
                } else {
                    remote_batches[target].push((slot, key_hash, Command::Get(key)));
                }
            }
            folded_mgets.push((idx, start, responses.len() - start));
        } else if let Command::Exists(keys) = cmd {
            stat_run.note("EXISTS");
            if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                for key in &keys {
                    record_client_read(router.port, client_id, key.as_ref());
                }
            }
            let start = responses.len();
            for key in keys {
                let slot = responses.len();
                responses.push(CompactResp::INT_0);
                let (target, key_hash) = router.target_shard_and_hash(key.as_ref());
                if target == router.shard_id {
                    let exists = local_db.table.exists_with_hash(key.as_ref(), key_hash);
                    crate::server_stats::note_key_lookup(exists);
                    if exists {
                        responses[slot] = CompactResp::INT_1;
                    }
                } else {
                    remote_batches[target].push((
                        slot,
                        key_hash,
                        Command::Exists(smallvec::smallvec![key]),
                    ));
                }
            }
            folded_int_sums.push((idx, start, responses.len() - start));
        } else if let Command::Touch(keys) = cmd {
            stat_run.note("TOUCH");
            let start = responses.len();
            for key in keys {
                let slot = responses.len();
                responses.push(CompactResp::INT_0);
                let (target, key_hash) = router.target_shard_and_hash(key.as_ref());
                if target == router.shard_id {
                    if local_db.table.touch_with_hash(key.as_ref(), key_hash) {
                        responses[slot] = CompactResp::INT_1;
                    }
                } else {
                    remote_batches[target].push((
                        slot,
                        key_hash,
                        Command::Touch(smallvec::smallvec![key]),
                    ));
                }
            }
            folded_int_sums.push((idx, start, responses.len() - start));
        } else if write_fast_path
            && !crate::search::has_active_search_indices()
            && matches!(cmd, Command::Del(_) | Command::Unlink(_))
        {
            let (is_unlink, keys) = match cmd {
                Command::Del(keys) => (false, keys),
                Command::Unlink(keys) => (true, keys),
                _ => unreachable!(),
            };
            stat_run.note(if is_unlink { "UNLINK" } else { "DEL" });
            let start = responses.len();
            for key in keys {
                let slot = responses.len();
                responses.push(CompactResp::INT_0);
                let (target, key_hash) = router.target_shard_and_hash(key.as_ref());
                if target == router.shard_id {
                    local_db.hydrate_key(key.as_ref());
                    let lazy = is_unlink && local_db.table.is_lazyfree_worthy(key.as_ref());
                    if local_db.del_with_hash(key.as_ref(), key_hash) {
                        if lazy {
                            crate::table::add_lazyfreed_objects(1);
                        }
                        has_local_writes = true;
                        if HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed) {
                            touch_watched_key(router.port, key.as_ref());
                        }
                        if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                            notify_key_invalidation(router.port, key.as_ref(), client_id);
                        }
                        responses[slot] = CompactResp::INT_1;
                    }
                } else {
                    if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                        notify_key_invalidation(router.port, key.as_ref(), client_id);
                    }
                    let single_cmd = if is_unlink {
                        Command::Unlink(smallvec::smallvec![key])
                    } else {
                        Command::Del(smallvec::smallvec![key])
                    };
                    remote_batches[target].push((slot, key_hash, single_cmd));
                }
            }
            folded_int_sums.push((idx, start, responses.len() - start));
        } else if write_fast_path && let Command::Mset(pairs) = cmd {
            stat_run.note("MSET");
            let watched = HAS_WATCHED_KEYS.load(std::sync::atomic::Ordering::Relaxed);
            let tracking = HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed);
            for (key, value) in pairs {
                let (target, key_hash) = router.target_shard_and_hash(key.as_ref());
                if target == router.shard_id {
                    has_local_writes = true;
                    if watched {
                        touch_watched_key(router.port, key.as_ref());
                    }
                    if tracking {
                        notify_key_invalidation(router.port, key.as_ref(), client_id);
                    }
                    local_db.table.set_with_hash(key, key_hash, value, None);
                } else {
                    let slot = responses.len();
                    responses.push(CompactResp::empty());
                    remote_batches[target].push((
                        slot,
                        key_hash,
                        Command::Set {
                            key,
                            value,
                            expire_in: None,
                            condition: crate::resp::SetCondition::None,
                            get: false,
                            keepttl: false,
                            past_expired: false,
                        },
                    ));
                }
            }
            responses[idx] = crate::shard::CompactResp::OK;
        } else if let Command::Mset(pairs) = cmd {
            local_db.drain_dropped_tier();
            drop(local_db);
            if remote_batches.iter().any(|b| !b.is_empty()) {
                stat_run.flush();
                flush_remote_batches(
                    router,
                    responders,
                    remote_batches,
                    items_pool,
                    responses,
                    is_resp3,
                )
                .await;
                stat_run.restart();
            }
            stat_run.note("MSET");
            if HAS_TRACKING_CLIENTS.load(std::sync::atomic::Ordering::Relaxed) {
                for (key, _) in &pairs {
                    notify_key_invalidation(router.port, key.as_ref(), client_id);
                }
            }
            match router.begin_mset(pairs) {
                Some(inflight) => inflight_msets.push((idx, inflight)),
                None => responses[idx] = crate::shard::CompactResp::OK,
            }
            local_db = router.local_db.borrow_mut();
            local_db.select_db(CURRENT_DB.get());
        } else if !matches!(
            cmd,
            Command::Ping(_)
                | Command::Quit
                | Command::Time
                | Command::Echo(_)
                | Command::PubsubHelp
                | Command::FunctionStats
                | Command::FunctionKill
        ) {
            local_db.drain_dropped_tier();
            drop(local_db);
            stat_run.flush();
            if remote_batches.iter().any(|b| !b.is_empty()) {
                flush_remote_batches(
                    router,
                    responders,
                    remote_batches,
                    items_pool,
                    responses,
                    is_resp3,
                )
                .await;
            }
            local_buf.clear();
            *fallbacks += 1;
            if execute_command(
                cmd,
                router,
                client_id,
                client_registry,
                &mut local_buf,
                asking,
                authenticated,
                auth_user,
            )
            .await
            {
                should_close = true;
            }
            responses[idx] = CompactResp::from_vec(std::mem::take(&mut local_buf));
            local_db = router.local_db.borrow_mut();
            local_db.select_db(CURRENT_DB.get());
            stat_run.restart();
        } else {
            stat_run.note(stat_cmd_name(&cmd));
            local_buf.clear();
            if execute_local_command(&cmd, &mut local_db, &mut local_buf, None) {
                should_close = true;
            }
            responses[idx] = CompactResp::from_vec(std::mem::take(&mut local_buf));
        }
    }
    local_db.drain_dropped_tier();
    drop(local_db);
    stat_run.flush();

    if has_local_writes {
        router.check_auto_tier_after_write();
    }

    // 2-3. Dispatch the remaining per-shard batches and wait for them.
    let cold_gets = std::mem::take(&mut local_cold_gets);
    flush_remote_batches(
        router,
        responders,
        remote_batches,
        items_pool,
        responses,
        is_resp3,
    )
    .await;
    for (cold_idx, cold_key) in cold_gets {
        if let Some(v) = router.stream_cold_read_local(&cold_key).await {
            responses[cold_idx] = CompactResp::Bulk(v);
        } else {
            responses[cold_idx] = crate::shard::CompactResp::null(is_resp3);
        }
    }
    // Log batch items that ran slow on other shards. Slots past the
    // pipeline's are per-key parts of folded multi-key commands, not
    // commands of their own, and are skipped.
    if slow_threshold.is_some() {
        for r in responders {
            for (slot, micros, cmd) in r.take_slow() {
                if slot < n {
                    log_batched_slow(&cmd, micros, client_id, client_registry);
                }
            }
        }
    }

    // 4. Sum multi-key DEL/EXISTS from their per-key slots, then gather the
    //    cross-shard MSETs dispatched in step 1. Folded MGET arrays are
    //    written straight from their per-key slots in step 5.
    for (idx, start, len) in folded_int_sums.drain(..) {
        let mut sum = 0i64;
        for slot in &responses[start..start + len] {
            if slot.as_slice() == b":1\r\n" {
                sum += 1;
            }
        }
        responses[idx] = match sum {
            0 => CompactResp::INT_0,
            1 => CompactResp::INT_1,
            n => CompactResp::from_integer(n),
        };
    }
    let batch_len = n;
    if inflight_msets.len() == batch_len {
        out.reserve(batch_len * 5);
        for (_idx, inflight) in inflight_msets.drain(..) {
            router.finish_mset(inflight).await;
            out.extend_from_slice(b"+OK\r\n");
        }
        return should_close;
    }
    if !inflight_msets.is_empty() {
        for (idx, inflight) in inflight_msets.drain(..) {
            router.finish_mset(inflight).await;
            responses[idx] = crate::shard::CompactResp::OK;
        }
    }

    if let Some((epoch, mgets)) = mget_check
        && !crate::router::xshard_read_is_consistent(epoch)
    {
        // A cross-shard write overlapped the folded MGETs: read them again,
        // in order, each checked on its own.
        folded_mgets.clear();
        responses.truncate(n);
        for cmd in mgets {
            if let Command::Mget(keys) = cmd {
                router.write_mget_resp_atomic(keys, out).await;
            }
        }
        return should_close;
    }

    // 5. Append responses in exact FIFO pipeline order with pre-reserved
    //    capacity. A folded MGET's array is written directly from its
    //    per-key slots (in `responses` past the pipeline's), so its values
    //    are copied once rather than into an intermediate array first.
    let total_len: usize = responses
        .iter()
        .map(|r| {
            r.prefetch();
            r.estimated_len()
        })
        .sum();
    out.reserve(total_len + folded_mgets.len() * 16);
    let mut mgets = folded_mgets.iter().peekable();
    for (i, resp) in responses[..n].iter().enumerate() {
        if let Some(&&(idx, start, len)) = mgets.peek()
            && idx == i
        {
            write_mget_reply(out, &responses[start..start + len], is_resp3);
            mgets.next();
            continue;
        }
        resp.write_to(out);
    }
    folded_mgets.clear();
    responses.truncate(n);

    should_close
}

/// True when a command emits the same keyspace notifications inside a batched
/// pipeline as when run on its own. Reads emit none; the listed writes were
/// checked event for event against unbatched execution (per-key order
/// included). Other writes keep the whole pipeline unbatched.
fn notifies_same_when_batched(cmd: &Command) -> bool {
    !cmd.is_write_command()
        || matches!(
            cmd,
            Command::Set { .. }
                | Command::Setnx { .. }
                | Command::Getset { .. }
                | Command::Getdel(_)
                | Command::Getex { .. }
                | Command::Append { .. }
                | Command::IncrBy(..)
                | Command::Incrbyfloat { .. }
                | Command::Setrange { .. }
                | Command::Setbit { .. }
                | Command::Del(_)
                | Command::Unlink(_)
                | Command::Expire { .. }
                | Command::Persist(_)
                | Command::Hset { .. }
                | Command::Hsetnx { .. }
                | Command::Hdel { .. }
                | Command::Hincrby { .. }
                | Command::Hincrbyfloat { .. }
                | Command::Lpush { .. }
                | Command::Rpush { .. }
                | Command::Lpop { .. }
                | Command::Rpop { .. }
                | Command::Lset { .. }
                | Command::Lrem { .. }
                | Command::Linsert { .. }
                | Command::Ltrim { .. }
                | Command::Sadd { .. }
                | Command::Srem { .. }
                | Command::Spop { .. }
                | Command::Zadd { .. }
                | Command::Zincrby { .. }
                | Command::Zrem { .. }
                | Command::Zpopmin { .. }
                | Command::Zpopmax { .. }
                | Command::Xadd { .. }
                | Command::Pfadd { .. }
        )
}

/// Builds an MGET array from per-key GET replies. MGET answers nil for a key
/// holding a non-string value, where GET answers WRONGTYPE.
/// Writes a folded MGET's reply array from its per-key GET replies; an error
/// for a key (e.g. WRONGTYPE) reads as nil, as MGET reports it.
fn write_mget_reply(out: &mut Vec<u8>, items: &[CompactResp], is_resp3: bool) {
    write_resp_array_header(out, items.len());
    for item in items {
        let is_error = matches!(
            item,
            CompactResp::Small { .. } | CompactResp::Big(_) | CompactResp::RawBytes(_)
        ) && item.as_slice().first() == Some(&b'-');
        if is_error {
            CompactResp::null(is_resp3).write_to(out);
        } else {
            item.write_to(out);
        }
    }
}

/// The SLOWLOG threshold in microseconds, or `None` when logging is off.
#[inline(always)]
pub(crate) fn batch_slow_threshold() -> Option<u64> {
    let t = crate::slowlog::SLOWLOG_LOG_SLOWER_THAN.load(std::sync::atomic::Ordering::Relaxed);
    (t >= 0).then_some(t as u64)
}

/// Logs a command that ran inside a batched pipeline (on this shard or,
/// reported back, on another) for `micros` µs, at or over the threshold:
/// what `execute_command`'s tracker does for commands run one at a time.
#[cold]
fn log_batched_slow(
    cmd: &Command,
    micros: u64,
    client_id: u64,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
) {
    if matches!(cmd, Command::Quit | Command::Exec | Command::Commandlog(_)) {
        return;
    }
    let ms = micros / 1000;
    if ms > 0 {
        record_latency_event("command", ms);
    }
    let (addr, name) = client_registry
        .borrow()
        .get(&client_id)
        .map(|c| (c.addr.to_string(), c.name.clone().unwrap_or_default()))
        .unwrap_or_default();
    crate::slowlog::log_command_if_slow(cmd, micros, &addr, &name);
}

/// Sends the per-shard batches built so far and waits until every shard has
/// written its replies into `responses`. Commands that are not batched must
/// call this first so they cannot overtake earlier commands of the same
/// pipeline (e.g. `SET k v` to a remote shard followed by `MGET k x`).
async fn flush_remote_batches(
    router: &Router,
    responders: &[std::sync::Arc<crate::mailbox::BatchResponder>],
    remote_batches: &mut [Vec<(usize, u64, Command)>],
    items_pool: &mut Vec<Vec<(usize, u64, Command)>>,
    responses: &mut [CompactResp],
    is_resp3: bool,
) {
    // Dispatch batched hops to all remote shards in parallel using pre-allocated channels
    let resp_base_ptr = responses.as_mut_ptr();
    let mut pending = crate::shardset::ShardSet::new();
    for (target_shard, items) in remote_batches.iter_mut().enumerate() {
        if !items.is_empty() {
            let responder = &responders[target_shard];
            responder.prepare(resp_base_ptr, responses.len());
            let next_items = items_pool.pop().unwrap_or_else(|| Vec::with_capacity(64));
            let msg = ShardMessage::Batch {
                db: CURRENT_DB.get(),
                items: std::mem::replace(items, next_items),
                responder: responder.clone(),
                is_resp3,
                client_id: requesting_client_id(),
                no_touch: CLIENT_NO_TOUCH.get(),
                defer_bcast: DEFER_BCAST_FLUSH.get(),
                replicate_as: None,
            };
            if router.senders[target_shard].send(msg).is_ok() {
                pending.insert(target_shard);
            }
        }
    }

    // Await the replies; optionally poll first (`cross-shard-spin`, default 0)
    // before parking so the event loop can serve other connections meanwhile.
    for _spin in 0..crate::mailbox::cross_shard_spin() {
        if pending.is_empty() {
            break;
        }
        for target_shard in pending.iter() {
            let responder = &responders[target_shard];
            if let Some(mut recycled_items) = responder.try_take() {
                // The remote shard leaves the commands in place; dropping them
                // here keeps their key refcounts on this core.
                recycled_items.clear();
                items_pool.push(recycled_items);
                pending.remove(target_shard);
            }
        }
        if pending.is_empty() {
            break;
        }
        std::hint::spin_loop();
    }
    while let Some(target_shard) = pending.first() {
        let responder = &responders[target_shard];
        if let Some(mut recycled_items) = responder.wait_take().await {
            recycled_items.clear();
            items_pool.push(recycled_items);
        }
        pending.remove(target_shard);
    }
}

/// Error returned when a script's declared keys live on different shards.
const SCRIPT_CROSS_SHARD_ERR: &[u8] = b"-CROSSSLOT Keys in request don't hash to the same slot\r\n";

/// True when the declared keys of a script (EVAL/EVALSHA/FCALL) are owned by
/// more than one shard. A script runs against a single shard's database, so
/// such a script would silently read and write the wrong data for the keys
/// owned elsewhere; it must be rejected instead.
fn keys_span_shards(keys: &[Bytes], shard_of: impl Fn(&[u8]) -> usize) -> bool {
    match keys.split_first() {
        Some((first, rest)) => {
            let owner = shard_of(first);
            rest.iter().any(|k| shard_of(k) != owner)
        }
        None => false,
    }
}

use crate::router::next_tx_id;

/// Runs `cmd` on `shard`, locally or through its mailbox, and returns the
/// RESP reply.
async fn exec_on_shard(router: &Router, shard: usize, cmd: Command) -> Vec<u8> {
    if shard == router.shard_id {
        let mut out = Vec::new();
        execute_local_command(
            &cmd,
            &mut router.local_db.borrow_mut(),
            &mut out,
            router.aof().as_deref(),
        );
        out
    } else {
        router.execute_remote(shard, cmd).await
    }
}

/// The source half of a cross-shard LMOVE / SMOVE, on the source's shard
/// thread: pops `pop` (its own propagation left out), then has
/// `dst_shard` push what it took and stream the `logical` command, and
/// waits for that, blocking this thread, before returning the pop's reply.
/// So no other write on this shard can come between the pop and the
/// logical command in the replication stream: a racing RPUSH to the
/// source of an RPOPLPUSH reaches replicas on the same side of the move as
/// it ran here. Costs this shard one cross-shard round trip per move.
///
/// Can't deadlock: a move holds both shards' VLL locks (taken in shard
/// order), so no other move can block the destination's thread on ours.
pub(crate) fn move_element_from_source(
    router: &Router,
    db: usize,
    pop: &Command,
    dst_shard: usize,
    push: crate::shard::MovePush,
    logical: Bytes,
    client_id: u64,
) -> Vec<u8> {
    // Earlier writes here go out before the move.
    crate::replication::flush_stream_batch();
    let prev_db = CURRENT_DB.replace(db);
    // Run as the requesting client (tracking, NOLOOP), like a Batch.
    let prev_cid = CURRENT_CLIENT_ID.replace(client_id);
    let mut popped = Vec::new();
    {
        let _no_repl = crate::replication::SuppressPropagation::new();
        let mut local = router.local_db.borrow_mut();
        local.select_db(db);
        execute_local_command(pop, &mut local, &mut popped, router.aof().as_deref());
    }
    CURRENT_DB.set(prev_db);
    CURRENT_CLIENT_ID.set(prev_cid);
    // The push, and what puts the popped element back if it never runs.
    let (push, undo) = match push {
        crate::shard::MovePush::List { key, to } => {
            let Some(elem) = resp_bulk_payload(&popped) else {
                return popped;
            };
            let undo_values = smallvec::smallvec![elem.clone()];
            let undo = match pop {
                Command::Rpop { key, .. } => Command::Rpush {
                    key: key.clone(),
                    values: undo_values,
                },
                _ => Command::Lpush {
                    key: cmd_primary_key(pop).cloned().unwrap_or_default(),
                    values: undo_values,
                },
            };
            let values = smallvec::smallvec![elem];
            let push = match to {
                crate::table::ListDirection::Left => Command::Lpush { key, values },
                crate::table::ListDirection::Right => Command::Rpush { key, values },
            };
            (push, undo)
        }
        crate::shard::MovePush::Set { key, member } => {
            if resp_integer(&popped) != Some(1) {
                return popped;
            }
            let undo = Command::Sadd {
                key: cmd_primary_key(pop).cloned().unwrap_or_default(),
                members: smallvec::smallvec![member.clone()],
            };
            let push = Command::Sadd {
                key,
                members: smallvec::smallvec![member],
            };
            (push, undo)
        }
    };
    // Puts the element back (unreplicated, like the pop) and fails the move.
    let undo_pop = |why: &str| -> Vec<u8> {
        let prev_db = CURRENT_DB.replace(db);
        let prev_cid = CURRENT_CLIENT_ID.replace(client_id);
        {
            let _no_repl = crate::replication::SuppressPropagation::new();
            let mut local = router.local_db.borrow_mut();
            local.select_db(db);
            let mut ignored = Vec::new();
            execute_local_command(&undo, &mut local, &mut ignored, router.aof().as_deref());
        }
        CURRENT_DB.set(prev_db);
        CURRENT_CLIENT_ID.set(prev_cid);
        format!("-TRYAGAIN cross-shard move {why}; nothing was moved\r\n").into_bytes()
    };
    let delay = CROSS_SHARD_MOVE_DELAY_US.load(std::sync::atomic::Ordering::Relaxed);
    if delay > 0 {
        std::thread::sleep(std::time::Duration::from_micros(delay));
    }
    let hash = cmd_primary_key(&push)
        .map(|k| crate::table::hash_key(k))
        .unwrap_or(0);
    // The reply slot lives on the heap; it is freed only once the
    // destination finished with it or can no longer touch it.
    let mut slot = Box::new(crate::shard::CompactResp::empty());
    let responder = std::sync::Arc::new(crate::mailbox::BatchResponder::new());
    responder.prepare(&mut *slot as *mut _, 1);
    responder.make_cancellable();
    let msg = ShardMessage::Batch {
        db,
        items: vec![(0, hash, push)],
        responder: responder.clone(),
        is_resp3: false,
        client_id,
        no_touch: false,
        defer_bcast: false,
        replicate_as: Some(logical),
    };
    if router.senders[dst_shard].send(msg).is_err() {
        crate::log_warning!("cross-shard move: shard {} is not reachable", dst_shard);
        return undo_pop("failed: the destination shard is not reachable");
    }
    let timeout_ms = CROSS_SHARD_MOVE_TIMEOUT_MS.load(std::sync::atomic::Ordering::Relaxed);
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    let mut waited_out = false;
    while responder.try_take().is_none() {
        if !waited_out && std::time::Instant::now() >= deadline {
            waited_out = true;
            if responder.cancel() {
                // The destination will skip the push: the element is ours.
                crate::log_warning!(
                    "cross-shard move: shard {} did not take the push in {} ms; undoing the pop",
                    dst_shard,
                    timeout_ms
                );
                drop(slot);
                return undo_pop("timed out");
            }
            // Already running there: it completes, wait for it.
        }
        std::thread::yield_now();
    }
    popped
}

/// Runs the source half of a cross-shard move on shard `s` (see
/// [`move_element_from_source`]) and returns the pop's reply.
async fn move_element_via_source(
    router: &Router,
    s: usize,
    d: usize,
    pop: Command,
    push: crate::shard::MovePush,
    logical: Bytes,
) -> Vec<u8> {
    let db = CURRENT_DB.get();
    let client_id = requesting_client_id();
    if s == router.shard_id {
        return move_element_from_source(router, db, &pop, d, push, logical, client_id);
    }
    let (tx, rx) = flume::bounded(1);
    let msg = ShardMessage::MoveElement(Box::new(crate::shard::MoveElement {
        db,
        pop,
        dst_shard: d,
        push,
        logical,
        client_id,
        is_resp3: CURRENT_CLIENT_RESP3.get(),
        responder: tx,
    }));
    if router.senders[s].send(msg).is_err() {
        return b"-ERR internal shard routing error\r\n".to_vec();
    }
    rx.recv_async()
        .await
        .unwrap_or_else(|_| b"-ERR internal shard routing error\r\n".to_vec())
}

/// The RESP form of `cmd` to replicate (empty if it has none).
fn logical_repl_bytes(cmd: &Command) -> Bytes {
    crate::aof::command_to_resp(cmd)
        .map(Bytes::from)
        .unwrap_or_default()
}

/// Payload of a `$N\r\n...\r\n` bulk reply; None for nil or anything else.
fn resp_bulk_payload(resp: &[u8]) -> Option<Bytes> {
    let rest = resp.strip_prefix(b"$")?;
    let nl = rest.iter().position(|&b| b == b'\n')?;
    let len: usize = std::str::from_utf8(&rest[..nl])
        .ok()?
        .trim_end()
        .parse()
        .ok()?;
    let body = rest.get(nl + 1..nl + 1 + len)?;
    Some(Bytes::copy_from_slice(body))
}

/// Value of a `:N\r\n` integer reply, keeping the sign.
fn resp_integer(resp: &[u8]) -> Option<i64> {
    resp.strip_prefix(b":")
        .and_then(|r| r.strip_suffix(b"\r\n"))
        .and_then(|n| std::str::from_utf8(n).ok())
        .and_then(|n| n.parse().ok())
}

/// How [`move_key_across_shards`] treats the source and an existing target.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CrossShardMove {
    /// RENAME: overwrite the target, delete the source.
    Rename,
    /// RENAMENX: fail if the target exists, delete the source.
    RenameNx,
    /// COPY without REPLACE: fail if the target exists, keep the source.
    Copy,
    /// COPY REPLACE: overwrite the target, keep the source.
    CopyReplace,
}

/// RENAME / RENAMENX / COPY for keys owned by different shards. The value
/// moves as a DUMP payload plus its absolute expiry and is RESTOREd on the
/// target shard, then the source is deleted for renames. Both shards are
/// held with the same tx locks EXEC uses, so this is serialized with
/// transactions and other cross-shard moves (two concurrent renames of one
/// key cannot both copy it), with the same isolation EXEC gives against
/// plain commands. Each shard logs and replicates its own RESTORE / DEL.
/// Returns the command's reply.
async fn move_key_across_shards(
    router: &Router,
    src: &Bytes,
    dst: &Bytes,
    dst_db: usize,
    mode: CrossShardMove,
    in_tx: bool,
) -> Vec<u8> {
    let src_db = CURRENT_DB.get();
    router.ensure_loaded(src).await;
    if dst_db != src_db {
        CURRENT_DB.set(dst_db);
        router.local_db.borrow_mut().select_db(dst_db);
    }
    router.ensure_loaded(dst).await;
    if dst_db != src_db {
        CURRENT_DB.set(src_db);
        router.local_db.borrow_mut().select_db(src_db);
    }
    let (s, d) = (router.target_shard(src), router.target_shard(dst));
    let mut shards = [s, d];
    shards.sort_unstable();
    // Inside EXEC the transaction already holds the locks of all its keys.
    let lock = !in_tx;
    let tx_id = next_tx_id();
    if lock {
        router.acquire_tx_locks(&shards, tx_id).await;
    }
    let xwrite = lock.then(crate::router::XShardWriteGuard::locked);
    let res = async {
        let dump = exec_on_shard(router, s, Command::Dump(src.clone())).await;
        let Some(payload) = resp_bulk_payload(&dump) else {
            return if dump.starts_with(b"-") {
                Err(dump)
            } else {
                Ok(None)
            };
        };
        let expire_at =
            resp_integer(&exec_on_shard(router, s, Command::Expiretime(src.clone(), true)).await)
                .unwrap_or(-1);
        let replace = matches!(mode, CrossShardMove::Rename | CrossShardMove::CopyReplace);
        if dst_db != src_db {
            CURRENT_DB.set(dst_db);
            router.local_db.borrow_mut().select_db(dst_db);
        }
        if !replace {
            let exists =
                exec_on_shard(router, d, Command::Exists(smallvec::smallvec![dst.clone()])).await;
            if resp_integer(&exists).unwrap_or(0) > 0 {
                if dst_db != src_db {
                    CURRENT_DB.set(src_db);
                    router.local_db.borrow_mut().select_db(src_db);
                }
                return Ok(Some(false));
            }
        }
        let restored = exec_on_shard(
            router,
            d,
            Command::Restore {
                key: dst.clone(),
                ttl_ms: expire_at.max(0) as u64,
                serialized: payload,
                replace: true,
                absttl: expire_at > 0,
                asking: false,
                idletime: None,
                freq: None,
            },
        )
        .await;
        if dst_db != src_db {
            CURRENT_DB.set(src_db);
            router.local_db.borrow_mut().select_db(src_db);
        }
        if restored.starts_with(b"-") {
            return Err(restored);
        }
        if matches!(mode, CrossShardMove::Rename | CrossShardMove::RenameNx) {
            let _ = exec_on_shard(router, s, Command::Del(smallvec::smallvec![src.clone()])).await;
        }
        Ok(Some(true))
    }
    .await;
    drop(xwrite);
    if lock {
        router.release_tx_locks(&shards, tx_id).await;
    }
    let renames = matches!(mode, CrossShardMove::Rename | CrossShardMove::RenameNx);
    match res {
        Ok(Some(true)) if mode == CrossShardMove::Rename => b"+OK\r\n".to_vec(),
        Ok(Some(true)) => b":1\r\n".to_vec(),
        Ok(Some(false)) => b":0\r\n".to_vec(),
        Ok(None) if renames => b"-ERR no such key\r\n".to_vec(),
        Ok(None) => b":0\r\n".to_vec(),
        Err(e) => e,
    }
}

/// An element move between keys on different shards.
enum CrossShardElementMove {
    /// LMOVE / RPOPLPUSH: pop from `from` of the source list, push to `to`
    /// of the destination list.
    List {
        from: crate::table::ListDirection,
        to: crate::table::ListDirection,
        is_rpoplpush: bool,
    },
    /// SMOVE of one member between sets.
    Set { member: Bytes },
}

/// LMOVE / RPOPLPUSH / SMOVE for keys owned by different shards, under the
/// same tx locks as [`move_key_across_shards`]. Checks the types like Redis
/// does (a missing source wins over a wrong-typed destination), then pops or
/// removes the element on the source shard and pushes or adds it on the
/// destination shard; each shard logs to its AOF and notifies its own half,
/// while a single logical LMOVE / RPOPLPUSH / SMOVE is propagated to replicas.
/// Returns the command's reply.
async fn move_element_across_shards(
    router: &Router,
    src: &Bytes,
    dst: &Bytes,
    op: CrossShardElementMove,
    in_tx: bool,
) -> Vec<u8> {
    router.ensure_loaded(src).await;
    router.ensure_loaded(dst).await;
    let (s, d) = (router.target_shard(src), router.target_shard(dst));
    let mut shards = [s, d];
    shards.sort_unstable();
    // Inside EXEC the transaction already holds the locks of all its keys.
    let lock = !in_tx;
    let tx_id = next_tx_id();
    if lock {
        router.acquire_tx_locks(&shards, tx_id).await;
    }
    let xwrite = lock.then(crate::router::XShardWriteGuard::locked);
    const WRONGTYPE: &[u8] =
        b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    let (kind, missing): (&[u8], Vec<u8>) = match op {
        CrossShardElementMove::List { .. } => {
            let mut nil = Vec::new();
            write_resp_null(&mut nil);
            (b"+list\r\n", nil)
        }
        CrossShardElementMove::Set { .. } => (b"+set\r\n", b":0\r\n".to_vec()),
    };
    let reply = async {
        let src_type = exec_on_shard(router, s, Command::Type(src.clone())).await;
        if src_type == b"+none\r\n" {
            return missing;
        }
        if src_type != kind {
            return WRONGTYPE.to_vec();
        }
        let dst_type = exec_on_shard(router, d, Command::Type(dst.clone())).await;
        if dst_type != b"+none\r\n" && dst_type != kind {
            return WRONGTYPE.to_vec();
        }
        match op {
            CrossShardElementMove::List {
                from,
                to,
                is_rpoplpush,
            } => {
                let pop = match from {
                    crate::table::ListDirection::Left => Command::Lpop {
                        key: src.clone(),
                        count: None,
                    },
                    crate::table::ListDirection::Right => Command::Rpop {
                        key: src.clone(),
                        count: None,
                    },
                };
                // The destination's shard replicates the logical LMOVE with
                // the push, so it keeps its place among that list's other
                // writes (a later RPUSH from another client stays later),
                // while the source's shard waits, so none of its writes
                // comes between the pop and the LMOVE either.
                let lmove = logical_repl_bytes(&Command::Lmove {
                    source: src.clone(),
                    destination: dst.clone(),
                    where_from: from,
                    where_to: to,
                    is_rpoplpush,
                });
                let push = crate::shard::MovePush::List {
                    key: dst.clone(),
                    to,
                };
                let popped = move_element_via_source(router, s, d, pop, push, lmove).await;
                let Some(elem) = resp_bulk_payload(&popped) else {
                    return popped;
                };
                crate::replication::wait_streams_flushed(router.base_port, router.num_shards).await;
                let mut out = Vec::new();
                write_resp_bulk(&mut out, &elem);
                out
            }
            CrossShardElementMove::Set { member } => {
                let smove = logical_repl_bytes(&Command::Smove {
                    source: src.clone(),
                    destination: dst.clone(),
                    member: member.clone(),
                });
                let srem = Command::Srem {
                    key: src.clone(),
                    members: smallvec::smallvec![member.clone()],
                };
                let push = crate::shard::MovePush::Set {
                    key: dst.clone(),
                    member,
                };
                let removed = move_element_via_source(router, s, d, srem, push, smove).await;
                if resp_integer(&removed) != Some(1) {
                    return removed;
                }
                crate::replication::wait_streams_flushed(router.base_port, router.num_shards).await;
                b":1\r\n".to_vec()
            }
        }
    }
    .await;
    drop(xwrite);
    if lock {
        router.release_tx_locks(&shards, tx_id).await;
    }
    reply
}

/// BLMOVE / BRPOPLPUSH for keys owned by different shards (standalone
/// mode). Moves an element like LMOVE does, under both shards' locks. If the
/// source is empty, blocks until it has an element; the source's shard then
/// only wakes the client (keeping one element back from the waiters behind
/// it), and the move is tried again the same way, blocking again for the
/// rest of the timeout if another client took the element first. The pop
/// and push so always run as one locked move, which replicas get as one
/// LMOVE / RPOPLPUSH in the right place: a full sync holds it back while it
/// cuts, and the source's shard can't stream another write between them.
/// Returns whether the client disconnected.
#[allow(clippy::too_many_arguments)]
async fn blmove_across_shards(
    router: &Router,
    client_registry: &RefCell<hashbrown::HashMap<u64, ClientInfo>>,
    client_id: u64,
    source: &Bytes,
    destination: &Bytes,
    where_from: crate::table::ListDirection,
    where_to: crate::table::ListDirection,
    timeout: f64,
    is_rpoplpush: bool,
    out: &mut Vec<u8>,
) -> bool {
    let deadline = (timeout > 0.0)
        .then(|| std::time::Instant::now() + std::time::Duration::from_secs_f64(timeout));
    let mut woken = false;
    loop {
        let op = CrossShardElementMove::List {
            from: where_from,
            to: where_to,
            is_rpoplpush,
        };
        let reply =
            move_element_across_shards(router, source, destination, op, in_tx(client_id)).await;
        let is_nil = reply == b"$-1\r\n" || reply == b"_\r\n";
        if !is_nil || in_tx(client_id) {
            if woken {
                // What this wakeup kept back may be more than we took.
                recheck_blocking_keys(router, std::iter::once(source));
                if !is_nil {
                    notify_key_invalidation(router.port, source.as_ref(), client_id);
                    notify_key_invalidation(router.port, destination.as_ref(), client_id);
                }
            }
            out.extend_from_slice(&reply);
            return false;
        }
        let remaining = match deadline {
            None => 0.0,
            Some(d) => {
                let left = d.saturating_duration_since(std::time::Instant::now());
                if left.is_zero() {
                    write_resp_null(out);
                    return false;
                }
                left.as_secs_f64()
            }
        };
        let _guard = BlockedClientGuard {
            port: router.port,
            client_id,
        };
        let (tx, rx) = flume::bounded(1);
        {
            let hub_arc = crate::block::get_block_hub_for_port(router.port);
            let mut hub = hub_arc.lock();
            hub.register_blocked_client(client_id, tx.clone());
            hub.register_list_wake_waiter(client_id, source.clone(), tx);
        }
        recheck_blocking_keys(router, std::iter::once(source));
        let raw_fd = client_registry.borrow().get(&client_id).map(|c| c.raw_fd);
        let (recv_res, client_disconnected) = wait_for_blocked_result(&rx, remaining, raw_fd).await;
        CURRENT_CLIENT_ID.set(client_id);
        EXECUTING_CLIENT_ID.set(client_id);
        match recv_res {
            _ if client_disconnected => return true,
            // Woken: the source has an element, try the move again.
            Some(crate::block::BlockedListResult::Popped(..)) => woken = true,
            Some(crate::block::BlockedListResult::Unblocked(
                crate::block::ClientUnblockType::WrongType,
            )) => {
                out.extend_from_slice(
                    b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                );
                return false;
            }
            Some(crate::block::BlockedListResult::Unblocked(
                crate::block::ClientUnblockType::NoPerm,
            )) => {
                out.extend_from_slice(BLOCKED_NOPERM_REPLY);
                return false;
            }
            Some(crate::block::BlockedListResult::Unblocked(
                crate::block::ClientUnblockType::Error,
            )) => {
                out.extend_from_slice(b"-UNBLOCKED client unblocked via CLIENT UNBLOCK\r\n");
                return false;
            }
            Some(crate::block::BlockedListResult::Unblocked(
                crate::block::ClientUnblockType::Moved(slot),
            )) => {
                blocked_moved_reply(router, slot, out);
                return false;
            }
            _ => {
                write_resp_null(out);
                return false;
            }
        }
    }
}

pub fn record_replica_cmd_stats(cmd: &Command, elapsed_ns: u64) {
    record_cmd_stats(stat_cmd_name(cmd), 1, elapsed_ns);
}

pub async fn execute_cross_shard_replica_command(router: &Router, cmd: Command) {
    if crate::shard::aof_image_active() {
        aof_image_before_command(&router.local_db.borrow(), &cmd, |k| {
            router.target_shard(k) == router.shard_id
        });
    }
    let start = std::time::Instant::now();
    let name = get_cmd_name(&cmd);
    let out = match cmd {
        Command::Lmove {
            ref source,
            ref destination,
            where_from,
            where_to,
            is_rpoplpush,
        } => {
            let op = CrossShardElementMove::List {
                from: where_from,
                to: where_to,
                is_rpoplpush,
            };
            move_element_across_shards(router, source, destination, op, false).await
        }
        Command::Smove {
            ref source,
            ref destination,
            ref member,
        } => {
            let op = CrossShardElementMove::Set {
                member: member.clone(),
            };
            move_element_across_shards(router, source, destination, op, false).await
        }
        Command::Rename {
            ref key,
            ref newkey,
            nx,
        } => {
            let mode = if nx {
                CrossShardMove::RenameNx
            } else {
                CrossShardMove::Rename
            };
            move_key_across_shards(router, key, newkey, CURRENT_DB.get(), mode, false).await
        }
        Command::Copy {
            ref source,
            ref destination,
            destination_db,
            replace,
        } => {
            let dst_db = destination_db
                .map(|d| d as usize)
                .unwrap_or_else(|| CURRENT_DB.get());
            let mode = if replace {
                CrossShardMove::CopyReplace
            } else {
                CrossShardMove::Copy
            };
            move_key_across_shards(router, source, destination, dst_db, mode, false).await
        }
        Command::Msetnx(ref pairs) => msetnx_across_shards(router, pairs, false).await,
        Command::Bitop {
            ref op,
            ref destkey,
            ref srckeys,
        } => bitop_across_shards(router, op, destkey, srckeys, false).await,
        // Index definitions live on every shard.
        Command::FtCreate {
            index,
            on_type,
            prefixes,
            fields,
            schema_fields,
        } => {
            let schema = crate::search::IndexSchema {
                name: index,
                on_type,
                prefixes,
                fields,
                schema_fields,
            };
            match router.create_search_index(schema).await {
                Ok(()) => b"+OK\r\n".to_vec(),
                Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
            }
        }
        Command::FtAlter {
            ref index,
            ref fields,
            ref schema_fields,
        } => match router
            .alter_search_index(index, fields.clone(), schema_fields.clone())
            .await
        {
            Ok(()) => b"+OK\r\n".to_vec(),
            Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
        },
        Command::FtDropIndex { ref index, dd } => match router.drop_search_index(index, dd).await {
            Ok(()) => b"+OK\r\n".to_vec(),
            Err(e) => format!("-ERR {}\r\n", e).into_bytes(),
        },
        other => {
            let mut dummy_out = Vec::new();
            execute_local_command(
                &other,
                &mut router.local_db.borrow_mut(),
                &mut dummy_out,
                router.aof().as_deref(),
            );
            dummy_out
        }
    };
    record_cmd_stats(name, 1, start.elapsed().as_nanos() as u64);
    if out.first() == Some(&b'-') {
        let end = out.iter().position(|&b| b == b'\r').unwrap_or(out.len());
        crate::log_warning!(
            "== CRITICAL == This replica is sending an error to its master: '{}' after processing the command '{}'",
            String::from_utf8_lossy(&out[1..end]),
            name.to_ascii_lowercase(),
        );
    }
}

/// Runs one command per `(shard, command)` pair, the local shard's inline
/// and all remote ones concurrently, and returns the replies in input order.
async fn exec_on_shards(router: &Router, cmds: Vec<(usize, Command)>) -> Vec<Vec<u8>> {
    let mut replies = vec![Vec::new(); cmds.len()];
    let mut remote_idx = Vec::new();
    let mut remote = Vec::new();
    for (i, (shard, cmd)) in cmds.into_iter().enumerate() {
        if shard == router.shard_id {
            replies[i] = exec_on_shard(router, shard, cmd).await;
        } else {
            remote_idx.push(i);
            remote.push((shard, cmd));
        }
    }
    if !remote.is_empty() {
        for (i, reply) in remote_idx
            .into_iter()
            .zip(router.execute_remote_many(remote).await)
        {
            replies[i] = reply;
        }
    }
    replies
}

/// MSETNX for keys owned by several shards. Holding all their tx locks
/// (unless inside EXEC, which already holds them), checks that none of the
/// keys exists, then MSETs each shard's pairs. Like EXEC, it is atomic
/// against transactions and other cross-shard moves, not against plain
/// commands. Returns the command's reply.
async fn msetnx_across_shards(router: &Router, pairs: &[(Bytes, Bytes)], in_tx: bool) -> Vec<u8> {
    let mut groups: Vec<(usize, Vec<(Bytes, Bytes)>)> = Vec::new();
    for (k, v) in pairs {
        router.ensure_loaded(k).await;
        let shard = router.target_shard(k);
        match groups.iter_mut().find(|(s, _)| *s == shard) {
            Some((_, group)) => group.push((k.clone(), v.clone())),
            None => groups.push((shard, vec![(k.clone(), v.clone())])),
        }
    }
    let mut shards: Vec<usize> = groups.iter().map(|(s, _)| *s).collect();
    shards.sort_unstable();
    let lock = !in_tx;
    let tx_id = next_tx_id();
    if lock {
        router.acquire_tx_locks(&shards, tx_id).await;
    }
    let xwrite = lock.then(crate::router::XShardWriteGuard::locked);
    let exists = exec_on_shards(
        router,
        groups
            .iter()
            .map(|(s, group)| {
                (
                    *s,
                    Command::Exists(group.iter().map(|(k, _)| k.clone()).collect()),
                )
            })
            .collect(),
    )
    .await;
    let reply = if let Some(err) = exists.iter().find(|r| r.starts_with(b"-")) {
        err.clone()
    } else if exists.iter().any(|r| resp_integer(r) != Some(0)) {
        b":0\r\n".to_vec()
    } else {
        let set = exec_on_shards(
            router,
            groups
                .into_iter()
                .map(|(s, group)| (s, Command::Mset(group)))
                .collect(),
        )
        .await;
        match set.into_iter().find(|r| r.starts_with(b"-")) {
            Some(err) => err,
            None => b":1\r\n".to_vec(),
        }
    };
    drop(xwrite);
    if lock {
        router.release_tx_locks(&shards, tx_id).await;
    }
    reply
}

/// BITOP for keys owned by several shards. Holding their tx locks (unless
/// inside EXEC), GETs the sources, computes the result here and stores it on
/// the destination's shard, deleting the destination for an empty result
/// like Redis. Returns the command's reply.
async fn bitop_across_shards(
    router: &Router,
    op: &str,
    destkey: &Bytes,
    srckeys: &[Bytes],
    in_tx: bool,
) -> Vec<u8> {
    let mut shards = Vec::with_capacity(srckeys.len() + 1);
    for k in std::iter::once(destkey).chain(srckeys) {
        router.ensure_loaded(k).await;
        shards.push(router.target_shard(k));
    }
    shards.sort_unstable();
    shards.dedup();
    let lock = !in_tx;
    let tx_id = next_tx_id();
    if lock {
        router.acquire_tx_locks(&shards, tx_id).await;
    }
    let xwrite = lock.then(crate::router::XShardWriteGuard::locked);
    let reply = async {
        let gets = exec_on_shards(
            router,
            srckeys
                .iter()
                .map(|k| (router.target_shard(k), Command::Get(k.clone())))
                .collect(),
        )
        .await;
        let mut buffers = Vec::with_capacity(gets.len());
        for reply in gets {
            if reply.starts_with(b"-") {
                return reply;
            }
            buffers.push(resp_bulk_payload(&reply).map_or_else(Vec::new, |b| b.to_vec()));
        }
        let result = match crate::table::RudisTable::bitop_compute(&op.to_uppercase(), &buffers) {
            Ok(result) => result,
            Err(err) => {
                let mut out = Vec::new();
                write_resp_err(&mut out, err);
                return out;
            }
        };
        let len = result.len();
        let dst = router.target_shard(destkey);
        let store = if len == 0 {
            Command::Del(smallvec::smallvec![destkey.clone()])
        } else {
            Command::Mset(vec![(destkey.clone(), Bytes::from(result))])
        };
        let stored = exec_on_shard(router, dst, store).await;
        if stored.starts_with(b"-") {
            return stored;
        }
        format!(":{len}\r\n").into_bytes()
    }
    .await;
    drop(xwrite);
    if lock {
        router.release_tx_locks(&shards, tx_id).await;
    }
    reply
}

/// Value of a `:N\r\n` integer reply; 0 for errors or any other reply.
fn resp_integer_or_zero(resp: &[u8]) -> usize {
    resp.strip_prefix(b":")
        .and_then(|r| r.strip_suffix(b"\r\n"))
        .and_then(|n| std::str::from_utf8(n).ok())
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

/// Runs one command per remote shard concurrently (see
/// `Router::execute_remote_many`) and sums their integer replies.
async fn sum_remote_counts(router: &Router, cmds: Vec<(usize, Command)>) -> usize {
    if cmds.is_empty() {
        return 0;
    }
    router
        .execute_remote_many(cmds)
        .await
        .iter()
        .map(|r| resp_integer_or_zero(r))
        .sum()
}

pub(crate) struct RecvBytesMut(pub BytesMut);

// SAFETY: `write_ptr` is the start of the BytesMut's spare capacity and
// `bytes_total` is exactly its length, so that range is writable memory owned
// by `self.0`; the heap buffer does not move when the wrapper is moved, and
// `set_init` only extends `len` over bytes the kernel has written.
unsafe impl monoio::buf::IoBufMut for RecvBytesMut {
    #[inline(always)]
    fn write_ptr(&mut self) -> *mut u8 {
        // SAFETY: `len <= capacity`, so the offset stays within (or one past the end
        // of) the BytesMut's allocation.
        unsafe { self.0.as_mut_ptr().add(self.0.len()) }
    }

    #[inline(always)]
    fn bytes_total(&mut self) -> usize {
        self.0.capacity() - self.0.len()
    }

    /// # Safety
    ///
    /// The first `read_len` bytes at `write_ptr()` must have been initialized
    /// (e.g. by a completed read) and `read_len <= bytes_total()`.
    #[inline(always)]
    unsafe fn set_init(&mut self, read_len: usize) {
        // SAFETY: By the caller contract, `len + read_len <= capacity` and those
        // `read_len` spare bytes are initialized.
        unsafe {
            self.0.set_len(self.0.len() + read_len);
        }
    }
}

// Parts of this module, in files of their own.
mod tracking;
pub use self::tracking::*;
mod config;
pub use self::config::*;
mod replica_sync;
use self::replica_sync::*;
mod info;
pub(crate) use self::info::*;
mod command;
use self::command::*;
mod local_command;
pub use self::local_command::*;
#[cfg(test)]
mod tests;
