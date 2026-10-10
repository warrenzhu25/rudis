use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

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
    /// Wakes the replica's writer task; the bytes are in `pending`.
    pub sender: flume::Sender<Vec<u8>>,
    pub listening_port: AtomicU64,
    pub ack_offset: AtomicU64,
    pub last_ack_time: AtomicU64,
    /// The replica's address as seen by the master (INFO and ROLE).
    pub ip: Option<std::net::IpAddr>,
    /// Set while the replica's full-sync snapshot is being taken.
    pub full_sync: Option<FullSyncCut>,
    /// Stream bytes not yet taken by the writer task, appended in offset
    /// order under the backlog lock.
    pending: Mutex<Vec<u8>>,
    /// A wakeup is queued on `sender` and the writer has not taken
    /// `pending` since, so appenders need not queue another.
    wake_queued: AtomicBool,
    /// Bytes the writer took from `pending` and is still writing.
    inflight: std::sync::atomic::AtomicUsize,
    /// Set once the buffered stream broke `client-output-buffer-limit
    /// replica`; nothing more is buffered and the writer drops the link.
    overflowed: AtomicBool,
    /// When the buffered stream last went above the soft limit (ms since
    /// `limit_clock_ms`'s epoch, 0 while below it).
    soft_since_ms: AtomicU64,
    /// The replica's socket while its connection owns it, so breaking the
    /// output limit can drop the link even while the writer is blocked.
    conn_fd: Mutex<Option<i32>>,
}

/// Below this many buffered bytes the output limits are not looked up.
const OUTPUT_LIMIT_CHECK_MIN: usize = 64 * 1024;

#[cfg(test)]
thread_local! {
    /// Replica output limit for tests on this thread, instead of the
    /// process-wide config.
    static TEST_REPLICA_LIMIT: std::cell::Cell<Option<crate::connection::BufferLimit>> =
        const { std::cell::Cell::new(None) };
}

fn limit_clock_ms() -> u64 {
    static EPOCH: LazyLock<std::time::Instant> = LazyLock::new(std::time::Instant::now);
    EPOCH.elapsed().as_millis() as u64 + 1
}

/// Per-shard cut of the replication stream for a replica in full sync.
///
/// There is no fork, so each shard serializes its part of the snapshot on
/// its own thread at a different moment. Shard `i` arms itself right after
/// serializing; since a shard applies and replicates its changes on its own
/// thread, everything it replicated before arming is in the snapshot and
/// everything after is not. Changes from armed shards are buffered in
/// `pre` until every shard is armed, then sent right after the snapshot.
pub struct FullSyncCut {
    armed: Vec<std::sync::atomic::AtomicBool>,
    all_armed: std::sync::atomic::AtomicBool,
    pre: Mutex<Vec<u8>>,
}

enum Delivery {
    Send,
    Buffer,
    Skip,
}

fn unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl ConnectedReplica {
    /// False while the replica's full-sync snapshot is still being taken
    /// (Redis's `wait_bgsave`).
    pub fn is_online(&self) -> bool {
        self.full_sync
            .as_ref()
            .is_none_or(|cut| cut.all_armed.load(Ordering::Acquire))
    }

    /// What to do with bytes replicated by `shard` (None: unknown shard).
    fn delivery(&self, shard: Option<usize>) -> Delivery {
        let Some(cut) = &self.full_sync else {
            return Delivery::Send;
        };
        if cut.all_armed.load(Ordering::Acquire) {
            return Delivery::Send;
        }
        match shard {
            Some(i) if !cut.armed.get(i).is_some_and(|a| a.load(Ordering::Acquire)) => {
                Delivery::Skip
            }
            _ => Delivery::Buffer,
        }
    }

    /// Appends stream bytes; returns true when the caller must wake the
    /// writer (after releasing the backlog lock).
    fn append_pending(&self, bytes: &[u8]) -> bool {
        if self.overflowed.load(Ordering::Relaxed) {
            return false;
        }
        let mut pending = self.pending.lock();
        pending.extend_from_slice(bytes);
        let buffered = pending.len() + self.inflight.load(Ordering::Relaxed);
        if self.over_output_limit(buffered) {
            *pending = Vec::new();
            drop(pending);
            // Always wake, so the writer notices and drops the link.
            self.wake_queued.store(true, Ordering::Release);
            return true;
        }
        drop(pending);
        !self.wake_queued.swap(true, Ordering::AcqRel)
    }

    /// Buffers bytes that must follow the full-sync snapshot; returns true
    /// when this broke the output limit.
    fn append_pre(&self, cut: &FullSyncCut, bytes: &[u8]) -> bool {
        if self.overflowed.load(Ordering::Relaxed) {
            return false;
        }
        let mut pre = cut.pre.lock();
        pre.extend_from_slice(bytes);
        if self.over_output_limit(pre.len()) {
            *pre = Vec::new();
            return true;
        }
        false
    }

    /// Checks `buffered` bytes against `client-output-buffer-limit replica`
    /// and marks the replica overflowed when it breaks the hard limit, or
    /// stays above the soft limit for the configured seconds.
    fn over_output_limit(&self, buffered: usize) -> bool {
        if buffered < OUTPUT_LIMIT_CHECK_MIN {
            if self.soft_since_ms.load(Ordering::Relaxed) != 0 {
                self.soft_since_ms.store(0, Ordering::Relaxed);
            }
            return false;
        }
        #[cfg(test)]
        if let Some(limit) = TEST_REPLICA_LIMIT.with(|l| l.get()) {
            return self.over_limit(buffered, limit);
        }
        let limit = crate::connection::get_client_output_buffer_limit(
            crate::connection::ClientClass::Replica,
        );
        self.over_limit(buffered, limit)
    }

    fn over_limit(&self, buffered: usize, limit: crate::connection::BufferLimit) -> bool {
        let used = buffered as u64;
        let over = if limit.hard_limit > 0 && used >= limit.hard_limit {
            true
        } else if limit.soft_limit > 0 && used >= limit.soft_limit {
            let now = limit_clock_ms();
            let since = self.soft_since_ms.load(Ordering::Relaxed);
            if since == 0 {
                self.soft_since_ms.store(now, Ordering::Relaxed);
                limit.soft_seconds == 0
            } else {
                now.saturating_sub(since) >= limit.soft_seconds.saturating_mul(1000)
            }
        } else {
            if self.soft_since_ms.load(Ordering::Relaxed) != 0 {
                self.soft_since_ms.store(0, Ordering::Relaxed);
            }
            false
        };
        if over {
            self.overflowed.store(true, Ordering::Release);
        }
        over
    }

    /// Lets `drop_link` shut down `fd`. Call `detach_fd` before the
    /// socket closes, so a reused fd number is never shut down.
    pub fn attach_fd(&self, fd: i32) {
        let mut slot = self.conn_fd.lock();
        *slot = Some(fd);
        if self.is_overflowed() {
            // SAFETY: shutdown(2) takes no pointers. `fd` is the live socket being
            // attached, and we hold the `conn_fd` lock that `detach_fd` takes before the
            // socket closes.
            unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
        }
    }

    pub fn detach_fd(&self) {
        *self.conn_fd.lock() = None;
    }

    /// Shuts the replica's socket down; its writer and reader then fail
    /// and the replica reconnects.
    pub fn drop_link(&self) {
        if let Some(fd) = *self.conn_fd.lock() {
            // SAFETY: shutdown(2) takes no pointers. The `conn_fd` lock is held for the
            // call and the owner runs `detach_fd` under it before closing the socket, so
            // `fd` is still open and not a reused number.
            unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
        }
    }

    /// True once the replica broke its output buffer limit.
    pub fn is_overflowed(&self) -> bool {
        self.overflowed.load(Ordering::Acquire)
    }

    /// Takes everything appended so far. The writer task calls this on each
    /// wakeup; an empty result just means a coalesced wakeup. The taken
    /// bytes count against the output limit until `finish_write`.
    pub fn take_pending(&self) -> Vec<u8> {
        // Clear the flag first: an append racing with the take then either
        // lands in this batch or queues a new wakeup.
        self.wake_queued.store(false, Ordering::Release);
        let mut pending = self.pending.lock();
        let data = std::mem::take(&mut *pending);
        self.inflight.store(data.len(), Ordering::Relaxed);
        data
    }

    /// The writer finished writing the batch from `take_pending`.
    pub fn finish_write(&self) {
        self.inflight.store(0, Ordering::Relaxed);
    }
}

pub struct ShardReplicaFlow {
    pub client_id: u64,
    pub shard_id: usize,
    pub sender: flume::Sender<Vec<u8>>,
    /// The flow's socket, shut down if it falls too far behind. Cleared
    /// before the socket closes so a reused fd is never shut down.
    fd: Mutex<Option<i32>>,
    pub lsn: AtomicU64,
    pub ack_lsn: AtomicU64,
}

impl ShardReplicaFlow {
    /// Forgets the socket; called before it closes.
    pub fn detach_fd(&self) {
        *self.fd.lock() = None;
    }

    fn drop_link(&self) {
        if let Some(fd) = *self.fd.lock() {
            // SAFETY: shutdown(2) takes no pointers. The `fd` lock is held for the call
            // and `detach_fd` clears it under the same lock before the socket closes.
            unsafe { libc::shutdown(fd, libc::SHUT_RDWR) };
        }
    }
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

    /// Drops the history; the next byte appended has offset
    /// `current_master_offset + 1`.
    pub fn clear(&mut self, current_master_offset: u64) {
        self.write_idx = 0;
        self.len = 0;
        self.first_byte_offset = current_master_offset + 1;
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

    /// Changes the capacity to `new_size` bytes, keeping the most recent
    /// history that fits.
    pub fn resize(&mut self, new_size: usize) {
        let new_size = new_size.max(1);
        if new_size == self.max_size {
            return;
        }
        let keep = self.len.min(new_size);
        let mut buffer = vec![0u8; new_size];
        let start = (self.write_idx + self.max_size - keep) % self.max_size;
        let first = (self.max_size - start).min(keep);
        buffer[..first].copy_from_slice(&self.buffer[start..start + first]);
        buffer[first..keep].copy_from_slice(&self.buffer[..keep - first]);
        self.first_byte_offset += (self.len - keep) as u64;
        self.buffer = buffer;
        self.write_idx = keep % new_size;
        self.len = keep;
        self.max_size = new_size;
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
    /// Replica offsets `[lo, hi)` from which a partial resync would be
    /// wrong: a full-synced replica got the bytes in that range in another
    /// order than the backlog holds them (see `finish_full_sync`).
    psync_holes: Mutex<Vec<(u64, u64)>>,
    /// Socket of the running replica worker's master link, so `stop_sync`
    /// can interrupt a worker parked in a read.
    sync_conn: Mutex<Option<std::os::unix::io::RawFd>>,
    /// Closed when the latest replica worker exits; the next worker waits
    /// on it so two workers never apply changes or update the offset at
    /// the same time.
    worker_exit: Mutex<Option<flume::Receiver<()>>>,
    /// Set while this server loads a master's full-sync RDB.
    loading: AtomicBool,
    /// Address details a connection reported before its PSYNC registered
    /// it as a replica (REPLCONF listening-port comes first), by client id.
    pending_peers: Mutex<HashMap<u64, PendingPeer>>,
    /// When the last replica disconnected (unix seconds), 0 while replicas
    /// are connected or before the first one: `repl-backlog-ttl` counts
    /// from it.
    no_replicas_since: AtomicU64,
}

/// `repl-backlog-ttl`: seconds without replicas after which a master drops
/// its backlog (0: never), so a replica returning after that full-syncs.
pub static REPL_BACKLOG_TTL: AtomicU64 = AtomicU64::new(3600);

/// See `ReplicationHub::pending_peers`.
#[derive(Default, Clone, Copy)]
struct PendingPeer {
    port: u16,
    ip: Option<std::net::IpAddr>,
}

/// Bound on `pending_peers`, which only clients that never send PSYNC
/// leave entries in.
const MAX_PENDING_PEERS: usize = 4096;

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
            backlog: RwLock::new(ReplicationBacklog::new(repl_backlog_size())),
            replicas: RwLock::new(HashMap::new()),
            cancel_sync: RwLock::new(None),
            shard_flows: RwLock::new(HashMap::new()),
            has_shard_flows: std::sync::atomic::AtomicBool::new(false),
            psync_holes: Mutex::new(Vec::new()),
            loading: AtomicBool::new(false),
            sync_conn: Mutex::new(None),
            worker_exit: Mutex::new(None),
            pending_peers: Mutex::new(HashMap::new()),
            no_replicas_since: AtomicU64::new(0),
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

        let mut role = self.role.write();
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

    /// After a failed or interrupted sync: marks the link down and drops
    /// the master's replication id and offset, so the next attempt is a full
    /// resync. The dataset may not match any offset of the master's.
    fn forget_master_history(&self) {
        // A worker that panicked holding the lock may have left the sync
        // state half written; it is reset right here.
        let mut role = self.role.write();
        if let ReplicationRole::Slave {
            ref mut link_status,
            ref mut master_replid,
            ref mut master_repl_offset,
            ref mut sync_in_progress,
            ..
        } = *role
        {
            *link_status = "down".to_string();
            master_replid.clear();
            *master_repl_offset = 0;
            *sync_in_progress = true;
        }
    }

    pub fn stop_sync(&self) {
        if let Some(cancel) = self.cancel_sync.write().take() {
            let _ = cancel.send(());
        }
        // Wake a worker blocked reading from its master so it sees the
        // cancel now rather than after the master's next write. The worker
        // clears the slot before closing the socket, so the fd is open.
        if let Some(fd) = *self.sync_conn.lock() {
            // SAFETY: shutdown(2) takes no pointers. The `sync_conn` lock is held for the
            // call and the worker clears the slot under it before closing the socket.
            unsafe {
                libc::shutdown(fd, libc::SHUT_RDWR);
            }
        }
    }

    /// CLIENT KILL of a replica's master link (`addr` is `host:port` of
    /// the configured master): breaks the link without stopping
    /// replication, so the worker reconnects and tries a partial resync.
    /// Returns whether there was such a link.
    pub fn drop_master_link_to(&self, addr: &str) -> bool {
        let matches = match &*self.role.read() {
            ReplicationRole::Slave {
                master_host,
                master_port,
                ..
            } => addr == format!("{master_host}:{master_port}"),
            ReplicationRole::Master { .. } => false,
        };
        if !matches {
            return false;
        }
        let conn = self.sync_conn.lock();
        let Some(fd) = *conn else {
            return false;
        };
        // SAFETY: shutdown(2) takes no pointers. The `sync_conn` lock is held for the
        // call and the worker clears the slot under it before closing the socket.
        unsafe {
            libc::shutdown(fd, libc::SHUT_RDWR);
        }
        true
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
        // Registering under the backlog lock orders it against propagation.
        let _backlog = self.backlog.write();
        self.insert_replica(id, sender, None)
    }

    fn insert_replica(
        &self,
        id: u64,
        sender: flume::Sender<Vec<u8>>,
        full_sync: Option<FullSyncCut>,
    ) -> Arc<ConnectedReplica> {
        let peer = self.pending_peers.lock().remove(&id).unwrap_or_default();
        let rep = Arc::new(ConnectedReplica {
            id,
            sender,
            listening_port: AtomicU64::new(peer.port as u64),
            ack_offset: AtomicU64::new(0),
            // Like Redis, the lag clock starts at registration, so a new
            // replica counts as good until it misses acks.
            last_ack_time: AtomicU64::new(unix_secs()),
            ip: peer.ip,
            full_sync,
            pending: Mutex::new(Vec::new()),
            wake_queued: AtomicBool::new(false),
            inflight: std::sync::atomic::AtomicUsize::new(0),
            overflowed: AtomicBool::new(false),
            soft_since_ms: AtomicU64::new(0),
            conn_fd: Mutex::new(None),
        });
        self.replicas.write().insert(id, rep.clone());
        self.no_replicas_since.store(0, Ordering::Relaxed);
        self.has_replicas.store(true, Ordering::Release);
        self.backlog_active.store(true, Ordering::Release);
        HAS_ACTIVE_REPLICATION.store(true, Ordering::Release);
        rep
    }

    /// Registers a replica about to receive a full sync of `num_shards`
    /// shards. Must happen before any shard serializes its snapshot; each
    /// shard then calls `arm_full_sync` right after serializing.
    pub fn register_full_sync_replica(
        &self,
        id: u64,
        sender: flume::Sender<Vec<u8>>,
        num_shards: usize,
    ) -> Arc<ConnectedReplica> {
        let cut = FullSyncCut {
            armed: (0..num_shards)
                .map(|_| std::sync::atomic::AtomicBool::new(false))
                .collect(),
            all_armed: std::sync::atomic::AtomicBool::new(false),
            pre: Mutex::new(Vec::new()),
        };
        let _backlog = self.backlog.write();
        self.insert_replica(id, sender, Some(cut))
    }

    /// Called on shard `shard_id`'s thread right after it serialized its
    /// snapshot for replica `id`.
    pub fn arm_full_sync(&self, id: u64, shard_id: usize) {
        let _backlog = self.backlog.write();
        if let Some(rep) = self.replicas.read().get(&id)
            && let Some(cut) = &rep.full_sync
            && let Some(armed) = cut.armed.get(shard_id)
        {
            armed.store(true, Ordering::Release);
        }
    }

    /// Ends the snapshot phase of replica `id` once every shard is armed.
    /// Returns the offset to announce in +FULLRESYNC and the bytes to send
    /// right after the snapshot; from then on the replica gets the live
    /// stream, so its offset matches the master's.
    pub fn finish_full_sync(&self, id: u64) -> (u64, Vec<u8>) {
        let _backlog = self.backlog.write();
        let offset = self.master_repl_offset.load(Ordering::SeqCst);
        let pre = match self.replicas.read().get(&id) {
            Some(rep) => match &rep.full_sync {
                Some(cut) => {
                    let pre = std::mem::take(&mut *cut.pre.lock());
                    cut.all_armed.store(true, Ordering::Release);
                    pre
                }
                None => Vec::new(),
            },
            None => Vec::new(),
        };
        let start = offset.saturating_sub(pre.len() as u64);
        if start < offset {
            self.psync_holes.lock().push((start, offset));
        }
        (start, pre)
    }

    fn in_psync_hole(&self, replica_offset: u64, backlog_start: u64) -> bool {
        let mut holes = self.psync_holes.lock();
        holes.retain(|&(_, hi)| hi >= backlog_start);
        holes
            .iter()
            .any(|&(lo, hi)| replica_offset >= lo && replica_offset < hi)
    }

    pub fn unregister_replica(&self, id: u64) {
        let mut reps = self.replicas.write();
        if reps.remove(&id).is_some() && reps.is_empty() {
            self.no_replicas_since.store(unix_secs(), Ordering::Relaxed);
        }
        if reps.is_empty() {
            self.has_replicas.store(false, Ordering::Release);
        }
    }

    pub fn update_replica_ack(&self, id: u64, offset: u64) {
        // Every byte is counted in the master offset before it is queued
        // for a replica, so an ack beyond it is a lie or a bug; counting it
        // would let WAIT succeed for writes the replica never received.
        if offset > self.master_repl_offset.load(Ordering::SeqCst) {
            return;
        }
        if let Some(rep) = self.replicas.read().get(&id) {
            rep.ack_offset.store(offset, Ordering::SeqCst);
            rep.last_ack_time.store(unix_secs(), Ordering::SeqCst);
        }
    }

    pub fn set_replica_port(&self, id: u64, port: u16) {
        if let Some(rep) = self.replicas.read().get(&id) {
            rep.listening_port.store(port as u64, Ordering::SeqCst);
            return;
        }
        // REPLCONF listening-port normally comes before PSYNC registers
        // the replica; keep it for `insert_replica`.
        self.note_pending_peer(id, |p| p.port = port);
    }

    /// Records the address of connection `id`, which is about to register
    /// as a replica.
    pub fn note_replica_ip(&self, id: u64, ip: std::net::IpAddr) {
        self.note_pending_peer(id, |p| p.ip = Some(ip));
    }

    fn note_pending_peer(&self, id: u64, f: impl FnOnce(&mut PendingPeer)) {
        let mut peers = self.pending_peers.lock();
        if peers.len() >= MAX_PENDING_PEERS && !peers.contains_key(&id) {
            peers.clear();
        }
        f(peers.entry(id).or_default());
    }

    /// Replicas whose last ack is at most `max_lag` seconds old: the
    /// "good" replicas `min-replicas-to-write` counts.
    pub fn good_replicas(&self, max_lag: u64) -> usize {
        let now = unix_secs();
        self.replicas
            .read()
            .values()
            .filter(|r| {
                r.is_online()
                    && now.saturating_sub(r.last_ack_time.load(Ordering::SeqCst)) <= max_lag
            })
            .count()
    }

    pub fn register_shard_flow(
        &self,
        shard_id: usize,
        client_id: u64,
        sender: flume::Sender<Vec<u8>>,
        fd: Option<i32>,
    ) -> Arc<ShardReplicaFlow> {
        let flow = Arc::new(ShardReplicaFlow {
            client_id,
            shard_id,
            sender,
            fd: Mutex::new(fd),
            lsn: AtomicU64::new(0),
            ack_lsn: AtomicU64::new(0),
        });
        let mut flows = self.shard_flows.write();
        flows
            .entry(shard_id)
            .or_default()
            .insert(client_id, flow.clone());
        self.has_shard_flows.store(true, Ordering::Release);
        HAS_ACTIVE_REPLICATION.store(true, Ordering::Release);
        flow
    }

    pub fn unregister_shard_flow(&self, shard_id: usize, client_id: u64) {
        let mut flows = self.shard_flows.write();
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
        let flows = self.shard_flows.read();
        if let Some(map) = flows.get(&shard_id)
            && let Some(flow) = map.get(&client_id)
        {
            flow.ack_lsn.store(ack_lsn, Ordering::Relaxed);
        }
    }

    /// Applies `repl-backlog-ttl`: once no replica has been connected for
    /// that long, the backlog is dropped. Checked when a replica asks for a
    /// partial resync, the only time the backlog is read.
    fn expire_idle_backlog(&self, backlog: &mut ReplicationBacklog) {
        if !self.backlog_idle_expired() {
            return;
        }
        backlog.clear(self.master_repl_offset.load(Ordering::SeqCst));
        self.no_replicas_since.store(0, Ordering::Relaxed);
    }

    fn backlog_idle_expired(&self) -> bool {
        let since = self.no_replicas_since.load(Ordering::Relaxed);
        let ttl = REPL_BACKLOG_TTL.load(Ordering::Relaxed);
        since != 0 && ttl != 0 && unix_secs().saturating_sub(since) >= ttl
    }

    pub fn try_partial_resync(
        &self,
        client_id: u64,
        sender: flume::Sender<Vec<u8>>,
        req_replid: &str,
        req_offset: i64,
    ) -> Option<(String, Vec<u8>, Arc<ConnectedReplica>)> {
        // Like Redis, a replica asks for the offset of the next byte it
        // needs: one past what it applied.
        if req_offset < 1 {
            return None;
        }
        let target_offset = req_offset as u64;
        let req_offset = req_offset - 1;

        let (current_replid, replid_matches) = {
            let role = self.role.read();
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

        // Hold the backlog lock until the replica is registered, so no
        // change lands between the diff and the live stream.
        let mut backlog = self.backlog.write();
        self.expire_idle_backlog(&mut backlog);
        let current_offset = self.master_repl_offset.load(Ordering::SeqCst);
        if !backlog.can_partial_sync(target_offset, current_offset)
            || self.in_psync_hole(req_offset as u64, backlog.first_byte_offset)
        {
            return None;
        }

        let diff = backlog.get_diff(target_offset, current_offset)?;
        let rep = self.insert_replica(client_id, sender, None);
        drop(backlog);
        Some((current_replid, diff, rep))
    }

    pub fn can_partial_resync(&self, req_replid: &str, req_offset: i64) -> bool {
        // See `try_partial_resync`: `req_offset` is the next byte wanted.
        if req_offset < 1 {
            return false;
        }
        let target_offset = req_offset as u64;
        let req_offset = req_offset - 1;
        let role = self.role.read();
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
        if self.backlog_idle_expired() {
            return false;
        }
        let backlog = self.backlog.read();
        let current_offset = self.master_repl_offset.load(Ordering::SeqCst);
        backlog.can_partial_sync(target_offset, current_offset)
            && !self.in_psync_hole(req_offset as u64, backlog.first_byte_offset)
    }

    pub fn propagate(&self, bytes: &[u8]) {
        self.deliver(None, bytes);
    }

    pub fn propagate_shard(&self, shard_id: usize, bytes: &[u8]) {
        self.deliver(Some(shard_id), bytes);
    }

    /// Appends `bytes` to the backlog and queues them for replicas. The
    /// offset, the backlog append and the per-replica appends happen under
    /// the backlog lock, so every replica and the backlog see the same order
    /// of changes, and an offset always names the same byte for all of them.
    /// Writer wakeups (which may syscall) happen after releasing the lock.
    fn deliver(&self, shard: Option<usize>, bytes: &[u8]) {
        if !self.is_master() {
            return;
        }
        if self.backlog_active.load(Ordering::Relaxed) || self.has_replicas.load(Ordering::Relaxed)
        {
            let mut wake: Vec<Arc<ConnectedReplica>> = Vec::new();
            {
                let mut backlog = self.backlog.write();
                if self.backlog_active.load(Ordering::Relaxed) {
                    let new_offset = self
                        .master_repl_offset
                        .fetch_add(bytes.len() as u64, Ordering::SeqCst)
                        + bytes.len() as u64;
                    backlog.append(bytes, new_offset);
                }
                if self.has_replicas.load(Ordering::Relaxed) {
                    let reps = self.replicas.read();
                    for rep in reps.values() {
                        match rep.delivery(shard) {
                            Delivery::Send => {
                                if rep.append_pending(bytes) {
                                    wake.push(rep.clone());
                                }
                            }
                            Delivery::Buffer => {
                                if let Some(cut) = &rep.full_sync
                                    && rep.append_pre(cut, bytes)
                                {
                                    wake.push(rep.clone());
                                }
                            }
                            Delivery::Skip => {}
                        }
                    }
                }
            }
            for rep in &wake {
                if rep.is_overflowed() {
                    // The writer may be blocked on the full socket.
                    rep.drop_link();
                }
            }
            let dead: Vec<u64> = wake
                .iter()
                .filter(|rep| rep.sender.send(Vec::new()).is_err())
                .map(|rep| rep.id)
                .collect();
            if !dead.is_empty() {
                let mut reps = self.replicas.write();
                for id in dead {
                    reps.remove(&id);
                }
                if reps.is_empty() {
                    self.has_replicas.store(false, Ordering::Release);
                }
            }
        }

        if !self.has_shard_flows.load(Ordering::Relaxed) {
            return;
        }
        let dead_flows: Vec<(usize, u64)> = {
            let flows = self.shard_flows.read();
            let mut dead = Vec::new();
            for (&sid, map) in flows.iter() {
                if shard.is_some_and(|s| s != sid) {
                    continue;
                }
                for (&cid, flow) in map {
                    flow.lsn.fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    // Never block the writing shard on a slow flow: once its
                    // queue is full the flow is dropped and its socket shut
                    // down, and the replica resyncs (like an output buffer
                    // limit).
                    match flow.sender.try_send(bytes.to_vec()) {
                        Ok(()) => {}
                        Err(flume::TrySendError::Full(_)) => {
                            flow.drop_link();
                            dead.push((sid, cid));
                        }
                        Err(flume::TrySendError::Disconnected(_)) => dead.push((sid, cid)),
                    }
                }
            }
            dead
        };
        for (sid, cid) in dead_flows {
            self.unregister_shard_flow(sid, cid);
        }
    }

    pub fn format_role_resp(&self) -> Vec<u8> {
        let role = self.role.read().clone();
        match role {
            ReplicationRole::Master { .. } => {
                let offset = self.master_repl_offset.load(Ordering::SeqCst);
                let reps = self.replicas.read();
                let mut out = Vec::with_capacity(256);
                out.extend_from_slice(b"*3\r\n$6\r\nmaster\r\n:");
                out.extend_from_slice(offset.to_string().as_bytes());
                out.extend_from_slice(format!("\r\n*{}\r\n", reps.len()).as_bytes());
                for rep in reps.values() {
                    let rport = rep.listening_port.load(Ordering::SeqCst);
                    let rack = rep.ack_offset.load(Ordering::SeqCst);
                    let ip = rep
                        .ip
                        .map_or_else(|| "127.0.0.1".to_string(), |ip| ip.to_string());
                    out.extend_from_slice(format!("*3\r\n${}\r\n{}\r\n$", ip.len(), ip).as_bytes());
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
        let role = self.role.read().clone();
        match role {
            ReplicationRole::Master {
                replid,
                replid2,
                second_offset,
            } => {
                let reps = self.replicas.read();
                let offset = self.master_repl_offset.load(Ordering::SeqCst);
                let backlog = self.backlog.read();
                // One `slaveN:` line per replica, oldest first, as Redis.
                let mut sorted: Vec<_> = reps.values().collect();
                sorted.sort_by_key(|r| r.id);
                let now = unix_secs();
                let mut slaves = String::new();
                for (i, rep) in sorted.iter().enumerate() {
                    let ip = rep
                        .ip
                        .map_or_else(|| "127.0.0.1".to_string(), |ip| ip.to_string());
                    let _ = std::fmt::Write::write_fmt(
                        &mut slaves,
                        format_args!(
                            "slave{}:ip={},port={},state={},offset={},lag={}\r\n",
                            i,
                            ip,
                            rep.listening_port.load(Ordering::SeqCst),
                            if rep.is_online() {
                                "online"
                            } else {
                                "wait_bgsave"
                            },
                            rep.ack_offset.load(Ordering::SeqCst),
                            now.saturating_sub(rep.last_ack_time.load(Ordering::SeqCst)),
                        ),
                    );
                }
                format!(
                    "# Replication\r\n\
                     role:master\r\n\
                     connected_slaves:{}\r\n\
                     {}\
                     master_replid:{}\r\n\
                     master_replid2:{}\r\n\
                     master_repl_offset:{}\r\n\
                     second_repl_offset:{}\r\n\
                     repl_backlog_active:1\r\n\
                     repl_backlog_size:{}\r\n\
                     repl_backlog_first_byte_offset:{}\r\n\
                     repl_backlog_histlen:{}\r\n",
                    reps.len(),
                    slaves,
                    replid,
                    replid2,
                    offset,
                    // Redis shows the first offset replid2 no longer
                    // covers: one past the switch offset.
                    if second_offset >= 0 {
                        second_offset + 1
                    } else {
                        -1
                    },
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

/// Full-sync loads running in this process, so commands pay one relaxed
/// load and only look up their server's flag while some load runs.
static FULL_SYNC_LOADS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// True while the server on `port` loads a master's full-sync RDB, when
/// data commands are refused with `-LOADING` like in Redis.
#[inline]
pub fn is_loading(port: u16) -> bool {
    FULL_SYNC_LOADS.load(Ordering::Relaxed) != 0
        && get_replication_hub(port).loading.load(Ordering::Relaxed)
}

/// Marks `hub` as loading until dropped, so a panicking load clears it too.
struct LoadingGuard<'a>(&'a ReplicationHub);

impl<'a> LoadingGuard<'a> {
    fn new(hub: &'a ReplicationHub) -> Self {
        hub.loading.store(true, Ordering::Relaxed);
        FULL_SYNC_LOADS.fetch_add(1, Ordering::Relaxed);
        Self(hub)
    }
}

impl Drop for LoadingGuard<'_> {
    fn drop(&mut self) {
        self.0.loading.store(false, Ordering::Relaxed);
        FULL_SYNC_LOADS.fetch_sub(1, Ordering::Relaxed);
    }
}

static REPLICATION_HUBS: LazyLock<RwLock<HashMap<u16, Arc<ReplicationHub>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

pub fn get_replication_hub(port: u16) -> Arc<ReplicationHub> {
    {
        let hubs = REPLICATION_HUBS.read();
        if let Some(hub) = hubs.get(&port) {
            return hub.clone();
        }
    }
    let mut hubs = REPLICATION_HUBS.write();
    hubs.entry(port)
        .or_insert_with(|| Arc::new(ReplicationHub::new(port)))
        .clone()
}

static REPL_BACKLOG_SIZE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(1024 * 1024);

/// `repl-backlog-size` in bytes.
pub fn repl_backlog_size() -> usize {
    REPL_BACKLOG_SIZE.load(Ordering::Relaxed)
}

/// Sets `repl-backlog-size` and resizes every existing backlog, keeping
/// its most recent history.
pub fn set_repl_backlog_size(bytes: usize) {
    let bytes = bytes.max(16 * 1024);
    REPL_BACKLOG_SIZE.store(bytes, Ordering::Relaxed);
    for hub in REPLICATION_HUBS.read().values() {
        hub.backlog.write().resize(bytes);
    }
}

#[inline(always)]
pub fn has_connected_replicas(port: u16) -> bool {
    if !HAS_ACTIVE_REPLICATION.load(Ordering::Relaxed) {
        return false;
    }
    let hubs = REPLICATION_HUBS.read();
    if let Some(hub) = hubs.get(&port) {
        hub.has_replicas.load(Ordering::Relaxed) || hub.backlog_active.load(Ordering::Relaxed)
    } else {
        false
    }
}

#[inline(always)]
pub fn get_connected_replicas_count(port: u16) -> usize {
    if !HAS_ACTIVE_REPLICATION.load(Ordering::Relaxed) {
        return 0;
    }
    let hubs = REPLICATION_HUBS.read();
    if let Some(hub) = hubs.get(&port) {
        if hub.has_replicas.load(Ordering::Relaxed) {
            hub.replicas.read().len()
        } else {
            0
        }
    } else {
        0
    }
}

pub async fn wait_replicas(port: u16, numreplicas: usize, timeout_ms: u64) -> usize {
    if !HAS_ACTIVE_REPLICATION.load(Ordering::Relaxed) {
        return 0;
    }
    let hub = get_replication_hub(port);
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_millis(timeout_ms);
    let target_offset = hub.master_repl_offset.load(Ordering::SeqCst);
    loop {
        let count = {
            let reps = hub.replicas.read();
            if reps.is_empty() {
                return 0;
            }
            if target_offset == 0 {
                reps.len()
            } else {
                reps.values()
                    .filter(|r| r.ack_offset.load(Ordering::SeqCst) >= target_offset)
                    .count()
            }
        };
        if count >= numreplicas || timeout_ms == 0 || start.elapsed() >= timeout {
            return count;
        }
        monoio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

thread_local! {
    /// Set while this shard thread re-applies data that is already on the
    /// replicas (`DEBUG LOADAOF` replaying the shard's AOF): nothing it
    /// changes must be propagated again.
    static PROPAGATION_SUPPRESSED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Suppresses replication propagation from this thread until dropped.
pub struct SuppressPropagation {
    prev: bool,
}

impl SuppressPropagation {
    pub fn new() -> Self {
        Self {
            prev: PROPAGATION_SUPPRESSED.replace(true),
        }
    }
}

impl Default for SuppressPropagation {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SuppressPropagation {
    fn drop(&mut self) {
        PROPAGATION_SUPPRESSED.set(self.prev);
    }
}

#[inline(always)]
pub fn propagate_bytes(port: u16, bytes: &[u8]) {
    if !HAS_ACTIVE_REPLICATION.load(Ordering::Relaxed) || PROPAGATION_SUPPRESSED.get() {
        return;
    }
    let hub = get_replication_hub(port);
    hub.propagate(bytes);
}

#[inline(always)]
pub fn propagate_shard_bytes(port: u16, shard_id: usize, bytes: &[u8]) {
    if !HAS_ACTIVE_REPLICATION.load(Ordering::Relaxed) || PROPAGATION_SUPPRESSED.get() {
        return;
    }
    let hub = get_replication_hub(port);
    hub.propagate_shard(shard_id, bytes);
}

/// Appends the mutation built by `make` to this shard's AOF and replicates
/// it. Must run on the thread of the shard that owns the data (`shard_id`),
/// right after applying the change, so that the AOF, the replication stream
/// and full-sync snapshots all see each shard's changes in apply order.
/// `make` only runs when there is somewhere to log to.
pub fn log_shard_mutation(
    port: u16,
    shard_id: usize,
    aof: Option<&std::cell::RefCell<crate::aof::AofWriter>>,
    make: impl FnOnce() -> crate::resp::Command,
) {
    let replicate = has_connected_replicas(port);
    if aof.is_none() && !replicate {
        return;
    }
    if let Some(bytes) = crate::aof::command_to_resp(&make()) {
        if let Some(aof) = aof {
            aof.borrow_mut().append(&bytes);
        }
        if replicate {
            propagate_shard_bytes(port, shard_id, &bytes);
        }
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
        let role = hub.role.read();
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

    *hub.role.write() = ReplicationRole::Slave {
        master_host: master_host.clone(),
        master_port,
        link_status: "connecting".to_string(),
        master_repl_offset: cached_offset,
        master_replid: cached_replid,
        sync_in_progress: true,
    };

    let (cancel_tx, cancel_rx) = flume::bounded(1);
    *hub.cancel_sync.write() = Some(cancel_tx);
    let (exit_tx, exit_rx) = flume::bounded::<()>(1);
    let prev_exit = hub.worker_exit.lock().replace(exit_rx);

    let hub_clone = hub.clone();
    monoio::spawn(async move {
        // The previous worker (stopped above) may still be applying a
        // command or updating the offset; the PSYNC offset must be read
        // after it is done, or the resumed stream repeats changes.
        if let Some(prev) = prev_exit {
            let _ = prev.recv_async().await;
        }
        let worker_cancel = cancel_rx.clone();
        let worker_hub = hub_clone.clone();
        supervise_replica_worker(
            || {
                run_replica_worker(
                    port,
                    master_host.clone(),
                    master_port,
                    router.clone(),
                    worker_cancel.clone(),
                    worker_hub.clone(),
                )
            },
            |msg| {
                eprintln!("Replication with MASTER {master_host}:{master_port} panicked: {msg}");
                crate::connection::inc_isolated_panics();
                // Stopped meanwhile (REPLICAOF NO ONE or another master):
                // the role belongs to whoever stopped us now.
                if cancel_rx.is_disconnected() {
                    return false;
                }
                hub_clone.forget_master_history();
                true
            },
            std::time::Duration::from_secs(1),
        )
        .await;
        drop(exit_tx);
    });
}

/// Runs the replica worker made by `start` until it returns. A panic in it
/// is reported to `on_panic`, and if that returns true the worker restarts
/// after `backoff`: replication reconnects instead of silently stopping.
async fn supervise_replica_worker<W: Future<Output = ()>>(
    mut start: impl FnMut() -> W,
    mut on_panic: impl FnMut(&str) -> bool,
    backoff: std::time::Duration,
) {
    loop {
        let Err(panic) = crate::server::catch_unwind_async(start()).await else {
            return;
        };
        let msg = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("unknown panic");
        if !on_panic(msg) {
            return;
        }
        monoio::time::sleep(backoff).await;
    }
}

/// `masteruser` / `masterauth` per server port.
static MASTER_AUTH: LazyLock<RwLock<HashMap<u16, (Option<String>, Option<String>)>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Sets `masterauth` (empty = none).
pub fn set_masterauth(port: u16, pass: &str) {
    let mut m = MASTER_AUTH.write();
    m.entry(port).or_default().1 = (!pass.is_empty()).then(|| pass.to_string());
}

/// Sets `masteruser` (empty = none, i.e. AUTH as the default user).
pub fn set_masteruser(port: u16, user: &str) {
    let mut m = MASTER_AUTH.write();
    m.entry(port).or_default().0 = (!user.is_empty()).then(|| user.to_string());
}

/// The AUTH command a replica sends to its master, if `masterauth` is set.
fn master_auth_command(port: u16) -> Option<Vec<u8>> {
    let m = MASTER_AUTH.read();
    let (user, pass) = m.get(&port)?;
    let pass = pass.as_ref()?;
    let mut args: Vec<&[u8]> = vec![b"AUTH"];
    if let Some(u) = user {
        args.push(u.as_bytes());
    }
    args.push(pass.as_bytes());
    let mut out = format!("*{}\r\n", args.len()).into_bytes();
    for a in args {
        out.extend_from_slice(format!("${}\r\n", a.len()).as_bytes());
        out.extend_from_slice(a);
        out.extend_from_slice(b"\r\n");
    }
    Some(out)
}

fn resolve_master_addr(host: &str, port: u16) -> Option<std::net::SocketAddr> {
    use std::net::ToSocketAddrs;
    let mut addrs = (host, port).to_socket_addrs().ok()?;
    let first = addrs.next()?;
    // Prefer IPv4 like the old "localhost" special case did.
    Some(
        std::iter::once(first)
            .chain(addrs)
            .find(|a| a.is_ipv4())
            .unwrap_or(first),
    )
}

/// `replicaof <host> <port>` from the config file, started once the server
/// is up (see server.rs).
static STARTUP_REPLICAOF: LazyLock<Mutex<HashMap<u16, (String, u16)>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Parses a config-file `replicaof` value and queues it for startup.
/// `no one` clears it.
pub fn set_startup_replicaof(port: u16, value: &str) -> Result<(), String> {
    let parts: Vec<&str> = value.split_whitespace().collect();
    let mut m = STARTUP_REPLICAOF.lock();
    match parts.as_slice() {
        [no, one] if no.eq_ignore_ascii_case("no") && one.eq_ignore_ascii_case("one") => {
            m.remove(&port);
            Ok(())
        }
        [host, mport] => {
            let mport = mport
                .parse::<u16>()
                .map_err(|_| format!("invalid master port '{}'", mport))?;
            m.insert(port, (host.to_string(), mport));
            Ok(())
        }
        _ => Err("expected <masterip> <masterport>".to_string()),
    }
}

pub fn take_startup_replicaof(port: u16) -> Option<(String, u16)> {
    STARTUP_REPLICAOF.lock().remove(&port)
}

#[inline]
fn is_sync_cancelled(rx: &flume::Receiver<()>) -> bool {
    rx.try_recv().is_ok() || rx.is_disconnected()
}

/// Clears `ReplicationHub::sync_conn` when the worker's master link goes
/// away (before its socket is closed).
struct SyncConnGuard<'a>(&'a ReplicationHub);

impl Drop for SyncConnGuard<'_> {
    fn drop(&mut self) {
        *self.0.sync_conn.lock() = None;
    }
}

/// Longest line accepted on a replication link outside the command stream:
/// a master's handshake replies and RDB `$<len>` header, and everything a
/// replica sends (REPLCONF ACKs). Like Redis' PROTO_INLINE_MAX_SIZE.
pub const MAX_REPL_LINE: usize = 64 * 1024;

/// How long a replica waits for each handshake reply (Redis' default
/// repl-timeout) before reconnecting.
const REPL_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// Splits the first CRLF-terminated line off `buf`. `Ok(None)` means more
/// data is needed; `Err` that `buf` holds a longer line than a master ever
/// sends, so the link is dropped rather than buffered without bound.
fn take_repl_line(buf: &mut bytes::BytesMut) -> Result<Option<bytes::BytesMut>, ()> {
    match buf.windows(2).position(|w| w == b"\r\n") {
        Some(pos) if pos <= MAX_REPL_LINE => Ok(Some(buf.split_to(pos + 2))),
        Some(_) => Err(()),
        None if buf.len() > MAX_REPL_LINE => Err(()),
        None => Ok(None),
    }
}

/// Takes the `$<len>` line that precedes a full-sync RDB, skipping the bare
/// `\n` keepalives Redis masters send while producing it. A length that is
/// not a plain number (including diskless `$EOF:` transfers, which are not
/// supported) is an error.
fn take_rdb_header(buf: &mut bytes::BytesMut) -> Result<Option<usize>, ()> {
    loop {
        let keepalives = buf.iter().take_while(|&&b| b == b'\n').count();
        bytes::Buf::advance(buf, keepalives);
        let Some(line) = take_repl_line(buf)? else {
            return Ok(None);
        };
        if let Some(len) = line.strip_prefix(b"$") {
            return crate::resp::parse_decimal_bytes(&len[..len.len() - 2])
                .map(Some)
                .ok_or(());
        }
    }
}

/// A replication id as masters send it: 40 alphanumeric characters.
fn valid_replid(id: &str) -> bool {
    id.len() == 40 && id.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// Parses a master's PSYNC reply: `+FULLRESYNC <replid> <offset>` or
/// `+CONTINUE [<replid>]`. Returns whether it continues, the replication id
/// and the offset to resume from; offsets are Redis' signed 64-bit values.
fn parse_psync_reply(
    line: &[u8],
    cached_replid: &str,
    cached_offset: u64,
) -> Option<(bool, String, u64)> {
    let text = std::str::from_utf8(line).ok()?;
    match text.split_whitespace().collect::<Vec<_>>().as_slice() {
        ["+CONTINUE"] => Some((true, cached_replid.to_string(), cached_offset)),
        ["+CONTINUE", id] if valid_replid(id) => Some((true, id.to_string(), cached_offset)),
        ["+FULLRESYNC", id, off] if valid_replid(id) => {
            let off = u64::try_from(off.parse::<i64>().ok()?).ok()?;
            Some((false, id.to_string(), off))
        }
        _ => None,
    }
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

    'reconnect_loop: loop {
        if is_sync_cancelled(&cancel_rx) {
            break 'reconnect_loop;
        }

        // Resolved on every attempt, so a master behind a DNS name (e.g. a
        // Kubernetes service) is found again after it moves.
        let addr = resolve_master_addr(&master_host, master_port);
        let connected = match addr {
            Some(addr) => TcpStream::connect(&addr).await.ok(),
            None => None,
        };
        let mut stream = match connected {
            Some(s) => s,
            None => {
                if let ReplicationRole::Slave {
                    ref mut link_status,
                    ..
                } = *hub.role.write()
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
        *hub.sync_conn.lock() = Some(std::os::unix::io::AsRawFd::as_raw_fd(&stream));
        // Declared after `stream`, so it is dropped (clearing the slot)
        // before the socket is closed.
        let _conn_guard = SyncConnGuard(&hub);
        if is_sync_cancelled(&cancel_rx) {
            break 'reconnect_loop;
        }

        let mut buf = bytes::BytesMut::with_capacity(65536);
        let mut read_buf = vec![0u8; 64 * 1024];

        macro_rules! send_and_expect_line {
            ($payload:expr) => {{
                if stream.write_all($payload).await.0.is_err() {
                    if let ReplicationRole::Slave {
                        ref mut link_status,
                        ..
                    } = *hub.role.write()
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
                    match take_repl_line(&mut buf) {
                        Ok(Some(line)) => {
                            line_res = Some(line);
                            break;
                        }
                        Ok(None) => {}
                        Err(()) => break,
                    }
                    let read = monoio::time::timeout(
                        REPL_HANDSHAKE_TIMEOUT,
                        stream.read(std::mem::take(&mut read_buf)),
                    )
                    .await;
                    let res = match read {
                        Ok((res, returned)) => {
                            read_buf = returned;
                            res
                        }
                        Err(_) => Err(std::io::ErrorKind::TimedOut.into()),
                    };
                    match res {
                        Ok(0) | Err(_) => {
                            if let ReplicationRole::Slave {
                                ref mut link_status,
                                ..
                            } = *hub.role.write()
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

        // 1. PING. A master with a password answers -NOAUTH before AUTH,
        // which Redis accepts here too.
        let line = send_and_expect_line!(b"*1\r\n$4\r\nPING\r\n");
        if !line.starts_with(b"+PONG") && !line.starts_with(b"-NOAUTH") {
            if is_sync_cancelled(&cancel_rx) {
                break 'reconnect_loop;
            }
            monoio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue 'reconnect_loop;
        }

        // 1b. AUTH with masteruser/masterauth.
        if let Some(auth) = master_auth_command(hub.port) {
            let line = send_and_expect_line!(auth);
            if !line.starts_with(b"+OK") {
                eprintln!(
                    "Unable to AUTH to MASTER {}:{}: {}",
                    master_host,
                    master_port,
                    String::from_utf8_lossy(&line).trim_end()
                );
                if is_sync_cancelled(&cancel_rx) {
                    break 'reconnect_loop;
                }
                monoio::time::sleep(std::time::Duration::from_millis(1000)).await;
                continue 'reconnect_loop;
            }
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
            let role = hub.role.read();
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
            // Like Redis, ask for the next byte: one past what was applied.
            let next = (cached_offset + 1).to_string();
            format!(
                "*3\r\n$5\r\nPSYNC\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                cached_replid.len(),
                cached_replid,
                next.len(),
                next
            )
            .into_bytes()
        } else {
            b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".to_vec()
        };

        let line = send_and_expect_line!(psync_payload);
        let Some((is_continue, new_replid, initial_offset)) =
            parse_psync_reply(&line, &cached_replid, cached_offset)
        else {
            if let ReplicationRole::Slave {
                ref mut link_status,
                ..
            } = *hub.role.write()
            {
                *link_status = "down".to_string();
            }
            if is_sync_cancelled(&cancel_rx) {
                break 'reconnect_loop;
            }
            monoio::time::sleep(std::time::Duration::from_millis(50)).await;
            continue 'reconnect_loop;
        };

        if !is_continue {
            // 5. Read RDB header: $<len>\r\n
            let rdb_len = loop {
                match take_rdb_header(&mut buf) {
                    Ok(Some(len)) => break Some(len),
                    Ok(None) => {}
                    Err(()) => break None,
                }
                let (res, returned) = stream.read(read_buf).await;
                read_buf = returned;
                match res {
                    Ok(0) | Err(_) => break None,
                    Ok(n) => buf.extend_from_slice(&read_buf[..n]),
                }
            };

            let rdb_len = match rdb_len {
                Some(l) => l,
                None => {
                    if let ReplicationRole::Slave {
                        ref mut link_status,
                        ..
                    } = *hub.role.write()
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
                } = *hub.role.write()
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

            // 7. Restore RDB into router. A failed load leaves the dataset
            // empty; forget the master's history too, so the next attempt
            // is a full resync rather than a partial one on top of nothing.
            let loaded = {
                let _loading = LoadingGuard::new(&hub);
                router.restore_rdb_bytes(rdb_bytes).await
            };
            if let Err(e) = loaded {
                eprintln!(
                    "Failed to load the RDB from MASTER {}:{}: {}",
                    master_host, master_port, e
                );
                hub.forget_master_history();
                if is_sync_cancelled(&cancel_rx) {
                    break 'reconnect_loop;
                }
                monoio::time::sleep(std::time::Duration::from_millis(1000)).await;
                continue 'reconnect_loop;
            }
        }

        // 8. Mark link_status up
        {
            let mut role = hub.role.write();
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

        // 9. Streaming loop: receive and apply mutations. Commands for other
        // shards are batched per read and applied with one message per
        // shard; the offset advances once the whole batch is applied.
        let mut current_offset = initial_offset;
        let mut batch: Vec<Vec<crate::resp::Command>> = vec![Vec::new(); router.num_shards];
        // Like a Redis replica, acknowledge the applied offset every second
        // (and at once on REPLCONF GETACK): the master needs it for replica
        // lag in INFO and for min-replicas-max-lag. A separate writer task
        // owns the write half, so waiting for the next ack never interrupts
        // a read of the stream.
        let (mut stream, mut ack_writer) = monoio::io::Splitable::into_split(stream);
        let applied = std::rc::Rc::new(std::cell::Cell::new(initial_offset));
        // Wakes the writer for an immediate ack (REPLCONF GETACK).
        let (ack_tx, ack_rx) = flume::unbounded::<()>();
        let applied_w = applied.clone();
        monoio::spawn(async move {
            loop {
                match monoio::time::timeout(std::time::Duration::from_secs(1), ack_rx.recv_async())
                    .await
                {
                    Ok(Ok(())) | Err(_) => {}
                    Ok(Err(_)) => break,
                }
                while ack_rx.try_recv().is_ok() {}
                let off = applied_w.get().to_string();
                let ack = format!(
                    "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${}\r\n{}\r\n",
                    off.len(),
                    off
                );
                if ack_writer.write_all(ack.into_bytes()).await.0.is_err() {
                    break;
                }
            }
        });
        loop {
            if is_sync_cancelled(&cancel_rx) {
                break 'reconnect_loop;
            }
            let mut protocol_error = false;

            while !buf.is_empty() {
                let initial_buf_len = buf.len();
                match crate::resp::parse_command(&mut buf) {
                    Ok(Some(cmd)) => {
                        let consumed = initial_buf_len - buf.len();
                        current_offset += consumed as u64;

                        match &cmd {
                            crate::resp::Command::Replconf(args) => {
                                if args.len() >= 2 && args[0].eq_ignore_ascii_case(b"getack") {
                                    // Acknowledge only what is applied.
                                    router.flush_replica_batch(&mut batch).await;
                                    applied.set(current_offset);
                                    let _ = ack_tx.send(());
                                }
                            }
                            crate::resp::Command::Ping(_) => {}
                            _ => {
                                router.apply_replica_command_batched(cmd, &mut batch).await;
                            }
                        }
                    }
                    Ok(None) => break,
                    // Skipping the bad bytes would silently diverge from
                    // the master; reconnect and resume from what was applied.
                    Err(_) => {
                        protocol_error = true;
                        break;
                    }
                }
            }
            router.flush_replica_batch(&mut batch).await;
            if let ReplicationRole::Slave {
                ref mut master_repl_offset,
                ..
            } = *hub.role.write()
            {
                *master_repl_offset = current_offset;
            }
            // The periodic ack reports this from its next tick.
            applied.set(current_offset);
            if protocol_error {
                break;
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
        } = *hub.role.write()
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
    } = *hub.role.write()
    {
        *link_status = "down".to_string();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[monoio::test(timer_enabled = true)]
    async fn test_supervisor_restarts_a_panicked_worker_until_it_returns() {
        let runs = std::cell::Cell::new(0);
        let mut panics = Vec::new();
        supervise_replica_worker(
            || {
                runs.set(runs.get() + 1);
                let run = runs.get();
                async move {
                    if run == 1 {
                        panic!("boom");
                    }
                }
            },
            |msg| {
                panics.push(msg.to_string());
                true
            },
            std::time::Duration::from_millis(1),
        )
        .await;
        assert_eq!(runs.get(), 2);
        assert_eq!(panics, ["boom"]);
    }

    #[monoio::test(timer_enabled = true)]
    async fn test_supervisor_stops_when_told_not_to_restart() {
        let runs = std::cell::Cell::new(0);
        supervise_replica_worker(
            || {
                runs.set(runs.get() + 1);
                async { panic!("{}", String::from("owned boom")) }
            },
            |msg| {
                assert_eq!(msg, "owned boom");
                false
            },
            std::time::Duration::from_millis(1),
        )
        .await;
        assert_eq!(runs.get(), 1);
    }

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
        let res = hub.try_partial_resync(1, tx.clone(), &replid, 1);
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
        let res2 = hub.try_partial_resync(2, tx.clone(), &replid, 1);
        assert!(res2.is_some());
        let (_, diff2, _) = res2.unwrap();
        assert_eq!(diff2, b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n");
        hub.unregister_replica(2);

        // Replay from current offset (up to date)
        let res3 = hub.try_partial_resync(3, tx.clone(), &replid, current_offset as i64 + 1);
        assert!(res3.is_some());
        let (_, diff3, _) = res3.unwrap();
        assert!(diff3.is_empty());
        hub.unregister_replica(3);

        // Offset beyond master fails
        assert!(
            hub.try_partial_resync(4, tx.clone(), &replid, (current_offset + 11) as i64)
                .is_none()
        );
    }

    #[test]
    fn test_backlog_resize_keeps_newest_bytes() {
        let mut backlog = ReplicationBacklog::new(8);
        backlog.append(b"abcdef", 6);
        backlog.append(b"ghij", 10); // wraps: holds "cdefghij", first byte 3
        assert_eq!(backlog.first_byte_offset, 3);

        backlog.resize(4);
        assert_eq!(backlog.len(), 4);
        assert_eq!(backlog.first_byte_offset, 7);
        assert_eq!(backlog.get_diff(7, 10), Some(b"ghij".to_vec()));
        assert_eq!(backlog.get_diff(6, 10), None);
        backlog.append(b"kl", 12);
        assert_eq!(backlog.get_diff(9, 12), Some(b"ijkl".to_vec()));

        backlog.resize(16);
        assert_eq!(backlog.first_byte_offset, 9);
        assert_eq!(backlog.get_diff(9, 12), Some(b"ijkl".to_vec()));
        backlog.append(b"mnop", 16);
        assert_eq!(backlog.get_diff(9, 16), Some(b"ijklmnop".to_vec()));
    }

    #[test]
    fn test_full_sync_cut_skips_buffers_then_streams() {
        let hub = ReplicationHub::new(19997);
        hub.propagate_shard(0, b"old");
        let (tx, rx) = flume::unbounded();
        let rep = hub.register_full_sync_replica(7, tx, 2);

        // Neither shard has serialized yet: the snapshot will contain it.
        hub.propagate_shard(1, b"in-snap");
        assert!(rx.try_recv().is_err());

        // Shard 0 serialized: its later changes are not in the snapshot.
        hub.arm_full_sync(7, 0);
        hub.propagate_shard(0, b"after0");
        hub.propagate_shard(1, b"in-snap2");
        assert!(rx.try_recv().is_err());
        hub.arm_full_sync(7, 1);
        hub.propagate_shard(1, b"after1");
        assert!(rx.try_recv().is_err());
        assert!(rep.take_pending().is_empty());

        let offset = hub.master_repl_offset.load(Ordering::SeqCst);
        let (start, pre) = hub.finish_full_sync(7);
        assert_eq!(pre, b"after0after1");
        assert_eq!(start, offset - pre.len() as u64);

        hub.propagate_shard(1, b"live");
        assert!(rx.try_recv().is_ok());
        assert_eq!(rep.take_pending(), b"live");

        // The replica's stream diverges from the backlog in [start, offset),
        // so a partial resync from inside it must be refused.
        let replid = hub.master_replid.clone();
        let (tx2, _rx2) = flume::unbounded();
        assert!(!hub.can_partial_resync(&replid, start as i64 + 1));
        assert!(
            hub.try_partial_resync(8, tx2.clone(), &replid, (start + 2) as i64)
                .is_none()
        );
        let now = hub.master_repl_offset.load(Ordering::SeqCst);
        assert!(
            hub.try_partial_resync(8, tx2, &replid, now as i64 + 1)
                .is_some()
        );
    }

    #[test]
    fn test_replica_output_limit_hard_and_soft() {
        use crate::connection::BufferLimit;
        let hub = ReplicationHub::new(19995);
        let replid = hub.master_replid.clone();
        let (tx, _rx) = flume::unbounded();
        let (_, _, rep) = hub.try_partial_resync(1, tx, &replid, 1).unwrap();
        let limit = BufferLimit::new(1000, 500, 1);
        assert!(!rep.over_limit(499, limit));
        // Above the soft limit: allowed for soft_seconds, then not.
        assert!(!rep.over_limit(600, limit));
        assert!(!rep.over_limit(700, limit));
        std::thread::sleep(std::time::Duration::from_millis(1050));
        assert!(rep.over_limit(700, limit));
        assert!(rep.is_overflowed());

        let (tx, _rx) = flume::unbounded();
        let (_, _, rep) = hub.try_partial_resync(2, tx, &replid, 1).unwrap();
        // Dropping below the soft limit restarts the soft timer.
        assert!(!rep.over_limit(600, limit));
        assert!(!rep.over_limit(100, limit));
        assert_eq!(rep.soft_since_ms.load(Ordering::Relaxed), 0);
        assert!(rep.over_limit(1000, limit));
        // 0 disables a limit.
        let (tx, _rx) = flume::unbounded();
        let (_, _, rep) = hub.try_partial_resync(3, tx, &replid, 1).unwrap();
        assert!(!rep.over_limit(usize::MAX / 2, BufferLimit::new(0, 0, 0)));
        // soft_seconds 0: over the soft limit at all is too much.
        assert!(rep.over_limit(600, BufferLimit::new(0, 500, 0)));
    }

    #[test]
    fn test_replica_over_output_limit_stops_buffering() {
        let hub = ReplicationHub::new(19994);
        let replid = hub.master_replid.clone();
        let (tx, rx) = flume::unbounded();
        let (_, _, rep) = hub.try_partial_resync(1, tx, &replid, 1).unwrap();
        TEST_REPLICA_LIMIT
            .with(|l| l.set(Some(crate::connection::BufferLimit::new(4 << 20, 0, 0))));
        // The writer is stuck, so the stream piles up past the hard limit.
        let chunk = vec![b'x'; 1 << 20];
        for _ in 0..5 {
            hub.propagate(&chunk);
        }
        assert!(rep.is_overflowed());
        assert!(rep.pending.lock().capacity() < (1 << 20));
        // The writer was woken to drop the link.
        assert!(rx.try_iter().count() >= 2);
        hub.propagate(b"more");
        assert!(rep.take_pending().is_empty());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_full_sync_buffer_counts_against_output_limit() {
        let hub = ReplicationHub::new(19993);
        let (tx, rx) = flume::unbounded();
        let rep = hub.register_full_sync_replica(9, tx, 1);
        hub.arm_full_sync(9, 0);
        TEST_REPLICA_LIMIT
            .with(|l| l.set(Some(crate::connection::BufferLimit::new(4 << 20, 0, 0))));
        let chunk = vec![b'x'; 1 << 20];
        for _ in 0..5 {
            hub.propagate_shard(0, &chunk);
        }
        assert!(rep.is_overflowed());
        assert!(rx.try_recv().is_ok());
        let (_, pre) = hub.finish_full_sync(9);
        assert!(pre.is_empty());
    }

    #[test]
    fn test_partial_resync_registers_before_next_change() {
        let hub = ReplicationHub::new(19996);
        hub.propagate(b"a");
        let replid = hub.master_replid.clone();
        let (tx, rx) = flume::unbounded();
        let (_, diff, rep) = hub.try_partial_resync(1, tx, &replid, 1).unwrap();
        assert_eq!(diff, b"a");
        hub.propagate(b"b");
        hub.propagate(b"c");
        // One wakeup for both changes, which arrive in order in one batch.
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err());
        assert_eq!(rep.take_pending(), b"bc");
        hub.propagate(b"d");
        assert!(rx.try_recv().is_ok());
        assert_eq!(rep.take_pending(), b"d");
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
    fn test_loading_flag_covers_only_its_server_and_clears_on_drop() {
        let hub = get_replication_hub(19991);
        assert!(!is_loading(19991));
        {
            let _loading = LoadingGuard::new(&hub);
            assert!(is_loading(19991));
            assert!(!is_loading(19990));
        }
        assert!(!is_loading(19991));
        let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _loading = LoadingGuard::new(&hub);
            panic!("load failed");
        }));
        assert!(panicked.is_err());
        assert!(!is_loading(19991));
    }

    #[test]
    fn test_replica_ack_beyond_master_offset_is_ignored() {
        let hub = ReplicationHub::new(19992);
        let (tx, _rx) = flume::unbounded();
        let rep = hub.register_replica(11, tx);
        hub.propagate(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
        let offset = hub.master_repl_offset.load(Ordering::SeqCst);
        // Registration starts the clock; zero it to see that bad acks
        // do not touch it.
        rep.last_ack_time.store(0, Ordering::SeqCst);

        hub.update_replica_ack(rep.id, offset + 1);
        hub.update_replica_ack(rep.id, u64::MAX);
        assert_eq!(rep.ack_offset.load(Ordering::SeqCst), 0);
        assert_eq!(rep.last_ack_time.load(Ordering::SeqCst), 0);

        hub.update_replica_ack(rep.id, offset);
        assert_eq!(rep.ack_offset.load(Ordering::SeqCst), offset);
        hub.unregister_replica(rep.id);
    }

    #[test]
    fn test_good_replicas_counts_recent_online_acks() {
        let hub = ReplicationHub::new(19993);
        let (tx, _rx) = flume::unbounded();
        let fresh = hub.register_replica(21, tx.clone());
        let stale = hub.register_replica(22, tx.clone());
        let syncing = hub.register_full_sync_replica(23, tx, 2);
        assert!(fresh.is_online() && !syncing.is_online());
        assert_eq!(hub.good_replicas(10), 2);
        stale
            .last_ack_time
            .store(unix_secs() - 11, Ordering::SeqCst);
        assert_eq!(hub.good_replicas(10), 1);
        assert_eq!(hub.good_replicas(20), 2);
        let _ = hub.finish_full_sync(23);
        assert!(syncing.is_online());
        assert_eq!(hub.good_replicas(10), 2);
        for id in [21, 22, 23] {
            hub.unregister_replica(id);
        }
        assert_eq!(hub.good_replicas(10), 0);
    }

    #[test]
    fn test_replica_address_reported_before_psync_shows_in_info_and_role() {
        let hub = ReplicationHub::new(19994);
        // REPLCONF listening-port arrives before PSYNC registers it.
        hub.set_replica_port(31, 6380);
        hub.note_replica_ip(31, "10.1.2.3".parse().unwrap());
        let (tx, _rx) = flume::unbounded();
        let rep = hub.register_replica(31, tx);
        assert_eq!(rep.listening_port.load(Ordering::SeqCst), 6380);
        assert!(hub.pending_peers.lock().is_empty());
        let info = hub.format_info_replication();
        assert!(
            info.contains(
                "connected_slaves:1\r\nslave0:ip=10.1.2.3,port=6380,state=online,offset=0,lag=0\r\n"
            ),
            "{info}"
        );
        let role = hub.format_role_resp();
        assert!(
            role.windows(b"$8\r\n10.1.2.3\r\n$4\r\n6380\r\n".len())
                .any(|w| w == b"$8\r\n10.1.2.3\r\n$4\r\n6380\r\n")
        );
        hub.unregister_replica(31);
    }

    #[test]
    fn test_replica_partial_resync_and_psync2_failover() {
        let hub = ReplicationHub::new(19997);
        let master_replid = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2".to_string();
        *hub.role.write() = ReplicationRole::Slave {
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
            let role = hub.role.read();
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
            hub.try_partial_resync(100, tx.clone(), &master_replid, 121)
                .is_some()
        );
        hub.unregister_replica(100);

        // Asking for replid2 at offset > second_offset fails
        assert!(
            hub.try_partial_resync(101, tx, &master_replid, 122)
                .is_none()
        );
    }

    #[test]
    fn test_per_shard_parallel_replication_flows() {
        let hub = ReplicationHub::new(19996);
        let (tx0, rx0) = flume::bounded(16);
        let (tx1, rx1) = flume::bounded(16);

        // Register flows for Shard 0 and Shard 1
        let flow0 = hub.register_shard_flow(0, 100, tx0, None);
        let flow1 = hub.register_shard_flow(1, 101, tx1, None);

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

    #[test]
    fn test_take_repl_line_bounds_lines() {
        let mut buf = BytesMut::from(&b"+PONG\r\n+OK"[..]);
        assert_eq!(
            &take_repl_line(&mut buf).unwrap().unwrap()[..],
            b"+PONG\r\n"
        );
        assert_eq!(take_repl_line(&mut buf), Ok(None));
        assert_eq!(&buf[..], b"+OK");

        let mut buf = BytesMut::from(&vec![b'a'; MAX_REPL_LINE + 1][..]);
        assert_eq!(take_repl_line(&mut buf), Err(()));
        let mut line = vec![b'a'; MAX_REPL_LINE + 1];
        line.extend_from_slice(b"\r\n");
        assert_eq!(take_repl_line(&mut BytesMut::from(&line[..])), Err(()));
        let mut line = vec![b'a'; MAX_REPL_LINE];
        line.extend_from_slice(b"\r\n");
        let mut buf = BytesMut::from(&line[..]);
        assert_eq!(
            take_repl_line(&mut buf).unwrap().unwrap().len(),
            MAX_REPL_LINE + 2
        );
    }

    #[test]
    fn test_take_rdb_header_rejects_bad_lengths() {
        let mut buf = BytesMut::from(&b"\n\n\n$5\r\nREDIS"[..]);
        assert_eq!(take_rdb_header(&mut buf), Ok(Some(5)));
        assert_eq!(&buf[..], b"REDIS");
        let mut buf = BytesMut::from(&b"\n\n$12"[..]);
        assert_eq!(take_rdb_header(&mut buf), Ok(None));
        for bad in [
            &b"$abc\r\n"[..],
            b"$\r\n",
            b"$-1\r\n",
            b"$+5\r\n",
            b"$ 5\r\n",
            b"$EOF:0123456789012345678901234567890123456789\r\n",
            b"$99999999999999999999999\r\n",
        ] {
            assert_eq!(
                take_rdb_header(&mut BytesMut::from(bad)),
                Err(()),
                "{:?}",
                bad
            );
        }
        let mut huge = b"$".to_vec();
        huge.extend(std::iter::repeat_n(b'1', MAX_REPL_LINE + 8));
        assert_eq!(take_rdb_header(&mut BytesMut::from(&huge[..])), Err(()));
    }

    #[test]
    fn test_parse_psync_reply_validates_fields() {
        let id = "a".repeat(40);
        let full = format!("+FULLRESYNC {} 42\r\n", id);
        assert_eq!(
            parse_psync_reply(full.as_bytes(), "", 0),
            Some((false, id.clone(), 42))
        );
        assert_eq!(
            parse_psync_reply(b"+CONTINUE\r\n", &id, 7),
            Some((true, id.clone(), 7))
        );
        let cont = format!("+CONTINUE {}\r\n", "b".repeat(40));
        assert_eq!(
            parse_psync_reply(cont.as_bytes(), &id, 7),
            Some((true, "b".repeat(40), 7))
        );
        for bad in [
            format!("+FULLRESYNC {} -5\r\n", id),
            format!("+FULLRESYNC {} x\r\n", id),
            format!("+FULLRESYNC {}\r\n", id),
            format!("+FULLRESYNC {} 1 2\r\n", id),
            "+FULLRESYNC short 1\r\n".to_string(),
            format!("+FULLRESYNC {}! 1\r\n", "a".repeat(39)),
            format!("+CONTINUE {}\r\n", "c".repeat(41)),
            "-ERR no\r\n".to_string(),
            "+FULLRESYNCX\r\n".to_string(),
        ] {
            assert_eq!(parse_psync_reply(bad.as_bytes(), &id, 0), None, "{}", bad);
        }
        assert_eq!(parse_psync_reply(b"+CONTINUE \xff\r\n", &id, 0), None);
    }

    /// Cheap deterministic fuzz of the replica's handshake decoders: they
    /// must never panic and never accept more than they bound.
    #[test]
    fn test_replica_handshake_decoders_fuzz() {
        let alphabet = b"$+-:\r\n0123456789aZ FULLRESYNCONTIE";
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..20_000 {
            let len = (next() % 96) as usize;
            let mut input: Vec<u8> = (0..len)
                .map(|_| alphabet[(next() % alphabet.len() as u64) as usize])
                .collect();
            if next() % 8 == 0 {
                input.splice(0..0, b"+FULLRESYNC ".iter().copied());
            }
            let mut buf = BytesMut::from(&input[..]);
            while let Ok(Some(line)) = take_repl_line(&mut buf) {
                assert!(line.ends_with(b"\r\n") && line.len() <= MAX_REPL_LINE + 2);
                if let Some((_, id, _)) = parse_psync_reply(&line, "", 0) {
                    assert!(valid_replid(&id));
                }
            }
            let mut buf = BytesMut::from(&input[..]);
            let _ = take_rdb_header(&mut buf);
        }
    }

    #[test]
    fn test_full_shard_flow_is_dropped_not_waited_on() {
        let hub = ReplicationHub::new(19995);
        let (tx, rx) = flume::bounded(1);
        let (ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
        let fd = std::os::unix::io::AsRawFd::as_raw_fd(&ours);
        hub.register_shard_flow(0, 100, tx, Some(fd));

        hub.propagate_shard(0, b"first");
        assert!(hub.has_shard_flows.load(Ordering::Relaxed));
        // The queue is full: the shard must not block, the flow is dropped
        // and its socket shut down so the replica resyncs.
        hub.propagate_shard(0, b"second");
        assert!(!hub.has_shard_flows.load(Ordering::Relaxed));
        assert_eq!(rx.try_recv().unwrap(), b"first".to_vec());
        let mut byte = [0u8; 1];
        assert_eq!(std::io::Read::read(&mut &theirs, &mut byte).unwrap(), 0);
    }

    #[test]
    fn test_detached_shard_flow_leaves_socket_alone() {
        let hub = ReplicationHub::new(19994);
        let (tx, _rx) = flume::bounded(1);
        let (ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
        let fd = std::os::unix::io::AsRawFd::as_raw_fd(&ours);
        let flow = hub.register_shard_flow(0, 100, tx, Some(fd));
        flow.detach_fd();
        hub.propagate_shard(0, b"first");
        hub.propagate_shard(0, b"second");
        assert!(!hub.has_shard_flows.load(Ordering::Relaxed));
        std::io::Write::write_all(&mut &ours, b"x").unwrap();
        let mut byte = [0u8; 1];
        assert_eq!(std::io::Read::read(&mut &theirs, &mut byte).unwrap(), 1);
    }
}
