use bytes::Bytes;
use smallvec::{SmallVec, smallvec};
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use crate::resp::Command;
use crate::shard::{ShardDb, ShardMessage};

/// Extracts the hash tag from a key if present (e.g. "{user:1}:profile" -> "user:1").
#[inline]
pub fn extract_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{')
        && let Some(close) = key[open + 1..].iter().position(|&b| b == b'}')
        && close > 0
    {
        return &key[open + 1..open + 1 + close];
    }
    key
}

/// Calculates the Redis Cluster 16384 slot for a given key.
#[inline]
pub fn key_slot(key: &[u8]) -> u16 {
    let tag = extract_hash_tag(key);
    crc16::State::<crc16::XMODEM>::calculate(tag) % 16384
}

pub fn pattern_hash_slot(pattern: &[u8]) -> Option<u16> {
    let mut s = None;
    for (i, &b) in pattern.iter().enumerate() {
        if b == b'*' || b == b'?' || b == b'[' || b == b'\\' {
            return None;
        } else if s.is_none() && b == b'{' {
            s = Some(i);
        } else if let Some(start) = s
            && b == b'}'
        {
            if i == start + 1 {
                s = None;
            } else {
                let tag = &pattern[start + 1..i];
                return Some(crc16::State::<crc16::XMODEM>::calculate(tag) % 16384);
            }
        }
    }
    None
}

fn parse_resp_array_to_bytes(resp: &[u8]) -> Vec<Bytes> {
    if !resp.starts_with(b"*") {
        return Vec::new();
    }
    let mut items = Vec::new();
    let mut i = match resp.windows(2).position(|w| w == b"\r\n") {
        Some(pos) => pos + 2,
        None => return items,
    };
    while i < resp.len() {
        if resp[i] == b'$'
            && let Some(nl) = resp[i..].windows(2).position(|w| w == b"\r\n")
        {
            let len_str = &resp[i + 1..i + nl];
            if let Ok(len) = std::str::from_utf8(len_str).unwrap_or("").parse::<isize>() {
                let data_start = i + nl + 2;
                if len < 0 {
                    i = data_start;
                } else {
                    let data_end = data_start + len as usize;
                    if data_end <= resp.len() {
                        items.push(Bytes::copy_from_slice(&resp[data_start..data_end]));
                    }
                    i = data_end + 2;
                }
                continue;
            }
        }
        break;
    }
    items
}

/// Maps a slot (0..16383) to an owning shard (0..num_shards-1).
#[inline]
pub fn slot_to_shard(slot: u16, num_shards: usize) -> usize {
    if num_shards <= 1 {
        0
    } else {
        ((slot as usize) * num_shards) / 16384
    }
}

/// Picks the shard for a key hash. FxHash carries entropy only upward (each
/// step multiplies), so its low bits ignore the high bytes of the last
/// chunk, and `hash % n` put keys differing only there on one shard: all of
/// "sk:0".."sk:9", or fixed-width keys like "key:0001".."key:9999". The
/// splitmix64 finalizer folds every bit into the low ones; its constants
/// differ from the table's own `mix_hash`, so shard choice and bucket
/// choice stay independent.
#[inline(always)]
pub fn shard_of_hash(hash: u64, num_shards: usize) -> usize {
    let mut x = hash;
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^= x >> 31;
    (x % num_shards as u64) as usize
}

/// Calculates the target shard ID for a given key.
/// In Redis Cluster mode, uses CRC16 slot mapping.
/// In standalone mode, uses high-performance 64-bit hashing for optimal key distribution across cores.
#[inline(always)]
pub fn target_shard(key: &[u8], num_shards: usize) -> usize {
    if num_shards <= 1 {
        0
    } else if crate::cluster::HAS_ACTIVE_CLUSTER.load(std::sync::atomic::Ordering::Relaxed) {
        let slot = key_slot(key);
        slot_to_shard(slot, num_shards)
    } else {
        let tag = extract_hash_tag(key);
        shard_of_hash(crate::table::hash_key(tag), num_shards)
    }
}

#[inline(always)]
pub fn target_shard_and_hash(key: &[u8], num_shards: usize) -> (usize, u64) {
    let key_hash = crate::table::hash_key(key);
    if num_shards <= 1 {
        (0, key_hash)
    } else if crate::cluster::HAS_ACTIVE_CLUSTER.load(std::sync::atomic::Ordering::Relaxed) {
        let slot = key_slot(key);
        (slot_to_shard(slot, num_shards), key_hash)
    } else {
        let tag = extract_hash_tag(key);
        let shard_hash = if tag.len() == key.len() {
            key_hash
        } else {
            crate::table::hash_key(tag)
        };
        (shard_of_hash(shard_hash, num_shards), key_hash)
    }
}

use std::sync::atomic::Ordering;

/// Internal `DEBUG` subcommands a connection sends to the other shards
/// (handled by `execute_local_command`, rejected from clients).
/// Replies with this shard's `DEBUG DIGEST` part.
pub const DEBUG_DIGEST_SHARD: &[u8] = b"__shard-digest";
/// Empties this shard and replays its AOF file.
pub const DEBUG_LOADAOF_SHARD: &[u8] = b"__shard-loadaof";
/// `__shard-populate <prefix> <size|""> <n>...`: creates this shard's
/// `DEBUG POPULATE` keys.
pub const DEBUG_POPULATE_SHARD: &[u8] = b"__shard-populate";

/// Handle for a cross-shard MGET that has been dispatched but not yet gathered.
///
/// Created by [`Router::begin_mget_resp`] and consumed by [`Router::finish_mget_resp`].
/// Keeping the dispatch and the wait separate lets a pipelined batch start several
/// MGETs before stalling on the first one.
pub struct MgetInFlight {
    descriptor: Arc<crate::mailbox::ScatterMgetDescriptor>,
    notify_tx: flume::Sender<()>,
    notify_rx: flume::Receiver<()>,
    total_keys: usize,
}

/// Handle for a cross-shard MSET that has been dispatched but not yet acknowledged.
///
/// Created by [`Router::begin_mset`] and consumed by [`Router::finish_mset`].
pub struct MsetInFlight {
    descriptor: Arc<crate::mailbox::ScatterMsetDescriptor>,
    notify_tx: flume::Sender<()>,
    notify_rx: flume::Receiver<()>,
}

const WRONGTYPE_ERR: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
/// Producer-side error when the RDB writer thread hung up; its own error wins.
const WRITER_GONE: &str = "rdb writer gone";

/// Upper bound on keys one `check_auto_tier` call tries to spill; it can run
/// inline on the write path, and the 20 ms cron picks up where it stopped.
const MAX_SPILL_ATTEMPTS_PER_CALL: usize = 1024;

/// The router handles dispatching operations.
/// If the key belongs to the current shard, it directly touches `local_db` without locking.
/// If the key belongs to a peer shard, it routes the message across cores via the mesh.
#[derive(Clone)]
pub struct Router {
    pub shard_id: usize,
    pub num_shards: usize,
    pub port: u16,
    pub base_port: u16,
    pub cluster_enabled: bool,
    pub local_db: Rc<RefCell<ShardDb>>,
    pub senders: Vec<crate::mailbox::ShardSender>,
    pub slot_states: Rc<RefCell<hashbrown::HashMap<u16, crate::shard::SlotState>>>,
    pub slot_owners: Rc<RefCell<Vec<usize>>>,
    pub aof: Option<Rc<RefCell<crate::aof::AofWriter>>>,
    pub pubsub: Rc<RefCell<crate::pubsub::PubSubHub>>,
    pub tx_lock: Rc<RefCell<Option<u64>>>,
    pub tx_waiters: Rc<RefCell<std::collections::VecDeque<(u64, flume::Sender<()>)>>>,
    pub is_saving: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub db_dir: std::path::PathBuf,
    pub is_auto_tiering: Rc<Cell<bool>>,
    /// Set after an auto-tier spill pass frees nothing; spilling is skipped
    /// until then so over-budget writes don't rescan the table each time.
    pub spill_backoff_until: Rc<Cell<Option<std::time::Instant>>>,
    /// Local `used_memory` when the spill backoff was set; growth past it
    /// (new, possibly spillable data) lifts the backoff early.
    pub spill_backoff_used: Rc<Cell<usize>>,
    /// Same as `spill_backoff_until`/`spill_backoff_used`, for asking the
    /// other shards to spill; keyed on the server-wide published total.
    pub remote_spill_backoff: Rc<Cell<Option<(std::time::Instant, usize)>>>,
    /// Same, for asking the other shards to evict.
    pub remote_evict_backoff: Rc<Cell<Option<(std::time::Instant, usize)>>>,
    /// `tiering::maxmemory_epoch()` the back-offs above were computed under;
    /// a config change or flush clears them.
    pub backoff_epoch: Rc<Cell<u64>>,
    /// Last time `refresh_published_usage` re-read the other shards' usage.
    pub last_usage_refresh: Rc<Cell<Option<std::time::Instant>>>,
    pub notify_channel_pool: Rc<RefCell<Vec<(flume::Sender<()>, flume::Receiver<()>)>>>,
    pub remote_responder_pool: Rc<RefCell<Vec<std::sync::Arc<crate::mailbox::BatchResponder>>>>,
    pub mget_batch_pool: Rc<RefCell<Vec<Vec<Vec<(usize, Bytes)>>>>>,
    pub mset_batch_pool: Rc<RefCell<Vec<Vec<Vec<(Bytes, Bytes)>>>>>,
    pub mget_desc_pool: Rc<RefCell<Vec<std::sync::Arc<crate::mailbox::ScatterMgetDescriptor>>>>,
    pub mset_desc_pool: Rc<RefCell<Vec<std::sync::Arc<crate::mailbox::ScatterMsetDescriptor>>>>,
    pub pubsub_responder_pool: Rc<RefCell<Vec<(flume::Sender<usize>, flume::Receiver<usize>)>>>,
    pub presence_table: std::sync::Arc<crate::pubsub::ShardedPresenceTable>,
    pub tier_stats: std::sync::Arc<crate::tiering::TieringStats>,
}

/// Usage that maxmemory eviction brings the server down to once it is over
/// `max_mem`: a little below the limit (1/64 of it, at most 1 MiB), so a
/// steady write stream pays one eviction round (and at most one mesh round
/// trip) per few thousand writes instead of one per write.
#[inline]
fn eviction_target(max_mem: usize) -> usize {
    max_mem - (max_mem / 64).min(1 << 20)
}

/// The calling shard thread's memory by the allocator's count, as the pair
/// (allocated minus freed by this thread, table arena space allocated but
/// free for reuse). Call on the shard's own thread. Without
/// `real_accounting` (routers built outside a server), the table's
/// estimate and no slack.
#[inline]
pub fn local_real_parts(
    stats: &crate::tiering::TieringStats,
    table: &crate::table::RudisTable,
) -> (i64, usize) {
    if stats.real_accounting.load(Ordering::Relaxed) {
        (
            crate::allocator::thread_net_bytes(),
            table.free_arena_bytes(),
        )
    } else {
        (table.used_memory() as i64, 0)
    }
}

/// [`local_real_parts`] as one number: the shard's contribution to
/// [`crate::tiering::TieringStats::real_used_total`].
#[inline]
pub fn local_real_used(
    stats: &crate::tiering::TieringStats,
    table: &crate::table::RudisTable,
) -> i64 {
    let (net, slack) = local_real_parts(stats, table);
    net - slack as i64
}

/// Clears `is_saving` and records a failed save or rewrite if the task
/// doing it ends without finishing (its future dropped part-way, e.g. by a
/// panic isolated with `catch_unwind`). Otherwise one such save refuses
/// every later SAVE/BGSAVE/BGREWRITEAOF as "in progress" and shutdown waits
/// forever in `save_before_shutdown`.
struct SaveFlagGuard {
    is_saving: std::sync::Arc<std::sync::atomic::AtomicBool>,
    base_port: u16,
    /// `Some(dirty_before)` for an RDB save, `None` for an AOF rewrite.
    rdb_dirty_before: Option<u64>,
    armed: bool,
}

impl SaveFlagGuard {
    fn new(router: &Router, rdb_dirty_before: Option<u64>) -> Self {
        Self {
            is_saving: router.is_saving.clone(),
            base_port: router.base_port,
            rdb_dirty_before,
            armed: true,
        }
    }

    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for SaveFlagGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.is_saving.store(false, Ordering::SeqCst);
        match self.rdb_dirty_before {
            Some(d) => crate::snapshot::state(self.base_port).finish(d, false),
            None => crate::snapshot::aof_rewrite_state(self.base_port).finish(false),
        }
    }
}

/// Hands a released VLL lock to the next waiter still waiting. A waiter whose
/// transaction was dropped while queued (its receiver is gone) is skipped;
/// granting it would leave the lock held by nobody, forever.
pub(crate) fn grant_next_tx_lock(
    waiters: &mut std::collections::VecDeque<(u64, flume::Sender<()>)>,
) -> Option<u64> {
    while let Some((next_tx, resp)) = waiters.pop_front() {
        if resp.send(()).is_ok() {
            return Some(next_tx);
        }
    }
    None
}

impl Router {
    /// Logs a mutation applied to this shard's data to the AOF and the
    /// replication stream (see `replication::log_shard_mutation`).
    pub fn log_mutation(&self, make: impl FnOnce() -> Command) {
        crate::replication::log_shard_mutation(self.port, self.shard_id, self.aof.as_deref(), make);
    }

    pub fn new(
        shard_id: usize,
        num_shards: usize,
        port: u16,
        local_db: Rc<RefCell<ShardDb>>,
        senders: Vec<crate::mailbox::ShardSender>,
        aof: Option<Rc<RefCell<crate::aof::AofWriter>>>,
        pubsub: Rc<RefCell<crate::pubsub::PubSubHub>>,
        db_dir: std::path::PathBuf,
    ) -> Self {
        let slot_states = hashbrown::HashMap::new();
        let mut slot_owners = Vec::with_capacity(16384);
        for s in 0..16384 {
            slot_owners.push(slot_to_shard(s as u16, num_shards));
        }
        Self {
            shard_id,
            num_shards,
            port,
            base_port: port,
            cluster_enabled: false,
            local_db,
            senders,
            slot_states: Rc::new(RefCell::new(slot_states)),
            slot_owners: Rc::new(RefCell::new(slot_owners)),
            aof,
            pubsub,
            tx_lock: Rc::new(RefCell::new(None)),
            tx_waiters: Rc::new(RefCell::new(std::collections::VecDeque::new())),
            is_saving: crate::snapshot::is_saving(port),
            db_dir,
            is_auto_tiering: Rc::new(Cell::new(false)),
            spill_backoff_until: Rc::new(Cell::new(None)),
            spill_backoff_used: Rc::new(Cell::new(0)),
            remote_spill_backoff: Rc::new(Cell::new(None)),
            remote_evict_backoff: Rc::new(Cell::new(None)),
            backoff_epoch: Rc::new(Cell::new(0)),
            last_usage_refresh: Rc::new(Cell::new(None)),
            notify_channel_pool: Rc::new(RefCell::new(Vec::new())),
            remote_responder_pool: Rc::new(RefCell::new(Vec::new())),
            mget_batch_pool: Rc::new(RefCell::new(Vec::new())),
            mset_batch_pool: Rc::new(RefCell::new(Vec::new())),
            mget_desc_pool: Rc::new(RefCell::new(Vec::new())),
            mset_desc_pool: Rc::new(RefCell::new(Vec::new())),
            pubsub_responder_pool: Rc::new(RefCell::new(Vec::new())),
            presence_table: crate::pubsub::get_presence_table(port),
            tier_stats: {
                let t = crate::tiering::get_tier_stats(port);
                // Sizes the published per-shard usage sum (see TieringStats::shard_used).
                t.num_shards.fetch_max(num_shards.max(1), Ordering::Relaxed);
                t
            },
        }
    }

    pub fn set_base_port(&mut self, base_port: u16) {
        self.base_port = base_port;
        self.is_saving = crate::snapshot::is_saving(base_port);
    }

    #[inline(always)]
    pub fn get_slot_state(&self, slot: u16) -> crate::shard::SlotState {
        self.slot_states
            .borrow()
            .get(&slot)
            .cloned()
            .unwrap_or(crate::shard::SlotState::Stable)
    }

    pub fn target_shard_for_slot(&self, slot: u16) -> usize {
        self.slot_owners.borrow()[slot as usize]
    }

    pub fn target_shard(&self, key: &[u8]) -> usize {
        if self.cluster_enabled || crate::cluster::has_active_cluster(self.port) {
            let slot = key_slot(key);
            self.target_shard_for_slot(slot)
        } else {
            target_shard(key, self.num_shards)
        }
    }

    #[inline(always)]
    pub fn target_shard_and_hash(&self, key: &[u8]) -> (usize, u64) {
        let key_hash = crate::table::hash_key(key);
        let target = self.target_shard(key);
        (target, key_hash)
    }

    pub fn check_slot_redirection(
        &self,
        slot: u16,
        key_exists: bool,
        asking: bool,
    ) -> Result<(), String> {
        let state = self.get_slot_state(slot);
        match state {
            crate::shard::SlotState::Migrating(target) => {
                if !key_exists {
                    return Err(format!("-ASK {} {}\r\n", slot, target));
                }
            }
            crate::shard::SlotState::Importing(source) => {
                if !asking {
                    return Err(format!("-MOVED {} {}\r\n", slot, source));
                }
            }
            crate::shard::SlotState::Moved(target) => {
                return Err(format!("-MOVED {} {}\r\n", slot, target));
            }
            crate::shard::SlotState::Stable => {}
        }
        Ok(())
    }

    pub fn set_slot_state(&self, slot: u16, state: crate::shard::SlotState) {
        if state == crate::shard::SlotState::Stable {
            self.slot_states.borrow_mut().remove(&slot);
        } else {
            self.slot_states.borrow_mut().insert(slot, state.clone());
        }
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let _ = sender.send(ShardMessage::SetSlotState {
                    slot,
                    state: state.clone(),
                });
            }
        }
    }

    pub fn set_slot_owner(&self, slot: u16, owner: usize) {
        self.slot_states.borrow_mut().remove(&slot);
        self.slot_owners.borrow_mut()[slot as usize] = owner;
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let _ = sender.send(ShardMessage::SetSlotOwner { slot, owner });
            }
        }
    }

    /// Waits until every other shard has applied the slot updates this shard
    /// sent before the call. `set_slot_state`/`set_slot_owner` only post the
    /// change, so without this a command on a connection owned by another
    /// shard can still see the old state after the caller has replied. Each
    /// ring is FIFO, so a barrier answered means everything ahead of it is
    /// applied.
    pub async fn sync_slot_tables(&self) {
        let mut waiters = Vec::with_capacity(self.senders.len());
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = self.acquire_notify_channel();
                if sender
                    .send(ShardMessage::SlotBarrier {
                        responder: tx.clone(),
                    })
                    .is_ok()
                {
                    waiters.push((tx, rx));
                } else {
                    self.release_notify_channel(tx, rx);
                }
            }
        }
        for (tx, rx) in waiters {
            let _ = rx.recv_async().await;
            self.release_notify_channel(tx, rx);
        }
    }

    pub async fn spill_local(&self, key: &[u8]) -> bool {
        self.spill_local_internal(key, true).await
    }

    pub async fn spill_local_internal(&self, key: &[u8], flush_bin: bool) -> bool {
        if self.local_db.borrow().is_sticky(key) {
            return false;
        }
        if self.local_db.borrow_mut().table.is_cooled(key).is_some() {
            return self.decommit_local(Some(key)) > 0;
        }

        let (entry_data, val_type) = match self.local_db.borrow_mut().table.get_value_for_spill(key)
        {
            Some(p) => p,
            None => return false,
        };

        let tm = match self.local_db.borrow().tier_manager.clone() {
            Some(tm) => tm,
            None => return false,
        };

        let key_bytes = Bytes::copy_from_slice(key);
        let ptr = match tm.stash_record(&key_bytes, &entry_data, val_type).await {
            Ok(p) => p,
            Err(_) => return false,
        };

        let updated = {
            let mut db = self.local_db.borrow_mut();
            let stats = db.tier_manager.as_ref().map(|tm| tm.stats.clone());
            if db
                .table
                .set_spilled_pointer_if_unchanged(key, &entry_data, ptr, false)
            {
                if let Some(stats) = stats {
                    stats.tiered_keys.fetch_add(1, Ordering::Relaxed);
                    stats
                        .tiered_bytes
                        .fetch_add(ptr.length as u64, Ordering::Relaxed);
                    stats
                        .ram_saved_bytes
                        .fetch_add(entry_data.len() as u64, Ordering::Relaxed);
                }
                true
            } else {
                false
            }
        };

        if updated {
            if flush_bin {
                let _ = tm.flush_active_bin().await;
            }
            true
        } else {
            // The key changed during the I/O: the stashed copy is garbage.
            tm.on_key_deleted(ptr);
            false
        }
    }

    pub async fn load_local(&self, key: &[u8]) -> bool {
        let ptr = self.local_db.borrow_mut().table.is_tiered(key);
        let ptr = match ptr {
            Some(p) => p,
            None => return false,
        };

        let tm = {
            let db = self.local_db.borrow();
            db.tier_manager.clone()
        };
        let tm = match tm {
            Some(tm) => tm,
            None => return false,
        };

        let (_record_key, val_payload) = match crate::tiering::read_tiered_record(
            &tm.file,
            &tm.op_manager,
            Some(&tm.small_bins),
            ptr,
            &tm.stats,
        )
        .await
        {
            Ok(pair) => pair,
            Err(_) => return false,
        };

        let (val, _) = match crate::table::RudisTable::deserialize_val_payload(&val_payload) {
            Ok(v) => v,
            Err(_) => return false,
        };

        let mut db = self.local_db.borrow_mut();
        if db.table.restore_tiered_value(key, val) {
            tm.stats.total_fetches.fetch_add(1, Ordering::Relaxed);
            tm.stats.tiered_keys.fetch_sub(1, Ordering::Relaxed);
            tm.stats.cooled_keys.fetch_add(1, Ordering::Relaxed);
            tm.stats
                .ram_saved_bytes
                .fetch_sub(val_payload.len() as u64, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub async fn cool_local(&self, key: &[u8]) -> bool {
        if self.local_db.borrow().is_sticky(key) {
            return false;
        }
        if self.local_db.borrow_mut().table.is_tiered(key).is_some()
            || self.local_db.borrow_mut().table.is_cooled(key).is_some()
        {
            return false;
        }

        let (entry_data, val_type) = match self.local_db.borrow_mut().table.get_value_for_spill(key)
        {
            Some(p) => p,
            None => return false,
        };

        let tm = match self.local_db.borrow().tier_manager.clone() {
            Some(tm) => tm,
            None => return false,
        };

        let key_bytes = Bytes::copy_from_slice(key);
        let ptr = match tm.stash_record(&key_bytes, &entry_data, val_type).await {
            Ok(p) => p,
            Err(_) => return false,
        };

        let updated = {
            let mut db = self.local_db.borrow_mut();
            let stats = db.tier_manager.as_ref().map(|tm| tm.stats.clone());
            if db
                .table
                .set_spilled_pointer_if_unchanged(key, &entry_data, ptr, true)
            {
                if let Some(stats) = stats {
                    stats.cooled_keys.fetch_add(1, Ordering::Relaxed);
                    stats
                        .tiered_bytes
                        .fetch_add(ptr.length as u64, Ordering::Relaxed);
                }
                true
            } else {
                false
            }
        };

        if updated {
            let _ = tm.flush_active_bin().await;
            true
        } else {
            tm.on_key_deleted(ptr);
            false
        }
    }

    pub async fn stream_cold_read_local(&self, key: &[u8]) -> Option<Bytes> {
        let ptr = self.local_db.borrow_mut().table.is_tiered(key)?;
        let tm = {
            let db = self.local_db.borrow();
            db.tier_manager.clone()?
        };

        let (_, val_payload) = crate::tiering::read_tiered_record(
            &tm.file,
            &tm.op_manager,
            Some(&tm.small_bins),
            ptr,
            &tm.stats,
        )
        .await
        .ok()?;
        let (val, _) = crate::table::RudisTable::deserialize_val_payload(&val_payload).ok()?;
        match val {
            crate::table::RudisValue::String(s) => Some(s.to_bytes()),
            crate::table::RudisValue::Int(n) => Some(crate::table::RudisTable::format_i64(n)),
            _ => None,
        }
    }

    pub fn decommit_local(&self, key: Option<&[u8]>) -> usize {
        let mut db = self.local_db.borrow_mut();
        let stats = match &db.tier_manager {
            Some(tm) => tm.stats.clone(),
            None => return 0,
        };

        if let Some(k) = key {
            if let Some((_, freed)) = db.table.decommit_cooled_key(k) {
                stats
                    .cooled_keys
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                stats
                    .tiered_keys
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                stats
                    .ram_saved_bytes
                    .fetch_add(freed as u64, std::sync::atomic::Ordering::Relaxed);
                stats
                    .decommit_count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                1
            } else {
                0
            }
        } else {
            // decommit_all_cooled walks the whole table, and this runs on
            // every over-budget write; skip it when nothing is cooled. The
            // counter is per port, so it is conservative for multi-shard.
            if stats.cooled_keys.load(std::sync::atomic::Ordering::Relaxed) == 0 {
                return 0;
            }
            let (count, freed) = db.table.decommit_all_cooled();
            if count > 0 {
                stats
                    .cooled_keys
                    .fetch_sub(count as u64, std::sync::atomic::Ordering::Relaxed);
                stats
                    .tiered_keys
                    .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
                stats
                    .ram_saved_bytes
                    .fetch_add(freed, std::sync::atomic::Ordering::Relaxed);
                stats
                    .decommit_count
                    .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
            }
            count
        }
    }

    pub async fn cool_key(&self, key: &[u8]) -> bool {
        let target = self.target_shard(key);
        if target == self.shard_id {
            self.cool_local(key).await
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::TierCool {
                key: Bytes::copy_from_slice(key),
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(false)
            } else {
                false
            }
        }
    }

    pub async fn decommit(&self, key: Option<&[u8]>) -> usize {
        if let Some(k) = key {
            let target = self.target_shard(k);
            if target == self.shard_id {
                self.decommit_local(Some(k))
            } else {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::TierDecommit {
                    key: Some(Bytes::copy_from_slice(k)),
                    responder: tx,
                };
                if self.senders[target].send(msg).is_ok() {
                    rx.recv_async().await.unwrap_or(0)
                } else {
                    0
                }
            }
        } else {
            let mut total = 0;
            for s in 0..self.num_shards {
                if s == self.shard_id {
                    total += self.decommit_local(None);
                } else {
                    let (tx, rx) = flume::bounded(1);
                    let msg = ShardMessage::TierDecommit {
                        key: None,
                        responder: tx,
                    };
                    if self.senders[s].send(msg).is_ok() {
                        total += rx.recv_async().await.unwrap_or(0);
                    }
                }
            }
            total
        }
    }

    /// Server-wide used memory by the allocator's count, after every shard
    /// republished its usage (a mesh round trip per shard; for INFO).
    pub async fn get_total_used_memory(&self) -> usize {
        for s in 0..self.num_shards {
            if s != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::GetUsedMemory { responder: tx };
                if self.senders[s].send(msg).is_ok()
                    && let Ok(used) = rx.recv_async().await
                {
                    self.tier_stats.publish_shard_used(s, used);
                }
            }
        }
        self.publish_memory_state();
        self.real_used_total()
    }

    /// Server-wide used memory by the allocator's count, from what every
    /// shard last published (see [`crate::tiering::TieringStats::real_used_total`]).
    #[inline]
    pub fn real_used_total(&self) -> usize {
        self.tier_stats.real_used_total(None).max(0) as usize
    }

    /// This shard's share of `maxmemory`, or `None` when no limit is set.
    #[inline]
    pub fn shard_memory_budget(&self) -> Option<usize> {
        let max_mem = self.tier_stats.max_memory.load(Ordering::Relaxed);
        (max_mem > 0).then(|| (max_mem / self.num_shards.max(1) as u64).max(1) as usize)
    }

    /// Publishes this shard's usage for the other shards (the table's
    /// estimate, for splitting eviction between shards, and the allocator's
    /// count) and returns whether the server as a whole is over `maxmemory`
    /// (Redis semantics: the limit is global, not per shard), judged by the
    /// allocator's count.
    #[inline]
    pub fn publish_memory_state(&self) -> bool {
        let (estimate, (net, slack)) = {
            let db = self.local_db.borrow();
            (
                db.table.used_memory(),
                local_real_parts(&self.tier_stats, &db.table),
            )
        };
        self.tier_stats.publish_shard_used(self.shard_id, estimate);
        self.tier_stats
            .publish_shard_real(self.shard_id, net, slack);
        let max_mem = self.tier_stats.max_memory.load(Ordering::Relaxed) as i64;
        max_mem > 0 && self.tier_stats.real_used_total(None) > max_mem
    }

    /// Recalibrates the memory not charged to any shard thread against
    /// jemalloc's process-wide count (~15 µs; one shard calls this from its
    /// cron). Skipped when other servers share the process, whose memory
    /// the process-wide count would include.
    pub fn calibrate_real_memory(&self) {
        if crate::tiering::sole_server_in_process()
            && let Some(global) = crate::allocator::global_allocated()
        {
            self.tier_stats.calibrate_unattributed(global);
        }
    }

    /// Clears the eviction/spill back-offs if `maxmemory`, the policy or the
    /// dataset was reset since they were set.
    #[inline]
    fn sync_backoff_epoch(&self) {
        let epoch = crate::tiering::maxmemory_epoch();
        if self.backoff_epoch.get() != epoch {
            self.backoff_epoch.set(epoch);
            self.spill_backoff_until.set(None);
            self.remote_spill_backoff.set(None);
            self.remote_evict_backoff.set(None);
        }
    }

    /// Evicts local keys under `policy` while the server is over `maxmemory`,
    /// this shard holds more than `floor` bytes, and something is evictable;
    /// publishes this shard's usage and returns whether the server ended
    /// within the limit. Any key frees memory toward the global limit, so
    /// with `floor == 0` a shard evicts even when it is under its own share
    /// (otherwise writes here would be refused while the shard holding the
    /// excess sees no writes).
    pub fn evict_local_until_under(&self, policy: &str, floor: usize) -> bool {
        let max_mem = self.tier_stats.max_memory.load(Ordering::Relaxed) as usize;
        if max_mem == 0 {
            self.publish_memory_state();
            return true;
        }
        if policy != "noeviction" {
            // Once over the limit, evict a little below it so the next
            // writes don't each trigger another (possibly remote) round.
            // The limit is checked by the allocator's count, which drops as
            // soon as a key is evicted (its heap memory goes back to the
            // allocator, its arena position becomes reusable slack); `floor`
            // is about this shard's fair share, by the table's estimate.
            let target = eviction_target(max_mem) as i64;
            let others = self.tier_stats.real_used_total(Some(self.shard_id));
            let mut db = self.local_db.borrow_mut();
            loop {
                let local = local_real_used(&self.tier_stats, &db.table);
                if others + local <= target || db.table.used_memory() <= floor {
                    break;
                }
                if db.table.try_evict_one_key(policy).is_none() {
                    break;
                }
            }
        }
        !self.publish_memory_state()
    }

    /// Whether the server is over `maxmemory`, using this shard's fresh
    /// usage (which it also publishes) and the others' published usage.
    #[inline]
    pub fn over_maxmemory(&self) -> bool {
        self.publish_memory_state()
    }

    /// Re-reads every other shard's `used_memory` over the mesh and publishes
    /// it on their behalf (each shard overwrites its slot again with fresher
    /// values). Used before acting on a published "over the limit", since a
    /// shard that shrank (DEL, FLUSH, decommit) republishes only on its cron.
    /// Rate-limited to once per millisecond per shard.
    pub async fn refresh_published_usage(&self) {
        if self.num_shards <= 1 {
            return;
        }
        let now = std::time::Instant::now();
        if self
            .last_usage_refresh
            .get()
            .is_some_and(|t| now.duration_since(t) < std::time::Duration::from_millis(1))
        {
            return;
        }
        self.last_usage_refresh.set(Some(now));
        for s in 0..self.num_shards {
            if s != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                if self.senders[s]
                    .send(ShardMessage::GetUsedMemory { responder: tx })
                    .is_ok()
                    && let Ok(used) = rx.recv_async().await
                {
                    self.tier_stats.publish_shard_used(s, used);
                }
            }
        }
    }

    /// Like [`Router::over_maxmemory`], but confirms a positive answer with
    /// fresh numbers from the other shards.
    pub async fn over_maxmemory_checked(&self) -> bool {
        if !self.over_maxmemory() {
            return false;
        }
        self.refresh_published_usage().await;
        self.over_maxmemory()
    }

    /// Asks shard `s` to evict down to `floor` while the server is over the
    /// limit, appending the invalidations owed to `client_id` to `track_out`.
    async fn evict_remote(
        &self,
        s: usize,
        policy: &str,
        floor: usize,
        client_id: u64,
        track_out: &mut Vec<u8>,
    ) {
        let (tx, rx) = flume::bounded(1);
        if self.senders[s]
            .send(ShardMessage::EvictUntilUnder {
                policy: policy.to_string(),
                floor,
                client_id,
                responder: tx,
            })
            .is_ok()
            && let Ok((used, track)) = rx.recv_async().await
        {
            self.tier_stats.publish_shard_used(s, used);
            track_out.extend_from_slice(&track);
        }
    }

    /// Enforces `maxmemory` before a command runs, evicting while the server
    /// is over the limit. Other shards are judged by the usage they publish
    /// (after every write batch and every 100 ms), refreshed over the mesh
    /// when that says "over". Returns whether the server is within its limit.
    ///
    /// Shards first shed what they hold beyond their fair share
    /// (`maxmemory / shards`): this shard locally, then the others from the
    /// most loaded down, so one connection's shard is not drained while the
    /// excess sits elsewhere. Only if that is not enough (e.g. under
    /// volatile-* the keys with a TTL live on a few shards) do shards evict
    /// below their share. Tracking invalidations owed to `client_id` are
    /// appended to `track_out`, ahead of the command's reply.
    pub async fn evict_until_under_maxmemory(
        &self,
        max_mem: usize,
        policy: &str,
        client_id: u64,
        track_out: &mut Vec<u8>,
    ) -> bool {
        self.sync_backoff_epoch();
        if !self.over_maxmemory_checked().await {
            return true;
        }
        let target = eviction_target(max_mem);
        let share = (target / self.num_shards.max(1)).max(1);
        let floors: &[usize] = if self.num_shards <= 1 {
            &[0]
        } else {
            &[share, 0]
        };
        let regrowth = (max_mem / 1024).max(1024);
        let total = || self.real_used_total();
        for &floor in floors {
            let (under, track) = crate::connection::with_evict_capture(client_id, || {
                self.evict_local_until_under(policy, floor)
            });
            track_out.extend_from_slice(&track);
            if self.num_shards <= 1 || policy == "noeviction" {
                return under;
            }
            if total() <= target {
                return true;
            }
            // A fan-out that ended over the limit backs off (like the
            // cross-shard spill), so commands don't each pay a mesh round
            // trip per shard while nothing evictable is left.
            if let Some((until, at_total)) = self.remote_evict_backoff.get()
                && total() < at_total.saturating_add(regrowth)
                && std::time::Instant::now() < until
            {
                return under;
            }
            let mut order: Vec<usize> = (0..self.num_shards)
                .filter(|&s| s != self.shard_id)
                .collect();
            order.sort_unstable_by_key(|&s| {
                std::cmp::Reverse(self.tier_stats.published_shard_used(s))
            });
            for s in order {
                if self.tier_stats.published_shard_used(s) <= floor {
                    break;
                }
                self.evict_remote(s, policy, floor, client_id, track_out)
                    .await;
                if total() <= target {
                    self.remote_evict_backoff.set(None);
                    return true;
                }
            }
            if !self.over_maxmemory() {
                self.remote_evict_backoff.set(None);
                return true;
            }
        }
        self.remote_evict_backoff.set(Some((
            std::time::Instant::now() + std::time::Duration::from_secs(1),
            total(),
        )));
        false
    }

    #[inline(always)]
    pub fn is_memory_constrained(&self) -> bool {
        let max_mem = self.tier_stats.max_memory.load(Ordering::Relaxed);
        if max_mem == 0 {
            return false;
        }
        let offload_pct = self
            .tier_stats
            .offload_threshold_pct
            .load(Ordering::Relaxed);
        // Server-wide, by the allocator's count as last published.
        self.real_used_total() as u64 >= max_mem / 100 * offload_pct
    }

    /// Decommit cooled keys, then spill hot keys to NVMe while the server is
    /// over `maxmemory`: this shard first, then (the limit being global) the
    /// other shards, since the excess may live where this shard cannot free
    /// it. Returns whether the server ended within the limit (also `true`
    /// when no limit is set).
    pub async fn check_auto_tier(&self) -> bool {
        if self.check_auto_tier_local().await || self.num_shards <= 1 {
            return !self.over_maxmemory();
        }
        // Fanning out costs a round trip per shard; after one that ends over
        // the limit, wait a second (or for the total to grow) before the next.
        let max_mem = self.tier_stats.max_memory.load(Ordering::Relaxed) as usize;
        let regrowth = (max_mem / 1024).max(1024);
        let total = || self.real_used_total();
        if let Some((until, at_total)) = self.remote_spill_backoff.get()
            && total() < at_total.saturating_add(regrowth)
            && std::time::Instant::now() < until
        {
            return false;
        }
        for s in 0..self.num_shards {
            if s == self.shard_id {
                continue;
            }
            let (tx, rx) = flume::bounded(1);
            if self.senders[s]
                .send(ShardMessage::TierAutoSpill { responder: tx })
                .is_ok()
                && let Ok(used) = rx.recv_async().await
            {
                self.tier_stats.publish_shard_used(s, used);
            }
            if !self.over_maxmemory() {
                self.remote_spill_backoff.set(None);
                return true;
            }
        }
        self.remote_spill_backoff.set(Some((
            std::time::Instant::now() + std::time::Duration::from_secs(1),
            total(),
        )));
        false
    }

    /// [`Router::check_auto_tier`] restricted to this shard's own keys.
    /// Returns whether the server ended within the limit.
    pub async fn check_auto_tier_local(&self) -> bool {
        let max_mem = self.tier_stats.max_memory.load(Ordering::Relaxed) as usize;
        if max_mem == 0 {
            return true;
        }
        // With an eviction policy, maxmemory is enforced by evicting keys
        // before commands (Redis semantics); the tier stands in for eviction
        // only under noeviction, where data must not be dropped.
        if !crate::connection::max_memory_policy_is_noeviction() {
            return !self.over_maxmemory();
        }
        self.sync_backoff_epoch();
        if self.over_maxmemory() {
            self.refresh_published_usage().await;
        }
        let under = || !self.over_maxmemory();
        // Another task on this shard is already spilling: wait (bounded) for
        // it to finish rather than reporting "over" while it is about to
        // make room, which would fail a noeviction write with OOM.
        let mut waits = 0;
        while self.is_auto_tiering.get() && !under() && waits < 100 {
            monoio::time::sleep(std::time::Duration::from_millis(1)).await;
            waits += 1;
        }
        // Without a tier there is nothing to spill to; the scan below would
        // walk every key and fail on each one.
        if self.is_auto_tiering.get() || under() || self.local_db.borrow().tier_manager.is_none() {
            return under();
        }
        self.is_auto_tiering.set(true);
        // Cleared however this ends (also if the future is dropped part-way,
        // which would otherwise stop auto-tiering on this shard for good).
        struct ClearOnDrop<'a>(&'a std::cell::Cell<bool>);
        impl Drop for ClearOnDrop<'_> {
            fn drop(&mut self) {
                self.0.set(false);
            }
        }
        let _clear = ClearOnDrop(&self.is_auto_tiering);

        // Phase 1: Instant Zero-I/O Decommit of all Cooled keys
        let decommitted = self.decommit_local(None);
        if decommitted > 0 && under() {
            self.is_auto_tiering.set(false);
            return true;
        }

        // Phase 2: Spill Hot keys to NVMe disk in 64-key slices until under target_mem.
        // Work per call is bounded (this runs inline on the write path when
        // over budget), progress is judged by used_memory actually dropping
        // (spilling small values can grow it: the tier pointer outweighs the
        // value), and after a pass that frees nothing we back off for a
        // second instead of rescanning the table on every write.
        let regrowth = (max_mem / 1024).max(1024);
        if self.local_db.borrow().table.used_memory()
            < self.spill_backoff_used.get().saturating_add(regrowth)
            && self
                .spill_backoff_until
                .get()
                .is_some_and(|t| std::time::Instant::now() < t)
        {
            self.is_auto_tiering.set(false);
            return under();
        }
        // Spill until the table's estimate has dropped by the server's
        // excess (by the allocator's count) over a target a little below the
        // limit. The estimate, not the allocator count, measures progress:
        // spilled values sit in the tier's write buffer until it flushes.
        let target_total = max_mem.saturating_sub((max_mem / 20).max(128 * 1024));
        let excess = self.real_used_total().saturating_sub(target_total);
        let start_used = self.local_db.borrow().table.used_memory();
        let target_mem = start_used.saturating_sub(excess);
        let mut attempts = 0usize;
        let mut exhausted = false;
        while attempts < MAX_SPILL_ATTEMPTS_PER_CALL {
            if self.local_db.borrow().table.used_memory() <= target_mem {
                break;
            }
            let hot_keys = self.local_db.borrow_mut().table.get_hot_keys_for_spill(64);
            if hot_keys.is_empty() {
                exhausted = true;
                break;
            }
            for k in hot_keys {
                attempts += 1;
                self.spill_local_internal(&k, false).await;
                if self.local_db.borrow().table.used_memory() <= target_mem {
                    break;
                }
            }
        }
        let end_used = self.local_db.borrow().table.used_memory();
        let freed_nothing = end_used >= start_used;
        self.spill_backoff_used.set(end_used);
        self.spill_backoff_until.set(
            (exhausted || freed_nothing)
                .then(|| std::time::Instant::now() + std::time::Duration::from_secs(1)),
        );

        let tm = self.local_db.borrow().tier_manager.clone();
        if let Some(tm) = tm {
            let _ = tm.flush_active_bin().await;
        }

        self.is_auto_tiering.set(false);
        under()
    }

    pub async fn spill_key(&self, key: &[u8]) -> bool {
        let target = self.target_shard(key);
        if target == self.shard_id {
            self.spill_local(key).await
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::TierSpill {
                key: Bytes::copy_from_slice(key),
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(false)
            } else {
                false
            }
        }
    }

    pub async fn ensure_loaded(&self, key: &[u8]) -> bool {
        if self.is_memory_constrained() {
            return false;
        }

        let target = self.target_shard(key);
        if target == self.shard_id {
            self.load_local(key).await
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::TierLoad {
                key: Bytes::copy_from_slice(key),
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(false)
            } else {
                false
            }
        }
    }

    pub async fn spill_all(&self) -> usize {
        let mut count = 0;
        let keys = self.local_db.borrow_mut().table.keys(b"*");
        for k in keys {
            if self.spill_local(&k).await {
                count += 1;
            }
        }
        count
    }

    pub fn gc_local(&self) -> usize {
        let tm = self.local_db.borrow().tier_manager.clone();
        if let Some(tm) = tm { tm.run_gc() } else { 0 }
    }

    pub async fn gc_all(&self) -> usize {
        let mut total = self.gc_local();
        for s in 0..self.num_shards {
            if s != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                if self.senders[s]
                    .send(ShardMessage::TierGc { responder: tx })
                    .is_ok()
                {
                    total += rx.recv_async().await.unwrap_or(0);
                }
            }
        }
        total
    }

    pub async fn snapshot_local(
        &self,
        backup_dir: &std::path::Path,
    ) -> Result<(bool, u64), String> {
        let tm = self.local_db.borrow().tier_manager.clone();
        if let Some(tm) = tm {
            tm.snapshot(backup_dir).await.map_err(|e| e.to_string())
        } else {
            Err("Tiered storage not enabled".to_string())
        }
    }

    pub async fn tier_snapshot_all(
        &self,
        backup_dir: std::path::PathBuf,
    ) -> Result<(bool, u64, usize), String> {
        let (local_reflink, local_bytes) = self.snapshot_local(&backup_dir).await?;
        let mut total_bytes = local_bytes;
        let mut all_reflink = local_reflink;
        let mut shard_count = 1;

        for s in 0..self.num_shards {
            if s != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                if self.senders[s]
                    .send(ShardMessage::TierSnapshot {
                        backup_dir: backup_dir.clone(),
                        responder: tx,
                    })
                    .is_ok()
                {
                    let res = rx.recv_async().await.map_err(|e| e.to_string())??;
                    total_bytes += res.1;
                    if !res.0 {
                        all_reflink = false;
                    }
                    shard_count += 1;
                }
            }
        }
        Ok((all_reflink, total_bytes, shard_count))
    }

    #[inline(always)]
    pub async fn read_cold_key_local(&self, key: &Bytes) -> Option<Bytes> {
        if self.is_memory_constrained() {
            let val = self.stream_cold_read_local(key).await;
            if val.is_some() {
                self.tier_stats
                    .streaming_reads
                    .fetch_add(1, Ordering::Relaxed);
                self.tier_stats.ram_misses.fetch_add(1, Ordering::Relaxed);
            }
            val
        } else {
            self.load_local(key).await;
            self.local_db.borrow_mut().get(key)
        }
    }

    #[inline(always)]
    pub async fn get_local_direct(&self, key: &Bytes) -> Option<Bytes> {
        let val = self.local_db.borrow_mut().get(key);
        if let Some(v) = val {
            self.tier_stats.ram_hits.fetch_add(1, Ordering::Relaxed);
            return Some(v);
        }
        if self.local_db.borrow().tier_manager.is_some()
            && self.local_db.borrow_mut().table.is_tiered(key).is_some()
        {
            return self.read_cold_key_local(key).await;
        }
        None
    }

    pub async fn get(&self, key: Bytes) -> Option<Bytes> {
        self.get_checked(key).await.unwrap_or(None)
    }

    /// Local half of [`Router::get_checked`].
    #[inline(always)]
    pub async fn get_local_checked(&self, key: &Bytes) -> Result<Option<Bytes>, &'static str> {
        let val = self.local_db.borrow_mut().get_checked(key)?;
        if let Some(v) = val {
            self.tier_stats.ram_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(v));
        }
        if self.local_db.borrow().tier_manager.is_some()
            && self.local_db.borrow_mut().table.is_tiered(key).is_some()
        {
            return Ok(self.read_cold_key_local(key).await);
        }
        Ok(None)
    }

    /// GET with Redis semantics: a key holding another type is a WRONGTYPE
    /// error rather than a missing key.
    pub async fn get_checked(&self, key: Bytes) -> Result<Option<Bytes>, &'static str> {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            self.get_local_checked(&key).await
        } else {
            let desc = Arc::new(crate::mailbox::FastGetDescriptor::new(key));
            let msg = ShardMessage::FastGet {
                descriptor: desc.clone(),
            };
            if self.senders[target].send(msg).is_ok() {
                desc.wait_done(crate::mailbox::cross_shard_spin()).await;
                if desc.wrong_type.load(Ordering::Relaxed) {
                    Err(WRONGTYPE_ERR)
                } else {
                    // SAFETY: `wait_done` observed the remote shard's Release store of `done`,
                    // which follows its write of `val` in `finish`; it never touches `val` again
                    // and we are the only reader.
                    Ok(unsafe { (*desc.val.get()).take() })
                }
            } else {
                Ok(None)
            }
        }
    }

    pub async fn hget(&self, key: Bytes, field: Bytes) -> Option<Bytes> {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            self.local_db.borrow_mut().hget(&key, &field).ok().flatten()
        } else {
            let res = self
                .execute_remote(target, Command::Hget { key, field })
                .await;
            if res.starts_with(b"$")
                && !res.starts_with(b"$-1")
                && let Some(pos) = res.windows(2).position(|w| w == b"\r\n")
            {
                let len_str = &res[1..pos];
                if let Ok(len) = std::str::from_utf8(len_str).unwrap_or("").parse::<usize>() {
                    let data_start = pos + 2;
                    if data_start + len <= res.len() {
                        return Some(Bytes::copy_from_slice(&res[data_start..data_start + len]));
                    }
                }
            }
            None
        }
    }

    pub async fn rpush(&self, key: Bytes, elements: Vec<Bytes>) -> usize {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            // The remote path runs RPUSH through `execute_local_command`,
            // which logs it; log the local one the same way.
            let logged = elements.clone();
            let len = self
                .local_db
                .borrow_mut()
                .rpush(key.clone(), elements)
                .unwrap_or(0);
            if len > 0 {
                self.log_mutation(|| Command::Rpush {
                    key: key.clone(),
                    values: logged.into(),
                });
            }
            crate::connection::notify_list_or_defer(&mut self.local_db.borrow_mut(), &key);
            len
        } else {
            let res = self
                .execute_remote(
                    target,
                    Command::Rpush {
                        key,
                        values: elements.into(),
                    },
                )
                .await;
            if res.starts_with(b":")
                && let Some(pos) = res.windows(2).position(|w| w == b"\r\n")
                && let Ok(cnt) = std::str::from_utf8(&res[1..pos])
                    .unwrap_or("")
                    .parse::<usize>()
            {
                return cnt;
            }
            0
        }
    }

    pub async fn get_collection_for_sort(
        &self,
        key: &Bytes,
    ) -> Result<(String, Vec<Bytes>), &'static str> {
        let target = self.target_shard(key);
        if target == self.shard_id {
            let mut db = self.local_db.borrow_mut();
            let t = db.type_of(key);
            match t {
                "none" => Ok(("none".to_string(), Vec::new())),
                "list" => Ok((
                    "list".to_string(),
                    db.lrange(key, 0, -1).unwrap_or_default(),
                )),
                "set" => Ok(("set".to_string(), db.smembers(key).unwrap_or_default())),
                "zset" => {
                    let items = db
                        .zrange(
                            key,
                            &crate::table::ZRangeOpts {
                                start: 0,
                                stop: -1,
                                ..Default::default()
                            },
                        )
                        .map(|pairs| pairs.into_iter().map(|(m, _)| m).collect())
                        .unwrap_or_default();
                    Ok(("zset".to_string(), items))
                }
                _ => Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
            }
        } else {
            let type_resp = self
                .execute_remote(target, Command::Type(key.clone()))
                .await;
            if type_resp == b"+none\r\n" {
                Ok(("none".to_string(), Vec::new()))
            } else if type_resp == b"+list\r\n" {
                let range_resp = self
                    .execute_remote(
                        target,
                        Command::Lrange {
                            key: key.clone(),
                            start: 0,
                            stop: -1,
                        },
                    )
                    .await;
                Ok(("list".to_string(), parse_resp_array_to_bytes(&range_resp)))
            } else if type_resp == b"+set\r\n" {
                let smembers_resp = self
                    .execute_remote(target, Command::Smembers(key.clone()))
                    .await;
                Ok(("set".to_string(), parse_resp_array_to_bytes(&smembers_resp)))
            } else if type_resp == b"+zset\r\n" {
                let zrange_resp = self
                    .execute_remote(
                        target,
                        Command::Zrange {
                            key: key.clone(),
                            opts: crate::table::ZRangeOpts {
                                start: 0,
                                stop: -1,
                                ..Default::default()
                            },
                        },
                    )
                    .await;
                Ok(("zset".to_string(), parse_resp_array_to_bytes(&zrange_resp)))
            } else {
                Err("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
        }
    }

    pub async fn set(&self, key: Bytes, value: Bytes, expire_in: Option<Duration>) {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            self.log_mutation(|| Command::Set {
                key: key.clone(),
                value: value.clone(),
                expire_in,
                condition: crate::resp::SetCondition::None,
                get: false,
                keepttl: false,
                past_expired: false,
            });
            let prev_kind = {
                let mut db = self.local_db.borrow_mut();
                let kind = db.type_of(&key);
                db.set(key.clone(), value, expire_in);
                crate::connection::notify_stream_or_defer(&mut db, &key);
                kind
            };
            crate::connection::notify_set_key_events(self, prev_kind, "string", &key);
            crate::connection::notify_keyspace_event_sync(
                self,
                crate::connection::NOTIFY_STRING,
                "set",
                &key,
            );
            if expire_in.is_some() {
                crate::connection::notify_keyspace_event_sync(
                    self,
                    crate::connection::NOTIFY_GENERIC,
                    "expire",
                    &key,
                );
            }
            if self.tier_stats.max_memory.load(Ordering::Relaxed) > 0 {
                self.start_auto_tier_if_over();
            }
        } else {
            let desc = Arc::new(crate::mailbox::FastSetDescriptor::new(
                key, value, expire_in,
            ));
            let msg = ShardMessage::FastSet {
                descriptor: desc.clone(),
            };
            if self.senders[target].send(msg).is_ok() {
                desc.wait_done(crate::mailbox::cross_shard_spin()).await;
            }
        }
    }

    #[inline(always)]
    pub(crate) fn check_auto_tier_after_write(&self) {
        let max_mem = self.tier_stats.max_memory.load(Ordering::Relaxed);
        if max_mem > 0 {
            if crate::connection::max_memory_policy_is_noeviction() {
                self.start_auto_tier_if_over();
            } else {
                // Publish this shard's usage for the other shards' gates. No
                // eviction here: like Redis, keys are evicted only before a
                // command runs (the gate in `execute_command`; squashed
                // batches fall back to it when over the limit). Evicting
                // after a write would send tracking invalidations ahead of
                // that write's reply.
                self.publish_memory_state();
            }
        }
    }

    /// Publishes this shard's usage and, if the server is over
    /// `maxmemory`, decommits cooled keys and, if that is not enough,
    /// starts spilling in the background.
    #[inline]
    fn start_auto_tier_if_over(&self) {
        if self.publish_memory_state() && !self.is_auto_tiering.get() {
            let decommitted = self.decommit_local(None);
            if (decommitted == 0 || self.over_maxmemory()) && !self.is_auto_tiering.get() {
                let r = self.clone();
                monoio::spawn(async move {
                    r.check_auto_tier_local().await;
                });
            }
        }
    }

    #[inline(always)]
    pub fn acquire_notify_channel(&self) -> (flume::Sender<()>, flume::Receiver<()>) {
        self.notify_channel_pool
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| flume::bounded(1))
    }

    #[inline(always)]
    pub fn release_notify_channel(&self, tx: flume::Sender<()>, rx: flume::Receiver<()>) {
        while rx.try_recv().is_ok() {}
        self.notify_channel_pool.borrow_mut().push((tx, rx));
    }

    #[inline(always)]
    pub fn acquire_pubsub_responder(&self) -> (flume::Sender<usize>, flume::Receiver<usize>) {
        self.pubsub_responder_pool
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| flume::bounded(1))
    }

    #[inline(always)]
    pub fn release_pubsub_responder(&self, tx: flume::Sender<usize>, rx: flume::Receiver<usize>) {
        while rx.try_recv().is_ok() {}
        self.pubsub_responder_pool.borrow_mut().push((tx, rx));
    }

    pub fn acquire_mget_descriptor(
        &self,
        total_keys: usize,
        pending_shards: usize,
        notify: flume::Sender<()>,
    ) -> Arc<crate::mailbox::ScatterMgetDescriptor> {
        let mut pool = self.mget_desc_pool.borrow_mut();
        let mut kept = Vec::new();
        let mut chosen = None;
        while let Some(desc) = pool.pop() {
            if desc.results.len() >= total_keys && desc.recycled_keys.len() >= self.num_shards {
                // Reuse only if the owner shard already dropped its clone;
                // otherwise keep it pooled rather than spin waiting for it.
                if Arc::strong_count(&desc) == 1 {
                    chosen = Some(desc);
                    break;
                }
            }
            kept.push(desc);
        }
        pool.extend(kept);
        if let Some(desc) = chosen {
            desc.reset(total_keys, pending_shards, notify);
            return desc;
        }
        Arc::new(crate::mailbox::ScatterMgetDescriptor::new(
            total_keys.max(64),
            self.num_shards,
            pending_shards,
            notify,
        ))
    }

    #[inline(always)]
    pub fn release_mget_descriptor(&self, desc: Arc<crate::mailbox::ScatterMgetDescriptor>) {
        if self.mget_desc_pool.borrow().len() < 32 {
            self.mget_desc_pool.borrow_mut().push(desc);
        }
    }

    pub fn acquire_mset_descriptor(
        &self,
        pending_shards: usize,
        notify: flume::Sender<()>,
    ) -> Arc<crate::mailbox::ScatterMsetDescriptor> {
        let mut pool = self.mset_desc_pool.borrow_mut();
        let mut kept = Vec::new();
        let mut chosen = None;
        while let Some(desc) = pool.pop() {
            if desc.recycled_pairs.len() >= self.num_shards {
                // Reuse only if the owner shard already dropped its clone;
                // otherwise keep it pooled rather than spin waiting for it.
                if Arc::strong_count(&desc) == 1 {
                    chosen = Some(desc);
                    break;
                }
            }
            kept.push(desc);
        }
        pool.extend(kept);
        if let Some(desc) = chosen {
            desc.reset(pending_shards, notify);
            return desc;
        }
        Arc::new(crate::mailbox::ScatterMsetDescriptor::new(
            self.num_shards,
            pending_shards,
            notify,
        ))
    }

    #[inline(always)]
    pub fn release_mset_descriptor(&self, desc: Arc<crate::mailbox::ScatterMsetDescriptor>) {
        if self.mset_desc_pool.borrow().len() < 32 {
            self.mset_desc_pool.borrow_mut().push(desc);
        }
    }

    pub async fn mget(&self, keys: Vec<Bytes>) -> Vec<Option<Bytes>> {
        if keys.is_empty() {
            return Vec::new();
        }

        let total_keys = keys.len();

        if self.num_shards <= 1 {
            let mut results = Vec::with_capacity(total_keys);
            for key in keys {
                results.push(self.get_local_direct(&key).await);
            }
            return results;
        }

        let mut remote_batches = self
            .mget_batch_pool
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| (0..self.num_shards).map(|_| Vec::new()).collect());
        let mut local_keys = Vec::with_capacity(keys.len().min(16));
        let mut has_remote = false;
        let mut num_remote_shards = 0;

        // Partition local keys and remote keys without holding RefCell borrow across await
        for (idx, key) in keys.into_iter().enumerate() {
            let target = self.target_shard(&key);
            if target == self.shard_id {
                local_keys.push((idx, key));
            } else {
                has_remote = true;
                if remote_batches[target].is_empty() {
                    num_remote_shards += 1;
                }
                remote_batches[target].push((idx, key));
            }
        }

        // Fast path: all keys are local - 0 channel operations
        if !has_remote {
            let mut results = Vec::with_capacity(total_keys);
            {
                let mut db = self.local_db.borrow_mut();
                for (_, key) in local_keys {
                    results.push(db.get(&key));
                }
            }
            self.mget_batch_pool.borrow_mut().push(remote_batches);
            return results;
        }

        let (notify_tx, notify_rx) = self.acquire_notify_channel();
        let descriptor =
            self.acquire_mget_descriptor(total_keys, num_remote_shards, notify_tx.clone());

        // Dispatch remote batches concurrently with direct scatter-gather shared memory
        for (target_shard, batch) in remote_batches.iter_mut().enumerate().take(self.num_shards) {
            let items = std::mem::take(batch);
            if !items.is_empty() {
                let msg = ShardMessage::ScatterMget {
                    shard_id: target_shard,
                    keys: items,
                    descriptor: descriptor.clone(),
                    no_touch: crate::connection::CLIENT_NO_TOUCH.get(),
                };
                if self.senders[target_shard].send(msg).is_err() {
                    descriptor.finish_shard();
                }
            }
        }

        // Execute local keys CONCURRENTLY while remote shards process their batches
        if !local_keys.is_empty() {
            let mut db = self.local_db.borrow_mut();
            for (idx, key) in local_keys {
                let val = db.get(&key);
                descriptor.write_result(idx, val);
            }
        }

        // Wait for all remote shards to complete their writes
        descriptor
            .wait_completed(crate::mailbox::cross_shard_spin(), &notify_rx)
            .await;

        let recycled = descriptor.take_recycled_keys();
        self.mget_batch_pool.borrow_mut().push(recycled);
        self.release_notify_channel(notify_tx, notify_rx);

        let results = descriptor.into_results_prefix(total_keys);
        self.release_mget_descriptor(descriptor);
        results
    }

    /// Dispatch phase of a cross-shard MGET.
    ///
    /// Buckets the keys per shard, fires the `ScatterMget` messages and serves the
    /// local keys, all **without awaiting** the remote shards. Returns `None` when
    /// the command was fully satisfied inline (single shard, or every key owned by
    /// this shard), in which case the RESP reply is already written into `out`.
    ///
    /// Handing back an in-flight handle instead of blocking lets a pipelined batch
    /// put several MGETs on the wire before paying a single round-trip stall.
    pub async fn begin_mget_resp(
        &self,
        keys: Vec<Bytes>,
        out: &mut Vec<u8>,
    ) -> Option<MgetInFlight> {
        if keys.is_empty() {
            crate::connection::write_resp_array_header(out, 0);
            return None;
        }

        let total_keys = keys.len();

        if self.num_shards <= 1 {
            out.reserve(total_keys * 32 + 16);
            crate::connection::write_resp_array_header(out, total_keys);
            for key in keys {
                if let Some(v) = self.get_local_direct(&key).await {
                    crate::connection::write_resp_bulk(out, &v);
                } else {
                    crate::connection::write_resp_null(out);
                }
            }
            return None;
        }

        let mut remote_batches = self
            .mget_batch_pool
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| (0..self.num_shards).map(|_| Vec::new()).collect());
        let mut local_keys = Vec::with_capacity(keys.len().min(16));
        let mut has_remote = false;
        let mut num_remote_shards = 0;

        for (idx, key) in keys.into_iter().enumerate() {
            let (target, key_hash) = self.target_shard_and_hash(key.as_ref());
            if target == self.shard_id {
                local_keys.push((idx, key, key_hash));
            } else {
                has_remote = true;
                if remote_batches[target].is_empty() {
                    num_remote_shards += 1;
                }
                remote_batches[target].push((idx, key));
            }
        }

        // Fast path: all keys are local - 0 channel operations
        if !has_remote {
            out.reserve(total_keys * 32 + 16);
            crate::connection::write_resp_array_header(out, total_keys);
            {
                let mut db = self.local_db.borrow_mut();
                for (_, key, key_hash) in local_keys {
                    if let Some(v) = db.get_with_hash(key.as_ref(), key_hash) {
                        crate::connection::write_resp_bulk(out, &v);
                    } else {
                        crate::connection::write_resp_null(out);
                    }
                }
            }
            self.mget_batch_pool.borrow_mut().push(remote_batches);
            return None;
        }

        let (notify_tx, notify_rx) = self.acquire_notify_channel();
        let descriptor =
            self.acquire_mget_descriptor(total_keys, num_remote_shards, notify_tx.clone());

        for (target_shard, batch) in remote_batches.iter_mut().enumerate().take(self.num_shards) {
            let items = std::mem::take(batch);
            if !items.is_empty() {
                let msg = ShardMessage::ScatterMget {
                    shard_id: target_shard,
                    keys: items,
                    descriptor: descriptor.clone(),
                    no_touch: crate::connection::CLIENT_NO_TOUCH.get(),
                };
                if self.senders[target_shard].send(msg).is_err() {
                    descriptor.finish_shard();
                }
            }
        }

        // Execute local keys CONCURRENTLY while remote shards process their batches
        if !local_keys.is_empty() {
            let mut db = self.local_db.borrow_mut();
            for (idx, key, key_hash) in local_keys {
                let val = db.get_with_hash(key.as_ref(), key_hash);
                descriptor.write_result(idx, val);
            }
        }

        Some(MgetInFlight {
            descriptor,
            notify_tx,
            notify_rx,
            total_keys,
        })
    }

    /// Completion phase of a cross-shard MGET: wait for every shard to publish its
    /// slots, then serialize the gathered values into `out`.
    pub async fn finish_mget_resp(&self, inflight: MgetInFlight, out: &mut Vec<u8>) {
        let MgetInFlight {
            descriptor,
            notify_tx,
            notify_rx,
            total_keys,
        } = inflight;

        // Wait for all remote shards to complete their writes
        descriptor
            .wait_completed(crate::mailbox::cross_shard_spin(), &notify_rx)
            .await;

        let recycled = descriptor.take_recycled_keys();
        self.mget_batch_pool.borrow_mut().push(recycled);
        self.release_notify_channel(notify_tx, notify_rx);

        out.reserve(total_keys * 32 + 16);
        crate::connection::write_resp_array_header(out, total_keys);
        // SAFETY: `wait_completed` observed DESC_COMPLETED (Acquire) after every
        // remote shard's `finish_shard`, so all result writes are visible and no
        // shard writes the slots anymore; `results[i]` is bounds-checked.
        unsafe {
            for i in 0..total_keys {
                let slot = (*descriptor.results[i].get()).take();
                match slot {
                    Some(ref v) => crate::connection::write_resp_bulk(out, v),
                    None => crate::connection::write_resp_null(out),
                }
            }
        }
        self.release_mget_descriptor(descriptor);
    }

    pub async fn write_mget_resp(&self, keys: Vec<Bytes>, out: &mut Vec<u8>) {
        if let Some(inflight) = self.begin_mget_resp(keys, out).await {
            self.finish_mget_resp(inflight, out).await;
        }
    }

    /// Dispatch phase of a cross-shard MSET. See [`Router::begin_mget_resp`].
    ///
    /// Applies the locally-owned pairs and fires `ScatterMset` to the other shards
    /// without awaiting. Returns `None` when no remote shard is involved.
    pub fn begin_mset(&self, pairs: Vec<(Bytes, Bytes)>) -> Option<MsetInFlight> {
        if pairs.is_empty() {
            return None;
        }

        if self.num_shards <= 1 {
            {
                let mut db = self.local_db.borrow_mut();
                for (k, v) in &pairs {
                    db.set(k.clone(), v.clone(), None);
                }
            }
            self.log_mutation(|| crate::resp::Command::Mset(pairs));
            self.check_auto_tier_after_write();
            return None;
        }

        let mut remote_batches = self
            .mset_batch_pool
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| (0..self.num_shards).map(|_| Vec::new()).collect());
        let mut local_batch = Vec::with_capacity(pairs.len().min(16));
        let mut has_remote = false;
        let mut num_remote_shards = 0;

        for (k, v) in pairs {
            let target = self.target_shard(&k);
            if target == self.shard_id {
                local_batch.push((k, v));
            } else {
                has_remote = true;
                if remote_batches[target].is_empty() {
                    num_remote_shards += 1;
                }
                remote_batches[target].push((k, v));
            }
        }

        // Fast path: all pairs are local
        if !has_remote {
            if !local_batch.is_empty() {
                self.log_mutation(|| crate::resp::Command::Mset(local_batch.clone()));
                {
                    let mut db = self.local_db.borrow_mut();
                    for (k, v) in local_batch {
                        db.set(k, v, None);
                    }
                }
                self.check_auto_tier_after_write();
            }
            self.mset_batch_pool.borrow_mut().push(remote_batches);
            return None;
        }

        let (notify_tx, notify_rx) = self.acquire_notify_channel();
        let descriptor = self.acquire_mset_descriptor(num_remote_shards, notify_tx.clone());

        for (target_shard, batch) in remote_batches.iter_mut().enumerate().take(self.num_shards) {
            let items = std::mem::take(batch);
            if !items.is_empty() {
                let msg = ShardMessage::ScatterMset {
                    shard_id: target_shard,
                    pairs: items,
                    descriptor: descriptor.clone(),
                };
                if self.senders[target_shard].send(msg).is_err() {
                    descriptor.finish_shard();
                }
            }
        }

        // Execute local batch CONCURRENTLY while remote shards process their batches
        if !local_batch.is_empty() {
            self.log_mutation(|| crate::resp::Command::Mset(local_batch.clone()));
            {
                let mut db = self.local_db.borrow_mut();
                for (k, v) in local_batch {
                    db.set(k, v, None);
                }
            }
            self.check_auto_tier_after_write();
        }

        Some(MsetInFlight {
            descriptor,
            notify_tx,
            notify_rx,
        })
    }

    /// Completion phase of a cross-shard MSET: wait until every shard applied its slice.
    pub async fn finish_mset(&self, inflight: MsetInFlight) {
        let MsetInFlight {
            descriptor,
            notify_tx,
            notify_rx,
        } = inflight;

        descriptor
            .wait_completed(crate::mailbox::cross_shard_spin(), &notify_rx)
            .await;

        let recycled = descriptor.take_recycled_pairs();
        self.mset_batch_pool.borrow_mut().push(recycled);
        self.release_notify_channel(notify_tx, notify_rx);
        self.release_mset_descriptor(descriptor);
    }

    pub async fn mset(&self, pairs: Vec<(Bytes, Bytes)>) {
        if let Some(inflight) = self.begin_mset(pairs) {
            self.finish_mset(inflight).await;
        }
    }

    pub async fn json_mget(&self, keys: Vec<Bytes>, path: &str) -> Vec<Option<String>> {
        if keys.is_empty() {
            return Vec::new();
        }

        let total_keys = keys.len();

        if self.num_shards <= 1 {
            let db = self.local_db.borrow();
            let mut results = Vec::with_capacity(total_keys);
            for key in keys {
                results.push(db.json_store.json_get(&key, &[path]));
            }
            return results;
        }

        let mut local_keys = Vec::with_capacity(total_keys.min(16));
        let mut remote_batches: Vec<Vec<(usize, Bytes)>> =
            (0..self.num_shards).map(|_| Vec::new()).collect();
        let mut has_remote = false;

        for (idx, key) in keys.into_iter().enumerate() {
            let target = self.target_shard(&key);
            if target == self.shard_id {
                local_keys.push((idx, key));
            } else {
                has_remote = true;
                remote_batches[target].push((idx, key));
            }
        }

        let mut results: Vec<Option<String>> = (0..total_keys).map(|_| None).collect();

        // 1. Evaluate all local keys directly in-place
        {
            let db = self.local_db.borrow();
            for (idx, key) in local_keys {
                results[idx] = db.json_store.json_get(&key, &[path]);
            }
        }

        if !has_remote {
            return results;
        }

        // 2. Dispatch batched requests to remote shards concurrently
        let mut responders = Vec::new();
        for (target_shard, shard_keys) in remote_batches.into_iter().enumerate() {
            if !shard_keys.is_empty() {
                let (tx, rx) = flume::bounded(1);
                if self.senders[target_shard]
                    .send(ShardMessage::JsonMget {
                        keys: shard_keys,
                        path: path.to_string(),
                        responder: tx,
                    })
                    .is_ok()
                {
                    responders.push(rx);
                }
            }
        }

        // 3. Concurrently await all remote responses in parallel
        for rx in responders {
            if let Ok(shard_results) = rx.recv_async().await {
                for (idx, val) in shard_results {
                    results[idx] = val;
                }
            }
        }

        results
    }

    pub async fn del(&self, key: Bytes) -> bool {
        let target = target_shard(&key, self.num_shards);
        if target == self.shard_id {
            self.del_local(key)
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Del { key, responder: tx };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(false)
            } else {
                false
            }
        }
    }

    /// Deletes a key owned by this shard, with the usual side effects
    /// (keyspace events, AOF/replication).
    pub fn del_local(&self, key: Bytes) -> bool {
        let mut db = self.local_db.borrow_mut();
        let deleted = db.del(&key);
        if deleted {
            db.delete_document_local(&String::from_utf8_lossy(&key));
            crate::connection::notify_keyspace_event_sync(
                self,
                crate::connection::NOTIFY_GENERIC,
                "del",
                &key,
            );
            crate::connection::notify_stream_or_defer(&mut db, &key);
            self.log_mutation(|| Command::Del(smallvec![key]));
        }
        deleted
    }

    /// Deletes `key` only if its DUMP is still `payload`, i.e. nothing
    /// changed it since it was dumped. The check and the delete run on the
    /// owning shard in one step.
    pub async fn del_if_unchanged(&self, key: Bytes, payload: Bytes) -> bool {
        let target = target_shard(&key, self.num_shards);
        if target == self.shard_id {
            self.del_if_unchanged_local(key, &payload)
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::DelIfUnchanged {
                key,
                payload,
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(false)
            } else {
                false
            }
        }
    }

    pub fn del_if_unchanged_local(&self, key: Bytes, payload: &[u8]) -> bool {
        let unchanged = self.local_db.borrow_mut().dump(&key).as_deref() == Some(payload);
        unchanged && self.del_local(key)
    }

    pub async fn del_keys(&self, mut keys: Vec<Bytes>) -> usize {
        if keys.is_empty() {
            return 0;
        }

        if keys.len() == 1 {
            return if self.del(keys.swap_remove(0)).await {
                1
            } else {
                0
            };
        }

        if self.num_shards <= 1 {
            let mut count = 0;
            let mut deleted_keys = Vec::with_capacity(keys.len());
            {
                let mut db = self.local_db.borrow_mut();
                for k in keys {
                    if db.del(&k) {
                        count += 1;
                        crate::connection::notify_keyspace_event_sync(
                            self,
                            crate::connection::NOTIFY_GENERIC,
                            "del",
                            &k,
                        );
                        crate::connection::notify_stream_or_defer(&mut db, &k);
                        deleted_keys.push(k);
                    }
                }
            }
            if count > 0 {
                self.log_mutation(|| Command::Del(SmallVec::from_vec(deleted_keys)));
            }
            return count;
        }

        let mut local_keys = Vec::with_capacity(keys.len().min(16));
        let mut remote_batches: Vec<Vec<Bytes>> =
            (0..self.num_shards).map(|_| Vec::new()).collect();
        let mut has_remote = false;

        for key in keys {
            let target = self.target_shard(&key);
            if target == self.shard_id {
                local_keys.push(key);
            } else {
                has_remote = true;
                remote_batches[target].push(key);
            }
        }

        // 1. Delete all local keys directly in-place
        let mut total_deleted = 0;
        if !local_keys.is_empty() {
            let mut deleted_local = Vec::with_capacity(local_keys.len());
            {
                let mut db = self.local_db.borrow_mut();
                for k in local_keys {
                    if db.del(&k) {
                        total_deleted += 1;
                        crate::connection::notify_keyspace_event_sync(
                            self,
                            crate::connection::NOTIFY_GENERIC,
                            "del",
                            &k,
                        );
                        crate::connection::notify_stream_or_defer(&mut db, &k);
                        deleted_local.push(k);
                    }
                }
            }
            if !deleted_local.is_empty() {
                self.log_mutation(|| Command::Del(SmallVec::from_vec(deleted_local)));
            }
        }

        if !has_remote {
            return total_deleted;
        }

        // 2. Dispatch batched requests to remote shards concurrently in parallel
        let mut responders = Vec::new();
        for (target_shard, shard_keys) in remote_batches.into_iter().enumerate() {
            if !shard_keys.is_empty() {
                let (tx, rx) = flume::bounded(1);
                if self.senders[target_shard]
                    .send(ShardMessage::DelKeys {
                        keys: shard_keys,
                        responder: tx,
                    })
                    .is_ok()
                {
                    responders.push(rx);
                }
            }
        }

        // 3. Concurrently await all remote responses in parallel
        for rx in responders {
            if let Ok(shard_deleted) = rx.recv_async().await {
                total_deleted += shard_deleted;
            }
        }

        total_deleted
    }

    pub async fn active_defrag(&self) -> usize {
        let mut total_freed = self.local_db.borrow_mut().active_defrag();
        let mut responders = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                if sender
                    .send(ShardMessage::ActiveDefrag { responder: tx })
                    .is_ok()
                {
                    responders.push(rx);
                }
            }
        }
        for rx in responders {
            if let Ok(freed) = rx.recv_async().await {
                total_freed += freed;
            }
        }
        total_freed
    }

    pub async fn exists(&self, key: Bytes) -> bool {
        let (target, hash) = self.target_shard_and_hash(&key);
        if target == self.shard_id {
            self.local_db.borrow_mut().exists_with_hash(&key, hash)
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Exists { key, responder: tx };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(false)
            } else {
                false
            }
        }
    }

    pub async fn incr_by(&self, key: Bytes, delta: i64) -> Result<i64, String> {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            let res = self.local_db.borrow_mut().incr_by(key.clone(), delta);
            if res.is_ok() {
                self.log_mutation(|| Command::IncrBy(key, delta, crate::resp::IncrName::IncrBy));
            }
            res
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::IncrBy {
                key,
                delta,
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async()
                    .await
                    .unwrap_or_else(|_| Err("shard disconnected".to_string()))
            } else {
                Err("failed to route to shard".to_string())
            }
        }
    }

    pub async fn expire(
        &self,
        key: Bytes,
        duration: Duration,
        opts: crate::resp::ExpireOptions,
    ) -> bool {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            let res = self.local_db.borrow_mut().expire(&key, duration, opts);
            if res {
                self.log_mutation(|| Command::Expire {
                    key,
                    duration,
                    opts,
                });
            }
            res
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Expire {
                key,
                duration,
                opts,
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(false)
            } else {
                false
            }
        }
    }

    pub async fn persist(&self, key: Bytes) -> bool {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            let res = self.local_db.borrow_mut().persist(&key);
            if res {
                self.log_mutation(|| Command::Persist(key));
            }
            res
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Persist { key, responder: tx };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(false)
            } else {
                false
            }
        }
    }

    pub async fn sync_aof(&self) {
        let (file, chunk, offset) = if let Some(aof) = &self.aof {
            let mut writer = aof.borrow_mut();
            let file = writer.get_file();
            if let Some((f, c, o)) = writer.take_flush_chunk() {
                (Some(f), c, o)
            } else {
                (file, Vec::new(), 0)
            }
        } else {
            (None, Vec::new(), 0)
        };
        if let Some(file) = file {
            if !chunk.is_empty() {
                let _ = file.write_all_at(chunk, offset).await;
            }
            let _ = file.sync_data().await;
        }
        let mut responders = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::SyncAof { responder: tx };
                if sender.send(msg).is_ok() {
                    responders.push(rx);
                }
            }
        }
        for rx in responders {
            let _ = rx.recv_async().await;
        }
    }

    pub async fn ttl(&self, key: Bytes, in_millis: bool) -> i64 {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            self.local_db.borrow_mut().ttl(&key, in_millis)
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Ttl {
                key,
                in_millis,
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(-2)
            } else {
                -2
            }
        }
    }

    pub async fn count_keys_in_slot(&self, slot: u16) -> usize {
        if self.cluster_enabled || crate::cluster::has_active_cluster(self.port) {
            let target = self.target_shard_for_slot(slot);
            if target == self.shard_id {
                self.local_db.borrow_mut().count_keys_in_slot(slot)
            } else {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::CountKeysInSlot {
                    slot,
                    responder: tx,
                };
                if self.senders[target].send(msg).is_ok() {
                    rx.recv_async().await.unwrap_or(0)
                } else {
                    0
                }
            }
        } else {
            let mut total = self.local_db.borrow_mut().count_keys_in_slot(slot);
            for s in 0..self.num_shards {
                if s == self.shard_id {
                    continue;
                }
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::CountKeysInSlot {
                    slot,
                    responder: tx,
                };
                if self.senders[s].send(msg).is_ok() {
                    total += rx.recv_async().await.unwrap_or(0);
                }
            }
            total
        }
    }

    pub async fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<Bytes> {
        if self.cluster_enabled || crate::cluster::has_active_cluster(self.port) {
            let target = self.target_shard_for_slot(slot);
            if target == self.shard_id {
                self.local_db.borrow_mut().get_keys_in_slot(slot, count)
            } else {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::GetKeysInSlot {
                    slot,
                    count,
                    responder: tx,
                };
                if self.senders[target].send(msg).is_ok() {
                    rx.recv_async().await.unwrap_or_default()
                } else {
                    Vec::new()
                }
            }
        } else {
            let mut results = self.local_db.borrow_mut().get_keys_in_slot(slot, count);
            for s in 0..self.num_shards {
                if results.len() >= count {
                    break;
                }
                if s == self.shard_id {
                    continue;
                }
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::GetKeysInSlot {
                    slot,
                    count: count - results.len(),
                    responder: tx,
                };
                if self.senders[s].send(msg).is_ok()
                    && let Ok(batch) = rx.recv_async().await
                {
                    results.extend(batch);
                }
            }
            results
        }
    }

    pub async fn flush_slots(&self, ranges: &[(u16, u16)]) -> usize {
        let mut total = 0;
        for s in 0..self.num_shards {
            if s == self.shard_id {
                total += self.local_db.borrow_mut().flush_slots(ranges);
            } else {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::FlushSlots {
                    ranges: ranges.to_vec(),
                    responder: tx,
                };
                if self.senders[s].send(msg).is_ok() {
                    total += rx.recv_async().await.unwrap_or(0);
                }
            }
        }
        total
    }

    pub async fn stick(&self, keys: &[Bytes]) -> usize {
        let mut count = 0;
        for key in keys {
            let target = self.target_shard(key);
            if target == self.shard_id {
                if self.local_db.borrow_mut().stick(key.clone()) {
                    count += 1;
                }
            } else {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::Stick {
                    keys: vec![key.clone()],
                    responder: tx,
                };
                if self.senders[target].send(msg).is_ok() {
                    count += rx.recv_async().await.unwrap_or(0);
                }
            }
        }
        count
    }

    pub async fn unstick(&self, keys: &[Bytes]) -> usize {
        let mut count = 0;
        for key in keys {
            let target = self.target_shard(key);
            if target == self.shard_id {
                if self.local_db.borrow_mut().unstick(key) {
                    count += 1;
                }
            } else {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::Unstick {
                    keys: vec![key.clone()],
                    responder: tx,
                };
                if self.senders[target].send(msg).is_ok() {
                    count += rx.recv_async().await.unwrap_or(0);
                }
            }
        }
        count
    }

    pub async fn is_sticky(&self, key: &Bytes) -> bool {
        let target = self.target_shard(key);
        if target == self.shard_id {
            self.local_db.borrow().is_sticky(key)
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::IsSticky {
                key: key.clone(),
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(false)
            } else {
                false
            }
        }
    }

    pub async fn delex(&self, key: Bytes, condition: Option<(String, Bytes)>) -> bool {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            let mut db = self.local_db.borrow_mut();
            let should_del = match condition {
                None => db.exists(&key),
                Some((op, expected)) => {
                    if let Some(val) = db.get(&key) {
                        match op.to_uppercase().as_str() {
                            "IFEQ" => val == expected,
                            "IFNE" => val != expected,
                            "IFDEQ" => {
                                crate::table::compute_digest(&val)
                                    == String::from_utf8_lossy(&expected)
                            }
                            "IFDNE" => {
                                crate::table::compute_digest(&val)
                                    != String::from_utf8_lossy(&expected)
                            }
                            "IFGT" => val > expected,
                            "IFLT" => val < expected,
                            _ => false,
                        }
                    } else {
                        false
                    }
                }
            };
            if should_del { db.del(&key) } else { false }
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Delex {
                key,
                condition: condition.map(Box::new),
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(false)
            } else {
                false
            }
        }
    }

    pub async fn client_list(
        &self,
        local_registry: &RefCell<hashbrown::HashMap<u64, crate::connection::ClientInfo>>,
        filter_ids: &[u64],
    ) -> String {
        let mut out = String::new();
        let now = std::time::Instant::now();
        for client in local_registry.borrow().values() {
            if client
                .stats
                .killed
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                continue;
            }
            if !filter_ids.is_empty() && !filter_ids.contains(&client.id) {
                continue;
            }
            let age = now.duration_since(client.connected_at).as_secs();
            let idle = now.duration_since(client.last_active).as_secs();
            let is_blocked = crate::block::get_block_hub_for_port(self.port)
                .lock()
                .is_blocked(client.id);
            let flags = if client.is_monitor {
                "O"
            } else if client
                .stats
                .is_replica
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                "S"
            } else if is_blocked {
                "b"
            } else {
                "N"
            };
            let (qbuf, qbuf_free) = client.effective_qbuf(idle);
            let tot_net_in = client
                .stats
                .tot_net_in
                .load(std::sync::atomic::Ordering::Relaxed);
            let tot_net_out = client
                .stats
                .tot_net_out
                .load(std::sync::atomic::Ordering::Relaxed);
            let tot_cmds = client
                .stats
                .tot_cmds
                .load(std::sync::atomic::Ordering::Relaxed);
            let read_events = client
                .stats
                .read_events
                .load(std::sync::atomic::Ordering::Relaxed);
            let pipe_sum = client
                .stats
                .pipeline_len_sum
                .load(std::sync::atomic::Ordering::Relaxed);
            let pipe_cnt = client
                .stats
                .pipeline_len_cnt
                .load(std::sync::atomic::Ordering::Relaxed);
            out.push_str(&format!(
                "id={} addr={} laddr=127.0.0.1:{} fd=8 name={} age={} idle={} flags={} db=0 sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf={} qbuf-free={} argv-mem=10 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem={} omem-shared=0 omem-unshared=0 tot-mem=22306 events=r cmd={} user=default redir=-1 resp=2 lib-name={} lib-ver={} io-thread=0 tot-net-in={} tot-net-out={} tot-cmds={} read-events={} avg-pipeline-len-sum={} avg-pipeline-len-cnt={}\n",
                client.id,
                client.addr,
                self.port,
                client.name.as_deref().unwrap_or(""),
                age,
                idle,
                flags,
                qbuf,
                qbuf_free,
                client.omem,
                client.last_cmd.to_lowercase(),
                client.lib_name.as_deref().unwrap_or(""),
                client.lib_ver.as_deref().unwrap_or(""),
                tot_net_in,
                tot_net_out,
                tot_cmds,
                read_events,
                pipe_sum,
                pipe_cnt,
            ));
        }

        for (shard_id, sender) in self.senders.iter().enumerate() {
            if shard_id != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::ClientList {
                    filter_ids: filter_ids.to_vec(),
                    responder: tx,
                };
                if sender.send(msg).is_ok()
                    && let Ok(peer_list) = rx.recv_async().await
                {
                    out.push_str(&peer_list);
                }
            }
        }
        out
    }

    pub async fn reset_command_stats(&self) {
        crate::connection::reset_local_cmd_stats();
        {
            let mut map = crate::connection::CMD_STATS.write();
            map.clear();
        }
        for (shard_id, sender) in self.senders.iter().enumerate() {
            if shard_id != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::ResetCommandStats { responder: tx };
                if sender.send(msg).is_ok() {
                    let _ = rx.recv_async().await;
                }
            }
        }
    }

    pub async fn flush_all_command_stats(&self) {
        crate::connection::flush_local_cmd_stats();
        for (shard_id, sender) in self.senders.iter().enumerate() {
            if shard_id != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::FlushCommandStats { responder: tx };
                if sender.send(msg).is_ok() {
                    let _ = rx.recv_async().await;
                }
            }
        }
    }

    pub async fn execute_remote(&self, target: usize, cmd: Command) -> Vec<u8> {
        let is_resp3 = crate::connection::CURRENT_CLIENT_RESP3.get();
        let responder = self
            .remote_responder_pool
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| std::sync::Arc::new(crate::mailbox::BatchResponder::new()));
        let mut slot = crate::shard::CompactResp::empty();
        responder.prepare(&mut slot as *mut _, 1);
        let h = crate::connection::cmd_primary_key(&cmd)
            .map(|k| crate::table::hash_key(k))
            .unwrap_or(0);
        let msg = ShardMessage::Batch {
            items: vec![(0, h, cmd)],
            responder: responder.clone(),
            is_resp3,
            client_id: crate::connection::requesting_client_id(),
            no_touch: crate::connection::CLIENT_NO_TOUCH.get(),
            defer_bcast: crate::connection::DEFER_BCAST_FLUSH.get(),
        };
        let res = if self.senders[target].send(msg).is_ok() {
            let mut completed = false;
            for _spin in 0..crate::mailbox::cross_shard_spin() {
                if responder.try_take().is_some() {
                    completed = true;
                    break;
                }
                std::hint::spin_loop();
            }
            if !completed {
                let _ = responder.wait_take().await;
            }
            slot.into_vec()
        } else {
            b"-ERR internal shard routing error\r\n".to_vec()
        };
        self.remote_responder_pool.borrow_mut().push(responder);
        res
    }

    /// Runs `cmd` on shard `target` (in place if that is this shard) and
    /// returns the reply.
    pub async fn execute_on_shard(&self, target: usize, cmd: Command) -> Vec<u8> {
        if target == self.shard_id {
            let mut out = Vec::new();
            crate::connection::execute_local_command(
                &cmd,
                &mut self.local_db.borrow_mut(),
                &mut out,
                self.aof.as_deref(),
            );
            out
        } else {
            self.execute_remote(target, cmd).await
        }
    }

    /// Splits a `CRDT.MERGE` payload into one payload per shard that owns
    /// some of its keys, so that each shard merges (and logs) only its own.
    pub fn split_crdt_payload(&self, payload: &[u8]) -> Result<Vec<(usize, Bytes)>, String> {
        let mut parts = vec![Vec::new(); self.num_shards];
        for entry in crate::crdt::decode_sync_payload(payload)? {
            entry.encode(&mut parts[self.target_shard(entry.key())]);
        }
        Ok(parts
            .into_iter()
            .enumerate()
            .filter(|(_, part)| !part.is_empty())
            .map(|(sid, part)| (sid, Bytes::from(part)))
            .collect())
    }

    /// Like [`Self::execute_remote`] for several `(shard, command)` pairs, but
    /// sends them all before waiting, so K remote shards cost one round trip
    /// instead of K. Replies are returned in input order.
    pub async fn execute_remote_many(&self, cmds: Vec<(usize, Command)>) -> Vec<Vec<u8>> {
        self.execute_remote_many_with(cmds, || {}).await
    }

    /// [`Self::execute_remote_many`] that runs `between` on this thread after
    /// every command has been sent and before waiting for the replies, so
    /// local work overlaps with the remote shards'.
    pub async fn execute_remote_many_with(
        &self,
        cmds: Vec<(usize, Command)>,
        between: impl FnOnce(),
    ) -> Vec<Vec<u8>> {
        let is_resp3 = crate::connection::CURRENT_CLIENT_RESP3.get();
        // Remote shards write into these slots through raw pointers: the
        // boxed slice is never resized and outlives every wait below.
        let mut slots: Box<[crate::shard::CompactResp]> = (0..cmds.len())
            .map(|_| crate::shard::CompactResp::empty())
            .collect();
        let mut pending = Vec::with_capacity(cmds.len());
        for (i, (target, cmd)) in cmds.into_iter().enumerate() {
            let responder = self
                .remote_responder_pool
                .borrow_mut()
                .pop()
                .unwrap_or_else(|| std::sync::Arc::new(crate::mailbox::BatchResponder::new()));
            responder.prepare(&mut slots[i] as *mut _, 1);
            let h = crate::connection::cmd_primary_key(&cmd)
                .map(|k| crate::table::hash_key(k))
                .unwrap_or(0);
            let msg = ShardMessage::Batch {
                items: vec![(0, h, cmd)],
                responder: responder.clone(),
                is_resp3,
                client_id: crate::connection::requesting_client_id(),
                no_touch: crate::connection::CLIENT_NO_TOUCH.get(),
                defer_bcast: crate::connection::DEFER_BCAST_FLUSH.get(),
            };
            let sent = self.senders[target].send(msg).is_ok();
            pending.push((responder, sent));
        }
        between();
        for (responder, sent) in &pending {
            if *sent {
                let _ = responder.wait_take().await;
            }
        }
        let out = slots
            .into_vec()
            .into_iter()
            .zip(&pending)
            .map(|(slot, (_, sent))| {
                if *sent {
                    slot.into_vec()
                } else {
                    b"-ERR internal shard routing error\r\n".to_vec()
                }
            })
            .collect();
        let mut pool = self.remote_responder_pool.borrow_mut();
        for (responder, sent) in pending {
            if sent {
                pool.push(responder);
            }
        }
        out
    }

    /// Splits `keys` into the ones this shard owns and one batch per other
    /// shard, keeping duplicates (EXISTS counts them).
    pub fn group_keys_by_shard(&self, keys: &[Bytes]) -> (Vec<Bytes>, Vec<(usize, Vec<Bytes>)>) {
        let mut local = Vec::new();
        let mut remote: Vec<Vec<Bytes>> = (0..self.num_shards).map(|_| Vec::new()).collect();
        for key in keys {
            let target = self.target_shard(key);
            if target == self.shard_id {
                local.push(key.clone());
            } else {
                remote[target].push(key.clone());
            }
        }
        let remote = remote
            .into_iter()
            .enumerate()
            .filter(|(_, ks)| !ks.is_empty())
            .collect();
        (local, remote)
    }

    /// Commands for every shard but this one, for fan-outs that go through
    /// [`Self::execute_remote_many`] and so cost one round trip in total.
    fn to_other_shards(&self, cmd: impl Fn() -> Command) -> Vec<(usize, Command)> {
        (0..self.num_shards)
            .filter(|&sid| sid != self.shard_id)
            .map(|sid| (sid, cmd()))
            .collect()
    }

    pub async fn dbsize(&self) -> usize {
        let mut total = self.local_db.borrow_mut().dbsize();
        for res in self
            .execute_remote_many(self.to_other_shards(|| Command::Dbsize))
            .await
        {
            if let Ok(s) = std::str::from_utf8(&res)
                && let Some(num_str) = s.strip_prefix(':').and_then(|x| x.split("\r\n").next())
                && let Ok(n) = num_str.parse::<usize>()
            {
                total += n;
            }
        }
        total
    }

    /// Server-wide `(keys, keys with a TTL)` for `INFO keyspace`.
    pub async fn keyspace_stats(&self) -> (usize, usize) {
        let (mut keys, mut expires) = {
            let mut db = self.local_db.borrow_mut();
            let k = db.dbsize();
            (k, db.table.num_expires.min(k))
        };
        for res in self
            .execute_remote_many(self.to_other_shards(|| Command::KeyspaceStats))
            .await
        {
            let mut nums = res
                .split(|&b| b == b'\n')
                .filter_map(|l| l.strip_prefix(b":"))
                .filter_map(|l| {
                    std::str::from_utf8(l)
                        .ok()?
                        .trim_end()
                        .parse::<usize>()
                        .ok()
                });
            keys += nums.next().unwrap_or(0);
            expires += nums.next().unwrap_or(0);
        }
        (keys, expires)
    }

    /// `DEBUG DIGEST`: XOR of every shard's key digests (see
    /// [`crate::digest`]), 20 zero bytes for an empty dataset.
    pub async fn debug_digest(&self) -> crate::digest::Digest20 {
        let mut parts = vec![crate::digest::shard_digest(&self.local_db.borrow())];
        let cmd = || Command::Debug(vec![Bytes::from_static(DEBUG_DIGEST_SHARD)]);
        for res in self.execute_remote_many(self.to_other_shards(cmd)).await {
            // A bulk string `$<len>\r\n<hex>:<count>\r\n`.
            let part = res
                .strip_prefix(b"$")
                .and_then(|r| {
                    let start = r.windows(2).position(|w| w == b"\r\n")? + 2;
                    let body = r.get(start..)?;
                    body.strip_suffix(b"\r\n")
                })
                .and_then(crate::digest::decode_shard_part);
            match part {
                Some(p) => parts.push(p),
                None => {
                    // A shard that could not answer must not make the
                    // digest look like a smaller, valid dataset.
                    let mut poisoned = crate::digest::ZERO;
                    crate::digest::mix_digest(&mut poisoned, &res);
                    parts.push((poisoned, 1));
                }
            }
        }
        crate::digest::combine_shards(parts)
    }

    /// `DEBUG DIGEST-VALUE`: per-key value digests, in `keys` order.
    pub async fn debug_digest_values(&self, keys: &[Bytes]) -> Vec<crate::digest::Digest20> {
        let mut out = vec![crate::digest::ZERO; keys.len()];
        let mut remote = Vec::new();
        let mut remote_idx = Vec::new();
        {
            let db = self.local_db.borrow();
            for (i, key) in keys.iter().enumerate() {
                let target = self.target_shard(key);
                if target == self.shard_id {
                    out[i] = crate::digest::key_value_digest(&db, key);
                } else {
                    remote.push((
                        target,
                        Command::Debug(vec![Bytes::from_static(b"digest-value"), key.clone()]),
                    ));
                    remote_idx.push(i);
                }
            }
        }
        for (res, i) in self
            .execute_remote_many(remote)
            .await
            .into_iter()
            .zip(remote_idx)
        {
            // `*1\r\n+<hex>\r\n`
            out[i] = res
                .iter()
                .position(|&b| b == b'+')
                .and_then(|p| res.get(p + 1..p + 41))
                .and_then(crate::digest::from_hex)
                .unwrap_or_else(|| {
                    let mut poisoned = crate::digest::ZERO;
                    crate::digest::mix_digest(&mut poisoned, &res);
                    poisoned
                });
        }
        out
    }

    /// `DEBUG SLEEP`: stalls every shard thread for `dur` at once, as
    /// Redis' single thread does, so no shard serves anything meanwhile.
    pub async fn debug_sleep_all(&self, dur: Duration, secs_arg: Bytes) {
        let cmd = || Command::Debug(vec![Bytes::from_static(b"sleep"), secs_arg.clone()]);
        self.execute_remote_many_with(self.to_other_shards(cmd), || std::thread::sleep(dur))
            .await;
    }

    /// `DEBUG RELOAD`: saves the RDB (unless `save` is false), then replaces
    /// the dataset with the file's contents.
    pub async fn debug_reload(&self, save: bool) -> Result<(), String> {
        if save {
            // Let an in-flight BGSAVE finish rather than fail on "in progress".
            while self.is_saving.load(Ordering::SeqCst) {
                monoio::time::sleep(Duration::from_millis(10)).await;
            }
            self.save_rdb().await?;
        }
        let path = self.db_dir.join(crate::config::dbfilename(self.base_port));
        let data = std::fs::read(&path).map_err(|e| format!("{}: {}", path.display(), e))?;
        self.restore_rdb_bytes(Bytes::from(data)).await
    }

    /// `DEBUG LOADAOF`: flushes every shard's AOF buffer to disk, then each
    /// shard empties its data and replays its AOF file. Nothing replayed is
    /// propagated to replicas or appended to the AOF again.
    pub async fn debug_loadaof(&self) -> Result<(), String> {
        if self.aof.is_none() {
            // Nothing to load from; Redis would leave an empty dataset, but
            // dropping everything with no AOF to restore it is never wanted.
            return Ok(());
        }
        self.sync_aof().await;
        let cmd = || Command::Debug(vec![Bytes::from_static(DEBUG_LOADAOF_SHARD)]);
        let mut replies = vec![self.execute_on_shard(self.shard_id, cmd()).await];
        replies.extend(self.execute_remote_many(self.to_other_shards(cmd)).await);
        let errors: Vec<String> = replies
            .iter()
            .filter(|r| r.first() == Some(&b'-'))
            .map(|r| String::from_utf8_lossy(&r[1..]).trim_end().to_string())
            .collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    /// `DEBUG POPULATE count [prefix] [size]`: creates the missing string
    /// keys `prefix:0` .. `prefix:<count-1>` with values `value:<n>`
    /// (padded with zero bytes or cut to `size` when it is given). Each
    /// shard builds its own keys from their numbers, in bounded rounds, so
    /// a large count never materializes in one message.
    pub async fn debug_populate(&self, count: u64, prefix: Bytes, size: Option<u64>) {
        const ROUND: u64 = 16 * 1024;
        let size_arg = Bytes::from(size.map_or_else(String::new, |s| s.to_string()));
        let mut start = 0u64;
        while start < count {
            let end = count.min(start + ROUND);
            let mut per_shard: Vec<Vec<Bytes>> = vec![Vec::new(); self.num_shards];
            let mut key = Vec::with_capacity(prefix.len() + 21);
            for j in start..end {
                key.clear();
                key.extend_from_slice(&prefix);
                key.push(b':');
                key.extend_from_slice(j.to_string().as_bytes());
                per_shard[self.target_shard(&key)].push(Bytes::from(j.to_string()));
            }
            let mut cmds = Vec::new();
            for (sid, nums) in per_shard.into_iter().enumerate() {
                if nums.is_empty() {
                    continue;
                }
                let mut args = Vec::with_capacity(nums.len() + 3);
                args.push(Bytes::from_static(DEBUG_POPULATE_SHARD));
                args.push(prefix.clone());
                args.push(size_arg.clone());
                args.extend(nums);
                cmds.push((sid, Command::Debug(args)));
            }
            let (local, remote): (Vec<_>, Vec<_>) =
                cmds.into_iter().partition(|(sid, _)| *sid == self.shard_id);
            for (sid, cmd) in local {
                self.execute_on_shard(sid, cmd).await;
            }
            self.execute_remote_many(remote).await;
            start = end;
        }
    }

    pub async fn flushdb(&self) {
        {
            let mut db = self.local_db.borrow_mut();
            db.flushdb();
            crate::block::get_block_hub_for_port(self.port)
                .lock()
                .notify_all_streams(&mut db);
        }
        self.log_mutation(|| Command::Flushdb);
        let _ = self
            .execute_remote_many(self.to_other_shards(|| Command::Flushdb))
            .await;
    }

    pub async fn publish(&self, channel: Bytes, message: Bytes) -> usize {
        let mut total = self.pubsub.borrow().publish(&channel, &message);
        let mut pending = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id && self.presence_table.is_shard_interested(sid, &channel) {
                let (tx, rx) = self.acquire_pubsub_responder();
                let msg = ShardMessage::Publish {
                    channel: channel.clone(),
                    message: message.clone(),
                    responder: tx.clone(),
                };
                if sender.send(msg).is_ok() {
                    pending.push((tx, rx));
                } else {
                    self.release_pubsub_responder(tx, rx);
                }
            }
        }
        for (tx, rx) in pending {
            if let Ok(count) = rx.recv_async().await {
                total += count;
            }
            self.release_pubsub_responder(tx, rx);
        }
        total
    }

    pub async fn pubsub_channels(&self, pattern: Option<Bytes>) -> Vec<Bytes> {
        let mut set = hashbrown::HashSet::new();
        for ch in self.pubsub.borrow().channels(pattern.as_deref()) {
            set.insert(ch);
        }
        let mut pending = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::PubsubChannels {
                    pattern: pattern.clone(),
                    responder: tx,
                };
                if sender.send(msg).is_ok() {
                    pending.push(rx);
                }
            }
        }
        for rx in pending {
            if let Ok(channels) = rx.recv_async().await {
                for ch in channels {
                    set.insert(ch);
                }
            }
        }
        let mut list: Vec<Bytes> = set.into_iter().collect();
        list.sort();
        list
    }

    pub async fn pubsub_numsub(&self, channels: Vec<Bytes>) -> Vec<(Bytes, usize)> {
        let mut counts: hashbrown::HashMap<Bytes, usize> = hashbrown::HashMap::new();
        {
            let hub = self.pubsub.borrow();
            for ch in &channels {
                counts.insert(ch.clone(), hub.numsub(ch));
            }
        }
        let mut pending = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::PubsubNumsub {
                    channels: channels.clone(),
                    responder: tx,
                };
                if sender.send(msg).is_ok() {
                    pending.push(rx);
                }
            }
        }
        for rx in pending {
            if let Ok(shard_counts) = rx.recv_async().await {
                for (ch, cnt) in shard_counts {
                    *counts.entry(ch).or_default() += cnt;
                }
            }
        }
        channels
            .into_iter()
            .map(|ch| {
                let cnt = counts.get(&ch).copied().unwrap_or(0);
                (ch, cnt)
            })
            .collect()
    }

    pub async fn pubsub_numpat(&self) -> usize {
        let mut unique: hashbrown::HashSet<Bytes> =
            self.pubsub.borrow().patterns.keys().cloned().collect();
        let mut pending = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::PubsubNumpat { responder: tx };
                if sender.send(msg).is_ok() {
                    pending.push(rx);
                }
            }
        }
        for rx in pending {
            if let Ok(pats) = rx.recv_async().await {
                for p in pats {
                    unique.insert(p);
                }
            }
        }
        unique.len()
    }

    pub async fn spublish(&self, channel: Bytes, message: Bytes) -> usize {
        let slot = key_slot(&channel);
        let target = slot_to_shard(slot, self.num_shards);
        if target == self.shard_id {
            self.pubsub.borrow().spublish(&channel, &message)
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Spublish {
                channel,
                message,
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(0)
            } else {
                0
            }
        }
    }

    pub async fn ssubscribe(
        &self,
        client_id: u64,
        channel: Bytes,
        tx: flume::Sender<Bytes>,
        is_resp3: bool,
    ) -> usize {
        let slot = key_slot(&channel);
        let target = slot_to_shard(slot, self.num_shards);
        if target == self.shard_id {
            self.pubsub
                .borrow_mut()
                .ssubscribe(client_id, channel, tx, is_resp3)
        } else {
            let (resp_tx, resp_rx) = flume::bounded(1);
            let msg = ShardMessage::Ssubscribe {
                client_id,
                channel: channel.clone(),
                sender: tx,
                is_resp3,
                responder: resp_tx,
            };
            if self.senders[target].send(msg).is_ok() {
                let _ = resp_rx.recv_async().await;
            }
            let mut hub = self.pubsub.borrow_mut();
            hub.client_shard_channels
                .entry(client_id)
                .or_default()
                .insert(channel);
            hub.total_subscriptions(client_id)
        }
    }

    pub async fn sunsubscribe(&self, client_id: u64, channel: Bytes) -> usize {
        let slot = key_slot(&channel);
        let target = slot_to_shard(slot, self.num_shards);
        if target == self.shard_id {
            self.pubsub.borrow_mut().sunsubscribe(client_id, &channel)
        } else {
            let (resp_tx, resp_rx) = flume::bounded(1);
            let msg = ShardMessage::Sunsubscribe {
                client_id,
                channel: channel.clone(),
                responder: resp_tx,
            };
            if self.senders[target].send(msg).is_ok() {
                let _ = resp_rx.recv_async().await;
            }
            let mut hub = self.pubsub.borrow_mut();
            if let Some(ch_set) = hub.client_shard_channels.get_mut(&client_id) {
                ch_set.remove(&channel);
                if ch_set.is_empty() {
                    hub.client_shard_channels.remove(&client_id);
                }
            }
            hub.total_subscriptions(client_id)
        }
    }

    pub async fn sunsubscribe_all(&self, client_id: u64) -> Vec<(Bytes, usize)> {
        let channels: Vec<Bytes> = self
            .pubsub
            .borrow()
            .client_shard_channels
            .get(&client_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        let mut res = Vec::new();
        for ch in channels {
            let remaining = self.sunsubscribe(client_id, ch.clone()).await;
            res.push((ch, remaining));
        }
        res
    }

    pub async fn pubsub_shardchannels(&self, pattern: Option<Bytes>) -> Vec<Bytes> {
        let mut set = hashbrown::HashSet::new();
        for ch in self.pubsub.borrow().shard_channels(pattern.as_deref()) {
            set.insert(ch);
        }
        let mut pending = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::PubsubShardchannels {
                    pattern: pattern.clone(),
                    responder: tx,
                };
                if sender.send(msg).is_ok() {
                    pending.push(rx);
                }
            }
        }
        for rx in pending {
            if let Ok(channels) = rx.recv_async().await {
                for ch in channels {
                    set.insert(ch);
                }
            }
        }
        let mut list: Vec<Bytes> = set.into_iter().collect();
        list.sort();
        list
    }

    pub async fn pubsub_shardnumsub(&self, channels: Vec<Bytes>) -> Vec<(Bytes, usize)> {
        let mut counts: hashbrown::HashMap<Bytes, usize> = hashbrown::HashMap::new();
        let mut shard_channels: hashbrown::HashMap<usize, Vec<Bytes>> = hashbrown::HashMap::new();
        for ch in &channels {
            let slot = key_slot(ch);
            let target = slot_to_shard(slot, self.num_shards);
            if target == self.shard_id {
                counts.insert(ch.clone(), self.pubsub.borrow().shard_numsub(ch));
            } else {
                shard_channels.entry(target).or_default().push(ch.clone());
            }
        }
        let mut pending = Vec::new();
        for (target, chs) in shard_channels {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::PubsubShardnumsub {
                channels: chs,
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                pending.push(rx);
            }
        }
        for rx in pending {
            if let Ok(shard_counts) = rx.recv_async().await {
                for (ch, cnt) in shard_counts {
                    counts.insert(ch, cnt);
                }
            }
        }
        channels
            .into_iter()
            .map(|ch| {
                let cnt = counts.get(&ch).copied().unwrap_or(0);
                (ch, cnt)
            })
            .collect()
    }

    pub fn has_search_index(&self, name: &str) -> bool {
        self.local_db.borrow().search_indices.contains_key(name)
            || crate::search::get_search_index(name).is_some()
    }

    pub async fn create_search_index(
        &self,
        schema: crate::search::IndexSchema,
    ) -> Result<(), String> {
        let name = schema.name.clone();
        if self.has_search_index(&name) {
            return Err(format!("Index already exists: {}", name));
        }

        // Register in global search registry first so per-shard backfill also populates the mirror
        let _ = crate::search::create_search_index(schema.clone());

        // Initialize and backfill on local shard
        self.local_db.borrow_mut().init_search_index(schema.clone());

        // Broadcast to all other shards
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::InitSearchIndex {
                    schema: Box::new(schema.clone()),
                    responder: tx,
                };
                if sender.send(msg).is_ok() {
                    let _ = rx.recv_async().await;
                }
            }
        }

        Ok(())
    }

    pub async fn drop_search_index(&self, name: &str) -> Result<(), String> {
        let mut any_dropped = self.local_db.borrow_mut().drop_search_index(name);

        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::DropSearchIndex {
                    name: name.to_string(),
                    responder: tx,
                };
                if sender.send(msg).is_ok()
                    && let Ok(dropped) = rx.recv_async().await
                {
                    any_dropped = any_dropped || dropped;
                }
            }
        }

        let _ = crate::search::drop_search_index(name);

        if any_dropped {
            Ok(())
        } else {
            Err(format!("Unknown Index name: {}", name))
        }
    }

    pub async fn alter_search_index(
        &self,
        index: &str,
        new_fields: std::collections::HashMap<String, crate::search::FieldType>,
        new_schema_fields: Vec<crate::search::SchemaField>,
    ) -> Result<(), String> {
        let mut schema = {
            let db = self.local_db.borrow();
            if let Some(idx) = db.search_indices.get(index)
                && let Some(s) = &idx.schema
            {
                s.clone()
            } else if let Some(idx_arc) = crate::search::get_search_index(index) {
                let idx = idx_arc.read();
                idx.schema
                    .clone()
                    .ok_or_else(|| format!("Unknown Index name: {}", index))?
            } else {
                return Err(format!("Unknown Index name: {}", index));
            }
        };

        for (k, v) in new_fields {
            schema.fields.insert(k, v);
        }
        for sf in new_schema_fields {
            if let Some(existing) = schema
                .schema_fields
                .iter_mut()
                .find(|e| e.alias == sf.alias || e.identifier == sf.identifier)
            {
                *existing = sf;
            } else {
                schema.schema_fields.push(sf);
            }
        }

        crate::search::reset_search_index(schema.clone());
        self.local_db.borrow_mut().init_search_index(schema.clone());

        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::InitSearchIndex {
                    schema: Box::new(schema.clone()),
                    responder: tx,
                };
                if sender.send(msg).is_ok() {
                    let _ = rx.recv_async().await;
                }
            }
        }

        Ok(())
    }

    pub async fn ft_search(
        &self,
        index: &str,
        ast: &crate::search::QueryAst,
        opts: &crate::search::SearchOptions,
    ) -> (usize, Vec<crate::search::SearchHit>) {
        if (opts.rrf_k.is_some() || opts.linear_weights.is_some())
            && let Some((bm25_ast, knn_ast)) = ast.as_hybrid_rrf()
        {
            let knn_limit = knn_ast.knn_k(opts).unwrap_or(10);
            let sub_opts = crate::search::SearchOptions {
                offset: 0,
                limit: opts
                    .offset
                    .saturating_add(opts.limit)
                    .max(knn_limit)
                    .max(100),
                rrf_k: None,
                linear_weights: None,
                ..opts.clone()
            };
            let (_bm25_total, bm25_hits) =
                Box::pin(self.ft_search(index, &bm25_ast, &sub_opts)).await;
            let (_knn_total, vector_hits) =
                Box::pin(self.ft_search(index, &knn_ast, &sub_opts)).await;
            let fused = if let Some((alpha, beta)) = opts.linear_weights {
                crate::search::linear_score_fusion(&bm25_hits, &vector_hits, alpha, beta)
            } else {
                crate::search::reciprocal_rank_fusion(
                    &bm25_hits,
                    &vector_hits,
                    opts.rrf_k.unwrap_or(60.0),
                )
            };
            let total = fused.len();
            let paged = fused
                .into_iter()
                .skip(opts.offset)
                .take(opts.limit)
                .collect();
            return (total, paged);
        }

        let scatter_opts = crate::search::SearchOptions {
            offset: 0,
            limit: opts.offset.saturating_add(opts.limit),
            ..opts.clone()
        };

        let (local_total, local_hits) = {
            let db = self.local_db.borrow();
            if let Some(idx) = db.search_indices.get(index) {
                crate::search::execute_search(idx, ast, &scatter_opts)
            } else if let Some(idx_arc) = crate::search::get_search_index(index) {
                let idx = idx_arc.read();
                crate::search::execute_search(&idx, ast, &scatter_opts)
            } else {
                (0, Vec::new())
            }
        };

        let mut all_totals = local_total;
        let mut all_hits = local_hits;

        if self.num_shards > 1 {
            let mut pending = Vec::new();
            for (sid, sender) in self.senders.iter().enumerate() {
                if sid != self.shard_id {
                    let (tx, rx) = flume::bounded(1);
                    let msg = ShardMessage::SearchQuery {
                        index: index.to_string(),
                        ast: Box::new(ast.clone()),
                        options: Box::new(scatter_opts.clone()),
                        responder: tx,
                    };
                    if sender.send(msg).is_ok() {
                        pending.push(rx);
                    }
                }
            }

            for rx in pending {
                if let Ok((remote_total, remote_hits)) = rx.recv_async().await {
                    all_totals += remote_total;
                    all_hits.extend(remote_hits);
                }
            }
        }

        // Sort combined hits from all shards
        if let Some((_sort_field, asc)) = &opts.sortby {
            if *asc {
                all_hits.sort_by(|a, b| {
                    a.sort_val
                        .partial_cmp(&b.sort_val)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            } else {
                all_hits.sort_by(|a, b| {
                    b.sort_val
                        .partial_cmp(&a.sort_val)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        } else {
            all_hits.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
        }

        let effective_limit = if let Some(k) = ast.knn_k(opts) {
            opts.limit.min(k)
        } else {
            opts.limit
        };

        let reported_total = if let Some(k) = ast.knn_k(opts) {
            all_totals.min(k)
        } else {
            all_totals
        };

        let paged: Vec<crate::search::SearchHit> = all_hits
            .into_iter()
            .skip(opts.offset)
            .take(effective_limit)
            .collect();

        (reported_total, paged)
    }

    pub async fn ft_aggregate(
        &self,
        index: &str,
        query: &str,
        options: crate::search::AggregateOptions,
    ) -> Vec<crate::search::AggregateRow> {
        let ast = crate::search::parse_query(query);
        let search_opts = crate::search::SearchOptions {
            limit: 100_000,
            offset: 0,
            nocontent: false,
            return_fields: if options.load_fields.is_empty() {
                None
            } else {
                Some(options.load_fields.clone())
            },
            sortby: None,
            ..Default::default()
        };
        let (_total, hits) = self.ft_search(index, &ast, &search_opts).await;
        crate::search::execute_aggregate_pipeline(hits, &options)
    }

    pub async fn keys(&self, pattern: &[u8]) -> Vec<Bytes> {
        let mut all_keys = self.local_db.borrow_mut().keys(pattern);
        let mut pending = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::Keys {
                    pattern: Bytes::copy_from_slice(pattern),
                    responder: tx,
                };
                if sender.send(msg).is_ok() {
                    pending.push(rx);
                }
            }
        }
        for rx in pending {
            if let Ok(shard_keys) = rx.recv_async().await {
                all_keys.extend(shard_keys);
            }
        }
        all_keys
    }

    pub async fn scan(
        &self,
        cursor: u64,
        pattern: Option<&[u8]>,
        count: usize,
        key_type: Option<&[u8]>,
    ) -> (u64, Vec<Bytes>) {
        let mut shard_id = (cursor >> 32) as usize;
        let mut slot_idx = (cursor & 0xFFFF_FFFF) as usize;
        if shard_id >= self.num_shards {
            return (0, Vec::new());
        }

        let mut all_keys = Vec::new();
        while shard_id < self.num_shards {
            let needed = count.saturating_sub(all_keys.len()).max(1);
            let (next_slot, keys) = if shard_id == self.shard_id {
                self.local_db
                    .borrow_mut()
                    .scan(slot_idx, pattern, needed, key_type)
            } else {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::Scan {
                    params: Box::new(crate::shard::ScanParams {
                        slot: slot_idx,
                        pattern: pattern.map(Bytes::copy_from_slice),
                        count: needed,
                        key_type: key_type.map(Bytes::copy_from_slice),
                    }),
                    responder: tx,
                };
                if self.senders[shard_id].send(msg).is_ok() {
                    rx.recv_async().await.unwrap_or((0, Vec::new()))
                } else {
                    (0, Vec::new())
                }
            };

            all_keys.extend(keys);

            if next_slot == 0 {
                shard_id += 1;
                slot_idx = 0;
                if all_keys.len() >= count || shard_id >= self.num_shards {
                    let next_cursor = if shard_id >= self.num_shards {
                        0
                    } else {
                        (shard_id as u64) << 32
                    };
                    return (next_cursor, all_keys);
                }
            } else {
                let next_cursor = ((shard_id as u64) << 32) | (next_slot as u64);
                return (next_cursor, all_keys);
            }
        }

        (0, all_keys)
    }

    pub async fn random_key(&self) -> Option<Bytes> {
        let num_shards = self.senders.len();
        if num_shards <= 1 {
            return self.local_db.borrow_mut().random_key();
        }
        let start = self.local_db.borrow_mut().next_rand() % num_shards;
        for i in 0..num_shards {
            let sid = (start + i) % num_shards;
            if sid == self.shard_id {
                if let Some(k) = self.local_db.borrow_mut().random_key() {
                    return Some(k);
                }
            } else {
                let (tx, rx) = flume::bounded(1);
                if self.senders[sid]
                    .send(ShardMessage::RandomKey { responder: tx })
                    .is_ok()
                    && let Ok(Some(k)) = rx.recv_async().await
                {
                    return Some(k);
                }
            }
        }
        None
    }

    pub async fn expiretime(&self, key: Bytes, in_millis: bool) -> i64 {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            self.local_db.borrow_mut().expiretime(&key, in_millis)
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::ExpireTime {
                key,
                in_millis,
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(-2)
            } else {
                -2
            }
        }
    }

    pub async fn acquire_tx_locks(&self, shard_ids: &[usize], tx_id: u64) {
        for &sid in shard_ids {
            if sid == self.shard_id {
                let rx = {
                    let mut lock = self.tx_lock.borrow_mut();
                    if lock.is_none() {
                        *lock = Some(tx_id);
                        None
                    } else {
                        let (tx, rx) = flume::bounded(1);
                        self.tx_waiters.borrow_mut().push_back((tx_id, tx));
                        Some(rx)
                    }
                };
                if let Some(rx) = rx {
                    let _ = rx.recv_async().await;
                }
            } else {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::AcquireTxLock {
                    tx_id,
                    responder: tx,
                };
                if self.senders[sid].send(msg).is_ok() {
                    let _ = rx.recv_async().await;
                }
            }
        }
    }

    pub async fn release_tx_locks(&self, shard_ids: &[usize], tx_id: u64) {
        self.release_tx_locks_now(shard_ids, tx_id);
    }

    /// [`Router::release_tx_locks`] without the `async`; it never waits, so
    /// it can also run from a drop guard. Releasing a lock `tx_id` does not
    /// hold is a no-op.
    pub fn release_tx_locks_now(&self, shard_ids: &[usize], tx_id: u64) {
        for &sid in shard_ids.iter().rev() {
            if sid == self.shard_id {
                let mut lock = self.tx_lock.borrow_mut();
                if *lock == Some(tx_id) {
                    *lock = grant_next_tx_lock(&mut self.tx_waiters.borrow_mut());
                }
            } else {
                let _ = self.senders[sid].send(ShardMessage::ReleaseTxLock { tx_id });
            }
        }
    }

    /// Unix time of the last successful save, shared by all shards.
    pub fn lastsave(&self) -> u64 {
        crate::snapshot::state(self.base_port).last_save_unix()
    }

    pub async fn save_rdb(&self) -> Result<(), String> {
        if self
            .is_saving
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err("Background save already in progress".to_string());
        }
        let dirty_before = crate::snapshot::state(self.base_port).begin();
        let guard = SaveFlagGuard::new(self, Some(dirty_before));
        self.sync_aof().await;
        let res = self.perform_save_rdb(dirty_before).await;
        guard.disarm();
        res
    }

    /// The save step of `SHUTDOWN [SAVE|NOSAVE]` and SIGTERM. Returns an
    /// error if a snapshot was required and failed; callers must then keep
    /// running rather than exit and lose the data, as Redis does.
    pub async fn save_before_shutdown(&self, save: Option<bool>) -> Result<(), String> {
        let has_points = !crate::config::save_points(self.base_port).is_empty();
        if !crate::config::should_save_on_shutdown(save, has_points) {
            return Ok(());
        }
        // Let an in-flight BGSAVE finish rather than fail on "in progress".
        while self.is_saving.load(Ordering::SeqCst) {
            monoio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        crate::log_notice!("Saving the final RDB snapshot before exiting.");
        // `perform_save_rdb` logs "DB saved on disk".
        self.save_rdb().await?;
        Ok(())
    }

    pub async fn bgsave(&self) -> Result<(), String> {
        if self
            .is_saving
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err("Background save already in progress".to_string());
        }
        // Begin before replying so INFO right after +OK shows the save.
        let dirty_before = crate::snapshot::state(self.base_port).begin();
        let router_clone = self.clone();
        crate::log_notice!("Background saving started");
        monoio::spawn(async move {
            let guard = SaveFlagGuard::new(&router_clone, Some(dirty_before));
            router_clone.sync_aof().await;
            match router_clone.perform_save_rdb(dirty_before).await {
                Ok(()) => crate::log_notice!("Background saving terminated with success"),
                Err(_) => crate::log_warning!("Background saving error"),
            }
            guard.disarm();
        });
        Ok(())
    }

    pub async fn bgrewriteaof(&self) -> Result<(), String> {
        if self
            .is_saving
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err("Background save or rewrite already in progress".to_string());
        }
        // Begin before replying so INFO right after +OK shows the rewrite
        // instead of the previous one's status.
        crate::snapshot::aof_rewrite_state(self.base_port).begin();
        crate::log_notice!("Background append only file rewriting started");
        let router_clone = self.clone();
        monoio::spawn(async move {
            let guard = SaveFlagGuard::new(&router_clone, None);
            let _ = router_clone.perform_rewrite_aof().await;
            guard.disarm();
        });
        Ok(())
    }

    /// Rewrites every shard's AOF. Clears `is_saving` (set by
    /// `bgrewriteaof`) however it ends, and records the outcome for INFO.
    /// The flag is cleared first: once INFO shows the rewrite finished, a
    /// new BGREWRITEAOF must be accepted.
    pub async fn perform_rewrite_aof(&self) -> Result<usize, String> {
        let res = self.rewrite_all_shard_aofs().await;
        match &res {
            Ok(_) => {
                crate::log_notice!("Background AOF rewrite terminated with success");
                crate::log_notice!("Background AOF rewrite finished successfully");
            }
            Err(e) => crate::log_warning!("[Shard {}] BGREWRITEAOF failed: {}", self.shard_id, e),
        }
        self.is_saving.store(false, Ordering::SeqCst);
        crate::snapshot::aof_rewrite_state(self.base_port).finish(res.is_ok());
        res
    }

    async fn rewrite_all_shard_aofs(&self) -> Result<usize, String> {
        self.sync_aof().await;
        let mut total_rewritten = {
            let mut db = self.local_db.borrow_mut();
            crate::aof::rewrite_and_swap_shard_aof(
                &mut db,
                &self.db_dir,
                self.shard_id,
                self.aof.as_ref(),
            )
            .map_err(|e| format!("shard {}: {}", self.shard_id, e))?
        };

        // Remote shards rewritten sequentially one-by-one to eliminate concurrent 15-shard I/O and memory spikes
        let mut errors = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let sent = sender
                    .send(ShardMessage::RewriteAof {
                        dir: self.db_dir.clone(),
                        shard_id: sid,
                        responder: tx,
                    })
                    .is_ok();
                match rx.recv_async().await {
                    Ok(Ok(count)) if sent => total_rewritten += count,
                    Ok(Err(e)) => errors.push(format!("shard {}: {}", sid, e)),
                    _ => errors.push(format!("shard {}: no reply", sid)),
                }
            }
        }
        if errors.is_empty() {
            Ok(total_rewritten)
        } else {
            Err(errors.join("; "))
        }
    }

    /// Serializes every shard into one RDB. With `arm_replica`, each shard
    /// arms that replica's stream cut right after serializing (see
    /// `replication::FullSyncCut`).
    pub async fn generate_full_rdb(&self, arm_replica: Option<u64>) -> Vec<u8> {
        let mut full_rdb = Vec::new();
        let used_mem = self.local_db.borrow().table.used_memory() as u64;
        crate::redis_rdb::write_file_header(&mut full_rdb, used_mem);

        // Local shard chunk
        self.local_db.borrow_mut().save_rdb_chunk(&mut full_rdb);
        if let Some(id) = arm_replica {
            crate::replication::get_replication_hub(self.port).arm_full_sync(id, self.shard_id);
        }

        // Remote shard chunks
        let mut responders = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                if sender
                    .send(ShardMessage::SaveRdbChunk {
                        responder: tx,
                        arm_replica,
                    })
                    .is_ok()
                {
                    responders.push(rx);
                }
            }
        }
        for rx in responders {
            if let Ok(chunk) = rx.recv_async().await {
                full_rdb.extend_from_slice(&chunk);
            }
        }

        full_rdb.push(0xFF);
        let crc = crate::table::crc64(&full_rdb);
        full_rdb.extend_from_slice(&crc.to_le_bytes());
        full_rdb
    }

    /// Replaces the whole dataset with a master's full-sync RDB. If any
    /// shard fails to load it, every shard is left empty and the error is
    /// returned, so nothing half loaded is served as in sync.
    pub async fn restore_rdb_bytes(&self, data: Bytes) -> Result<(), String> {
        let result = self.load_full_sync_rdb_on_all_shards(data).await;
        if result.is_err() {
            // Shards that did load drop their part too.
            let _ = self.load_full_sync_rdb_on_all_shards(Bytes::new()).await;
        }
        result
    }

    async fn load_full_sync_rdb_on_all_shards(&self, data: Bytes) -> Result<(), String> {
        let aof_dir = self.aof.as_ref().map(|_| self.db_dir.clone());
        let mut responders = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                sender
                    .send(ShardMessage::RestoreRdbChunk {
                        data: data.clone(),
                        aof_dir: aof_dir.clone(),
                        responder: tx,
                    })
                    .map_err(|_| format!("shard {sid} is gone"))?;
                responders.push(rx);
            }
        }
        let local_aof = aof_dir.as_deref().zip(self.aof.as_ref());
        let mut result = self.local_db.borrow_mut().load_full_sync_rdb(
            &data,
            self.shard_id,
            self.num_shards,
            local_aof,
        );
        for rx in responders {
            let shard_result = rx
                .recv_async()
                .await
                .unwrap_or_else(|_| Err("shard dropped the load".to_string()));
            result = result.and(shard_result);
        }
        result
    }

    /// Applies a replicated command, deferring commands for other shards
    /// into `batch` (one Vec per shard) until `flush_replica_batch`. Each
    /// shard still applies its commands in stream order; commands that span
    /// shards flush the batch first, so they see every earlier change.
    pub async fn apply_replica_command_batched(&self, cmd: Command, batch: &mut [Vec<Command>]) {
        match crate::connection::target_shard_of_cmd(&cmd, self.num_shards) {
            Some(target) if target == self.shard_id => {
                let mut dummy_out = Vec::new();
                crate::connection::apply_replicated_command(
                    &cmd,
                    &mut self.local_db.borrow_mut(),
                    &mut dummy_out,
                    self.aof.as_deref(),
                );
            }
            Some(target) if target < batch.len() => batch[target].push(cmd),
            _ => {
                self.flush_replica_batch(batch).await;
                self.execute_replica_command(cmd).await;
            }
        }
    }

    /// Sends each shard its deferred replicated commands in one message and
    /// waits until all of them are applied.
    pub async fn flush_replica_batch(&self, batch: &mut [Vec<Command>]) {
        let (tx, rx) = flume::unbounded();
        let mut sent = 0;
        for (target, cmds) in batch.iter_mut().enumerate() {
            if cmds.is_empty() {
                continue;
            }
            let msg = ShardMessage::ExecuteReplicaCmds {
                cmds: std::mem::take(cmds),
                responder: tx.clone(),
            };
            if self.senders[target].send(msg).is_ok() {
                sent += 1;
            }
        }
        drop(tx);
        for _ in 0..sent {
            if rx.recv_async().await.is_err() {
                break;
            }
        }
    }

    /// Applies a replicated command on shard `target` and waits for it.
    async fn apply_replica_on(&self, target: usize, cmd: Command) {
        if target == self.shard_id {
            let mut dummy_out = Vec::new();
            crate::connection::apply_replicated_command(
                &cmd,
                &mut self.local_db.borrow_mut(),
                &mut dummy_out,
                self.aof.as_deref(),
            );
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::ExecuteReplicaCmd {
                cmd: Box::new(cmd),
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                let _ = rx.recv_async().await;
            }
        }
    }

    pub async fn execute_replica_command(&self, cmd: Command) {
        if let Some(target) = crate::connection::target_shard_of_cmd(&cmd, self.num_shards) {
            self.apply_replica_on(target, cmd).await;
        } else {
            match cmd {
                Command::Flushall | Command::Flushdb => {
                    self.local_db.borrow_mut().flushdb();
                    for (sid, sender) in self.senders.iter().enumerate() {
                        if sid != self.shard_id {
                            let (tx, rx) = flume::bounded(1);
                            if sender
                                .send(ShardMessage::ExecuteReplicaCmd {
                                    cmd: Box::new(Command::Flushall),
                                    responder: tx,
                                })
                                .is_ok()
                            {
                                let _ = rx.recv_async().await;
                            }
                        }
                    }
                }
                Command::Mset(pairs) => {
                    for (k, v) in pairs {
                        self.set(k, v, None).await;
                    }
                }
                Command::Del(keys) => {
                    for k in keys {
                        self.del(k).await;
                    }
                }
                // The master logs CRDT changes per shard, but they arrive
                // here on one stream: hand each shard the keys it owns.
                Command::CrdtMerge(payload) => {
                    if let Ok(parts) = self.split_crdt_payload(&payload) {
                        for (sid, part) in parts {
                            self.apply_replica_on(sid, Command::CrdtMerge(part)).await;
                        }
                    }
                }
                // Every master shard logs the same absolute cutoff, so
                // applying it to every shard is exact (and idempotent).
                Command::CrdtGc(horizon) => {
                    for sid in 0..self.num_shards {
                        self.apply_replica_on(sid, Command::CrdtGc(horizon)).await;
                    }
                }
                _ => {
                    let mut dummy_out = Vec::new();
                    crate::connection::execute_local_command(
                        &cmd,
                        &mut self.local_db.borrow_mut(),
                        &mut dummy_out,
                        self.aof.as_deref(),
                    );
                }
            }
        }
    }

    /// Writes the RDB for a save whose state the caller began with
    /// `snapshot::state(..).begin()`, which returned `dirty_before`.
    pub async fn perform_save_rdb(&self, dirty_before: u64) -> Result<(), String> {
        static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let tmp_id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_filename = self
            .db_dir
            .join(format!("temp-{}-{}.rdb", std::process::id(), tmp_id));
        let res = self.write_rdb_file(&tmp_filename).await;
        match res {
            Ok(()) => crate::log_notice!("DB saved on disk"),
            Err(ref e) => {
                crate::log_warning!("Error saving DB on disk: {}", e);
                let _ = std::fs::remove_file(&tmp_filename);
            }
        }
        // Clear the flag on every path, or one failed save blocks all later ones.
        // Clear it before `finish` so a save seen as finished is not still
        // holding the flag.
        self.is_saving.store(false, Ordering::SeqCst);
        crate::snapshot::state(self.base_port).finish(dirty_before, res.is_ok());
        res
    }

    async fn write_rdb_file(&self, tmp_filename: &std::path::Path) -> Result<(), String> {
        let filename = self.db_dir.join(crate::config::dbfilename(self.base_port));

        // CRC64 and write/fsync of a large dump take hundreds of ms; doing them
        // here would stall every client on this shard. A writer thread does them
        // instead. Shards stream ~4 MB pieces through a rendezvous channel,
        // so only a few pieces are alive at once, however big the dataset.
        let (chunk_tx, chunk_rx) = flume::bounded::<Option<Vec<u8>>>(0);
        let (done_tx, done_rx) = flume::bounded::<Result<(), String>>(1);
        let tmp_path = tmp_filename.to_path_buf();
        let mut header = Vec::new();
        let used_mem = self.local_db.borrow().table.used_memory() as u64;
        crate::redis_rdb::write_file_header(&mut header, used_mem);
        std::thread::Builder::new()
            .name("rdb-writer".into())
            .spawn(move || {
                let _ = done_tx.send(Self::rdb_writer(&tmp_path, &filename, &header, chunk_rx));
            })
            .map_err(|e| e.to_string())?;

        let produced = self.produce_rdb_chunks(&chunk_tx).await;
        if produced.is_ok() {
            // If the writer already failed it has hung up; its error is
            // reported below.
            let _ = chunk_tx.send_async(None).await;
        }
        drop(chunk_tx);
        // Always wait for the writer, so the caller's cleanup of the temp file
        // cannot race with the writer still creating or writing it.
        let written = done_rx
            .recv_async()
            .await
            .unwrap_or_else(|_| Err("rdb writer thread died".to_string()));
        match produced {
            Err(e) if e != WRITER_GONE => Err(e),
            _ => written,
        }
    }

    /// Stream every shard's keyspace, one shard after another, into `chunk_tx`.
    async fn produce_rdb_chunks(
        &self,
        chunk_tx: &flume::Sender<Option<Vec<u8>>>,
    ) -> Result<(), String> {
        if !crate::shard::save_rdb_chunk_yielding(&self.local_db, chunk_tx).await {
            return Err(WRITER_GONE.to_string());
        }

        // Shards stream in turn (each finishes before the next starts), so
        // pieces from different shards never interleave.
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (done_tx, done_rx) = flume::bounded(1);
                // A missing shard would silently drop its keys from the dump.
                sender
                    .send(ShardMessage::StreamRdbChunk {
                        sink: chunk_tx.clone(),
                        done: done_tx,
                    })
                    .map_err(|_| format!("shard {} is not reachable", sid))?;
                match done_rx.recv_async().await {
                    Ok(true) => {}
                    Ok(false) => return Err(WRITER_GONE.to_string()),
                    Err(_) => return Err(format!("shard {} did not return its data", sid)),
                }
            }
        }
        Ok(())
    }

    /// Body of the `rdb-writer` thread: header, chunks, EOF + CRC64, fsync,
    /// atomic rename. A channel closed without the `None` terminator aborts.
    fn rdb_writer(
        tmp_filename: &std::path::Path,
        filename: &std::path::Path,
        header: &[u8],
        chunks: flume::Receiver<Option<Vec<u8>>>,
    ) -> Result<(), String> {
        use std::io::Write;
        let file = std::fs::File::create(tmp_filename).map_err(|e| e.to_string())?;
        let mut out = std::io::BufWriter::with_capacity(1 << 20, file);

        out.write_all(header).map_err(|e| e.to_string())?;
        let mut crc = crate::table::crc64(header);
        loop {
            match chunks.recv() {
                Ok(Some(chunk)) => {
                    if !chunk.is_empty() {
                        crc = crate::table::crc64_update(crc, &chunk);
                        out.write_all(&chunk).map_err(|e| e.to_string())?;
                    }
                }
                Ok(None) => break,
                Err(_) => return Err("save aborted".to_string()),
            }
        }

        let eof = [0xFF];
        crc = crate::table::crc64_update(crc, &eof);
        out.write_all(&eof).map_err(|e| e.to_string())?;
        out.write_all(&crc.to_le_bytes())
            .map_err(|e| e.to_string())?;
        let file = out.into_inner().map_err(|e| e.error().to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        std::fs::rename(tmp_filename, filename).map_err(|e| e.to_string())?;
        let _ = crate::aof::sync_parent_dir(filename);
        Ok(())
    }

    pub fn my_id(&self) -> String {
        crate::cluster::get_cluster_hub(self.port).my_id()
    }

    pub fn cluster_meet(&self, ip: String, port: u16) -> Result<(), String> {
        crate::cluster::get_cluster_hub(self.port).cluster_meet(&ip, port)
    }

    pub fn cluster_nodes(&self) -> String {
        crate::cluster::get_cluster_hub(self.port).cluster_nodes()
    }

    pub fn cluster_info(&self) -> String {
        crate::cluster::get_cluster_hub(self.port).cluster_info()
    }

    pub fn cluster_failover(&self, force: bool) -> Result<(), String> {
        crate::cluster::get_cluster_hub(self.port).cluster_failover(force)
    }

    pub fn cluster_reset(&self, hard: bool) -> Result<(), String> {
        crate::cluster::get_cluster_hub(self.port).cluster_reset(hard)
    }

    pub fn cluster_forget(&self, node_id: &str) -> Result<(), String> {
        crate::cluster::get_cluster_hub(self.port).cluster_forget(node_id)
    }

    pub fn cluster_replicate(&self, node_id: &str) -> Result<(), String> {
        crate::cluster::get_cluster_hub(self.port).cluster_replicate(node_id)
    }

    pub fn cluster_slots(&self, out: &mut Vec<u8>) {
        crate::cluster::get_cluster_hub(self.port).cluster_slots(out, self.num_shards);
    }

    pub fn cluster_shards(&self, out: &mut Vec<u8>) {
        crate::cluster::get_cluster_hub(self.port).cluster_shards(out);
    }

    pub fn cluster_links(&self, out: &mut Vec<u8>) {
        crate::cluster::get_cluster_hub(self.port).cluster_links(out);
    }

    pub fn cluster_addslots(&self, slots: &[u16]) -> Result<(), String> {
        let res = crate::cluster::get_cluster_hub(self.port).cluster_addslots(slots);
        if res.is_ok() {
            for &s in slots {
                self.set_slot_state(s, crate::shard::SlotState::Stable);
                self.set_slot_owner(s, slot_to_shard(s, self.num_shards));
            }
        }
        res
    }

    pub fn cluster_delslots(&self, slots: &[u16]) -> Result<(), String> {
        crate::cluster::get_cluster_hub(self.port).cluster_delslots(slots)
    }

    pub fn cluster_addslotsrange(&self, ranges: &[(u16, u16)]) -> Result<(), String> {
        let res = crate::cluster::get_cluster_hub(self.port).cluster_addslotsrange(ranges);
        if res.is_ok() {
            for &(start, end) in ranges {
                for s in start..=end {
                    self.set_slot_state(s, crate::shard::SlotState::Stable);
                    self.set_slot_owner(s, slot_to_shard(s, self.num_shards));
                }
            }
        }
        res
    }

    pub fn cluster_delslotsrange(&self, ranges: &[(u16, u16)]) -> Result<(), String> {
        crate::cluster::get_cluster_hub(self.port).cluster_delslotsrange(ranges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keys_differing_in_last_bytes_spread_over_shards() {
        // Short keys and fixed-width keys used to all land on one shard.
        let families: [Vec<String>; 3] = [
            (0..64).map(|i| format!("sk:{}", i % 10)).collect(),
            (0..1000).map(|i| format!("key:{i:04}")).collect(),
            (0..1000).map(|i| format!("user:{i:08}")).collect(),
        ];
        for keys in &families {
            let distinct: std::collections::HashSet<&String> = keys.iter().collect();
            for n in [2usize, 4, 8] {
                let mut counts = vec![0usize; n];
                for k in &distinct {
                    counts[target_shard(k.as_bytes(), n)] += 1;
                }
                let fair = distinct.len() / n;
                let min = *counts.iter().min().unwrap();
                let used = counts.iter().filter(|&&c| c > 0).count();
                let balanced = if distinct.len() < 100 {
                    used * 2 >= n
                } else {
                    min * 2 > fair
                };
                assert!(balanced, "{n} shards, keys like {:?}: {counts:?}", keys[0]);
            }
        }
        // Hash tags still pin keys to one shard.
        assert_eq!(target_shard(b"{u1}:a", 4), target_shard(b"{u1}:b", 4));
        assert_eq!(
            target_shard_and_hash(b"{u1}:a", 4).0,
            target_shard(b"u1", 4)
        );
    }

    fn single_shard_router(port: u16) -> (Router, Rc<RefCell<ShardDb>>) {
        let db = Rc::new(RefCell::new(ShardDb::new(port)));
        let (senders_mesh, _rx) = crate::mailbox::create_shard_mesh(1);
        let router = Router::new(
            0,
            1,
            port,
            db.clone(),
            senders_mesh[0].clone(),
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        (router, db)
    }

    fn fill_and_limit(router: &Router, db: &Rc<RefCell<ShardDb>>, port: u16) -> usize {
        let pairs = (0..64)
            .map(|i| {
                (
                    Bytes::from(format!("tier_k{}", i)),
                    Bytes::from(vec![b'v'; 4096]),
                )
            })
            .collect();
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap()
            .block_on(router.mset(pairs));
        let used = db.borrow().table.used_memory();
        // Limit below current usage but above what remains once values are offloaded.
        let limit = used - 64 * 2048;
        crate::tiering::set_max_memory(port, limit as u64);
        limit
    }

    #[test]
    fn test_check_auto_tier_terminates_when_nothing_can_spill() {
        let port = 19871;
        let (router, db) = single_shard_router(port);
        fill_and_limit(&router, &db, port);
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        // No tier manager: every spill fails, so this must give up, not spin.
        assert!(!rt.block_on(router.check_auto_tier()));
        assert!(!router.is_auto_tiering.get());
        crate::tiering::set_max_memory(port, 0);
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn test_del_if_unchanged_only_deletes_the_dumped_value() {
        let (router, db) = single_shard_router(19874);
        let key = Bytes::from_static(b"migrating");
        block_on(router.set(key.clone(), Bytes::from_static(b"v1"), None));
        let stale = Bytes::from(db.borrow_mut().dump(&key).unwrap());
        // Written again after the dump: the migrated copy is out of date.
        block_on(router.set(key.clone(), Bytes::from_static(b"v2"), None));
        assert!(!router.del_if_unchanged_local(key.clone(), &stale));
        assert!(db.borrow_mut().dump(&key).is_some());

        let current = Bytes::from(db.borrow_mut().dump(&key).unwrap());
        assert!(block_on(
            router.del_if_unchanged(key.clone(), current.clone())
        ));
        assert!(db.borrow_mut().dump(&key).is_none());
        // A key that is gone is not "unchanged".
        assert!(!router.del_if_unchanged_local(key, &current));
    }

    #[test]
    fn test_spill_loses_to_a_write_or_delete_made_during_its_io() {
        use crate::tiering::run_during_first_wait;
        let port = 19876;
        let (router, db) = single_shard_router(port);
        let dir = std::env::temp_dir().join(format!("rudis-spillrace-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        block_on(async {
            let tm = Rc::new(
                crate::tiering::ShardTierManager::open(0, port, &dir)
                    .await
                    .unwrap(),
            );
            db.borrow_mut().tier_manager = Some(tm.clone());
            // Large values: their stash waits on a write before committing.
            let old = Bytes::from(vec![b'1'; 5000]);
            let new = Bytes::from(vec![b'2'; 5000]);

            db.borrow_mut().set(Bytes::from("k"), old.clone(), None);
            let (spilled, _) = run_during_first_wait(router.spill_local(b"k"), async {
                db.borrow_mut().set(Bytes::from("k"), new.clone(), None)
            })
            .await;
            assert!(!spilled);
            assert_eq!(db.borrow_mut().get_checked(b"k"), Ok(Some(new)));
            // The stale copy was freed, not leaked.
            assert_eq!(tm.free_extents.borrow().len(), 1);

            db.borrow_mut().set(Bytes::from("j"), old, None);
            let (spilled, _) = run_during_first_wait(router.spill_local(b"j"), async {
                db.borrow_mut().del(b"j")
            })
            .await;
            assert!(!spilled);
            assert_eq!(db.borrow_mut().get_checked(b"j"), Ok(None));
            assert_eq!(
                tm.free_extents.borrow().len(),
                1,
                "extent reused, then freed"
            );
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_rename_and_copy_of_tiered_and_cooled_keys() {
        let port = 19877;
        let (router, db) = single_shard_router(port);
        let dir = std::env::temp_dir().join(format!("rudis-tiercopy-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        block_on(async {
            let tm = Rc::new(
                crate::tiering::ShardTierManager::open(0, port, &dir)
                    .await
                    .unwrap(),
            );
            db.borrow_mut().tier_manager = Some(tm.clone());
            let v1 = Bytes::from(vec![b'a'; 5000]);
            let v2 = Bytes::from(vec![b'b'; 5000]);

            // A renamed tiered key loads back under its new name, not the
            // old name recorded in the disk header.
            db.borrow_mut().set(Bytes::from("old"), v1.clone(), None);
            assert!(router.spill_local(b"old").await);
            assert_eq!(
                db.borrow_mut().rename(b"old", Bytes::from("new"), false),
                Ok(true)
            );
            assert_eq!(router.get(Bytes::from("old")).await, None);
            assert_eq!(router.get(Bytes::from("new")).await, Some(v1.clone()));
            assert!(db.borrow_mut().del(b"new"));

            // COPY of a tiered key hydrates the copy and frees an overwritten
            // destination's extent; deleting both keys frees each extent once.
            tm.free_extents.borrow_mut().clear();
            db.borrow_mut().set(Bytes::from("src"), v1.clone(), None);
            db.borrow_mut().set(Bytes::from("dst"), v2, None);
            assert!(router.spill_local(b"src").await);
            assert!(router.spill_local(b"dst").await);
            assert_eq!(
                db.borrow_mut().copy(b"src", Bytes::from("dst"), true),
                Ok(true)
            );
            assert_eq!(tm.free_extents.borrow().len(), 1, "overwritten dst freed");
            assert_eq!(router.get(Bytes::from("dst")).await, Some(v1.clone()));
            assert!(db.borrow_mut().del(b"src"));
            assert!(db.borrow_mut().del(b"dst"));
            assert_eq!(tm.free_extents.borrow().len(), 2, "src freed once");

            // COPY of a cooled key does not share its disk pointer with the copy.
            tm.free_extents.borrow_mut().clear();
            db.borrow_mut().set(Bytes::from("cool"), v1.clone(), None);
            assert!(router.cool_local(b"cool").await);
            assert_eq!(
                db.borrow_mut()
                    .copy(b"cool", Bytes::from("cool_copy"), false),
                Ok(true)
            );
            assert!(db.borrow_mut().del(b"cool"));
            assert!(db.borrow_mut().del(b"cool_copy"));
            assert_eq!(
                tm.free_extents.borrow().len(),
                1,
                "cooled extent freed once"
            );
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_failed_save_clears_in_progress_flag_and_temp_file() {
        let (mut router, db) = single_shard_router(19873);
        db.borrow_mut()
            .set(Bytes::from("k"), Bytes::from("v"), None);
        let dir = std::env::temp_dir().join(format!("rudis-savefail-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        router.db_dir = dir.clone();
        // dump.rdb is a directory, so the final rename fails.
        std::fs::create_dir_all(dir.join("dump.rdb")).unwrap();
        let err = block_on(router.save_rdb()).unwrap_err();
        assert!(!err.contains("in progress"), "{err}");
        // The flag was cleared: a retry fails for the real reason again.
        let err2 = block_on(router.save_rdb()).unwrap_err();
        assert!(!err2.contains("in progress"), "{err2}");
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            1,
            "temp file left"
        );
        std::fs::remove_dir(dir.join("dump.rdb")).unwrap();
        block_on(router.save_rdb()).unwrap();
        assert!(dir.join("dump.rdb").is_file());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_is_saving_flag_is_shared_across_shards_of_same_server() {
        let port0 = 19882;
        let port1 = 19883;
        let base_port = 19882;
        let (mut router0, _db0) = single_shard_router(port0);
        router0.set_base_port(base_port);
        let (mut router1, _db1) = single_shard_router(port1);
        router1.set_base_port(base_port);

        assert!(!router0.is_saving.load(Ordering::SeqCst));
        assert!(!router1.is_saving.load(Ordering::SeqCst));

        // When Shard 0 marks a save in progress, Shard 1 immediately sees it
        assert!(
            router0
                .is_saving
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        );
        assert!(router1.is_saving.load(Ordering::SeqCst));
        assert!(
            router1
                .is_saving
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_err()
        );

        router0.is_saving.store(false, Ordering::SeqCst);
        assert!(!router1.is_saving.load(Ordering::SeqCst));
    }

    #[test]
    fn test_bg_save_and_rewrite_in_progress_from_reply_until_next_accepted() {
        let (mut router, db) = single_shard_router(19875);
        db.borrow_mut()
            .set(Bytes::from("k"), Bytes::from("v"), None);
        let dir = std::env::temp_dir().join(format!("rudis-bgstate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        router.db_dir = dir.clone();
        let rewrite = crate::snapshot::aof_rewrite_state(router.base_port);
        let save = crate::snapshot::state(router.base_port);
        block_on(async {
            for _ in 0..2 {
                router.bgrewriteaof().await.unwrap();
                // Before the spawned rewrite has run at all.
                assert!(rewrite.in_progress());
                while rewrite.in_progress() {
                    monoio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                assert!(!router.is_saving.load(Ordering::SeqCst));
                assert!(rewrite.last_ok());

                router.bgsave().await.unwrap();
                assert!(save.in_progress());
                while save.in_progress() {
                    monoio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                assert!(!router.is_saving.load(Ordering::SeqCst));
                assert!(save.last_save_ok());
            }
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_failed_rewrite_clears_flag_and_temp_file() {
        let port = 19871;
        let (mut router, db) = single_shard_router(port);
        db.borrow_mut()
            .set(Bytes::from("k"), Bytes::from("v"), None);
        let dir = std::env::temp_dir().join(format!("rudis-rewritefail-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        router.db_dir = dir.clone();
        let status = crate::snapshot::aof_rewrite_state(router.base_port);
        // The target is a directory, so publishing the rewrite fails.
        std::fs::create_dir_all(dir.join("appendonly-0.aof")).unwrap();
        for _ in 0..2 {
            // As BGREWRITEAOF does before spawning the rewrite.
            router.is_saving.store(true, Ordering::SeqCst);
            assert!(block_on(router.perform_rewrite_aof()).is_err());
            assert!(!router.is_saving.load(Ordering::SeqCst), "flag left set");
            assert!(!status.last_ok());
            assert!(!status.in_progress());
        }
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            1,
            "temp file left"
        );
        std::fs::remove_dir(dir.join("appendonly-0.aof")).unwrap();
        router.is_saving.store(true, Ordering::SeqCst);
        assert_eq!(block_on(router.perform_rewrite_aof()), Ok(1));
        assert!(status.last_ok());
        assert!(dir.join("appendonly-0.aof").is_file());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_check_auto_tier_spills_under_limit() {
        let port = 19872;
        // Auto-tier spilling only runs under `noeviction`; pin it.
        let _policy_guard = crate::connection::MAX_MEMORY_POLICY_TEST_LOCK.lock();
        crate::connection::set_max_memory_policy("noeviction");
        let (router, db) = single_shard_router(port);
        let limit = fill_and_limit(&router, &db, port);
        let dir = std::env::temp_dir().join(format!("rudis-autotier-{}", std::process::id()));
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let tm = crate::tiering::ShardTierManager::open(0, port, &dir)
                .await
                .unwrap();
            db.borrow_mut().tier_manager = Some(Rc::new(tm));
            assert!(router.check_auto_tier().await);
            assert!(db.borrow().table.used_memory() <= limit);
            assert_eq!(
                router.get(Bytes::from_static(b"tier_k0")).await,
                Some(Bytes::from(vec![b'v'; 4096]))
            );
        });
        crate::tiering::set_max_memory(port, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_router_mget_mset_fanout() {
        let num_shards = 2;
        let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(num_shards);
        let senders = senders_mesh.remove(0);
        drop(senders_mesh);

        let db0 = Rc::new(RefCell::new(ShardDb::new(9999)));

        let router = Router::new(
            0,
            num_shards,
            9999,
            db0.clone(),
            senders.clone(),
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        let mut k_shard0 = Bytes::from("k0");
        let mut k_shard1 = Bytes::from("k1");
        for i in 0..1000 {
            let candidate = Bytes::from(format!("key_{}", i));
            if target_shard(&candidate, num_shards) == 0 && k_shard0 == "k0" {
                k_shard0 = candidate.clone();
            }
            if target_shard(&candidate, num_shards) == 1 && k_shard1 == "k1" {
                k_shard1 = candidate;
            }
        }
        assert_eq!(target_shard(&k_shard0, num_shards), 0);
        assert_eq!(target_shard(&k_shard1, num_shards), 1);

        let rx1 = receivers.remove(1);
        let k_shard1_verify = k_shard1.clone();
        let (done_tx, done_rx) = flume::bounded(1);
        let h = std::thread::spawn(move || {
            let mut db = ShardDb::new(9999);
            while let Ok(msg) = rx1.recv() {
                match msg {
                    ShardMessage::ScatterMset {
                        shard_id,
                        mut pairs,
                        descriptor,
                    } => {
                        for (k, v) in &pairs {
                            db.set(k.clone(), v.clone(), None);
                        }
                        pairs.clear();
                        descriptor.recycle_pairs(shard_id, pairs);
                        descriptor.finish_shard();
                    }
                    ShardMessage::ScatterMget {
                        shard_id,
                        mut keys,
                        descriptor,
                        ..
                    } => {
                        for (idx, key) in &keys {
                            let val = db.get(key);
                            descriptor.write_result(*idx, val);
                        }
                        keys.clear();
                        descriptor.recycle_keys(shard_id, keys);
                        descriptor.finish_shard();
                    }
                    ShardMessage::Mset { pairs, responder } => {
                        for (k, v) in &pairs {
                            db.set(k.clone(), v.clone(), None);
                        }
                        let _ = responder.send(pairs);
                    }
                    ShardMessage::Mget {
                        mut keys,
                        responder,
                    } => {
                        for item in &mut keys {
                            let val = db.get(item.1.as_ref().unwrap());
                            item.1 = val;
                        }
                        let _ = responder.send(keys);
                    }
                    _ => break,
                }
            }
            let _ = done_tx.send(db.get(&k_shard1_verify));
        });

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            // MSET across both shards simultaneously
            let pairs = vec![
                (k_shard0.clone(), Bytes::from("val0")),
                (k_shard1.clone(), Bytes::from("val1")),
            ];
            router.mset(pairs).await;

            // Shard 0 local_db should have k_shard0
            assert_eq!(db0.borrow_mut().get(&k_shard0), Some(Bytes::from("val0")));

            // MGET across both shards including nonexistent and duplicate keys
            let missing_key = Bytes::from("missing_key_xyz");
            let keys = vec![
                k_shard1.clone(),
                missing_key.clone(),
                k_shard0.clone(),
                k_shard1.clone(),
            ];
            let values = router.mget(keys).await;
            assert_eq!(values.len(), 4);
            assert_eq!(values[0], Some(Bytes::from("val1")));
            assert_eq!(values[1], None);
            assert_eq!(values[2], Some(Bytes::from("val0")));
            assert_eq!(values[3], Some(Bytes::from("val1")));
        });

        drop(senders);
        let _ = h.join();
        assert_eq!(done_rx.recv().unwrap(), Some(Bytes::from("val1")));
    }

    #[test]
    fn test_router_mget_all_local_fast_path() {
        let db0 = Rc::new(RefCell::new(ShardDb::new(9999)));
        let (senders_mesh, _rx) = crate::mailbox::create_shard_mesh(1);
        let router = Router::new(
            0,
            1,
            9999,
            db0.clone(),
            senders_mesh[0].clone(),
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let pairs = vec![
                (Bytes::from("local_k1"), Bytes::from("val1")),
                (Bytes::from("local_k2"), Bytes::from("val2")),
            ];
            router.mset(pairs).await;

            let keys = vec![
                Bytes::from("local_k1"),
                Bytes::from("local_missing"),
                Bytes::from("local_k2"),
            ];
            let values = router.mget(keys).await;
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], Some(Bytes::from("val1")));
            assert_eq!(values[1], None);
            assert_eq!(values[2], Some(Bytes::from("val2")));
        });
    }

    #[test]
    fn test_mget_single_pass_and_bitmask_fanout() {
        let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
        let senders = senders_mesh.remove(0);
        drop(senders_mesh);
        let rx1 = receivers.remove(1);
        let db0 = Rc::new(RefCell::new(ShardDb::new(9999)));
        let router = Router::new(
            0,
            2,
            9999,
            db0.clone(),
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        let mut k_shard0 = None;
        let mut k_shard1 = None;
        for i in 0..1000 {
            let k = Bytes::from(format!("key_{}", i));
            let target = router.target_shard(&k);
            if target == 0 && k_shard0.is_none() {
                k_shard0 = Some(k);
            } else if target == 1 && k_shard1.is_none() {
                k_shard1 = Some(k);
            }
            if k_shard0.is_some() && k_shard1.is_some() {
                break;
            }
        }
        let k0 = k_shard0.unwrap();
        let k1 = k_shard1.unwrap();

        let rx1_clone = rx1;
        std::thread::spawn(move || {
            let mut remote_db = ShardDb::new(9999);
            while let Ok(msg) = rx1_clone.recv() {
                match msg {
                    ShardMessage::ScatterMset {
                        shard_id,
                        mut pairs,
                        descriptor,
                    } => {
                        for (k, v) in &pairs {
                            remote_db.set(k.clone(), v.clone(), None);
                        }
                        pairs.clear();
                        descriptor.recycle_pairs(shard_id, pairs);
                        descriptor.finish_shard();
                    }
                    ShardMessage::ScatterMget {
                        shard_id,
                        mut keys,
                        descriptor,
                        ..
                    } => {
                        for (idx, key) in &keys {
                            let val = remote_db.get(key);
                            descriptor.write_result(*idx, val);
                        }
                        keys.clear();
                        descriptor.recycle_keys(shard_id, keys);
                        descriptor.finish_shard();
                    }
                    ShardMessage::Mset { pairs, responder } => {
                        for (k, v) in &pairs {
                            remote_db.set(k.clone(), v.clone(), None);
                        }
                        let _ = responder.send(pairs);
                    }
                    ShardMessage::Mget {
                        mut keys,
                        responder,
                    } => {
                        for item in &mut keys {
                            let val = remote_db.get(item.1.as_ref().unwrap());
                            item.1 = val;
                        }
                        let _ = responder.send(keys);
                    }
                    _ => break,
                }
            }
        });

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let pairs = vec![
                (k0.clone(), Bytes::from("v0")),
                (k1.clone(), Bytes::from("v1")),
            ];
            router.mset(pairs).await;

            let keys = vec![k0.clone(), Bytes::from("nonexistent"), k1.clone()];
            let values = router.mget(keys).await;
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], Some(Bytes::from("v0")));
            assert_eq!(values[1], None);
            assert_eq!(values[2], Some(Bytes::from("v1")));
        });
    }

    #[test]
    fn test_mget_user_space_fast_harvest_try_recv() {
        let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
        let senders = senders_mesh.remove(0);
        drop(senders_mesh);
        let rx1 = receivers.remove(1);
        let db0 = Rc::new(RefCell::new(ShardDb::new(9998)));
        let router = Router::new(
            0,
            2,
            9998,
            db0.clone(),
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        let mut k_shard0 = None;
        let mut k_shard1 = None;
        for i in 0..1000 {
            let k = Bytes::from(format!("h_{}", i));
            let target = router.target_shard(&k);
            if target == 0 && k_shard0.is_none() {
                k_shard0 = Some(k);
            } else if target == 1 && k_shard1.is_none() {
                k_shard1 = Some(k);
            }
            if k_shard0.is_some() && k_shard1.is_some() {
                break;
            }
        }
        let k0 = k_shard0.unwrap();
        let k1 = k_shard1.unwrap();

        let rx1_clone = rx1;
        let k1_remote = k1.clone();
        std::thread::spawn(move || {
            let mut remote_db = ShardDb::new(9998);
            remote_db.set(k1_remote, Bytes::from("remote_harvest_val"), None);
            while let Ok(msg) = rx1_clone.recv() {
                match msg {
                    ShardMessage::ScatterMget {
                        shard_id,
                        mut keys,
                        descriptor,
                        ..
                    } => {
                        for (idx, key) in &keys {
                            let val = remote_db.get(key);
                            descriptor.write_result(*idx, val);
                        }
                        keys.clear();
                        descriptor.recycle_keys(shard_id, keys);
                        descriptor.finish_shard();
                    }
                    ShardMessage::Mget {
                        mut keys,
                        responder,
                    } => {
                        for item in &mut keys {
                            let val = remote_db.get(item.1.as_ref().unwrap());
                            item.1 = val;
                        }
                        let _ = responder.send(keys);
                    }
                    _ => break,
                }
            }
        });

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            db0.borrow_mut()
                .set(k0.clone(), Bytes::from("local_harvest_val"), None);
            let keys = vec![k0.clone(), k1.clone()];
            let values = router.mget(keys).await;
            assert_eq!(values.len(), 2);
            assert!(values[0].is_some());
            assert!(values[1].is_some());
        });
    }

    #[test]
    fn test_router_channel_pool_reuse() {
        let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
        let senders = senders_mesh.remove(0);
        drop(senders_mesh);
        let rx1 = receivers.remove(1);

        let db0 = Rc::new(RefCell::new(ShardDb::new(9997)));
        let router = Router::new(
            0,
            2,
            9997,
            db0.clone(),
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        let mut k_shard1 = None;
        for i in 0..1000 {
            let k = Bytes::from(format!("key_pool_{}", i));
            if target_shard(&k, 2) == 1 {
                k_shard1 = Some(k);
                break;
            }
        }
        let k1 = k_shard1.unwrap();
        let rx1_clone = rx1;
        let k1_remote = k1.clone();

        std::thread::spawn(move || {
            let mut remote_db = ShardDb::new(9997);
            remote_db.set(k1_remote.clone(), Bytes::from("pool_val"), None);
            while let Ok(msg) = rx1_clone.recv() {
                match msg {
                    ShardMessage::FastGet { descriptor } => {
                        let val = remote_db.get(&descriptor.key);
                        descriptor.finish(val);
                    }
                    ShardMessage::Get { key, responder } => {
                        let val = remote_db.get(&key);
                        let _ = responder.send(val);
                    }
                    ShardMessage::Batch {
                        mut items,
                        responder,
                        ..
                    } => {
                        for (idx, _h, _cmd) in items.drain(..) {
                            responder.write_slot(
                                idx,
                                crate::shard::CompactResp::from_slice(b"+PONG\r\n"),
                            );
                        }
                        responder.finish(items);
                    }
                    _ => break,
                }
            }
        });

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            // Remote GETs signal through the descriptor's own doorbell and
            // no longer take a channel from the pool.
            assert_eq!(router.notify_channel_pool.borrow().len(), 0);
            let val1 = router.get(k1.clone()).await;
            assert_eq!(val1, Some(Bytes::from("pool_val")));
            assert_eq!(router.notify_channel_pool.borrow().len(), 0);

            let val2 = router.get(k1.clone()).await;
            assert_eq!(val2, Some(Bytes::from("pool_val")));
            assert_eq!(router.notify_channel_pool.borrow().len(), 0);

            assert_eq!(router.remote_responder_pool.borrow().len(), 0);
            let resp1 = router.execute_remote(1, Command::Ping(None)).await;
            assert_eq!(resp1, b"+PONG\r\n");
            assert_eq!(router.remote_responder_pool.borrow().len(), 1);

            let resp2 = router.execute_remote(1, Command::Ping(None)).await;
            assert_eq!(resp2, b"+PONG\r\n");
            assert_eq!(router.remote_responder_pool.borrow().len(), 1);
        });
    }

    #[test]
    fn test_router_mget_mset_batch_pool_reuse() {
        let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
        let senders = senders_mesh.remove(0);
        drop(senders_mesh);
        let rx1 = receivers.remove(1);

        let db0 = Rc::new(RefCell::new(ShardDb::new(9996)));
        let router = Router::new(
            0,
            2,
            9996,
            db0.clone(),
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        let mut k_shard0 = None;
        let mut k_shard1 = None;
        for i in 0..1000 {
            let k = Bytes::from(format!("batch_pool_k_{}", i));
            if target_shard(&k, 2) == 0 && k_shard0.is_none() {
                k_shard0 = Some(k);
            } else if target_shard(&k, 2) == 1 && k_shard1.is_none() {
                k_shard1 = Some(k);
            }
            if k_shard0.is_some() && k_shard1.is_some() {
                break;
            }
        }
        let k0 = k_shard0.unwrap();
        let k1 = k_shard1.unwrap();
        let rx1_clone = rx1;

        std::thread::spawn(move || {
            let mut remote_db = ShardDb::new(9996);
            while let Ok(msg) = rx1_clone.recv() {
                match msg {
                    ShardMessage::ScatterMset {
                        shard_id,
                        mut pairs,
                        descriptor,
                    } => {
                        for (k, v) in &pairs {
                            remote_db.set(k.clone(), v.clone(), None);
                        }
                        pairs.clear();
                        descriptor.recycle_pairs(shard_id, pairs);
                        descriptor.finish_shard();
                    }
                    ShardMessage::ScatterMget {
                        shard_id,
                        mut keys,
                        descriptor,
                        ..
                    } => {
                        for (idx, key) in &keys {
                            let val = remote_db.get(key);
                            descriptor.write_result(*idx, val);
                        }
                        keys.clear();
                        descriptor.recycle_keys(shard_id, keys);
                        descriptor.finish_shard();
                    }
                    ShardMessage::Mget {
                        mut keys,
                        responder,
                    } => {
                        for item in &mut keys {
                            let val = remote_db.get(item.1.as_ref().unwrap());
                            item.1 = val;
                        }
                        let _ = responder.send(keys);
                    }
                    ShardMessage::Mset { pairs, responder } => {
                        for (k, v) in &pairs {
                            remote_db.set(k.clone(), v.clone(), None);
                        }
                        let _ = responder.send(pairs);
                    }
                    _ => break,
                }
            }
        });

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            assert_eq!(router.mset_batch_pool.borrow().len(), 0);
            assert_eq!(router.mget_batch_pool.borrow().len(), 0);

            // MSET scattered
            router
                .mset(vec![
                    (k0.clone(), Bytes::from("v0")),
                    (k1.clone(), Bytes::from("v1")),
                ])
                .await;
            assert_eq!(router.mset_batch_pool.borrow().len(), 1);

            router
                .mset(vec![
                    (k0.clone(), Bytes::from("v0_new")),
                    (k1.clone(), Bytes::from("v1_new")),
                ])
                .await;
            assert_eq!(router.mset_batch_pool.borrow().len(), 1);

            // MGET scattered
            let res1 = router.mget(vec![k0.clone(), k1.clone()]).await;
            assert_eq!(
                res1,
                vec![Some(Bytes::from("v0_new")), Some(Bytes::from("v1_new"))]
            );
            assert_eq!(router.mget_batch_pool.borrow().len(), 1);

            let res2 = router.mget(vec![k0.clone(), k1.clone()]).await;
            assert_eq!(
                res2,
                vec![Some(Bytes::from("v0_new")), Some(Bytes::from("v1_new"))]
            );
            assert_eq!(router.mget_batch_pool.borrow().len(), 1);
            assert!(router.mset_batch_pool.borrow()[0][1].capacity() > 0);
            assert!(router.mget_batch_pool.borrow()[0][1].capacity() > 0);
        });
    }

    #[test]
    fn test_router_mget_mset_in_place_recycling_zero_alloc() {
        let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
        let senders = senders_mesh.remove(0);
        drop(senders_mesh);
        let rx1 = receivers.remove(1);

        let db0 = Rc::new(RefCell::new(ShardDb::new(9994)));
        let router = Router::new(
            0,
            2,
            9994,
            db0.clone(),
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        let mut k_shard0 = None;
        let mut k_shard1 = None;
        for i in 0..1000 {
            let k = Bytes::from(format!("zero_alloc_k_{}", i));
            if target_shard(&k, 2) == 0 && k_shard0.is_none() {
                k_shard0 = Some(k);
            } else if target_shard(&k, 2) == 1 && k_shard1.is_none() {
                k_shard1 = Some(k);
            }
            if k_shard0.is_some() && k_shard1.is_some() {
                break;
            }
        }
        let k0 = k_shard0.unwrap();
        let k1 = k_shard1.unwrap();
        let rx1_clone = rx1;

        std::thread::spawn(move || {
            let mut remote_db = ShardDb::new(9994);
            while let Ok(msg) = rx1_clone.recv() {
                match msg {
                    ShardMessage::ScatterMset {
                        shard_id,
                        mut pairs,
                        descriptor,
                    } => {
                        for (k, v) in &pairs {
                            remote_db.set(k.clone(), v.clone(), None);
                        }
                        pairs.clear();
                        descriptor.recycle_pairs(shard_id, pairs);
                        descriptor.finish_shard();
                    }
                    ShardMessage::ScatterMget {
                        shard_id,
                        mut keys,
                        descriptor,
                        ..
                    } => {
                        for (idx, key) in &keys {
                            let val = remote_db.get(key);
                            descriptor.write_result(*idx, val);
                        }
                        keys.clear();
                        descriptor.recycle_keys(shard_id, keys);
                        descriptor.finish_shard();
                    }
                    ShardMessage::Mget {
                        mut keys,
                        responder,
                    } => {
                        for item in &mut keys {
                            let val = remote_db.get(item.1.as_ref().unwrap());
                            item.1 = val;
                        }
                        let _ = responder.send(keys);
                    }
                    ShardMessage::Mset { pairs, responder } => {
                        for (k, v) in &pairs {
                            remote_db.set(k.clone(), v.clone(), None);
                        }
                        let _ = responder.send(pairs);
                    }
                    _ => break,
                }
            }
        });

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            for round in 0..10 {
                let v0 = Bytes::from(format!("val0_{}", round));
                let v1 = Bytes::from(format!("val1_{}", round));
                router
                    .mset(vec![(k0.clone(), v0.clone()), (k1.clone(), v1.clone())])
                    .await;
                let res = router.mget(vec![k0.clone(), k1.clone()]).await;
                assert_eq!(res, vec![Some(v0), Some(v1)]);
                assert!(router.mset_batch_pool.borrow()[0][1].capacity() >= 1);
                assert!(router.mget_batch_pool.borrow()[0][1].capacity() >= 1);
            }
        });
    }

    #[test]
    fn test_router_mget_mset_reactive_await_clean_harvest() {
        let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
        let senders = senders_mesh.remove(0);
        drop(senders_mesh);
        let rx1 = receivers.remove(1);

        let db0 = Rc::new(RefCell::new(ShardDb::new(9995)));
        let router = Router::new(
            0,
            2,
            9995,
            db0.clone(),
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        let mut k_shard0 = None;
        let mut k_shard1 = None;
        for i in 0..1000 {
            let k = Bytes::from(format!("react_k_{}", i));
            if target_shard(&k, 2) == 0 && k_shard0.is_none() {
                k_shard0 = Some(k);
            } else if target_shard(&k, 2) == 1 && k_shard1.is_none() {
                k_shard1 = Some(k);
            }
            if k_shard0.is_some() && k_shard1.is_some() {
                break;
            }
        }
        let k0 = k_shard0.unwrap();
        let k1 = k_shard1.unwrap();
        let rx1_clone = rx1;

        std::thread::spawn(move || {
            let mut remote_db = ShardDb::new(9995);
            while let Ok(msg) = rx1_clone.recv() {
                match msg {
                    ShardMessage::ScatterMset {
                        shard_id,
                        mut pairs,
                        descriptor,
                    } => {
                        for (k, v) in &pairs {
                            remote_db.set(k.clone(), v.clone(), None);
                        }
                        pairs.clear();
                        descriptor.recycle_pairs(shard_id, pairs);
                        descriptor.finish_shard();
                    }
                    ShardMessage::ScatterMget {
                        shard_id,
                        mut keys,
                        descriptor,
                        ..
                    } => {
                        for (idx, key) in &keys {
                            let val = remote_db.get(key);
                            descriptor.write_result(*idx, val);
                        }
                        keys.clear();
                        descriptor.recycle_keys(shard_id, keys);
                        descriptor.finish_shard();
                    }
                    ShardMessage::Mget {
                        mut keys,
                        responder,
                    } => {
                        for item in &mut keys {
                            let val = remote_db.get(item.1.as_ref().unwrap());
                            item.1 = val;
                        }
                        let _ = responder.send(keys);
                    }
                    ShardMessage::Mset { pairs, responder } => {
                        for (k, v) in &pairs {
                            remote_db.set(k.clone(), v.clone(), None);
                        }
                        let _ = responder.send(pairs);
                    }
                    _ => break,
                }
            }
        });

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            // MSET scattered
            router
                .mset(vec![
                    (k0.clone(), Bytes::from("val_k0")),
                    (k1.clone(), Bytes::from("val_k1")),
                ])
                .await;

            // MGET scattered
            let res = router.mget(vec![k0.clone(), k1.clone()]).await;
            assert_eq!(res.len(), 2);
            assert_eq!(res[0], Some(Bytes::from("val_k0")));
            assert_eq!(res[1], Some(Bytes::from("val_k1")));
        });
    }

    #[test]
    fn test_router_json_mget_cross_shard_fanout() {
        let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
        let senders = senders_mesh.remove(0);
        drop(senders_mesh);
        let rx1 = receivers.remove(1);

        let db0 = Rc::new(RefCell::new(ShardDb::new(9996)));
        let router = Router::new(
            0,
            2,
            9996,
            db0.clone(),
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        let mut k_shard0 = None;
        let mut k_shard1 = None;
        for i in 0..1000 {
            let k = Bytes::from(format!("jmget_k_{}", i));
            if target_shard(&k, 2) == 0 && k_shard0.is_none() {
                k_shard0 = Some(k);
            } else if target_shard(&k, 2) == 1 && k_shard1.is_none() {
                k_shard1 = Some(k);
            }
            if k_shard0.is_some() && k_shard1.is_some() {
                break;
            }
        }
        let k0 = k_shard0.unwrap();
        let k1 = k_shard1.unwrap();
        let rx1_clone = rx1;

        std::thread::spawn(move || {
            let remote_db = ShardDb::new(9996);
            while let Ok(msg) = rx1_clone.recv() {
                match msg {
                    ShardMessage::JsonMget {
                        keys,
                        path,
                        responder,
                    } => {
                        let path_ref = path.as_str();
                        let mut results = Vec::with_capacity(keys.len());
                        for (idx, key) in keys {
                            let val = remote_db.json_store.json_get(&key, &[path_ref]);
                            results.push((idx, val));
                        }
                        let _ = responder.send(results);
                    }
                    _ => break,
                }
            }
        });

        // Populate local shard key
        db0.borrow_mut()
            .json_store
            .json_set(&k0, "$", r#"{"score":100}"#, false, false)
            .unwrap();

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let res = router
                .json_mget(
                    vec![k0.clone(), k1.clone(), Bytes::from("nonexistent")],
                    "$.score",
                )
                .await;
            assert_eq!(res.len(), 3);
            assert_eq!(res[0], Some("100".to_string()));
            assert_eq!(res[1], None);
            assert_eq!(res[2], None);
        });
    }

    #[test]
    fn test_router_del_keys_cross_shard_fanout() {
        let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(2);
        let senders = senders_mesh.remove(0);
        drop(senders_mesh);
        let rx1 = receivers.remove(1);

        let db0 = Rc::new(RefCell::new(ShardDb::new(9997)));
        let router = Router::new(
            0,
            2,
            9997,
            db0.clone(),
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        let mut k_shard0 = None;
        let mut k_shard1 = None;
        for i in 0..1000 {
            let k = Bytes::from(format!("del_k_{}", i));
            if target_shard(&k, 2) == 0 && k_shard0.is_none() {
                k_shard0 = Some(k);
            } else if target_shard(&k, 2) == 1 && k_shard1.is_none() {
                k_shard1 = Some(k);
            }
            if k_shard0.is_some() && k_shard1.is_some() {
                break;
            }
        }
        let k0 = k_shard0.unwrap();
        let k1 = k_shard1.unwrap();
        let rx1_clone = rx1;

        std::thread::spawn(move || {
            let mut remote_db = ShardDb::new(9997);
            while let Ok(msg) = rx1_clone.recv() {
                match msg {
                    ShardMessage::Set {
                        key,
                        value,
                        expire_in,
                        responder,
                    } => {
                        remote_db.set(key, value, expire_in);
                        let _ = responder.send(());
                    }
                    ShardMessage::FastSet { descriptor } => {
                        remote_db.set(
                            descriptor.key.clone(),
                            descriptor.value.clone(),
                            descriptor.expire_in,
                        );
                        descriptor.finish();
                    }
                    ShardMessage::DelKeys { keys, responder } => {
                        let mut count = 0;
                        for k in keys {
                            if remote_db.del(&k) {
                                count += 1;
                            }
                        }
                        let _ = responder.send(count);
                    }
                    ShardMessage::Exists { key, responder } => {
                        let ex = remote_db.exists(&key);
                        let _ = responder.send(ex);
                    }
                    _ => break,
                }
            }
        });

        // Set local key
        db0.borrow_mut().set(k0.clone(), Bytes::from("val0"), None);

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            // Set remote key via router
            router.set(k1.clone(), Bytes::from("val1"), None).await;

            assert!(router.exists(k0.clone()).await);
            assert!(router.exists(k1.clone()).await);

            // Delete both keys in parallel plus a non-existent key
            let deleted = router
                .del_keys(vec![k0.clone(), k1.clone(), Bytes::from("missing")])
                .await;
            assert_eq!(deleted, 2);

            assert!(!router.exists(k0).await);
            assert!(!router.exists(k1).await);
        });
    }

    #[test]
    fn test_router_del_keys_publishes_del_events() {
        let _flags = crate::connection::NOTIFY_FLAGS_TEST_LOCK.lock();
        let (mut senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(1);
        let db0 = Rc::new(RefCell::new(ShardDb::new(9996)));
        let pubsub = Rc::new(RefCell::new(crate::pubsub::PubSubHub::new()));
        let router = Router::new(
            0,
            1,
            9996,
            db0.clone(),
            senders_mesh.remove(0),
            None,
            pubsub.clone(),
            std::env::temp_dir(),
        );
        let (tx, rx) = flume::unbounded();
        pubsub
            .borrow_mut()
            .psubscribe(1, Bytes::from_static(b"__keyevent@0__:del"), tx, false);
        db0.borrow_mut()
            .set(Bytes::from("d1"), Bytes::from("v"), None);
        db0.borrow_mut()
            .set(Bytes::from("d2"), Bytes::from("v"), None);
        let prev = crate::connection::get_notify_keyspace_events_str();
        crate::connection::set_notify_keyspace_events_str("KEA");

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        let deleted = rt.block_on(router.del_keys(vec![
            Bytes::from("d1"),
            Bytes::from("missing"),
            Bytes::from("d2"),
        ]));
        crate::connection::set_notify_keyspace_events_str(&prev);

        assert_eq!(deleted, 2);
        let frames: Vec<String> = rx
            .try_iter()
            .map(|f| String::from_utf8_lossy(&f).to_string())
            .collect();
        assert_eq!(frames.len(), 2, "{frames:?}");
        assert!(frames[0].ends_with("\r\nd1\r\n"), "{frames:?}");
        assert!(frames[1].ends_with("\r\nd2\r\n"), "{frames:?}");
    }

    #[test]
    fn test_descriptor_pool_retention() {
        let num_shards = 4;
        let (mut senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(num_shards);
        let senders = senders_mesh.remove(0);
        let db0 = Rc::new(RefCell::new(crate::shard::ShardDb::new(9999)));
        let router = Router::new(
            0,
            num_shards,
            9999,
            db0,
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        let (notify_tx, _notify_rx) = router.acquire_notify_channel();

        // Acquire and release mget descriptors multiple times
        let desc1 = router.acquire_mget_descriptor(8, 2, notify_tx.clone());
        router.release_mget_descriptor(desc1);
        let desc2 = router.acquire_mget_descriptor(8, 2, notify_tx.clone());
        router.release_mget_descriptor(desc2);
        assert_eq!(router.mget_desc_pool.borrow().len(), 1);

        // Acquire and release mset descriptors multiple times
        let desc_mset1 = router.acquire_mset_descriptor(2, notify_tx.clone());
        router.release_mset_descriptor(desc_mset1);
        let desc_mset2 = router.acquire_mset_descriptor(2, notify_tx);
        router.release_mset_descriptor(desc_mset2);
        assert_eq!(router.mset_desc_pool.borrow().len(), 1);
    }

    #[test]
    fn test_begin_mget_mset_dispatch_without_blocking() {
        let num_shards = 4;
        let (mut senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(num_shards);
        let senders = senders_mesh.remove(0);
        let db0 = Rc::new(RefCell::new(crate::shard::ShardDb::new(9997)));
        let router = Router::new(
            0,
            num_shards,
            9997,
            db0,
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        // Pick one key owned by this shard and one owned by a peer shard.
        let mut local_key = None;
        let mut remote_key = None;
        for i in 0..1000 {
            let k = Bytes::from(format!("split_k_{i}"));
            if router.target_shard(&k) == 0 {
                if local_key.is_none() {
                    local_key = Some(k);
                }
            } else if remote_key.is_none() {
                remote_key = Some(k);
            }
            if local_key.is_some() && remote_key.is_some() {
                break;
            }
        }
        let local_key = local_key.expect("no key hashed to shard 0");
        let remote_key = remote_key.expect("no key hashed to a peer shard");

        router
            .local_db
            .borrow_mut()
            .set(local_key.clone(), Bytes::from_static(b"v1"), None);

        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            // All keys local: served inline, no in-flight handle, reply already written.
            let mut out = Vec::new();
            let inflight = router
                .begin_mget_resp(vec![local_key.clone()], &mut out)
                .await;
            assert!(inflight.is_none());
            assert_eq!(out, b"*1\r\n$2\r\nv1\r\n".to_vec());

            // Cross-shard: dispatch returns immediately with a pending shard and
            // writes nothing, leaving serialization to the gather phase.
            let mut out = Vec::new();
            let inflight = router
                .begin_mget_resp(vec![local_key, remote_key.clone()], &mut out)
                .await
                .expect("cross-shard mget must yield an in-flight handle");
            assert!(out.is_empty());
            assert_eq!(inflight.descriptor.pending.load(Ordering::Acquire), 1);
            assert_eq!(inflight.total_keys, 2);
            drop(inflight);

            // MSET dispatches the same way.
            let inflight = router
                .begin_mset(vec![(remote_key, Bytes::from_static(b"v2"))])
                .expect("cross-shard mset must yield an in-flight handle");
            assert_eq!(inflight.descriptor.pending.load(Ordering::Acquire), 1);
        });
    }

    #[test]
    fn test_dynamic_slot_routing_and_presence_table_multi_word() {
        let num_shards = 4;
        let (mut senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(num_shards);
        let senders = senders_mesh.remove(0);
        let db0 = Rc::new(RefCell::new(crate::shard::ShardDb::new(9995)));
        let mut router = Router::new(
            0,
            num_shards,
            9995,
            db0,
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        router.cluster_enabled = true;

        let key = b"my_cluster_key";
        let slot = key_slot(key);
        let default_target = router.target_shard(key);
        assert_eq!(default_target, router.target_shard_for_slot(slot));

        // Reassign slot owner dynamically (as in cluster migration)
        let new_owner = (default_target + 1) % num_shards;
        router.set_slot_owner(slot, new_owner);
        assert_eq!(router.target_shard(key), new_owner);
        assert_eq!(router.target_shard_for_slot(slot), new_owner);
        let (dyn_target, _) = router.target_shard_and_hash(key);
        assert_eq!(dyn_target, new_owner);

        // Multi-word presence table (>64 shards)
        let presence = crate::pubsub::ShardedPresenceTable::new();
        presence.add_subscriber(100, b"news");
        assert!(presence.is_shard_interested(100, b"news"));
        assert!(!presence.is_shard_interested(101, b"news"));
        presence.remove_subscriber(100, b"news");
        assert!(!presence.is_shard_interested(100, b"news"));
    }

    #[test]
    fn test_group_keys_by_shard_keeps_duplicates_and_owner() {
        let (mut senders_mesh, _receivers) = crate::mailbox::create_shard_mesh(3);
        let router = Router::new(
            0,
            3,
            9986,
            Rc::new(RefCell::new(ShardDb::new(9986))),
            senders_mesh.remove(0),
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        let mut keys: Vec<Bytes> = (0..30).map(|i| Bytes::from(format!("g{i}"))).collect();
        keys.push(keys[0].clone());
        let (local, remote) = router.group_keys_by_shard(&keys);
        assert!(local.iter().all(|k| router.target_shard(k) == 0));
        for (shard, ks) in &remote {
            assert_ne!(*shard, 0);
            assert!(!ks.is_empty());
            assert!(ks.iter().all(|k| router.target_shard(k) == *shard));
        }
        let total = local.len() + remote.iter().map(|(_, ks)| ks.len()).sum::<usize>();
        assert_eq!(total, keys.len());
    }

    #[test]
    fn test_execute_remote_many_sends_to_all_shards_before_waiting() {
        let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(3);
        let senders = senders_mesh.remove(0);
        drop(senders_mesh);
        let rx2 = receivers.remove(2);
        let rx1 = receivers.remove(1);
        let shard2_got_msg = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Fake shards reply with an integer. Shard 1 only answers 1 if shard 2
        // already holds its request, i.e. both were sent before any wait.
        let seen = shard2_got_msg.clone();
        let t1 = std::thread::spawn(move || {
            if let Ok(ShardMessage::Batch {
                items, responder, ..
            }) = rx1.recv()
            {
                let deadline = std::time::Instant::now() + Duration::from_secs(2);
                while !seen.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
                    std::thread::yield_now();
                }
                let n = if seen.load(Ordering::Acquire) { 1 } else { 0 };
                responder.write_slot(
                    0,
                    crate::shard::CompactResp::from_slice(format!(":{n}\r\n").as_bytes()),
                );
                responder.finish(items);
            }
        });
        let seen = shard2_got_msg.clone();
        let t2 = std::thread::spawn(move || {
            if let Ok(ShardMessage::Batch {
                items, responder, ..
            }) = rx2.recv()
            {
                seen.store(true, Ordering::Release);
                responder.write_slot(0, crate::shard::CompactResp::from_slice(b":7\r\n"));
                responder.finish(items);
            }
        });

        let router = Router::new(
            0,
            3,
            9985,
            Rc::new(RefCell::new(ShardDb::new(9985))),
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        let replies = rt.block_on(async {
            router
                .execute_remote_many(vec![
                    (1, Command::Exists(smallvec![Bytes::from("a")])),
                    (2, Command::Exists(smallvec![Bytes::from("b")])),
                ])
                .await
        });
        assert_eq!(replies, vec![b":1\r\n".to_vec(), b":7\r\n".to_vec()]);
        assert_eq!(router.remote_responder_pool.borrow().len(), 2);
        t1.join().unwrap();
        t2.join().unwrap();
    }

    #[test]
    fn test_sync_slot_tables_waits_for_earlier_slot_updates() {
        let (mut senders_mesh, mut receivers) = crate::mailbox::create_shard_mesh(3);
        let senders = senders_mesh.remove(0);
        let _rx0 = receivers.remove(0);
        let rx1 = receivers.remove(0);
        let rx2 = receivers.remove(0);

        let applied = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let spawn_worker =
            |rx: crate::mailbox::ShardReceiver, applied: Arc<std::sync::atomic::AtomicUsize>| {
                std::thread::spawn(move || {
                    while let Ok(msg) = rx.recv() {
                        match msg {
                            ShardMessage::SetSlotState { .. }
                            | ShardMessage::SetSlotOwner { .. } => {
                                std::thread::sleep(Duration::from_millis(5));
                                applied.fetch_add(1, Ordering::Release);
                            }
                            ShardMessage::SlotBarrier { responder } => {
                                let _ = responder.send(());
                                break;
                            }
                            _ => {}
                        }
                    }
                })
            };
        let t1 = spawn_worker(rx1, applied.clone());
        let t2 = spawn_worker(rx2, applied.clone());

        let router = Router::new(
            0,
            3,
            9984,
            Rc::new(RefCell::new(ShardDb::new(9984))),
            senders,
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            router.set_slot_owner(42, 1);
            router.set_slot_state(42, crate::shard::SlotState::Stable);
            router.sync_slot_tables().await;
        });
        assert_eq!(applied.load(Ordering::Acquire), 4);
        assert_eq!(router.notify_channel_pool.borrow().len(), 2);
        t1.join().unwrap();
        t2.join().unwrap();
    }
}
