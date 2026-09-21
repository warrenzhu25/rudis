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

/// Maps a slot (0..16383) to an owning shard (0..num_shards-1).
#[inline]
pub fn slot_to_shard(slot: u16, num_shards: usize) -> usize {
    if num_shards <= 1 {
        0
    } else {
        ((slot as usize) * num_shards) / 16384
    }
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
        (crate::table::hash_key(tag) as usize) % num_shards
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
        ((shard_hash as usize) % num_shards, key_hash)
    }
}

use std::sync::atomic::Ordering;

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
    pub last_save_time: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub db_dir: std::path::PathBuf,
    pub is_auto_tiering: Rc<Cell<bool>>,
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

impl Router {
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
            is_saving: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            last_save_time: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            db_dir,
            is_auto_tiering: Rc::new(Cell::new(false)),
            notify_channel_pool: Rc::new(RefCell::new(Vec::new())),
            remote_responder_pool: Rc::new(RefCell::new(Vec::new())),
            mget_batch_pool: Rc::new(RefCell::new(Vec::new())),
            mset_batch_pool: Rc::new(RefCell::new(Vec::new())),
            mget_desc_pool: Rc::new(RefCell::new(Vec::new())),
            mset_desc_pool: Rc::new(RefCell::new(Vec::new())),
            pubsub_responder_pool: Rc::new(RefCell::new(Vec::new())),
            presence_table: crate::pubsub::get_presence_table(port),
            tier_stats: crate::tiering::get_tier_stats(port),
        }
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
        if self.cluster_enabled || crate::cluster::HAS_ACTIVE_CLUSTER.load(Ordering::Relaxed) {
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
            if db.table.set_tiered_pointer(key, ptr) {
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

        let (record_key, val_payload) = match crate::tiering::read_tiered_record(
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
        if db.table.restore_tiered_value(&record_key, val) {
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
            if db.table.set_cooled_pointer(key, ptr) {
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
            crate::table::RudisValue::String(s) => Some(s),
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

    pub async fn get_total_used_memory(&self) -> usize {
        let mut total = self.local_db.borrow().table.used_memory;
        for s in 0..self.num_shards {
            if s != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::GetUsedMemory { responder: tx };
                if self.senders[s].send(msg).is_ok() {
                    total += rx.recv_async().await.unwrap_or(0);
                }
            }
        }
        total
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
        let used_mem = self.local_db.borrow().table.used_memory;
        let shard_threshold = (max_mem / self.num_shards.max(1) as u64) as usize;
        used_mem >= (shard_threshold * offload_pct as usize) / 100
    }

    pub async fn check_auto_tier(&self) {
        if self.is_auto_tiering.get() {
            return;
        }
        self.is_auto_tiering.set(true);

        let max_mem = self.tier_stats.max_memory.load(Ordering::Relaxed);
        if max_mem == 0 {
            self.is_auto_tiering.set(false);
            return;
        }
        let shard_max_mem = (max_mem / self.num_shards.max(1) as u64).max(1) as usize;

        let used = self.local_db.borrow().table.used_memory;
        if used <= shard_max_mem {
            self.is_auto_tiering.set(false);
            return;
        }

        // Phase 1: Instant Zero-I/O Decommit of all Cooled keys
        let decommitted = self.decommit_local(None);
        if decommitted > 0 {
            let used_after = self.local_db.borrow().table.used_memory;
            if used_after <= shard_max_mem {
                self.is_auto_tiering.set(false);
                return;
            }
        }

        // Phase 2: Spill Hot keys to NVMe disk until under shard_max_mem
        let hot_keys = self.local_db.borrow_mut().table.get_hot_keys_for_spill(256);
        for k in hot_keys {
            let _ = self.spill_local_internal(&k, false).await;
            if self.local_db.borrow().table.used_memory <= shard_max_mem {
                break;
            }
        }

        let tm = self.local_db.borrow().tier_manager.clone();
        if let Some(tm) = tm {
            let _ = tm.flush_active_bin().await;
        }

        self.is_auto_tiering.set(false);
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
        let target = self.target_shard(&key);
        if target == self.shard_id {
            self.get_local_direct(&key).await
        } else {
            let (tx, rx) = self.acquire_notify_channel();
            let desc = Arc::new(crate::mailbox::FastGetDescriptor::new(key, tx.clone()));
            let msg = ShardMessage::FastGet {
                descriptor: desc.clone(),
            };
            let res = if self.senders[target].send(msg).is_ok() {
                if !desc.done.load(Ordering::Acquire) {
                    for _ in 0..32 {
                        std::hint::spin_loop();
                        if desc.done.load(Ordering::Acquire) {
                            break;
                        }
                    }
                    if !desc.done.load(Ordering::Acquire) {
                        let _ = rx.recv_async().await;
                    }
                }
                while rx.try_recv().is_ok() {}
                unsafe { (*desc.val.get()).take() }
            } else {
                None
            };
            self.release_notify_channel(tx, rx);
            res
        }
    }

    pub async fn dump_key(
        &self,
        key: Bytes,
    ) -> Option<(crate::table::RudisValue, Option<Duration>)> {
        self.ensure_loaded(&key).await;
        let target = self.target_shard(&key);
        if target == self.shard_id {
            self.local_db.borrow_mut().get_entry(&key)
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::DumpKey { key, responder: tx };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.ok().flatten()
            } else {
                None
            }
        }
    }

    pub async fn set(&self, key: Bytes, value: Bytes, expire_in: Option<Duration>) {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            if let Some(aof) = &self.aof
                && let Some(bytes) = crate::aof::command_to_resp(&Command::Set {
                    key: key.clone(),
                    value: value.clone(),
                    expire_in,
                    condition: crate::resp::SetCondition::None,
                    get: false,
                    keepttl: false,
                    past_expired: false,
                })
            {
                aof.borrow_mut().append(&bytes);
            }
            self.local_db.borrow_mut().set(key, value, expire_in);
            let max_mem = self.tier_stats.max_memory.load(Ordering::Relaxed);
            if max_mem > 0 {
                let used = self.local_db.borrow().table.used_memory;
                let shard_max_mem = (max_mem / self.num_shards.max(1) as u64) as usize;
                if used > shard_max_mem && !self.is_auto_tiering.get() {
                    let decommitted = self.decommit_local(None);
                    let used_after = self.local_db.borrow().table.used_memory;
                    if (decommitted == 0 || used_after > shard_max_mem)
                        && !self.is_auto_tiering.get()
                    {
                        let r = self.clone();
                        monoio::spawn(async move {
                            r.check_auto_tier().await;
                        });
                    }
                }
            }
        } else {
            let (tx, rx) = self.acquire_notify_channel();
            let desc = Arc::new(crate::mailbox::FastSetDescriptor::new(
                key,
                value,
                expire_in,
                tx.clone(),
            ));
            let msg = ShardMessage::FastSet {
                descriptor: desc.clone(),
            };
            if self.senders[target].send(msg).is_ok() && !desc.done.load(Ordering::Acquire) {
                for _ in 0..32 {
                    std::hint::spin_loop();
                    if desc.done.load(Ordering::Acquire) {
                        break;
                    }
                }
                if !desc.done.load(Ordering::Acquire) {
                    let _ = rx.recv_async().await;
                }
                while rx.try_recv().is_ok() {}
            }
            self.release_notify_channel(tx, rx);
        }
    }

    #[inline(always)]
    pub(crate) fn check_auto_tier_after_write(&self) {
        let max_mem = self.tier_stats.max_memory.load(Ordering::Relaxed);
        if max_mem > 0 {
            let used = self.local_db.borrow().table.used_memory;
            let shard_max_mem = (max_mem / self.num_shards.max(1) as u64) as usize;
            if used > shard_max_mem && !self.is_auto_tiering.get() {
                let decommitted = self.decommit_local(None);
                let used_after = self.local_db.borrow().table.used_memory;
                if (decommitted == 0 || used_after > shard_max_mem) && !self.is_auto_tiering.get() {
                    let r = self.clone();
                    monoio::spawn(async move {
                        r.check_auto_tier().await;
                    });
                }

                // If memory is still above threshold after decommit/offload, evict keys based on maxmemory-policy
                let policy = crate::connection::get_max_memory_policy();
                if policy != "noeviction" {
                    let mut db = self.local_db.borrow_mut();
                    let mut attempts = 0;
                    while db.table.used_memory > shard_max_mem && attempts < 32 {
                        attempts += 1;
                        if db.table.try_evict_one_key(&policy).is_none() {
                            break;
                        }
                    }
                }
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
                for _ in 0..128 {
                    if Arc::strong_count(&desc) == 1 {
                        break;
                    }
                    std::hint::spin_loop();
                }
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
                for _ in 0..128 {
                    if Arc::strong_count(&desc) == 1 {
                        break;
                    }
                    std::hint::spin_loop();
                }
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
        descriptor.wait_completed(64, &notify_rx).await;

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
        descriptor.wait_completed(256, &notify_rx).await;

        let recycled = descriptor.take_recycled_keys();
        self.mget_batch_pool.borrow_mut().push(recycled);
        self.release_notify_channel(notify_tx, notify_rx);

        let mut total_bytes = 16;
        unsafe {
            for i in 0..total_keys {
                if let Some(v) = &*descriptor.results[i].get() {
                    total_bytes += v.len() + 16;
                } else {
                    total_bytes += 5;
                }
            }
        }
        out.reserve(total_bytes);
        crate::connection::write_resp_array_header(out, total_keys);
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
            if let Some(aof) = &self.aof
                && let Some(bytes) = crate::aof::command_to_resp(&crate::resp::Command::Mset(pairs))
            {
                aof.borrow_mut().append(&bytes);
            }
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
                if let Some(aof) = &self.aof
                    && let Some(bytes) = crate::aof::command_to_resp(&crate::resp::Command::Mset(
                        local_batch.clone(),
                    ))
                {
                    aof.borrow_mut().append(&bytes);
                }
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
            if let Some(aof) = &self.aof
                && let Some(bytes) =
                    crate::aof::command_to_resp(&crate::resp::Command::Mset(local_batch.clone()))
            {
                aof.borrow_mut().append(&bytes);
            }
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

        descriptor.wait_completed(64, &notify_rx).await;

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
            let mut db = self.local_db.borrow_mut();
            let deleted = db.del(&key);
            if deleted {
                db.delete_document_local(&String::from_utf8_lossy(&key));
                if let Some(aof) = &self.aof
                    && let Some(bytes) = crate::aof::command_to_resp(&Command::Del(smallvec![key]))
                {
                    aof.borrow_mut().append(&bytes);
                }
            }
            deleted
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
                        deleted_keys.push(k);
                    }
                }
            }
            if count > 0
                && let Some(aof) = &self.aof
                && let Some(bytes) =
                    crate::aof::command_to_resp(&Command::Del(SmallVec::from_vec(deleted_keys)))
            {
                aof.borrow_mut().append(&bytes);
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
                        deleted_local.push(k);
                    }
                }
            }
            if !deleted_local.is_empty()
                && let Some(aof) = &self.aof
                && let Some(bytes) =
                    crate::aof::command_to_resp(&Command::Del(SmallVec::from_vec(deleted_local)))
            {
                aof.borrow_mut().append(&bytes);
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
            if res.is_ok()
                && let Some(aof) = &self.aof
                && let Some(bytes) = crate::aof::command_to_resp(&Command::IncrBy(key, delta))
            {
                aof.borrow_mut().append(&bytes);
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

    pub async fn expire(&self, key: Bytes, duration: Duration) -> bool {
        let target = self.target_shard(&key);
        if target == self.shard_id {
            let res = self.local_db.borrow_mut().expire(&key, duration);
            if res
                && let Some(aof) = &self.aof
                && let Some(bytes) = crate::aof::command_to_resp(&Command::Expire(key, duration))
            {
                aof.borrow_mut().append(&bytes);
            }
            res
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Expire {
                key,
                duration,
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
            if res
                && let Some(aof) = &self.aof
                && let Some(bytes) = crate::aof::command_to_resp(&Command::Persist(key))
            {
                aof.borrow_mut().append(&bytes);
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
        if self.cluster_enabled || crate::cluster::HAS_ACTIVE_CLUSTER.load(Ordering::Relaxed) {
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
        if self.cluster_enabled || crate::cluster::HAS_ACTIVE_CLUSTER.load(Ordering::Relaxed) {
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
            if !filter_ids.is_empty() && !filter_ids.contains(&client.id) {
                continue;
            }
            let age = now.duration_since(client.connected_at).as_secs();
            let idle = now.duration_since(client.last_active).as_secs();
            let is_blocked = crate::block::get_block_hub_for_port(self.port)
                .lock()
                .unwrap()
                .is_blocked(client.id);
            let flags = if is_blocked { "b" } else { "N" };
            out.push_str(&format!(
                "id={} addr={} laddr=127.0.0.1:{} fd=8 name={} age={} idle={} flags={} db=0 sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=20448 argv-mem=10 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem={} omem-shared=0 omem-unshared=0 tot-mem=22306 events=r cmd={} user=default redir=-1 resp=2 lib-name= lib-ver= io-thread=0 tot-net-in=0 tot-net-out=0 tot-cmds=0 read-events=0 avg-pipeline-len-sum=0 avg-pipeline-len-cnt=0\n",
                client.id,
                client.addr,
                self.port,
                client.name.as_deref().unwrap_or(""),
                age,
                idle,
                flags,
                client.omem,
                client.last_cmd.to_lowercase()
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
        if let Ok(mut map) = crate::connection::CMD_STATS.write() {
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
        responder.prepare(&mut slot as *mut _);
        let h = crate::connection::cmd_primary_key(&cmd)
            .map(|k| crate::table::hash_key(k))
            .unwrap_or(0);
        let msg = ShardMessage::Batch {
            items: vec![(0, h, cmd)],
            responder: responder.clone(),
            is_resp3,
        };
        let res = if self.senders[target].send(msg).is_ok() {
            let mut completed = false;
            for _spin in 0..48 {
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

    pub async fn dbsize(&self) -> usize {
        let mut total = self.local_db.borrow_mut().dbsize();
        for sid in 0..self.num_shards {
            if sid != self.shard_id {
                let res = self.execute_remote(sid, Command::Dbsize).await;
                if let Ok(s) = std::str::from_utf8(&res)
                    && let Some(num_str) = s.strip_prefix(':').and_then(|x| x.split("\r\n").next())
                    && let Ok(n) = num_str.parse::<usize>()
                {
                    total += n;
                }
            }
        }
        total
    }

    pub async fn flushdb(&self) {
        self.local_db.borrow_mut().flushdb();
        if let Some(aof) = &self.aof
            && let Some(bytes) = crate::aof::command_to_resp(&Command::Flushdb)
        {
            aof.borrow_mut().append(&bytes);
        }
        for sid in 0..self.num_shards {
            if sid != self.shard_id {
                let _ = self.execute_remote(sid, Command::Flushdb).await;
            }
        }
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
        let mut total = self.pubsub.borrow().numpat();
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
            if let Ok(count) = rx.recv_async().await {
                total += count;
            }
        }
        total
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

        // Initialize on local shard
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

        // Also register in global search registry
        let _ = crate::search::create_search_index(schema);

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

    pub async fn ft_search(
        &self,
        index: &str,
        ast: &crate::search::QueryAst,
        opts: &crate::search::SearchOptions,
    ) -> (usize, Vec<crate::search::SearchHit>) {
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
                let idx = idx_arc.read().unwrap();
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

        let effective_limit = if let Some(k) = ast.knn_k() {
            opts.limit.min(k)
        } else {
            opts.limit
        };

        let reported_total = if let Some(k) = ast.knn_k() {
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
    ) -> (u64, Vec<Bytes>) {
        let shard_id = (cursor >> 32) as usize;
        let slot_idx = (cursor & 0xFFFF_FFFF) as usize;
        if shard_id >= self.num_shards {
            return (0, Vec::new());
        }

        let (next_slot, keys) = if shard_id == self.shard_id {
            self.local_db.borrow_mut().scan(slot_idx, pattern, count)
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Scan {
                slot: slot_idx,
                pattern: pattern.map(Bytes::copy_from_slice),
                count,
                responder: tx,
            };
            if self.senders[shard_id].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or((0, Vec::new()))
            } else {
                (0, Vec::new())
            }
        };

        let next_cursor = if next_slot == 0 {
            if shard_id + 1 < self.num_shards {
                ((shard_id + 1) as u64) << 32
            } else {
                0
            }
        } else {
            ((shard_id as u64) << 32) | (next_slot as u64)
        };

        (next_cursor, keys)
    }

    pub async fn random_key(&self) -> Option<Bytes> {
        if let Some(k) = self.local_db.borrow_mut().random_key() {
            return Some(k);
        }
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                if sender
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
        for &sid in shard_ids.iter().rev() {
            if sid == self.shard_id {
                let mut lock = self.tx_lock.borrow_mut();
                if *lock == Some(tx_id) {
                    if let Some((next_tx, next_resp)) = self.tx_waiters.borrow_mut().pop_front() {
                        *lock = Some(next_tx);
                        let _ = next_resp.send(());
                    } else {
                        *lock = None;
                    }
                }
            } else {
                let _ = self.senders[sid].send(ShardMessage::ReleaseTxLock { tx_id });
            }
        }
    }

    pub fn lastsave(&self) -> u64 {
        let ts = self.last_save_time.load(Ordering::Relaxed);
        if ts == 0 {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let _ =
                self.last_save_time
                    .compare_exchange(0, now, Ordering::Relaxed, Ordering::Relaxed);
            self.last_save_time.load(Ordering::Relaxed)
        } else {
            ts
        }
    }

    pub async fn save_rdb(&self) -> Result<(), String> {
        if self
            .is_saving
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err("Background save already in progress".to_string());
        }
        self.sync_aof().await;
        self.perform_save_rdb().await
    }

    pub async fn bgsave(&self) -> Result<(), String> {
        if self
            .is_saving
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err("Background save already in progress".to_string());
        }
        let router_clone = self.clone();
        monoio::spawn(async move {
            router_clone.sync_aof().await;
            let _ = router_clone.perform_save_rdb().await;
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
        let router_clone = self.clone();
        monoio::spawn(async move {
            let _ = router_clone.perform_rewrite_aof().await;
        });
        Ok(())
    }

    pub async fn perform_rewrite_aof(&self) -> Result<usize, String> {
        self.sync_aof().await;
        let mut total_rewritten = {
            let mut db = self.local_db.borrow_mut();
            crate::aof::rewrite_shard_aof(&mut db, &self.db_dir, self.shard_id)
                .map_err(|e| e.to_string())?
        };
        if let Some(aof) = &self.aof {
            let _ = crate::aof::AofWriter::reopen_after_rewrite(aof).await;
        }

        // Remote shards rewritten sequentially one-by-one to eliminate concurrent 15-shard I/O and memory spikes
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                if sender
                    .send(ShardMessage::RewriteAof {
                        dir: self.db_dir.clone(),
                        shard_id: sid,
                        responder: tx,
                    })
                    .is_ok()
                    && let Ok(Ok(count)) = rx.recv_async().await
                {
                    total_rewritten += count;
                }
            }
        }

        self.is_saving.store(false, Ordering::SeqCst);
        Ok(total_rewritten)
    }

    pub async fn generate_full_rdb(&self) -> Vec<u8> {
        let mut full_rdb = Vec::new();
        full_rdb.extend_from_slice(b"REDIS0011");
        full_rdb.extend_from_slice(&[0xFE, 0x00]);

        // Local shard chunk
        self.local_db.borrow_mut().save_rdb_chunk(&mut full_rdb);

        // Remote shard chunks
        let mut responders = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                if sender
                    .send(ShardMessage::SaveRdbChunk { responder: tx })
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

    pub async fn restore_rdb_bytes(&self, data: Bytes) {
        let _ = crate::table::load_rdb_bytes(
            &data,
            &mut self.local_db.borrow_mut(),
            self.shard_id,
            self.num_shards,
        );
        let mut responders = Vec::new();
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                if sender
                    .send(ShardMessage::RestoreRdbChunk {
                        data: data.clone(),
                        responder: tx,
                    })
                    .is_ok()
                {
                    responders.push(rx);
                }
            }
        }
        for rx in responders {
            let _ = rx.recv_async().await;
        }
    }

    pub async fn execute_replica_command(&self, cmd: Command) {
        if let Some(target) = crate::connection::target_shard_of_cmd(&cmd, self.num_shards) {
            if target == self.shard_id {
                let mut dummy_out = Vec::new();
                crate::connection::execute_local_command(
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

    pub async fn perform_save_rdb(&self) -> Result<(), String> {
        static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let tmp_id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let filename = self.db_dir.join("dump.rdb");
        let tmp_filename =
            self.db_dir
                .join(format!("dump.rdb.tmp.{}_{}", std::process::id(), tmp_id));

        use std::io::Write;
        let mut file = std::fs::File::create(&tmp_filename).map_err(|e| e.to_string())?;

        let header = b"REDIS0011\xFE\x00";
        file.write_all(header).map_err(|e| e.to_string())?;
        let mut crc = crate::table::crc64(header);

        // Local shard chunk
        let mut local_chunk = Vec::new();
        self.local_db.borrow_mut().save_rdb_chunk(&mut local_chunk);
        if !local_chunk.is_empty() {
            crc = crate::table::crc64_update(crc, &local_chunk);
            file.write_all(&local_chunk).map_err(|e| e.to_string())?;
        }
        drop(local_chunk);

        // Remote shard chunks streamed sequentially one-by-one:
        // Only one shard ever holds a serialized chunk in memory at any time.
        for (sid, sender) in self.senders.iter().enumerate() {
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                if sender
                    .send(ShardMessage::SaveRdbChunk { responder: tx })
                    .is_ok()
                    && let Ok(chunk) = rx.recv_async().await
                {
                    if !chunk.is_empty() {
                        crc = crate::table::crc64_update(crc, &chunk);
                        file.write_all(&chunk).map_err(|e| e.to_string())?;
                    }
                    drop(chunk);
                }
            }
        }

        let eof = [0xFF];
        crc = crate::table::crc64_update(crc, &eof);
        file.write_all(&eof).map_err(|e| e.to_string())?;
        file.write_all(&crc.to_le_bytes())
            .map_err(|e| e.to_string())?;
        file.sync_all().map_err(|e| e.to_string())?;
        std::fs::rename(&tmp_filename, &filename).map_err(|e| e.to_string())?;
        let _ = crate::aof::sync_parent_dir(std::path::Path::new(&filename));

        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_save_time.store(now_unix, Ordering::Relaxed);
        self.is_saving.store(false, Ordering::SeqCst);
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

        let rx1_clone = rx1.clone();
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

        let rx1_clone = rx1.clone();
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
        let rx1_clone = rx1.clone();
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
            assert_eq!(router.notify_channel_pool.borrow().len(), 0);
            let val1 = router.get(k1.clone()).await;
            assert_eq!(val1, Some(Bytes::from("pool_val")));
            assert_eq!(router.notify_channel_pool.borrow().len(), 1);

            let val2 = router.get(k1.clone()).await;
            assert_eq!(val2, Some(Bytes::from("pool_val")));
            assert_eq!(router.notify_channel_pool.borrow().len(), 1);

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
        let rx1_clone = rx1.clone();

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
        let rx1_clone = rx1.clone();

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
        let rx1_clone = rx1.clone();

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
        let rx1_clone = rx1.clone();

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
        let rx1_clone = rx1.clone();

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
}
