use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
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

/// Calculates the target shard ID for a given key using CRC16 slot mapping.
#[inline]
pub fn target_shard(key: &[u8], num_shards: usize) -> usize {
    let slot = key_slot(key);
    slot_to_shard(slot, num_shards)
}

use std::sync::atomic::Ordering;

pub type MgetChannel = (
    flume::Sender<Vec<(usize, Option<Bytes>)>>,
    flume::Receiver<Vec<(usize, Option<Bytes>)>>,
);
pub type SetChannel = (flume::Sender<()>, flume::Receiver<()>);
pub type MsetChannel = (
    flume::Sender<Vec<(Bytes, Bytes)>>,
    flume::Receiver<Vec<(Bytes, Bytes)>>,
);

/// The router handles dispatching operations.
/// If the key belongs to the current shard, it directly touches `local_db` without locking.
/// If the key belongs to a peer shard, it routes the message across cores via the mesh.
#[derive(Clone)]
pub struct Router {
    pub shard_id: usize,
    pub num_shards: usize,
    pub port: u16,
    pub local_db: Rc<RefCell<ShardDb>>,
    pub senders: Vec<flume::Sender<ShardMessage>>,
    pub slot_states: Rc<RefCell<Vec<crate::shard::SlotState>>>,
    pub slot_owners: Rc<RefCell<Vec<usize>>>,
    pub aof: Option<Rc<RefCell<crate::aof::AofWriter>>>,
    pub pubsub: Rc<RefCell<crate::pubsub::PubSubHub>>,
    pub tx_lock: Rc<RefCell<Option<u64>>>,
    pub tx_waiters: Rc<RefCell<std::collections::VecDeque<(u64, flume::Sender<()>)>>>,
    pub is_saving: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub last_save_time: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub db_dir: std::path::PathBuf,
    pub is_auto_tiering: Rc<Cell<bool>>,
    pub mget_channel_pool: Rc<RefCell<Vec<Vec<MgetChannel>>>>,
    pub mset_channel_pool: Rc<RefCell<Vec<Vec<MsetChannel>>>>,
    pub set_channel_pool: Rc<RefCell<Vec<SetChannel>>>,
    pub get_channel_pool: Rc<RefCell<Vec<(flume::Sender<Option<Bytes>>, flume::Receiver<Option<Bytes>>)>>>,
    pub remote_responder_pool: Rc<RefCell<Vec<crate::connection::ResponderChannel>>>,
    pub mget_batch_pool: Rc<RefCell<Vec<Vec<Vec<(usize, Option<Bytes>)>>>>>,
    pub mset_batch_pool: Rc<RefCell<Vec<Vec<Vec<(Bytes, Bytes)>>>>>,
    pub tier_stats: std::sync::Arc<crate::tiering::TieringStats>,
}

impl Router {
    pub fn new(
        shard_id: usize,
        num_shards: usize,
        port: u16,
        local_db: Rc<RefCell<ShardDb>>,
        senders: Vec<flume::Sender<ShardMessage>>,
        aof: Option<Rc<RefCell<crate::aof::AofWriter>>>,
        pubsub: Rc<RefCell<crate::pubsub::PubSubHub>>,
        db_dir: std::path::PathBuf,
    ) -> Self {
        let mut slot_states = Vec::with_capacity(16384);
        let mut slot_owners = Vec::with_capacity(16384);
        for s in 0..16384 {
            slot_states.push(crate::shard::SlotState::Stable);
            slot_owners.push(slot_to_shard(s as u16, num_shards));
        }
        Self {
            shard_id,
            num_shards,
            port,
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
            mget_channel_pool: Rc::new(RefCell::new(Vec::new())),
            mset_channel_pool: Rc::new(RefCell::new(Vec::new())),
            set_channel_pool: Rc::new(RefCell::new(Vec::new())),
            get_channel_pool: Rc::new(RefCell::new(Vec::new())),
            remote_responder_pool: Rc::new(RefCell::new(Vec::new())),
            mget_batch_pool: Rc::new(RefCell::new(Vec::new())),
            mset_batch_pool: Rc::new(RefCell::new(Vec::new())),
            tier_stats: crate::tiering::get_tier_stats(port),
        }
    }

    pub fn target_shard_for_slot(&self, slot: u16) -> usize {
        self.slot_owners.borrow()[slot as usize]
    }

    pub fn target_shard(&self, key: &[u8]) -> usize {
        let slot = key_slot(key);
        self.target_shard_for_slot(slot)
    }

    pub fn check_slot_redirection(
        &self,
        slot: u16,
        key_exists: bool,
        asking: bool,
    ) -> Result<(), String> {
        let state = self.slot_states.borrow()[slot as usize].clone();
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
        self.slot_states.borrow_mut()[slot as usize] = state.clone();
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
        self.slot_states.borrow_mut()[slot as usize] = crate::shard::SlotState::Stable;
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
        let target = target_shard(key, self.num_shards);
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
            let target = target_shard(k, self.num_shards);
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

    pub async fn check_auto_tier(&self) {
        if self.is_auto_tiering.get() {
            return;
        }
        self.is_auto_tiering.set(true);

        let max_mem = crate::tiering::get_max_memory(self.port);
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
        let target = target_shard(key, self.num_shards);
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
        let max_mem = crate::tiering::get_max_memory(self.port);
        let offload_pct = crate::tiering::get_offload_threshold_pct(self.port);
        if max_mem > 0 {
            let used_mem = self.local_db.borrow().table.used_memory;
            let shard_threshold = (max_mem / self.num_shards.max(1) as u64) as usize;
            if used_mem >= (shard_threshold * offload_pct as usize) / 100 {
                return false;
            }
        }

        let target = target_shard(key, self.num_shards);
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
    pub async fn get_local_direct(&self, key: &Bytes) -> Option<Bytes> {
        let val = self.local_db.borrow_mut().get(key);
        if let Some(v) = val {
            self.tier_stats.ram_hits.fetch_add(1, Ordering::Relaxed);
            return Some(v);
        }
        if self.local_db.borrow_mut().table.is_tiered(key).is_some() {
            let max_mem = crate::tiering::get_max_memory(self.port);
            let offload_pct = crate::tiering::get_offload_threshold_pct(self.port);
            let is_constrained = if max_mem > 0 {
                let used_mem = self.local_db.borrow().table.used_memory;
                let shard_threshold = (max_mem / self.num_shards.max(1) as u64) as usize;
                used_mem >= (shard_threshold * offload_pct as usize) / 100
            } else {
                false
            };
            if is_constrained {
                if let Some(val) = self.stream_cold_read_local(key).await {
                    let stats = crate::tiering::get_tier_stats(self.port);
                    stats.streaming_reads.fetch_add(1, Ordering::Relaxed);
                    stats.ram_misses.fetch_add(1, Ordering::Relaxed);
                    return Some(val);
                }
            } else {
                self.load_local(key).await;
                return self.local_db.borrow_mut().get(key);
            }
        }
        None
    }

    pub async fn get(&self, key: Bytes) -> Option<Bytes> {
        let target = target_shard(&key, self.num_shards);
        if target == self.shard_id {
            self.get_local_direct(&key).await
        } else {
            // Unified remote shard Get with pooled channel
            let (tx, rx) = self
                .get_channel_pool
                .borrow_mut()
                .pop()
                .unwrap_or_else(|| flume::bounded(1));
            let msg = ShardMessage::Get {
                key,
                responder: tx.clone(),
            };
            let res = if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.ok().flatten()
            } else {
                None
            };
            self.get_channel_pool.borrow_mut().push((tx, rx));
            res
        }
    }

    pub async fn dump_key(
        &self,
        key: Bytes,
    ) -> Option<(crate::table::RudisValue, Option<Duration>)> {
        self.ensure_loaded(&key).await;
        let target = target_shard(&key, self.num_shards);
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
        let target = target_shard(&key, self.num_shards);
        if target == self.shard_id {
            self.local_db
                .borrow_mut()
                .set(key.clone(), value.clone(), expire_in);
            if let Some(aof) = &self.aof
                && let Some(bytes) = crate::aof::command_to_resp(&Command::Set {
                    key,
                    value,
                    expire_in,
                    condition: crate::resp::SetCondition::None,
                    get: false,
                    keepttl: false,
                    past_expired: false,
                })
            {
                aof.borrow_mut().append(&bytes);
            }
            let max_mem = crate::tiering::get_max_memory(self.port);
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
            let (tx, rx) = self
                .set_channel_pool
                .borrow_mut()
                .pop()
                .unwrap_or_else(|| flume::bounded(1));
            let msg = ShardMessage::Set {
                key,
                value,
                expire_in,
                responder: tx.clone(),
            };
            if self.senders[target].send(msg).is_ok() {
                let _ = rx.recv_async().await;
            }
            self.set_channel_pool.borrow_mut().push((tx, rx));
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
    }

    #[inline(always)]
    pub fn acquire_mget_channels(&self) -> Vec<MgetChannel> {
        if let Some(channels) = self.mget_channel_pool.borrow_mut().pop() {
            channels
        } else {
            (0..self.num_shards).map(|_| flume::bounded(1)).collect()
        }
    }

    #[inline(always)]
    pub fn release_mget_channels(&self, channels: Vec<MgetChannel>) {
        self.mget_channel_pool.borrow_mut().push(channels);
    }

    #[inline(always)]
    pub fn acquire_mset_channels(&self) -> Vec<MsetChannel> {
        if let Some(channels) = self.mset_channel_pool.borrow_mut().pop() {
            channels
        } else {
            (0..self.num_shards).map(|_| flume::bounded(1)).collect()
        }
    }

    #[inline(always)]
    pub fn release_mset_channels(&self, channels: Vec<MsetChannel>) {
        self.mset_channel_pool.borrow_mut().push(channels);
    }

    pub async fn mget(&self, keys: Vec<Bytes>) -> Vec<Option<Bytes>> {
        if keys.is_empty() {
            return Vec::new();
        }

        let total_keys = keys.len();
        let mut results: Vec<Option<Bytes>> = vec![None; total_keys];

        if self.num_shards <= 1 {
            for (idx, key) in keys.into_iter().enumerate() {
                results[idx] = self.get_local_direct(&key).await;
            }
            return results;
        }

        let mut remote_batches = self.mget_batch_pool.borrow_mut().pop().unwrap_or_else(|| {
            (0..self.num_shards).map(|_| Vec::new()).collect()
        });
        let mut local_keys = Vec::with_capacity(keys.len().min(16));
        let mut has_remote = false;

        // Partition local keys and remote keys without holding RefCell borrow across await
        {
            let owners = self.slot_owners.borrow();
            for (idx, key) in keys.into_iter().enumerate() {
                let slot = key_slot(&key);
                let target = owners[slot as usize];
                if target == self.shard_id {
                    local_keys.push((idx, key));
                } else {
                    has_remote = true;
                    remote_batches[target].push((idx, Some(key)));
                }
            }
        }

        // Execute local keys
        for (idx, key) in local_keys {
            results[idx] = self.get_local_direct(&key).await;
        }

        // Fast path: all keys are local - 0 channel operations
        if !has_remote {
            self.mget_batch_pool.borrow_mut().push(remote_batches);
            return results;
        }

        // Dispatch remote batches concurrently with pooled channels
        let channel_set = self.acquire_mget_channels();
        let mut sent_mask: u64 = 0;
        for target_shard in 0..self.num_shards {
            let items = std::mem::take(&mut remote_batches[target_shard]);
            if !items.is_empty() {
                let (tx, _) = &channel_set[target_shard];
                let msg = ShardMessage::Mget {
                    keys: items,
                    responder: tx.clone(),
                };
                if self.senders[target_shard].send(msg).is_ok() && target_shard < 64 {
                    sent_mask |= 1 << target_shard;
                }
            }
        }

        let mut pending_mask = sent_mask;
        for (target_shard, (_, rx)) in channel_set.iter().enumerate() {
            if target_shard < 64
                && (pending_mask & (1 << target_shard)) != 0
                && let Ok(mut shard_results) = rx.try_recv()
            {
                pending_mask &= !(1 << target_shard);
                for (idx, val) in &mut shard_results {
                    results[*idx] = val.take();
                }
                shard_results.clear();
                remote_batches[target_shard] = shard_results;
            }
        }

        if pending_mask != 0 {
            for (target_shard, (_, rx)) in channel_set.iter().enumerate() {
                if target_shard < 64
                    && (pending_mask & (1 << target_shard)) != 0
                    && let Ok(mut shard_results) = rx.recv_async().await
                {
                    for (idx, val) in &mut shard_results {
                        results[*idx] = val.take();
                    }
                    shard_results.clear();
                    remote_batches[target_shard] = shard_results;
                }
            }
        }
        self.mget_batch_pool.borrow_mut().push(remote_batches);

        self.release_mget_channels(channel_set);

        results
    }

    pub async fn mset(&self, pairs: Vec<(Bytes, Bytes)>) {
        if pairs.is_empty() {
            return;
        }

        if self.num_shards <= 1 {
            {
                let mut db = self.local_db.borrow_mut();
                for (k, v) in &pairs {
                    db.set(k.clone(), v.clone(), None);
                }
            }
            if let Some(aof) = &self.aof
                && let Some(bytes) =
                    crate::aof::command_to_resp(&crate::resp::Command::Mset(pairs))
                {
                    aof.borrow_mut().append(&bytes);
                }
            self.check_auto_tier_after_write();
            return;
        }

        let mut remote_batches = self.mset_batch_pool.borrow_mut().pop().unwrap_or_else(|| {
            (0..self.num_shards).map(|_| Vec::new()).collect()
        });
        let mut local_batch = Vec::with_capacity(pairs.len().min(16));
        let mut has_remote = false;

        {
            let owners = self.slot_owners.borrow();
            for (k, v) in pairs {
                let slot = key_slot(&k);
                let target = owners[slot as usize];
                if target == self.shard_id {
                    local_batch.push((k, v));
                } else {
                    has_remote = true;
                    remote_batches[target].push((k, v));
                }
            }
        }

        if !local_batch.is_empty() {
            {
                let mut db = self.local_db.borrow_mut();
                for (k, v) in &local_batch {
                    db.set(k.clone(), v.clone(), None);
                }
            }
            if let Some(aof) = &self.aof
                && let Some(bytes) =
                    crate::aof::command_to_resp(&crate::resp::Command::Mset(local_batch))
                {
                    aof.borrow_mut().append(&bytes);
                }
            self.check_auto_tier_after_write();
        }

        // Fast path: all pairs are local
        if !has_remote {
            self.mset_batch_pool.borrow_mut().push(remote_batches);
            return;
        }

        let channel_set = self.acquire_mset_channels();
        let mut sent_mask: u64 = 0;
        for target_shard in 0..self.num_shards {
            let items = std::mem::take(&mut remote_batches[target_shard]);
            if !items.is_empty() {
                let (tx, _) = &channel_set[target_shard];
                let msg = ShardMessage::Mset {
                    pairs: items,
                    responder: tx.clone(),
                };
                if self.senders[target_shard].send(msg).is_ok() && target_shard < 64 {
                    sent_mask |= 1 << target_shard;
                }
            }
        }

        let mut pending_mask = sent_mask;
        for (target_shard, (_, rx)) in channel_set.iter().enumerate() {
            if target_shard < 64
                && (pending_mask & (1 << target_shard)) != 0
                && let Ok(mut recycled_pairs) = rx.try_recv()
            {
                pending_mask &= !(1 << target_shard);
                recycled_pairs.clear();
                remote_batches[target_shard] = recycled_pairs;
            }
        }

        if pending_mask != 0 {
            for (target_shard, (_, rx)) in channel_set.iter().enumerate() {
                if target_shard < 64
                    && (pending_mask & (1 << target_shard)) != 0
                    && let Ok(mut recycled_pairs) = rx.recv_async().await
                {
                    recycled_pairs.clear();
                    remote_batches[target_shard] = recycled_pairs;
                }
            }
        }
        self.mset_batch_pool.borrow_mut().push(remote_batches);

        self.release_mset_channels(channel_set);
    }

    pub async fn del(&self, key: Bytes) -> bool {
        let target = target_shard(&key, self.num_shards);
        if target == self.shard_id {
            let deleted = self.local_db.borrow_mut().del(&key);
            if deleted
                && let Some(aof) = &self.aof
                && let Some(bytes) = crate::aof::command_to_resp(&Command::Del(vec![key]))
            {
                aof.borrow_mut().append(&bytes);
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

    pub async fn exists(&self, key: Bytes) -> bool {
        let target = target_shard(&key, self.num_shards);
        if target == self.shard_id {
            self.local_db.borrow_mut().exists(&key)
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
        let target = target_shard(&key, self.num_shards);
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
        let target = target_shard(&key, self.num_shards);
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
        let target = target_shard(&key, self.num_shards);
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
        let target = target_shard(&key, self.num_shards);
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
        let target = slot_to_shard(slot, self.num_shards);
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
    }

    pub async fn get_keys_in_slot(&self, slot: u16, count: usize) -> Vec<Bytes> {
        let target = slot_to_shard(slot, self.num_shards);
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
            let target = target_shard(key, self.num_shards);
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
            let target = target_shard(key, self.num_shards);
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
        let target = target_shard(key, self.num_shards);
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
        let target = target_shard(&key, self.num_shards);
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
                condition,
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
                "id={} addr={} laddr=127.0.0.1:{} fd=8 name={} age={} idle={} flags={} db=0 sub=0 psub=0 ssub=0 multi=-1 watch=0 qbuf=0 qbuf-free=20448 argv-mem=10 multi-mem=0 rbs=1024 rbp=0 obl=0 oll=0 omem=0 omem-shared=0 omem-unshared=0 tot-mem=22306 events=r cmd={} user=default redir=-1 resp=2 lib-name= lib-ver= io-thread=0 tot-net-in=0 tot-net-out=0 tot-cmds=0 read-events=0 avg-pipeline-len-sum=0 avg-pipeline-len-cnt=0\n",
                client.id,
                client.addr,
                self.port,
                client.name.as_deref().unwrap_or(""),
                age,
                idle,
                flags,
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

    pub async fn execute_remote(&self, target: usize, cmd: Command) -> Vec<u8> {
        let is_resp3 = crate::connection::CURRENT_CLIENT_RESP3.get();
        let (tx, rx) = self
            .remote_responder_pool
            .borrow_mut()
            .pop()
            .unwrap_or_else(|| flume::bounded(1));
        let msg = ShardMessage::Batch {
            items: vec![(0, cmd)],
            responder: tx.clone(),
            is_resp3,
        };
        let res = if self.senders[target].send(msg).is_ok()
            && let Ok(mut res) = rx.recv_async().await
            && let Some((_, out)) = res.pop()
        {
            out.into_vec()
        } else {
            b"-ERR internal shard routing error\r\n".to_vec()
        };
        self.remote_responder_pool.borrow_mut().push((tx, rx));
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
            if sid != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::Publish {
                    channel: channel.clone(),
                    message: message.clone(),
                    responder: tx,
                };
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
                let msg = ShardMessage::ExecuteReplicaCmd { cmd, responder: tx };
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
                                    cmd: Command::Flushall,
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
        let full_rdb = self.generate_full_rdb().await;
        static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let tmp_id = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let filename = self.db_dir.join("dump.rdb");
        let tmp_filename =
            self.db_dir
                .join(format!("dump.rdb.tmp.{}_{}", std::process::id(), tmp_id));
        let res = (|| -> Result<(), String> {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp_filename).map_err(|e| e.to_string())?;
            file.write_all(&full_rdb).map_err(|e| e.to_string())?;
            file.sync_all().map_err(|e| e.to_string())?;
            std::fs::rename(&tmp_filename, &filename).map_err(|e| e.to_string())?;
            Ok(())
        })();

        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.last_save_time.store(now_unix, Ordering::Relaxed);
        self.is_saving.store(false, Ordering::SeqCst);
        res
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
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..num_shards {
            let (tx, rx) = flume::unbounded::<ShardMessage>();
            senders.push(tx);
            receivers.push(rx);
        }

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
                    ShardMessage::Mset { pairs, responder } => {
                        for (k, v) in &pairs {
                            db.set(k.clone(), v.clone(), None);
                        }
                        let _ = responder.send(pairs);
                    }
                    ShardMessage::Mget { mut keys, responder } => {
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

        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
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
        let (tx, _rx) = flume::unbounded();
        let router = Router::new(
            0,
            1,
            9999,
            db0.clone(),
            vec![tx],
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
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
    fn test_channel_pool_acquire_and_release() {
        let db0 = Rc::new(RefCell::new(ShardDb::new(9999)));
        let (tx, _rx) = flume::unbounded();
        let router = Router::new(
            0,
            4,
            9999,
            db0.clone(),
            vec![tx.clone(), tx.clone(), tx.clone(), tx],
            None,
            Rc::new(RefCell::new(crate::pubsub::PubSubHub::new())),
            std::env::temp_dir(),
        );

        assert_eq!(router.mget_channel_pool.borrow().len(), 0);
        assert_eq!(router.mset_channel_pool.borrow().len(), 0);

        let ch1 = router.acquire_mget_channels();
        assert_eq!(ch1.len(), 4);
        assert_eq!(router.mget_channel_pool.borrow().len(), 0);

        router.release_mget_channels(ch1);
        assert_eq!(router.mget_channel_pool.borrow().len(), 1);

        let ch2 = router.acquire_mget_channels();
        assert_eq!(ch2.len(), 4);
        assert_eq!(router.mget_channel_pool.borrow().len(), 0);

        let mch1 = router.acquire_mset_channels();
        assert_eq!(mch1.len(), 4);
        router.release_mset_channels(mch1);
        assert_eq!(router.mset_channel_pool.borrow().len(), 1);
    }

    #[test]
    fn test_mget_single_pass_and_bitmask_fanout() {
        let (tx0, _rx0) = flume::unbounded();
        let (tx1, rx1) = flume::unbounded();
        let db0 = Rc::new(RefCell::new(ShardDb::new(9999)));
        let router = Router::new(
            0,
            2,
            9999,
            db0.clone(),
            vec![tx0, tx1],
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
                    ShardMessage::Mset { pairs, responder } => {
                        for (k, v) in &pairs {
                            remote_db.set(k.clone(), v.clone(), None);
                        }
                        let _ = responder.send(pairs);
                    }
                    ShardMessage::Mget { mut keys, responder } => {
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

        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let pairs = vec![
                (k0.clone(), Bytes::from("v0")),
                (k1.clone(), Bytes::from("v1")),
            ];
            router.mset(pairs).await;

            let keys = vec![
                k0.clone(),
                Bytes::from("nonexistent"),
                k1.clone(),
            ];
            let values = router.mget(keys).await;
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], Some(Bytes::from("v0")));
            assert_eq!(values[1], None);
            assert_eq!(values[2], Some(Bytes::from("v1")));
        });
    }

    #[test]
    fn test_mget_user_space_fast_harvest_try_recv() {
        let (tx0, _rx0) = flume::unbounded();
        let (tx1, rx1) = flume::unbounded();
        let db0 = Rc::new(RefCell::new(ShardDb::new(9998)));
        let router = Router::new(
            0,
            2,
            9998,
            db0.clone(),
            vec![tx0, tx1],
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
                    ShardMessage::Mget { mut keys, responder } => {
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

        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            db0.borrow_mut().set(k0.clone(), Bytes::from("local_harvest_val"), None);
            let keys = vec![k0.clone(), k1.clone()];
            let values = router.mget(keys).await;
            assert_eq!(values.len(), 2);
            assert!(values[0].is_some());
            assert!(values[1].is_some());
        });
    }

    #[test]
    fn test_router_channel_pool_reuse() {
        let (tx0, _rx0) = flume::unbounded();
        let (tx1, rx1) = flume::unbounded();

        let db0 = Rc::new(RefCell::new(ShardDb::new(9997)));
        let senders = vec![tx0, tx1];
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
                    ShardMessage::Get { key, responder } => {
                        let val = remote_db.get(&key);
                        let _ = responder.send(val);
                    }
                    ShardMessage::Batch { items, responder, .. } => {
                        let mut res = Vec::new();
                        for (idx, _cmd) in items {
                            res.push((idx, crate::shard::CompactResp::from_slice(b"+PONG\r\n")));
                        }
                        let _ = responder.send(res);
                    }
                    _ => break,
                }
            }
        });

        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            assert_eq!(router.get_channel_pool.borrow().len(), 0);
            let val1 = router.get(k1.clone()).await;
            assert_eq!(val1, Some(Bytes::from("pool_val")));
            assert_eq!(router.get_channel_pool.borrow().len(), 1);

            let val2 = router.get(k1.clone()).await;
            assert_eq!(val2, Some(Bytes::from("pool_val")));
            assert_eq!(router.get_channel_pool.borrow().len(), 1);

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
        let (tx0, _rx0) = flume::unbounded();
        let (tx1, rx1) = flume::unbounded();

        let db0 = Rc::new(RefCell::new(ShardDb::new(9996)));
        let senders = vec![tx0, tx1];
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
                    ShardMessage::Mget { mut keys, responder } => {
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

        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            assert_eq!(router.mset_batch_pool.borrow().len(), 0);
            assert_eq!(router.mget_batch_pool.borrow().len(), 0);

            // MSET scattered
            router.mset(vec![(k0.clone(), Bytes::from("v0")), (k1.clone(), Bytes::from("v1"))]).await;
            assert_eq!(router.mset_batch_pool.borrow().len(), 1);

            router.mset(vec![(k0.clone(), Bytes::from("v0_new")), (k1.clone(), Bytes::from("v1_new"))]).await;
            assert_eq!(router.mset_batch_pool.borrow().len(), 1);

            // MGET scattered
            let res1 = router.mget(vec![k0.clone(), k1.clone()]).await;
            assert_eq!(res1, vec![Some(Bytes::from("v0_new")), Some(Bytes::from("v1_new"))]);
            assert_eq!(router.mget_batch_pool.borrow().len(), 1);

            let res2 = router.mget(vec![k0.clone(), k1.clone()]).await;
            assert_eq!(res2, vec![Some(Bytes::from("v0_new")), Some(Bytes::from("v1_new"))]);
            assert_eq!(router.mget_batch_pool.borrow().len(), 1);
            assert!(router.mset_batch_pool.borrow()[0][1].capacity() > 0);
            assert!(router.mget_batch_pool.borrow()[0][1].capacity() > 0);
        });
    }

    #[test]
    fn test_router_mget_mset_in_place_recycling_zero_alloc() {
        let (tx0, _rx0) = flume::unbounded();
        let (tx1, rx1) = flume::unbounded();

        let db0 = Rc::new(RefCell::new(ShardDb::new(9994)));
        let senders = vec![tx0, tx1];
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
                    ShardMessage::Mget { mut keys, responder } => {
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

        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            for round in 0..10 {
                let v0 = Bytes::from(format!("val0_{}", round));
                let v1 = Bytes::from(format!("val1_{}", round));
                router.mset(vec![(k0.clone(), v0.clone()), (k1.clone(), v1.clone())]).await;
                let res = router.mget(vec![k0.clone(), k1.clone()]).await;
                assert_eq!(res, vec![Some(v0), Some(v1)]);
                assert!(router.mset_batch_pool.borrow()[0][1].capacity() >= 1);
                assert!(router.mget_batch_pool.borrow()[0][1].capacity() >= 1);
            }
        });
    }

    #[test]
    fn test_router_mget_mset_reactive_await_clean_harvest() {
        let (tx0, _rx0) = flume::unbounded();
        let (tx1, rx1) = flume::unbounded();

        let db0 = Rc::new(RefCell::new(ShardDb::new(9995)));
        let senders = vec![tx0, tx1];
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
                    ShardMessage::Mget { mut keys, responder } => {
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

        let mut rt = monoio::RuntimeBuilder::<monoio::IoUringDriver>::new()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            // MSET scattered
            router.mset(vec![(k0.clone(), Bytes::from("val_k0")), (k1.clone(), Bytes::from("val_k1"))]).await;

            // MGET scattered
            let res = router.mget(vec![k0.clone(), k1.clone()]).await;
            assert_eq!(res.len(), 2);
            assert_eq!(res[0], Some(Bytes::from("val_k0")));
            assert_eq!(res[1], Some(Bytes::from("val_k1")));
        });
    }
}

