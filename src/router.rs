use bytes::Bytes;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::Duration;

use crate::resp::Command;
use crate::shard::{ShardDb, ShardMessage};

/// Extracts the hash tag from a key if present (e.g. "{user:1}:profile" -> "user:1").
#[inline]
pub fn extract_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&b| b == b'{') {
        if let Some(close) = key[open + 1..].iter().position(|&b| b == b'}') {
            if close > 0 {
                return &key[open + 1..open + 1 + close];
            }
        }
    }
    key
}

/// Calculates the Redis Cluster 16384 slot for a given key.
#[inline]
pub fn key_slot(key: &[u8]) -> u16 {
    let tag = extract_hash_tag(key);
    (crc16::State::<crc16::XMODEM>::calculate(tag) % 16384) as u16
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
        if self.local_db.borrow_mut().table.is_cooled(key).is_some() {
            return self.decommit_local(Some(key)) > 0;
        }

        let (entry_data, val_type) = match self.local_db.borrow_mut().table.get_value_for_spill(key) {
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

        let mut db = self.local_db.borrow_mut();
        let stats = db.tier_manager.as_ref().map(|tm| tm.stats.clone());
        if db.table.set_tiered_pointer(key, ptr) {
            if let Some(stats) = stats {
                stats.tiered_keys.fetch_add(1, Ordering::Relaxed);
                stats.tiered_bytes.fetch_add(ptr.length as u64, Ordering::Relaxed);
                stats.ram_saved_bytes.fetch_add(entry_data.len() as u64, Ordering::Relaxed);
            }
            drop(db);
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
        ).await {
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
            tm.stats.ram_saved_bytes.fetch_sub(val_payload.len() as u64, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub async fn cool_local(&self, key: &[u8]) -> bool {
        if self.local_db.borrow_mut().table.is_tiered(key).is_some()
            || self.local_db.borrow_mut().table.is_cooled(key).is_some()
        {
            return false;
        }

        let (entry_data, val_type) = match self.local_db.borrow_mut().table.get_value_for_spill(key) {
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

        let mut db = self.local_db.borrow_mut();
        let stats = db.tier_manager.as_ref().map(|tm| tm.stats.clone());
        if db.table.set_cooled_pointer(key, ptr) {
            if let Some(stats) = stats {
                stats.cooled_keys.fetch_add(1, Ordering::Relaxed);
                stats.tiered_bytes.fetch_add(ptr.length as u64, Ordering::Relaxed);
            }
            drop(db);
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
        ).await.ok()?;
        let (val, _) = crate::table::RudisTable::deserialize_val_payload(&val_payload).ok()?;
        match val {
            crate::table::RudisValue::String(s) => Some(s),
            crate::table::RudisValue::Int(n) => Some(Bytes::from(crate::table::RudisTable::format_i64(n))),
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
                stats.cooled_keys.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                stats.tiered_keys.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                stats.ram_saved_bytes.fetch_add(freed as u64, std::sync::atomic::Ordering::Relaxed);
                stats.decommit_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                1
            } else {
                0
            }
        } else {
            let (count, freed) = db.table.decommit_all_cooled();
            if count > 0 {
                stats.cooled_keys.fetch_sub(count as u64, std::sync::atomic::Ordering::Relaxed);
                stats.tiered_keys.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
                stats.ram_saved_bytes.fetch_add(freed, std::sync::atomic::Ordering::Relaxed);
                stats.decommit_count.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
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

    pub async fn get(&self, key: Bytes) -> Option<Bytes> {
        let target = target_shard(&key, self.num_shards);
        if target == self.shard_id {
            // 1. Fast DRAM path
            let val = self.local_db.borrow_mut().get(&key);
            if let Some(v) = val {
                let stats = crate::tiering::get_tier_stats(self.port);
                stats.ram_hits.fetch_add(1, Ordering::Relaxed);
                return Some(v);
            }
            // 2. Cold tiered path
            if self.local_db.borrow_mut().table.is_tiered(&key).is_some() {
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
                    if let Some(val) = self.stream_cold_read_local(&key).await {
                        let stats = crate::tiering::get_tier_stats(self.port);
                        stats.streaming_reads.fetch_add(1, Ordering::Relaxed);
                        stats.ram_misses.fetch_add(1, Ordering::Relaxed);
                        return Some(val);
                    }
                } else {
                    self.load_local(&key).await;
                    return self.local_db.borrow_mut().get(&key);
                }
            }
            None
        } else {
            // Unified remote shard Get (handles DRAM and tiered in a single message)
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Get { key, responder: tx };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.ok().flatten()
            } else {
                None
            }
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
            if let Some(aof) = &self.aof {
                if let Some(bytes) = crate::aof::command_to_resp(&Command::Set {
                    key,
                    value,
                    expire_in,
                }) {
                    aof.borrow_mut().append(&bytes);
                }
            }
            let max_mem = crate::tiering::get_max_memory(self.port);
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
                }
            }
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::Set {
                key,
                value,
                expire_in,
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                let _ = rx.recv_async().await;
            }
        }
    }

    pub async fn del(&self, key: Bytes) -> bool {
        let target = target_shard(&key, self.num_shards);
        if target == self.shard_id {
            let deleted = self.local_db.borrow_mut().del(&key);
            if deleted {
                if let Some(aof) = &self.aof {
                    if let Some(bytes) = crate::aof::command_to_resp(&Command::Del(vec![key])) {
                        aof.borrow_mut().append(&bytes);
                    }
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
            if res.is_ok() {
                if let Some(aof) = &self.aof {
                    if let Some(bytes) = crate::aof::command_to_resp(&Command::IncrBy(key, delta)) {
                        aof.borrow_mut().append(&bytes);
                    }
                }
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
            if res {
                if let Some(aof) = &self.aof {
                    if let Some(bytes) =
                        crate::aof::command_to_resp(&Command::Expire(key, duration))
                    {
                        aof.borrow_mut().append(&bytes);
                    }
                }
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
            if res {
                if let Some(aof) = &self.aof {
                    if let Some(bytes) = crate::aof::command_to_resp(&Command::Persist(key)) {
                        aof.borrow_mut().append(&bytes);
                    }
                }
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

    pub async fn client_list(
        &self,
        local_registry: &RefCell<hashbrown::HashMap<u64, crate::connection::ClientInfo>>,
    ) -> String {
        let mut out = String::new();
        let now = std::time::Instant::now();
        for client in local_registry.borrow().values() {
            let age = now.duration_since(client.connected_at).as_secs();
            let idle = now.duration_since(client.last_active).as_secs();
            out.push_str(&format!(
                "id={} addr={} name={} age={} idle={} cmd={}\n",
                client.id,
                client.addr,
                client.name.as_deref().unwrap_or(""),
                age,
                idle,
                client.last_cmd
            ));
        }

        for (shard_id, sender) in self.senders.iter().enumerate() {
            if shard_id != self.shard_id {
                let (tx, rx) = flume::bounded(1);
                let msg = ShardMessage::ClientList { responder: tx };
                if sender.send(msg).is_ok() {
                    if let Ok(peer_list) = rx.recv_async().await {
                        out.push_str(&peer_list);
                    }
                }
            }
        }
        out
    }

    pub async fn execute_remote(&self, target: usize, cmd: Command) -> Vec<u8> {
        let (tx, rx) = flume::bounded(1);
        let msg = ShardMessage::Batch {
            items: vec![(0, cmd)],
            responder: tx,
        };
        if self.senders[target].send(msg).is_ok() {
            if let Ok(mut res) = rx.recv_async().await {
                if let Some((_, out)) = res.pop() {
                    return out.into_vec();
                }
            }
        }
        b"-ERR internal shard routing error\r\n".to_vec()
    }

    pub async fn dbsize(&self) -> usize {
        let mut total = self.local_db.borrow_mut().dbsize();
        for sid in 0..self.num_shards {
            if sid != self.shard_id {
                let res = self.execute_remote(sid, Command::Dbsize).await;
                if let Ok(s) = std::str::from_utf8(&res) {
                    if let Some(num_str) = s.strip_prefix(':').and_then(|x| x.split("\r\n").next())
                    {
                        if let Ok(n) = num_str.parse::<usize>() {
                            total += n;
                        }
                    }
                }
            }
        }
        total
    }

    pub async fn flushdb(&self) {
        self.local_db.borrow_mut().flushdb();
        if let Some(aof) = &self.aof {
            if let Some(bytes) = crate::aof::command_to_resp(&Command::Flushdb) {
                aof.borrow_mut().append(&bytes);
            }
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
                if sender.send(ShardMessage::RandomKey { responder: tx }).is_ok() {
                    if let Ok(Some(k)) = rx.recv_async().await {
                        return Some(k);
                    }
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
            let _ = self.last_save_time.compare_exchange(0, now, Ordering::Relaxed, Ordering::Relaxed);
            self.last_save_time.load(Ordering::Relaxed)
        } else {
            ts
        }
    }

    pub async fn save_rdb(&self) -> Result<(), String> {
        if self.is_saving.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return Err("Background save already in progress".to_string());
        }
        self.sync_aof().await;
        self.perform_save_rdb().await
    }

    pub async fn bgsave(&self) -> Result<(), String> {
        if self.is_saving.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
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
                if sender.send(ShardMessage::SaveRdbChunk { responder: tx }).is_ok() {
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
                    cmd,
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
        let tmp_filename = self
            .db_dir
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
}
