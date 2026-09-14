use bytes::Bytes;
use std::cell::RefCell;
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
use std::sync::RwLock;

#[derive(Clone, Debug)]
pub struct RemoteClusterNode {
    pub id: String,
    pub ip: String,
    pub port: u16,
    pub cport: u16,
    pub flags: String,
    pub master_id: String,
    pub ping_sent: u64,
    pub pong_recv: u64,
    pub config_epoch: u64,
    pub link_state: String,
}

pub static REMOTE_CLUSTER_NODES: std::sync::LazyLock<RwLock<Vec<RemoteClusterNode>>> =
    std::sync::LazyLock::new(|| RwLock::new(Vec::new()));

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

    pub async fn get(&self, key: Bytes) -> Option<Bytes> {
        let target = target_shard(&key, self.num_shards);
        if target == self.shard_id {
            self.local_db.borrow_mut().get(&key)
        } else {
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

    pub async fn perform_save_rdb(&self) -> Result<(), String> {
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
        format!("{:040x}", self.shard_id + 1)
    }

    pub fn cluster_meet(&self, ip: String, port: u16) -> Result<(), String> {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let mut nodes = REMOTE_CLUSTER_NODES.write().map_err(|e| e.to_string())?;
        if nodes.iter().any(|n| n.ip == ip && n.port == port) {
            return Ok(());
        }
        use fxhash::hash64;
        let node_hash = hash64(format!("{}:{}", ip, port).as_bytes());
        let node_id = format!("{:040x}", node_hash);
        let next_epoch = (nodes.len() + self.num_shards + 1) as u64;
        nodes.push(RemoteClusterNode {
            id: node_id,
            ip,
            port,
            cport: port + 10000,
            flags: "master".to_string(),
            master_id: "-".to_string(),
            ping_sent: now,
            pong_recv: now,
            config_epoch: next_epoch,
            link_state: "connected".to_string(),
        });
        Ok(())
    }

    pub fn cluster_nodes(&self) -> String {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let mut nodes = String::new();
        for s in 0..self.num_shards {
            let start_slot = s * 16384 / self.num_shards;
            let end_slot = if s == self.num_shards - 1 {
                16383
            } else {
                (s + 1) * 16384 / self.num_shards - 1
            };
            let node_id = format!("{:040x}", s + 1);
            let myself = if s == self.shard_id { "myself," } else { "" };
            nodes.push_str(&format!(
                "{} 127.0.0.1:{}@{} {}master - 0 0 {} connected {}-{}\n",
                node_id,
                self.port,
                self.port + 10000,
                myself,
                s + 1,
                start_slot,
                end_slot
            ));
        }
        if let Ok(mut remote_nodes) = REMOTE_CLUSTER_NODES.write() {
            for node in remote_nodes.iter_mut() {
                if now.saturating_sub(node.pong_recv) > 10000 {
                    node.flags = "fail".to_string();
                } else if now.saturating_sub(node.pong_recv) > 5000 {
                    node.flags = "fail?".to_string();
                } else {
                    node.flags = "master".to_string();
                }
                nodes.push_str(&format!(
                    "{} {}:{}@{} {} {} {} {} {} {}\n",
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
                ));
            }
        }
        nodes
    }

    pub fn cluster_info(&self) -> String {
        let remote_count = REMOTE_CLUSTER_NODES.read().map(|r| r.len()).unwrap_or(0);
        let total_nodes = self.num_shards + remote_count;
        let pfail_count = REMOTE_CLUSTER_NODES.read().map(|r| r.iter().filter(|n| n.flags == "fail?").count()).unwrap_or(0);
        let fail_count = REMOTE_CLUSTER_NODES.read().map(|r| r.iter().filter(|n| n.flags == "fail").count()).unwrap_or(0);
        let state = if fail_count > 0 { "fail" } else { "ok" };
        format!(
            "cluster_state:{}\r\ncluster_slots_assigned:16384\r\ncluster_slots_ok:16384\r\ncluster_slots_pfail:{}\r\ncluster_slots_fail:{}\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\ncluster_current_epoch:1\r\ncluster_my_epoch:1\r\ncluster_stats_messages_sent:0\r\ncluster_stats_messages_received:0\r\n",
            state, pfail_count, fail_count, total_nodes, total_nodes
        )
    }
}

pub async fn cluster_gossip_tick() {
    let nodes_to_ping: Vec<(String, u16)> = if let Ok(nodes) = REMOTE_CLUSTER_NODES.read() {
        nodes.iter().map(|n| (n.ip.clone(), n.port)).collect()
    } else {
        return;
    };

    if nodes_to_ping.is_empty() {
        return;
    }

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    for (ip, port) in nodes_to_ping {
        if let Ok(mut nodes) = REMOTE_CLUSTER_NODES.write() {
            if let Some(node) = nodes.iter_mut().find(|n| n.ip == ip && n.port == port) {
                node.ping_sent = now;
            }
        }

        let addr = format!("{}:{}", ip, port);
        let success = match monoio::net::TcpStream::connect(&addr).await {
            Ok(mut stream) => {
                use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
                let (res, _) = stream.write_all(b"*1\r\n$4\r\nPING\r\n".to_vec()).await;
                if res.is_ok() {
                    let buf = vec![0u8; 64];
                    let (res, read_buf) = stream.read(buf).await;
                    if let Ok(n) = res {
                        read_buf[..n].starts_with(b"+PONG")
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Err(_) => false,
        };

        if let Ok(mut nodes) = REMOTE_CLUSTER_NODES.write() {
            if let Some(node) = nodes.iter_mut().find(|n| n.ip == ip && n.port == port) {
                if success {
                    node.pong_recv = now;
                    node.link_state = "connected".to_string();
                    node.flags = "master".to_string();
                } else {
                    node.link_state = "disconnected".to_string();
                }
            }
        }
    }
}
