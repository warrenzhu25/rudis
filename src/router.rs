use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use bytes::Bytes;

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

/// The router handles dispatching operations.
/// If the key belongs to the current shard, it directly touches `local_db` without locking.
/// If the key belongs to a peer shard, it routes the message across cores via the mesh.
pub struct Router {
    pub shard_id: usize,
    pub num_shards: usize,
    pub port: u16,
    pub local_db: Rc<RefCell<ShardDb>>,
    pub senders: Vec<flume::Sender<ShardMessage>>,
    pub slot_states: Rc<RefCell<Vec<crate::shard::SlotState>>>,
    pub slot_owners: Rc<RefCell<Vec<usize>>>,
    pub aof: Option<Rc<RefCell<crate::aof::AofWriter>>>,
}

impl Router {
    pub fn new(
        shard_id: usize,
        num_shards: usize,
        port: u16,
        local_db: Rc<RefCell<ShardDb>>,
        senders: Vec<flume::Sender<ShardMessage>>,
        aof: Option<Rc<RefCell<crate::aof::AofWriter>>>,
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
        }
    }

    pub fn target_shard_for_slot(&self, slot: u16) -> usize {
        self.slot_owners.borrow()[slot as usize]
    }

    pub fn target_shard(&self, key: &[u8]) -> usize {
        let slot = key_slot(key);
        self.target_shard_for_slot(slot)
    }

    pub fn check_slot_redirection(&self, slot: u16, key_exists: bool, asking: bool) -> Result<(), String> {
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
            let msg = ShardMessage::Get {
                key,
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.ok().flatten()
            } else {
                None
            }
        }
    }

    pub async fn dump_key(&self, key: Bytes) -> Option<(crate::table::RudisValue, Option<Duration>)> {
        let target = target_shard(&key, self.num_shards);
        if target == self.shard_id {
            self.local_db.borrow_mut().get_entry(&key)
        } else {
            let (tx, rx) = flume::bounded(1);
            let msg = ShardMessage::DumpKey {
                key,
                responder: tx,
            };
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
            self.local_db.borrow_mut().set(key.clone(), value.clone(), expire_in);
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
            let msg = ShardMessage::Del {
                key,
                responder: tx,
            };
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
            let msg = ShardMessage::Exists {
                key,
                responder: tx,
            };
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
                    if let Some(bytes) = crate::aof::command_to_resp(&Command::Expire(key, duration)) {
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
            let msg = ShardMessage::Persist {
                key,
                responder: tx,
            };
            if self.senders[target].send(msg).is_ok() {
                rx.recv_async().await.unwrap_or(false)
            } else {
                false
            }
        }
    }

    pub async fn sync_aof(&self) {
        if let Some(aof) = &self.aof {
            let _ = aof.borrow_mut().sync().await;
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
            let msg = ShardMessage::CountKeysInSlot { slot, responder: tx };
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
                    return out;
                }
            }
        }
        b"-ERR internal shard routing error\r\n".to_vec()
    }
}
