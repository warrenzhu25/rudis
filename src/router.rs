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
}

impl Router {
    pub fn new(
        shard_id: usize,
        num_shards: usize,
        port: u16,
        local_db: Rc<RefCell<ShardDb>>,
        senders: Vec<flume::Sender<ShardMessage>>,
    ) -> Self {
        Self {
            shard_id,
            num_shards,
            port,
            local_db,
            senders,
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

    pub async fn set(&self, key: Bytes, value: Bytes, expire_in: Option<Duration>) {
        let target = target_shard(&key, self.num_shards);
        if target == self.shard_id {
            self.local_db.borrow_mut().set(key, value, expire_in);
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
            self.local_db.borrow_mut().del(&key)
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
            self.local_db.borrow_mut().incr_by(key, delta)
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
            self.local_db.borrow_mut().expire(&key, duration)
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
            self.local_db.borrow_mut().persist(&key)
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
