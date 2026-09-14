use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;
use bytes::Bytes;

use crate::shard::{ShardDb, ShardMessage};

/// Calculates the target shard ID for a given key using CRC16.
#[inline]
pub fn target_shard(key: &[u8], num_shards: usize) -> usize {
    if num_shards <= 1 {
        return 0;
    }
    (crc16::State::<crc16::XMODEM>::calculate(key) as usize) % num_shards
}

/// The router handles dispatching operations.
/// If the key belongs to the current shard, it directly touches `local_db` without locking.
/// If the key belongs to a peer shard, it routes the message across cores via the mesh.
pub struct Router {
    pub shard_id: usize,
    pub num_shards: usize,
    pub local_db: Rc<RefCell<ShardDb>>,
    pub senders: Vec<flume::Sender<ShardMessage>>,
}

impl Router {
    pub fn new(
        shard_id: usize,
        num_shards: usize,
        local_db: Rc<RefCell<ShardDb>>,
        senders: Vec<flume::Sender<ShardMessage>>,
    ) -> Self {
        Self {
            shard_id,
            num_shards,
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
}
