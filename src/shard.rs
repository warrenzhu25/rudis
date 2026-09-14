use std::time::Duration;
use bytes::Bytes;

use crate::resp::Command;

/// Messages passed across CPU cores to access or mutate a shard's data.
pub enum ShardMessage {
    Get {
        key: Bytes,
        responder: flume::Sender<Option<Bytes>>,
    },
    Set {
        key: Bytes,
        value: Bytes,
        expire_in: Option<Duration>,
        responder: flume::Sender<()>,
    },
    Del {
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    Exists {
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    IncrBy {
        key: Bytes,
        delta: i64,
        responder: flume::Sender<Result<i64, String>>,
    },
    Expire {
        key: Bytes,
        duration: Duration,
        responder: flume::Sender<bool>,
    },
    Persist {
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    Ttl {
        key: Bytes,
        in_millis: bool,
        responder: flume::Sender<i64>,
    },
    CountKeysInSlot {
        slot: u16,
        responder: flume::Sender<usize>,
    },
    GetKeysInSlot {
        slot: u16,
        count: usize,
        responder: flume::Sender<Vec<Bytes>>,
    },
    ClientList {
        responder: flume::Sender<String>,
    },
    Batch {
        items: Vec<(usize, Command)>,
        responder: flume::Sender<Vec<(usize, Vec<u8>)>>,
    },
}

/// A purely thread-local key-value store for one shard powered by RudisTable.
/// Because this shard is accessed only by the thread running on its assigned CPU core,
/// it requires NO Mutex and NO cross-thread synchronization.
pub struct ShardDb {
    table: crate::table::RudisTable,
}

impl ShardDb {
    pub fn new() -> Self {
        Self {
            table: crate::table::RudisTable::new(),
        }
    }

    #[inline]
    pub fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        self.table.get(key).ok().flatten()
    }

    #[inline]
    pub fn set(&mut self, key: Bytes, value: Bytes, expire_in: Option<Duration>) {
        self.table.set(key, value, expire_in);
    }

    #[inline]
    pub fn del(&mut self, key: &[u8]) -> bool {
        self.table.del(key)
    }

    #[inline]
    pub fn exists(&mut self, key: &[u8]) -> bool {
        self.table.exists(key)
    }

    #[inline]
    pub fn incr_by(&mut self, key: Bytes, delta: i64) -> Result<i64, String> {
        self.table.incr_by(key, delta)
    }

    #[inline]
    pub fn expire(&mut self, key: &[u8], duration: Duration) -> bool {
        self.table.expire(key, duration)
    }

    #[inline]
    pub fn persist(&mut self, key: &[u8]) -> bool {
        self.table.persist(key)
    }

    #[inline]
    pub fn ttl(&mut self, key: &[u8], in_millis: bool) -> i64 {
        self.table.ttl(key, in_millis)
    }

    #[inline]
    pub fn hset(&mut self, key: Bytes, fields: Vec<(Bytes, Bytes)>) -> Result<usize, &'static str> {
        self.table.hset(key, fields)
    }

    #[inline]
    pub fn hget(&mut self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, &'static str> {
        self.table.hget(key, field)
    }

    #[inline]
    pub fn hmget(&mut self, key: &[u8], fields: &[Bytes]) -> Result<Vec<Option<Bytes>>, &'static str> {
        self.table.hmget(key, fields)
    }

    #[inline]
    pub fn hdel(&mut self, key: &[u8], fields: &[Bytes]) -> Result<usize, &'static str> {
        self.table.hdel(key, fields)
    }

    #[inline]
    pub fn hexists(&mut self, key: &[u8], field: &[u8]) -> Result<bool, &'static str> {
        self.table.hexists(key, field)
    }

    #[inline]
    pub fn hlen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        self.table.hlen(key)
    }

    #[inline]
    pub fn hgetall(&mut self, key: &[u8]) -> Result<Vec<(Bytes, Bytes)>, &'static str> {
        self.table.hgetall(key)
    }

    #[inline]
    pub fn hkeys(&mut self, key: &[u8]) -> Result<Vec<Bytes>, &'static str> {
        self.table.hkeys(key)
    }

    #[inline]
    pub fn hvals(&mut self, key: &[u8]) -> Result<Vec<Bytes>, &'static str> {
        self.table.hvals(key)
    }

    #[inline]
    pub fn count_keys_in_slot(&mut self, slot: u16) -> usize {
        self.table.count_keys_in_slot(slot)
    }

    #[inline]
    pub fn get_keys_in_slot(&mut self, slot: u16, count: usize) -> Vec<Bytes> {
        self.table.get_keys_in_slot(slot, count)
    }

    #[inline]
    pub fn active_expire_cycle(&mut self) -> usize {
        self.table.active_expire_cycle()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }
}

