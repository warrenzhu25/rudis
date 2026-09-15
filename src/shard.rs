use bytes::Bytes;
use std::time::Duration;

use crate::resp::Command;

/// Inline compact representation of Redis responses for cross-core batch transfers.
/// Avoids heap allocations for responses up to 30 bytes (integers, OK, simple errors, small bulk strings).
#[derive(Clone, Debug)]
pub enum CompactResp {
    Small { len: u8, data: [u8; 30] },
    Big(Vec<u8>),
}

impl CompactResp {
    #[inline(always)]
    pub const fn empty() -> Self {
        CompactResp::Small {
            len: 0,
            data: [0u8; 30],
        }
    }

    #[inline(always)]
    pub fn from_slice(bytes: &[u8]) -> Self {
        let len = bytes.len();
        if len <= 30 {
            let mut data = [0u8; 30];
            data[..len].copy_from_slice(bytes);
            CompactResp::Small {
                len: len as u8,
                data,
            }
        } else {
            CompactResp::Big(bytes.to_vec())
        }
    }

    #[inline(always)]
    pub fn from_vec(vec: Vec<u8>) -> Self {
        if vec.len() <= 30 {
            Self::from_slice(&vec)
        } else {
            CompactResp::Big(vec)
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            CompactResp::Small { len, data } => &data[..*len as usize],
            CompactResp::Big(vec) => vec.as_slice(),
        }
    }

    #[inline(always)]
    pub fn into_vec(self) -> Vec<u8> {
        match self {
            CompactResp::Small { len, data } => data[..len as usize].to_vec(),
            CompactResp::Big(vec) => vec,
        }
    }
}

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
        filter_ids: Vec<u64>,
        responder: flume::Sender<String>,
    },
    Batch {
        items: Vec<(usize, Command)>,
        responder: flume::Sender<Vec<(usize, CompactResp)>>,
        is_resp3: bool,
    },
    NotifyList {
        keys: Vec<Bytes>,
    },
    SetSlotState {
        slot: u16,
        state: SlotState,
    },
    SetSlotOwner {
        slot: u16,
        owner: usize,
    },
    DumpKey {
        key: Bytes,
        responder: flume::Sender<Option<(crate::table::RudisValue, Option<Duration>)>>,
    },
    SyncAof {
        responder: flume::Sender<()>,
    },
    SaveRdbChunk {
        responder: flume::Sender<Vec<u8>>,
    },
    Publish {
        channel: Bytes,
        message: Bytes,
        responder: flume::Sender<usize>,
    },
    PubsubChannels {
        pattern: Option<Bytes>,
        responder: flume::Sender<Vec<Bytes>>,
    },
    PubsubNumsub {
        channels: Vec<Bytes>,
        responder: flume::Sender<Vec<(Bytes, usize)>>,
    },
    PubsubNumpat {
        responder: flume::Sender<usize>,
    },
    Keys {
        pattern: Bytes,
        responder: flume::Sender<Vec<Bytes>>,
    },
    Scan {
        slot: usize,
        pattern: Option<Bytes>,
        count: usize,
        responder: flume::Sender<(usize, Vec<Bytes>)>,
    },
    RandomKey {
        responder: flume::Sender<Option<Bytes>>,
    },
    ExpireTime {
        key: Bytes,
        in_millis: bool,
        responder: flume::Sender<i64>,
    },
    AcquireTxLock {
        tx_id: u64,
        responder: flume::Sender<()>,
    },
    ReleaseTxLock {
        tx_id: u64,
    },
    RestoreRdbChunk {
        data: Bytes,
        responder: flume::Sender<()>,
    },
    ExecuteReplicaCmd {
        cmd: Command,
        responder: flume::Sender<()>,
    },
    TierSpill {
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    TierLoad {
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    TierSpillAll {
        responder: flume::Sender<usize>,
    },
    TierCool {
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    TierDecommit {
        key: Option<Bytes>,
        responder: flume::Sender<usize>,
    },
    GetUsedMemory {
        responder: flume::Sender<usize>,
    },
    StreamColdRead {
        key: Bytes,
        responder: flume::Sender<Option<Bytes>>,
    },
    TierGc {
        responder: flume::Sender<usize>,
    },
    TierSnapshot {
        backup_dir: std::path::PathBuf,
        responder: flume::Sender<Result<(bool, u64), String>>,
    },
    FlushSlots {
        ranges: Vec<(u16, u16)>,
        responder: flume::Sender<usize>,
    },
    Stick {
        keys: Vec<Bytes>,
        responder: flume::Sender<usize>,
    },
    Unstick {
        keys: Vec<Bytes>,
        responder: flume::Sender<usize>,
    },
    IsSticky {
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    Delex {
        key: Bytes,
        condition: Option<(String, Bytes)>,
        responder: flume::Sender<bool>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotState {
    Stable,
    Migrating(String),
    Importing(String),
    Moved(String),
}

/// A purely thread-local key-value store for one shard powered by RudisTable.
/// Because this shard is accessed only by the thread running on its assigned CPU core,
/// it requires NO Mutex and NO cross-thread synchronization.
pub struct ShardDb {
    pub table: crate::table::RudisTable,
    pub port: u16,
    pub tier_manager: Option<std::rc::Rc<crate::tiering::ShardTierManager>>,
    pub vector_indexes: std::collections::HashMap<String, crate::vector::HnswIndex>,
    pub crdt_store: crate::crdt::CrdtStore,
    pub json_store: crate::json::JsonStore,
    pub probabilistic_store: crate::probabilistic::ProbabilisticStore,
    pub sticky_keys: hashbrown::HashSet<Bytes>,
}

impl ShardDb {
    pub fn new(port: u16) -> Self {
        Self {
            table: crate::table::RudisTable::new(),
            port,
            tier_manager: None,
            vector_indexes: std::collections::HashMap::new(),
            crdt_store: crate::crdt::CrdtStore::new(port),
            json_store: crate::json::JsonStore::new(),
            probabilistic_store: crate::probabilistic::ProbabilisticStore::new(),
            sticky_keys: hashbrown::HashSet::new(),
        }
    }

    #[inline]
    pub fn get_entry(
        &mut self,
        key: &[u8],
    ) -> Option<(crate::table::RudisValue, Option<Duration>)> {
        self.table.get_entry(key)
    }

    #[inline]
    pub fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        self.table.get(key).ok().flatten()
    }

    #[inline]
    pub fn set(&mut self, key: Bytes, value: Bytes, expire_in: Option<Duration>) {
        if let Some(tm) = &self.tier_manager {
            tm.op_manager.cancel_pending_stash(&key);
        }
        if let Some(ptr) = self.table.is_tiered(&key) {
            if let Some(tm) = &self.tier_manager {
                tm.on_key_deleted(ptr);
                tm.stats
                    .tiered_keys
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                tm.stats
                    .total_deletes
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        } else if let Some(ptr) = self.table.is_cooled(&key)
            && let Some(tm) = &self.tier_manager
        {
            tm.on_key_deleted(ptr);
            tm.stats
                .cooled_keys
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            tm.stats
                .total_deletes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.table.set(key, value, expire_in);
    }

    #[inline]
    pub fn set_extended(
        &mut self,
        key: Bytes,
        value: Bytes,
        expire_in: Option<Duration>,
        keepttl: bool,
    ) {
        if let Some(tm) = &self.tier_manager {
            tm.op_manager.cancel_pending_stash(&key);
        }
        if let Some(ptr) = self.table.is_tiered(&key) {
            if let Some(tm) = &self.tier_manager {
                tm.on_key_deleted(ptr);
                tm.stats
                    .tiered_keys
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                tm.stats
                    .total_deletes
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        } else if let Some(ptr) = self.table.is_cooled(&key)
            && let Some(tm) = &self.tier_manager
        {
            tm.on_key_deleted(ptr);
            tm.stats
                .cooled_keys
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            tm.stats
                .total_deletes
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.table.set_extended(key, value, expire_in, keepttl);
    }

    #[inline]
    pub fn del(&mut self, key: &[u8]) -> bool {
        if let Some(tm) = &self.tier_manager {
            tm.op_manager.cancel_pending_stash(key);
        }
        let ptr = self.table.is_tiered(key);
        let cooled_ptr = if ptr.is_none() {
            self.table.is_cooled(key)
        } else {
            None
        };
        let deleted = self.table.del(key);
        if deleted {
            self.sticky_keys.remove(key);
            if let Some(ptr) = ptr {
                if let Some(tm) = &self.tier_manager {
                    tm.on_key_deleted(ptr);
                    tm.stats
                        .tiered_keys
                        .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    tm.stats
                        .total_deletes
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            } else if let Some(ptr) = cooled_ptr
                && let Some(tm) = &self.tier_manager
            {
                tm.on_key_deleted(ptr);
                tm.stats
                    .cooled_keys
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                tm.stats
                    .total_deletes
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        deleted
    }

    #[inline]
    pub fn is_sticky(&self, key: &[u8]) -> bool {
        self.sticky_keys.contains(key)
    }

    #[inline]
    pub fn stick(&mut self, key: Bytes) -> bool {
        if self.table.exists(&key) {
            self.sticky_keys.insert(key);
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn unstick(&mut self, key: &[u8]) -> bool {
        self.sticky_keys.remove(key)
    }

    #[inline]
    pub fn flush_slots(&mut self, ranges: &[(u16, u16)]) -> usize {
        let count = self.table.flush_slots(ranges);
        self.sticky_keys.retain(|k| {
            let s = crate::router::key_slot(k);
            !ranges.iter().any(|&(start, end)| s >= start && s <= end)
        });
        count
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
    pub fn hsetnx(
        &mut self,
        key: Bytes,
        field: Bytes,
        value: Bytes,
    ) -> Result<usize, &'static str> {
        self.table.hsetnx(key, field, value)
    }

    #[inline]
    pub fn hget(&mut self, key: &[u8], field: &[u8]) -> Result<Option<Bytes>, &'static str> {
        self.table.hget(key, field)
    }

    #[inline]
    pub fn hmget(
        &mut self,
        key: &[u8],
        fields: &[Bytes],
    ) -> Result<Vec<Option<Bytes>>, &'static str> {
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
    pub fn hstrlen(&mut self, key: &[u8], field: &[u8]) -> Result<usize, &'static str> {
        self.table.hstrlen(key, field)
    }

    #[inline]
    pub fn hgetdel(
        &mut self,
        key: &[u8],
        fields: &[Bytes],
    ) -> Result<(Vec<Option<Bytes>>, Vec<Bytes>), &'static str> {
        self.table.hgetdel(key, fields)
    }

    #[inline]
    pub fn object_encoding(&mut self, key: &[u8]) -> Option<&'static str> {
        self.table.object_encoding(key)
    }

    #[inline]
    pub fn hincrby(&mut self, key: Bytes, field: Bytes, delta: i64) -> Result<i64, &'static str> {
        self.table.hincrby(key, field, delta)
    }

    #[inline]
    pub fn hincrbyfloat(
        &mut self,
        key: Bytes,
        field: Bytes,
        delta: f64,
    ) -> Result<f64, &'static str> {
        self.table.hincrbyfloat(key, field, delta)
    }

    #[inline]
    pub fn hrandfield(
        &mut self,
        key: &[u8],
        count: Option<i64>,
        with_values: bool,
    ) -> Result<Vec<Bytes>, &'static str> {
        self.table.hrandfield(key, count, with_values)
    }

    #[inline]
    pub fn hscan(
        &mut self,
        key: &[u8],
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> Result<(usize, Vec<Bytes>), &'static str> {
        self.table.hscan(key, cursor, pattern, count)
    }

    // LIST METHODS
    #[inline]
    pub fn lpush(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.table.lpush(key, values)
    }

    #[inline]
    pub fn rpush(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.table.rpush(key, values)
    }

    #[inline]
    pub fn lpushx(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.table.lpushx(key, values)
    }

    #[inline]
    pub fn rpushx(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.table.rpushx(key, values)
    }

    #[inline]
    pub fn lpop(&mut self, key: &[u8], count: usize) -> Result<Vec<Bytes>, &'static str> {
        self.table.lpop(key, count)
    }

    #[inline]
    pub fn rpop(&mut self, key: &[u8], count: usize) -> Result<Vec<Bytes>, &'static str> {
        self.table.rpop(key, count)
    }

    #[inline]
    pub fn llen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        self.table.llen(key)
    }

    #[inline]
    pub fn lindex(&mut self, key: &[u8], index: i64) -> Result<Option<Bytes>, &'static str> {
        self.table.lindex(key, index)
    }

    #[inline]
    pub fn lrange(
        &mut self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<Vec<Bytes>, &'static str> {
        self.table.lrange(key, start, stop)
    }

    #[inline]
    pub fn ltrim(&mut self, key: &[u8], start: i64, stop: i64) -> Result<(), &'static str> {
        self.table.ltrim(key, start, stop)
    }

    #[inline]
    pub fn lset(&mut self, key: &[u8], index: i64, element: Bytes) -> Result<(), &'static str> {
        self.table.lset(key, index, element)
    }

    #[inline]
    pub fn lrem(&mut self, key: &[u8], count: i64, element: &[u8]) -> Result<usize, &'static str> {
        self.table.lrem(key, count, element)
    }

    #[inline]
    pub fn lpos(
        &mut self,
        key: &[u8],
        element: &[u8],
        rank: i64,
        count: Option<usize>,
        maxlen: Option<usize>,
    ) -> Result<Vec<usize>, &'static str> {
        self.table.lpos(key, element, rank, count, maxlen)
    }

    #[inline]
    pub fn linsert(
        &mut self,
        key: Bytes,
        before: bool,
        pivot: &[u8],
        element: Bytes,
    ) -> Result<i64, &'static str> {
        self.table.linsert(key, before, pivot, element)
    }

    #[inline]
    pub fn lmove(
        &mut self,
        source: &[u8],
        destination: Bytes,
        where_from: crate::table::ListDirection,
        where_to: crate::table::ListDirection,
    ) -> Result<Option<Bytes>, &'static str> {
        self.table.lmove(source, destination, where_from, where_to)
    }

    // SET METHODS
    #[inline]
    pub fn sadd(&mut self, key: Bytes, members: Vec<Bytes>) -> Result<usize, &'static str> {
        self.table.sadd(key, members)
    }

    #[inline]
    pub fn srem(&mut self, key: &[u8], members: &[Bytes]) -> Result<usize, &'static str> {
        self.table.srem(key, members)
    }

    #[inline]
    pub fn smembers(&mut self, key: &[u8]) -> Result<Vec<Bytes>, &'static str> {
        self.table.smembers(key)
    }

    #[inline]
    pub fn sismember(&mut self, key: &[u8], member: &[u8]) -> Result<bool, &'static str> {
        self.table.sismember(key, member)
    }

    #[inline]
    pub fn scard(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        self.table.scard(key)
    }

    #[inline]
    pub fn spop(&mut self, key: &[u8], count: usize) -> Result<Vec<Bytes>, &'static str> {
        self.table.spop(key, count)
    }

    #[inline]
    pub fn sinter(&mut self, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        self.table.sinter(keys)
    }

    #[inline]
    pub fn sunion(&mut self, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        self.table.sunion(keys)
    }

    #[inline]
    pub fn sdiff(&mut self, keys: &[Bytes]) -> Result<Vec<Bytes>, &'static str> {
        self.table.sdiff(keys)
    }

    #[inline]
    pub fn sinterstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        self.table.sinterstore(dest, keys)
    }

    #[inline]
    pub fn sunionstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        self.table.sunionstore(dest, keys)
    }

    #[inline]
    pub fn sdiffstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        self.table.sdiffstore(dest, keys)
    }

    #[inline]
    pub fn sintercard(&mut self, keys: &[Bytes], limit: usize) -> Result<usize, &'static str> {
        self.table.sintercard(keys, limit)
    }

    #[inline]
    pub fn sunioncard(&mut self, keys: &[Bytes], limit: usize) -> Result<usize, &'static str> {
        self.table.sunioncard(keys, limit)
    }

    #[inline]
    pub fn sdiffcard(&mut self, keys: &[Bytes], limit: usize) -> Result<usize, &'static str> {
        self.table.sdiffcard(keys, limit)
    }

    #[inline]
    pub fn smismember(&mut self, key: &[u8], members: &[Bytes]) -> Result<Vec<bool>, &'static str> {
        self.table.smismember(key, members)
    }

    #[inline]
    pub fn srandmember(
        &mut self,
        key: &[u8],
        count: Option<i64>,
    ) -> Result<Vec<Bytes>, &'static str> {
        self.table.srandmember(key, count)
    }

    #[inline]
    pub fn smove(
        &mut self,
        source: &[u8],
        destination: Bytes,
        member: Bytes,
    ) -> Result<crate::table::SmoveResult, &'static str> {
        self.table.smove(source, destination, member)
    }

    #[inline]
    pub fn sscan(
        &mut self,
        key: &[u8],
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> Result<(usize, Vec<Bytes>), &'static str> {
        self.table.sscan(key, cursor, pattern, count)
    }

    #[inline]
    pub fn zadd(
        &mut self,
        key: Bytes,
        elements: Vec<(f64, Bytes)>,
        flags: crate::table::ZAddFlags,
    ) -> Result<(usize, Option<f64>), &'static str> {
        self.table.zadd(key, elements, flags)
    }

    #[inline]
    pub fn zrem(&mut self, key: &[u8], members: &[Bytes]) -> Result<usize, &'static str> {
        self.table.zrem(key, members)
    }

    #[inline]
    pub fn zscore(&mut self, key: &[u8], member: &[u8]) -> Result<Option<f64>, &'static str> {
        self.table.zscore(key, member)
    }

    #[inline]
    pub fn zcard(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        self.table.zcard(key)
    }

    #[inline]
    pub fn zrank(
        &mut self,
        key: &[u8],
        member: &[u8],
        rev: bool,
        with_score: bool,
    ) -> Result<Option<(usize, Option<f64>)>, &'static str> {
        self.table.zrank(key, member, rev, with_score)
    }

    #[inline]
    pub fn zcount(
        &mut self,
        key: &[u8],
        min: f64,
        min_inc: bool,
        max: f64,
        max_inc: bool,
    ) -> Result<usize, &'static str> {
        self.table.zcount(key, min, min_inc, max, max_inc)
    }

    #[inline]
    pub fn zincrby(&mut self, key: Bytes, delta: f64, member: Bytes) -> Result<f64, &'static str> {
        self.table.zincrby(key, delta, member)
    }

    #[inline]
    pub fn zrange(
        &mut self,
        key: &[u8],
        opts: &crate::table::ZRangeOpts,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        self.table.zrange(key, opts)
    }

    #[inline]
    pub fn zrangestore(
        &mut self,
        dst: &[u8],
        src: &[u8],
        opts: &crate::table::ZRangeOpts,
    ) -> Result<usize, &'static str> {
        self.table.zrangestore(dst, src, opts)
    }

    #[inline]
    pub fn zpopmin(&mut self, key: &[u8], count: usize) -> Result<Vec<(Bytes, f64)>, &'static str> {
        self.table.zpopmin(key, count)
    }

    #[inline]
    pub fn zpopmax(&mut self, key: &[u8], count: usize) -> Result<Vec<(Bytes, f64)>, &'static str> {
        self.table.zpopmax(key, count)
    }

    #[inline]
    pub fn zunionstore(
        &mut self,
        dest: Bytes,
        keys: &[Bytes],
        weights: &[f64],
        agg: crate::table::Aggregate,
    ) -> Result<usize, &'static str> {
        self.table.zunionstore(dest, keys, weights, agg)
    }

    #[inline]
    pub fn zinterstore(
        &mut self,
        dest: Bytes,
        keys: &[Bytes],
        weights: &[f64],
        agg: crate::table::Aggregate,
    ) -> Result<usize, &'static str> {
        self.table.zinterstore(dest, keys, weights, agg)
    }

    #[inline]
    pub fn zdiffstore(&mut self, dest: Bytes, keys: &[Bytes]) -> Result<usize, &'static str> {
        self.table.zdiffstore(dest, keys)
    }

    #[inline]
    pub fn zdiff(
        &mut self,
        keys: &[Bytes],
        with_scores: bool,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        self.table.zdiff(keys, with_scores)
    }

    #[inline]
    pub fn zinter(
        &mut self,
        keys: &[Bytes],
        weights: &[f64],
        agg: crate::table::Aggregate,
        with_scores: bool,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        self.table.zinter(keys, weights, agg, with_scores)
    }

    #[inline]
    pub fn zintercard(&mut self, keys: &[Bytes], limit: usize) -> Result<usize, &'static str> {
        self.table.zintercard(keys, limit)
    }

    #[inline]
    pub fn zunion(
        &mut self,
        keys: &[Bytes],
        weights: &[f64],
        agg: crate::table::Aggregate,
        with_scores: bool,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        self.table.zunion(keys, weights, agg, with_scores)
    }

    #[inline]
    pub fn zmscore(
        &mut self,
        key: &[u8],
        members: &[Bytes],
    ) -> Result<Vec<Option<f64>>, &'static str> {
        self.table.zmscore(key, members)
    }

    #[inline]
    pub fn zrandmember(
        &mut self,
        key: &[u8],
        count: Option<i64>,
        with_scores: bool,
    ) -> Result<Vec<(Bytes, f64)>, &'static str> {
        self.table.zrandmember(key, count, with_scores)
    }

    #[inline]
    pub fn zremrangebyrank(
        &mut self,
        key: &[u8],
        start: i64,
        stop: i64,
    ) -> Result<usize, &'static str> {
        self.table.zremrangebyrank(key, start, stop)
    }

    #[inline]
    pub fn zremrangebyscore(
        &mut self,
        key: &[u8],
        min: f64,
        min_inc: bool,
        max: f64,
        max_inc: bool,
    ) -> Result<usize, &'static str> {
        self.table.zremrangebyscore(key, min, min_inc, max, max_inc)
    }

    #[inline]
    pub fn zremrangebylex(
        &mut self,
        key: &[u8],
        min: &crate::table::LexBound,
        max: &crate::table::LexBound,
    ) -> Result<usize, &'static str> {
        self.table.zremrangebylex(key, min, max)
    }

    #[inline]
    pub fn zlexcount(
        &mut self,
        key: &[u8],
        min: &crate::table::LexBound,
        max: &crate::table::LexBound,
    ) -> Result<usize, &'static str> {
        self.table.zlexcount(key, min, max)
    }

    #[inline]
    pub fn zscan(
        &mut self,
        key: &[u8],
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> Result<(usize, Vec<(Bytes, f64)>), &'static str> {
        self.table.zscan(key, cursor, pattern, count)
    }

    #[inline]
    pub fn flushdb(&mut self) {
        self.table.flushdb();
    }

    #[inline]
    pub fn dbsize(&mut self) -> usize {
        self.table.dbsize()
    }

    #[inline]
    pub fn type_of(&mut self, key: &[u8]) -> &'static str {
        self.table.type_of(key)
    }

    #[inline]
    pub fn touch(&mut self, keys: &[Bytes]) -> usize {
        self.table.touch(keys)
    }

    #[inline]
    pub fn rename(&mut self, src: &[u8], dst: Bytes, nx: bool) -> Result<bool, &'static str> {
        self.table.rename(src, dst, nx)
    }

    #[inline]
    pub fn setnx(&mut self, key: Bytes, value: Bytes) -> bool {
        self.table.setnx(key, value)
    }

    #[inline]
    pub fn getset(&mut self, key: Bytes, value: Bytes) -> Result<Option<Bytes>, &'static str> {
        self.table.getset(key, value)
    }

    #[inline]
    pub fn getdel(&mut self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        self.table.getdel(key)
    }

    #[inline]
    pub fn append(&mut self, key: Bytes, val_to_append: &[u8]) -> Result<usize, &'static str> {
        self.table.append(key, val_to_append)
    }

    #[inline]
    pub fn strlen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        self.table.strlen(key)
    }

    #[inline]
    pub fn incrbyfloat(&mut self, key: Bytes, delta: f64) -> Result<f64, &'static str> {
        self.table.incrbyfloat(key, delta)
    }

    #[inline]
    pub fn setrange(
        &mut self,
        key: Bytes,
        offset: usize,
        value: &[u8],
    ) -> Result<usize, &'static str> {
        self.table.setrange(key, offset, value)
    }

    #[inline]
    pub fn getrange(&mut self, key: &[u8], start: i64, end: i64) -> Result<Bytes, &'static str> {
        self.table.getrange(key, start, end)
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

    #[inline]
    pub fn keys(&mut self, pattern: &[u8]) -> Vec<Bytes> {
        self.table.keys(pattern)
    }

    #[inline]
    pub fn scan(
        &mut self,
        cursor: usize,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> (usize, Vec<Bytes>) {
        self.table.scan(cursor, pattern, count)
    }

    #[inline]
    pub fn random_key(&mut self) -> Option<Bytes> {
        self.table.random_key()
    }

    #[inline]
    pub fn expiretime(&mut self, key: &[u8], in_millis: bool) -> i64 {
        self.table.expiretime(key, in_millis)
    }

    #[inline]
    pub fn setbit(&mut self, key: Bytes, offset: usize, value: u8) -> Result<u8, &'static str> {
        self.table.setbit(key, offset, value)
    }

    #[inline]
    pub fn getbit(&mut self, key: &[u8], offset: usize) -> Result<u8, &'static str> {
        self.table.getbit(key, offset)
    }

    #[inline]
    pub fn bitcount(
        &mut self,
        key: &[u8],
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<usize, &'static str> {
        self.table.bitcount(key, start, end)
    }

    #[inline]
    pub fn bitpos(
        &mut self,
        key: &[u8],
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<i64, &'static str> {
        self.table.bitpos(key, bit, start, end)
    }

    #[inline]
    pub fn bitop(
        &mut self,
        op: &str,
        destkey: Bytes,
        srckeys: &[Bytes],
    ) -> Result<usize, &'static str> {
        self.table.bitop(op, destkey, srckeys)
    }

    #[inline]
    pub fn pfadd(&mut self, key: Bytes, elements: &[Bytes]) -> Result<bool, &'static str> {
        self.table.pfadd(key, elements)
    }

    #[inline]
    pub fn pfcount(&mut self, keys: &[Bytes]) -> Result<u64, &'static str> {
        self.table.pfcount(keys)
    }

    #[inline]
    pub fn pfmerge(&mut self, destkey: Bytes, srckeys: &[Bytes]) -> Result<(), &'static str> {
        self.table.pfmerge(destkey, srckeys)
    }

    #[inline]
    pub fn dump(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.table.dump(key)
    }

    #[inline]
    pub fn restore(
        &mut self,
        key: Bytes,
        ttl_ms: u64,
        serialized: &[u8],
        replace: bool,
        absttl: bool,
    ) -> Result<(), &'static str> {
        self.table.restore(key, ttl_ms, serialized, replace, absttl)
    }

    #[inline]
    pub fn xadd(
        &mut self,
        key: Bytes,
        add_id: crate::table::StreamAddId,
        fields: Vec<(Bytes, Bytes)>,
        nomkstream: bool,
        maxlen: Option<usize>,
        minid: Option<crate::table::StreamId>,
    ) -> Result<Option<crate::table::StreamId>, &'static str> {
        self.table
            .xadd(key, add_id, fields, nomkstream, maxlen, minid)
    }

    #[inline]
    pub fn xlen(&mut self, key: &[u8]) -> Result<usize, &'static str> {
        self.table.xlen(key)
    }

    #[inline]
    pub fn xrange(
        &mut self,
        key: &[u8],
        start: &str,
        end: &str,
        count: Option<usize>,
    ) -> Result<Vec<(crate::table::StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        self.table.xrange(key, start, end, count)
    }

    #[inline]
    pub fn xrevrange(
        &mut self,
        key: &[u8],
        end: &str,
        start: &str,
        count: Option<usize>,
    ) -> Result<Vec<(crate::table::StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        self.table.xrevrange(key, end, start, count)
    }

    #[inline]
    pub fn xread(
        &mut self,
        keys: &[Bytes],
        ids: &[String],
        count: Option<usize>,
    ) -> Result<Vec<(Bytes, Vec<(crate::table::StreamId, Vec<(Bytes, Bytes)>)>)>, &'static str>
    {
        self.table.xread(keys, ids, count)
    }

    #[inline]
    pub fn xdel(
        &mut self,
        key: &[u8],
        ids: &[crate::table::StreamId],
    ) -> Result<usize, &'static str> {
        self.table.xdel(key, ids)
    }

    #[inline]
    pub fn xtrim(
        &mut self,
        key: &[u8],
        maxlen: Option<usize>,
        minid: Option<crate::table::StreamId>,
    ) -> Result<usize, &'static str> {
        self.table.xtrim(key, maxlen, minid)
    }

    #[inline]
    pub fn save_rdb_chunk(&mut self, buf: &mut Vec<u8>) {
        self.table.save_rdb_chunk(buf);
    }

    #[inline]
    pub fn restore_rdb_chunk(&mut self, data: &[u8]) -> Result<(), &'static str> {
        self.table.restore_rdb_chunk(data)
    }

    #[inline]
    pub fn xgroup_create(
        &mut self,
        key: Bytes,
        group: Bytes,
        id_str: &str,
        mkstream: bool,
    ) -> Result<(), &'static str> {
        self.table.xgroup_create(key, group, id_str, mkstream)
    }

    #[inline]
    pub fn xgroup_destroy(&mut self, key: &[u8], group: &[u8]) -> Result<bool, &'static str> {
        self.table.xgroup_destroy(key, group)
    }

    #[inline]
    pub fn xgroup_createconsumer(
        &mut self,
        key: &[u8],
        group: &[u8],
        consumer: Bytes,
    ) -> Result<bool, &'static str> {
        self.table.xgroup_createconsumer(key, group, consumer)
    }

    #[inline]
    pub fn xgroup_delconsumer(
        &mut self,
        key: &[u8],
        group: &[u8],
        consumer: &[u8],
    ) -> Result<usize, &'static str> {
        self.table.xgroup_delconsumer(key, group, consumer)
    }

    #[inline]
    pub fn xreadgroup(
        &mut self,
        key: &[u8],
        group: &[u8],
        consumer: Bytes,
        id_str: &str,
        count: Option<usize>,
        noack: bool,
    ) -> Result<Vec<(crate::table::StreamId, Vec<(Bytes, Bytes)>)>, &'static str> {
        self.table
            .xreadgroup(key, group, consumer, id_str, count, noack)
    }

    #[inline]
    pub fn xack(
        &mut self,
        key: &[u8],
        group: &[u8],
        ids: &[crate::table::StreamId],
    ) -> Result<usize, &'static str> {
        self.table.xack(key, group, ids)
    }

    #[inline]
    pub fn xpending_summary(
        &mut self,
        key: &[u8],
        group: &[u8],
    ) -> Result<
        (
            usize,
            Option<crate::table::StreamId>,
            Option<crate::table::StreamId>,
            Vec<(Bytes, usize)>,
        ),
        &'static str,
    > {
        self.table.xpending_summary(key, group)
    }

    #[inline]
    pub fn xpending_range(
        &mut self,
        key: &[u8],
        group: &[u8],
        start: crate::table::StreamId,
        end: crate::table::StreamId,
        count: usize,
        consumer: Option<&[u8]>,
    ) -> Result<Vec<(crate::table::StreamId, Bytes, u64, usize)>, &'static str> {
        self.table
            .xpending_range(key, group, start, end, count, consumer)
    }

    // Vector operations
    pub fn vadd(
        &mut self,
        index_name: &str,
        key: Bytes,
        vector: Vec<f32>,
        metric: Option<crate::vector::VectorMetric>,
        quantize: bool,
        pq: bool,
        tiered: bool,
    ) -> Result<(), &'static str> {
        let dim = vector.len();
        let idx = self
            .vector_indexes
            .entry(index_name.to_string())
            .or_insert_with(|| {
                crate::vector::HnswIndex::new(
                    index_name.to_string(),
                    dim,
                    metric.unwrap_or(crate::vector::VectorMetric::Cosine),
                )
            });
        idx.add_quantized_ext(key, vector, quantize, pq, tiered)
    }

    pub fn vquery(
        &self,
        index_name: &str,
        query: &[f32],
        k: usize,
        rerank: bool,
    ) -> Vec<(Bytes, f32)> {
        if let Some(idx) = self.vector_indexes.get(index_name) {
            idx.search_tiered(query, k, rerank)
        } else {
            Vec::new()
        }
    }

    pub fn vsim(
        &self,
        index_name: &str,
        k1: &Bytes,
        k2: &Bytes,
        metric_override: Option<crate::vector::VectorMetric>,
    ) -> Result<f32, &'static str> {
        if let Some(idx) = self.vector_indexes.get(index_name) {
            let v1 = idx.get_vector(k1).ok_or("vector 1 not found")?;
            let v2 = idx.get_vector(k2).ok_or("vector 2 not found")?;
            let metric = metric_override.unwrap_or(idx.metric);
            Ok(crate::vector::compute_distance(v1, v2, metric))
        } else {
            Err("index not found")
        }
    }

    pub fn vdel(&mut self, index_name: &str, key: &Bytes) -> bool {
        if let Some(idx) = self.vector_indexes.get_mut(index_name) {
            idx.remove(key)
        } else {
            false
        }
    }

    pub fn vinfo(&self, index_name: &str) -> Option<(usize, usize, &'static str, usize)> {
        self.vector_indexes
            .get(index_name)
            .map(|idx| (idx.len(), idx.dim, idx.metric.as_str(), idx.max_layer))
    }

    // Active-Active CRDT operations
    pub fn crdt_set(&mut self, key: Bytes, val: Bytes) -> crate::crdt::HlcTimestamp {
        self.crdt_store.set(key, val)
    }

    pub fn crdt_get(&self, key: &Bytes) -> Option<Bytes> {
        self.crdt_store.get(key)
    }

    pub fn crdt_del(&mut self, key: &Bytes) -> bool {
        self.crdt_store.del(key)
    }

    pub fn crdt_incrby(&mut self, key: Bytes, delta: i64) -> i64 {
        self.crdt_store.counter_incr(key, delta)
    }

    pub fn crdt_counter_get(&self, key: &Bytes) -> i64 {
        self.crdt_store.counter_get(key)
    }

    pub fn crdt_sadd(&mut self, key: Bytes, member: Bytes) -> bool {
        self.crdt_store.set_add(key, member)
    }

    pub fn crdt_smembers(&self, key: &Bytes) -> Vec<Bytes> {
        self.crdt_store.set_members(key)
    }

    pub fn crdt_srem(&mut self, key: &Bytes, member: &Bytes) -> bool {
        self.crdt_store.set_rem(key, member)
    }

    pub fn crdt_dump(&self) -> Vec<u8> {
        self.crdt_store.export_sync_payload()
    }

    pub fn crdt_merge(&mut self, payload: &[u8]) -> Result<usize, String> {
        self.crdt_store.merge_sync_payload(payload)
    }

    pub fn crdt_gc(&mut self, ttl_ms: Option<u64>) -> (usize, usize) {
        let ttl = ttl_ms.unwrap_or(86_400_000); // 24 hours default
        self.crdt_store.gc_tombstones(ttl)
    }
}
