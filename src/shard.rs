use bytes::Bytes;
use std::time::Duration;

use crate::resp::Command;

/// Inline compact representation of Redis responses for cross-core batch transfers.
/// Avoids heap allocations for responses up to 30 bytes (integers, OK, simple errors, small bulk strings).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactResp {
    Small { len: u8, data: [u8; 30] },
    Big(Vec<u8>),
    Bulk(Bytes),
    Array1Bulk(Bytes),
}

pub const DIGIT_PAIRS: &[u8; 200] = b"\
00010203040506070809\
10111213141516171819\
20212223242526272829\
30313233343536373839\
40414243444546474849\
50515253545556575859\
60616263646566676869\
70717273747576777879\
80818283848586878889\
90919293949596979899";

impl CompactResp {
    #[inline(always)]
    pub const fn empty() -> Self {
        CompactResp::Small {
            len: 0,
            data: [0u8; 30],
        }
    }

    pub const OK: Self = CompactResp::Small {
        len: 5,
        data: [
            b'+', b'O', b'K', b'\r', b'\n', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ],
    };

    pub const INT_0: Self = CompactResp::Small {
        len: 4,
        data: [
            b':', b'0', b'\r', b'\n', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
        ],
    };

    pub const INT_1: Self = CompactResp::Small {
        len: 4,
        data: [
            b':', b'1', b'\r', b'\n', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
        ],
    };

    pub const NULL: Self = CompactResp::Small {
        len: 5,
        data: [
            b'$', b'-', b'1', b'\r', b'\n', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0,
        ],
    };

    pub const EMPTY_ARRAY: Self = CompactResp::Small {
        len: 4,
        data: [
            b'*', b'0', b'\r', b'\n', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
        ],
    };

    #[inline(always)]
    pub fn from_bulk(bytes: &Bytes) -> Self {
        let n = bytes.len();
        if n <= 20 {
            let mut data = [0u8; 30];
            data[0] = b'$';
            let mut len = 1;
            if n < 10 {
                data[len] = b'0' + n as u8;
                len += 1;
            } else if n < 100 {
                data[len] = b'0' + (n / 10) as u8;
                data[len + 1] = b'0' + (n % 10) as u8;
                len += 2;
            } else {
                let mut rev = [0u8; 10];
                let mut rev_len = 0;
                let mut val = n;
                while val > 0 {
                    rev[rev_len] = b'0' + (val % 10) as u8;
                    rev_len += 1;
                    val /= 10;
                }
                for i in (0..rev_len).rev() {
                    data[len] = rev[i];
                    len += 1;
                }
            }
            data[len] = b'\r';
            data[len + 1] = b'\n';
            len += 2;
            data[len..len + n].copy_from_slice(bytes.as_ref());
            len += n;
            data[len] = b'\r';
            data[len + 1] = b'\n';
            len += 2;
            CompactResp::Small {
                len: len as u8,
                data,
            }
        } else {
            CompactResp::Bulk(bytes.clone())
        }
    }

    #[inline(always)]
    pub fn from_owned_bulk(bytes: Bytes) -> Self {
        let n = bytes.len();
        if n <= 20 {
            Self::from_bulk(&bytes)
        } else {
            CompactResp::Bulk(bytes)
        }
    }

    #[inline(always)]
    pub fn from_integer(val: i64) -> Self {
        let mut data = [0u8; 30];
        data[0] = b':';

        match val {
            0 => return Self::INT_0,
            1 => return Self::INT_1,
            2..=9 => {
                data[1] = b'0' + val as u8;
                data[2] = b'\r';
                data[3] = b'\n';
                return CompactResp::Small { len: 4, data };
            }
            10..=99 => {
                let p = (val as usize) * 2;
                data[1] = DIGIT_PAIRS[p];
                data[2] = DIGIT_PAIRS[p + 1];
                data[3] = b'\r';
                data[4] = b'\n';
                return CompactResp::Small { len: 5, data };
            }
            100..=999 => {
                let h = (val / 100) as u8;
                let p = ((val % 100) as usize) * 2;
                data[1] = b'0' + h;
                data[2] = DIGIT_PAIRS[p];
                data[3] = DIGIT_PAIRS[p + 1];
                data[4] = b'\r';
                data[5] = b'\n';
                return CompactResp::Small { len: 6, data };
            }
            1000..=9999 => {
                let p1 = ((val / 100) as usize) * 2;
                let p2 = ((val % 100) as usize) * 2;
                data[1] = DIGIT_PAIRS[p1];
                data[2] = DIGIT_PAIRS[p1 + 1];
                data[3] = DIGIT_PAIRS[p2];
                data[4] = DIGIT_PAIRS[p2 + 1];
                data[5] = b'\r';
                data[6] = b'\n';
                return CompactResp::Small { len: 7, data };
            }
            _ => {}
        }

        let mut len = 1;
        let mut n = if val < 0 {
            data[1] = b'-';
            len = 2;
            val.unsigned_abs()
        } else {
            val as u64
        };
        let mut rev = [0u8; 20];
        let mut rev_len = 0;
        while n > 0 {
            rev[rev_len] = b'0' + (n % 10) as u8;
            rev_len += 1;
            n /= 10;
        }
        for i in (0..rev_len).rev() {
            data[len] = rev[i];
            len += 1;
        }
        data[len] = b'\r';
        data[len + 1] = b'\n';
        CompactResp::Small {
            len: (len + 2) as u8,
            data,
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
    pub fn estimated_len(&self) -> usize {
        match self {
            CompactResp::Small { len, .. } => *len as usize,
            CompactResp::Big(vec) => vec.len(),
            CompactResp::Bulk(bytes) => bytes.len() + 16,
            CompactResp::Array1Bulk(bytes) => bytes.len() + 20,
        }
    }

    #[inline(always)]
    pub fn write_to(&self, out: &mut Vec<u8>) {
        match self {
            CompactResp::Small { len, data } => out.extend_from_slice(&data[..*len as usize]),
            CompactResp::Big(vec) => out.extend_from_slice(vec),
            CompactResp::Bulk(bytes) => crate::connection::write_resp_bulk(out, bytes),
            CompactResp::Array1Bulk(bytes) => {
                out.extend_from_slice(b"*1\r\n");
                crate::connection::write_resp_bulk(out, bytes);
            }
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            CompactResp::Small { len, data } => &data[..*len as usize],
            CompactResp::Big(vec) => vec.as_slice(),
            CompactResp::Bulk(bytes) | CompactResp::Array1Bulk(bytes) => bytes.as_ref(),
        }
    }

    #[inline(always)]
    pub fn into_vec(self) -> Vec<u8> {
        match self {
            CompactResp::Small { len, data } => data[..len as usize].to_vec(),
            CompactResp::Big(vec) => vec,
            CompactResp::Bulk(bytes) => {
                let mut v = Vec::with_capacity(bytes.len() + 16);
                crate::connection::write_resp_bulk(&mut v, &bytes);
                v
            }
            CompactResp::Array1Bulk(bytes) => {
                let mut v = Vec::with_capacity(bytes.len() + 20);
                v.extend_from_slice(b"*1\r\n");
                crate::connection::write_resp_bulk(&mut v, &bytes);
                v
            }
        }
    }
}

/// Messages passed across CPU cores to access or mutate a shard's data.
pub enum ShardMessage {
    /// Transfer an accepted connection to a less loaded shard.
    ///
    /// `SO_REUSEPORT` assigns connections by kernel 4-tuple hash, which is
    /// uneven (see `crate::conn_balance`). The accepting shard hands the raw fd
    /// to the least loaded shard instead of keeping it. All shards are threads
    /// in one process sharing a single fd table, so passing the integer is
    /// sufficient -- no `SCM_RIGHTS`. The sender relinquishes ownership without
    /// closing; the receiver takes it over.
    AdoptConnection {
        fd: std::os::unix::io::RawFd,
        peer: std::net::SocketAddr,
    },
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
    DelKeys {
        keys: Vec<Bytes>,
        responder: flume::Sender<usize>,
    },
    ActiveDefrag {
        responder: flume::Sender<usize>,
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
        items: Vec<(usize, u64, Command)>,
        results: Vec<(usize, CompactResp)>,
        responder: std::sync::Arc<crate::mailbox::BatchResponder>,
        is_resp3: bool,
    },
    Mget {
        keys: Vec<(usize, Option<Bytes>)>,
        responder: flume::Sender<Vec<(usize, Option<Bytes>)>>,
    },
    ScatterMget {
        shard_id: usize,
        keys: Vec<(usize, Bytes)>,
        descriptor: std::sync::Arc<crate::mailbox::ScatterMgetDescriptor>,
    },
    Mset {
        pairs: Vec<(Bytes, Bytes)>,
        responder: flume::Sender<Vec<(Bytes, Bytes)>>,
    },
    JsonMget {
        keys: Vec<(usize, Bytes)>,
        path: String,
        responder: flume::Sender<Vec<(usize, Option<String>)>>,
    },
    RewriteAof {
        dir: std::path::PathBuf,
        shard_id: usize,
        responder: flume::Sender<Result<usize, String>>,
    },
    ScatterMset {
        shard_id: usize,
        pairs: Vec<(Bytes, Bytes)>,
        descriptor: std::sync::Arc<crate::mailbox::ScatterMsetDescriptor>,
    },
    FastGet {
        descriptor: std::sync::Arc<crate::mailbox::FastGetDescriptor>,
    },
    FastSet {
        descriptor: std::sync::Arc<crate::mailbox::FastSetDescriptor>,
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
    FlushCommandStats {
        responder: flume::Sender<()>,
    },
    ResetCommandStats {
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
    Spublish {
        channel: Bytes,
        message: Bytes,
        responder: flume::Sender<usize>,
    },
    Ssubscribe {
        client_id: u64,
        channel: Bytes,
        sender: flume::Sender<Bytes>,
        is_resp3: bool,
        responder: flume::Sender<()>,
    },
    Sunsubscribe {
        client_id: u64,
        channel: Bytes,
        responder: flume::Sender<()>,
    },
    PubsubChannels {
        pattern: Option<Bytes>,
        responder: flume::Sender<Vec<Bytes>>,
    },
    PubsubShardchannels {
        pattern: Option<Bytes>,
        responder: flume::Sender<Vec<Bytes>>,
    },
    PubsubNumsub {
        channels: Vec<Bytes>,
        responder: flume::Sender<Vec<(Bytes, usize)>>,
    },
    PubsubShardnumsub {
        channels: Vec<Bytes>,
        responder: flume::Sender<Vec<(Bytes, usize)>>,
    },
    PubsubNumpat {
        responder: flume::Sender<usize>,
    },
    RemoveClientPubSub {
        client_id: u64,
    },
    InitSearchIndex {
        schema: crate::search::IndexSchema,
        responder: flume::Sender<()>,
    },
    DropSearchIndex {
        name: String,
        responder: flume::Sender<bool>,
    },
    SearchQuery {
        index: String,
        ast: crate::search::QueryAst,
        options: crate::search::SearchOptions,
        responder: flume::Sender<(usize, Vec<crate::search::SearchHit>)>,
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
    pub search_indices: std::collections::HashMap<String, crate::search::InvertedIndex>,
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
            search_indices: std::collections::HashMap::new(),
        }
    }

    #[inline(always)]
    pub fn has_search_indices(&self) -> bool {
        !self.search_indices.is_empty()
    }

    pub fn init_search_index(&mut self, schema: crate::search::IndexSchema) {
        self.search_indices.insert(
            schema.name.clone(),
            crate::search::InvertedIndex::new(schema),
        );
    }

    pub fn drop_search_index(&mut self, name: &str) -> bool {
        self.search_indices.remove(name).is_some()
    }

    pub fn index_document_local(
        &mut self,
        key: &str,
        fields: std::collections::HashMap<String, String>,
    ) {
        for idx in self.search_indices.values_mut() {
            if let Some(schema) = &idx.schema {
                if schema.on_type.to_uppercase() != "HASH" {
                    continue;
                }
                let matched = if schema.prefixes.is_empty() {
                    true
                } else {
                    schema.prefixes.iter().any(|p| key.starts_with(p))
                };
                if matched {
                    idx.add_document(key, fields.clone(), None);
                }
            }
        }
    }

    pub fn delete_document_local(&mut self, key: &str) {
        for idx in self.search_indices.values_mut() {
            idx.remove_document(key);
        }
    }

    pub fn index_json_document_local(&mut self, key: &str, root: &serde_json::Value) {
        for idx in self.search_indices.values_mut() {
            if let Some(schema) = &idx.schema {
                if schema.on_type.to_uppercase() != "JSON" {
                    continue;
                }
                let matched = if schema.prefixes.is_empty() {
                    true
                } else {
                    schema.prefixes.iter().any(|p| key.starts_with(p))
                };
                if !matched {
                    continue;
                }
                let (extracted_fields, extracted_vectors) =
                    crate::search::extract_json_fields(schema, root);
                idx.add_document(key, extracted_fields, extracted_vectors);
            }
        }
    }

    #[inline]
    pub fn get_entry(
        &mut self,
        key: &[u8],
    ) -> Option<(crate::table::RudisValue, Option<Duration>)> {
        self.table.get_entry(key)
    }

    #[inline(always)]
    pub fn get(&mut self, key: &[u8]) -> Option<Bytes> {
        self.table.get(key).ok().flatten()
    }

    #[inline(always)]
    pub fn get_with_hash(&mut self, key: &[u8], h: u64) -> Option<Bytes> {
        self.table.get_with_hash(key, h).ok().flatten()
    }

    #[inline(always)]
    pub fn write_get_resp(&mut self, key: &[u8], out: &mut Vec<u8>) -> Result<bool, &'static str> {
        self.table.write_get_resp(key, out)
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

    #[inline(always)]
    pub fn del(&mut self, key: &[u8]) -> bool {
        if self.tier_manager.is_none() && self.sticky_keys.is_empty() {
            return self.table.del(key);
        }
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

    #[inline(always)]
    pub fn del_with_hash(&mut self, key: &[u8], hash: u64) -> bool {
        if self.tier_manager.is_none() && self.sticky_keys.is_empty() {
            return self.table.del_with_hash(key, hash);
        }
        self.del(key)
    }

    #[inline]
    pub fn active_defrag(&mut self) -> usize {
        self.table.active_defrag()
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

    #[inline(always)]
    pub fn exists(&mut self, key: &[u8]) -> bool {
        self.table.exists(key)
    }

    #[inline(always)]
    pub fn exists_with_hash(&mut self, key: &[u8], hash: u64) -> bool {
        self.table.exists_with_hash(key, hash)
    }

    #[inline(always)]
    pub fn lpop_one(&mut self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        self.table.lpop_one(key)
    }

    #[inline(always)]
    pub fn rpop_one(&mut self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        self.table.rpop_one(key)
    }

    #[inline(always)]
    pub fn get_compact(&mut self, key: &[u8]) -> Result<Option<CompactResp>, &'static str> {
        self.table.get_compact(key)
    }

    #[inline(always)]
    pub fn hget_compact(&mut self, key: &[u8], field: &[u8]) -> Result<CompactResp, &'static str> {
        self.table.hget_compact(key, field)
    }

    #[inline(always)]
    pub fn incr_by_slice_fast(&mut self, key: &Bytes, delta: i64) -> Result<i64, &'static str> {
        self.table.incr_by_slice_fast(key, delta)
    }

    #[inline]
    pub fn incr_by_slice(&mut self, key: &[u8], delta: i64) -> Result<i64, &'static str> {
        self.table.incr_by_slice(key, delta)
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

    #[inline(always)]
    pub fn hset_slice_fast(
        &mut self,
        key: &Bytes,
        fields: &[(Bytes, Bytes)],
    ) -> Result<usize, &'static str> {
        self.table.hset_slice_fast(key, fields)
    }

    #[inline]
    pub fn hset_slice(
        &mut self,
        key: &[u8],
        fields: &[(Bytes, Bytes)],
    ) -> Result<usize, &'static str> {
        self.table.hset_slice(key, fields)
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

    #[inline(always)]
    pub fn write_hget_resp(
        &mut self,
        key: &[u8],
        field: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), &'static str> {
        self.table.write_hget_resp(key, field, out)
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
    #[inline(always)]
    pub fn lpush_slice_fast(
        &mut self,
        key: &Bytes,
        values: &[Bytes],
    ) -> Result<usize, &'static str> {
        self.table.lpush_slice_fast(key, values)
    }

    #[inline]
    pub fn lpush_slice(&mut self, key: &[u8], values: &[Bytes]) -> Result<usize, &'static str> {
        self.table.lpush_slice(key, values)
    }

    #[inline]
    pub fn lpush(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.table.lpush(key, values)
    }

    #[inline]
    pub fn rpush_slice_fast(
        &mut self,
        key: &Bytes,
        values: &[Bytes],
    ) -> Result<usize, &'static str> {
        self.table.rpush_slice_fast(key, values)
    }

    #[inline]
    pub fn rpush_slice(&mut self, key: &[u8], values: &[Bytes]) -> Result<usize, &'static str> {
        self.table.rpush_slice(key, values)
    }

    #[inline]
    pub fn rpush(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.table.rpush(key, values)
    }

    #[inline]
    pub fn lpushx_slice(&mut self, key: &[u8], values: &[Bytes]) -> Result<usize, &'static str> {
        self.table.lpushx_slice(key, values)
    }

    #[inline]
    pub fn lpushx(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.table.lpushx(key, values)
    }

    #[inline]
    pub fn rpushx_slice(&mut self, key: &[u8], values: &[Bytes]) -> Result<usize, &'static str> {
        self.table.rpushx_slice(key, values)
    }

    #[inline]
    pub fn rpushx(&mut self, key: Bytes, values: Vec<Bytes>) -> Result<usize, &'static str> {
        self.table.rpushx(key, values)
    }

    #[inline]
    pub fn lpop(&mut self, key: &[u8], count: usize) -> Result<Vec<Bytes>, &'static str> {
        self.table.lpop(key, count)
    }

    #[inline(always)]
    pub fn write_lpop_resp(
        &mut self,
        key: &[u8],
        count: Option<usize>,
        out: &mut Vec<u8>,
    ) -> Result<bool, &'static str> {
        self.table.write_lpop_resp(key, count, out)
    }

    #[inline(always)]
    pub fn write_lpop_resp_with_hash(
        &mut self,
        key: &[u8],
        h: u64,
        count: Option<usize>,
        out: &mut Vec<u8>,
    ) -> Result<bool, &'static str> {
        self.table.write_lpop_resp_with_hash(key, h, count, out)
    }

    #[inline]
    pub fn rpop(&mut self, key: &[u8], count: usize) -> Result<Vec<Bytes>, &'static str> {
        self.table.rpop(key, count)
    }

    #[inline(always)]
    pub fn write_rpop_resp(
        &mut self,
        key: &[u8],
        count: Option<usize>,
        out: &mut Vec<u8>,
    ) -> Result<bool, &'static str> {
        self.table.write_rpop_resp(key, count, out)
    }

    #[inline(always)]
    pub fn write_rpop_resp_with_hash(
        &mut self,
        key: &[u8],
        h: u64,
        count: Option<usize>,
        out: &mut Vec<u8>,
    ) -> Result<bool, &'static str> {
        self.table.write_rpop_resp_with_hash(key, h, count, out)
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

    #[inline(always)]
    pub fn write_lrange_resp(
        &mut self,
        key: &[u8],
        start: i64,
        stop: i64,
        out: &mut Vec<u8>,
    ) -> Result<(), &'static str> {
        self.table.write_lrange_resp(key, start, stop, out)
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
    #[inline(always)]
    pub fn sadd_slice_fast(
        &mut self,
        key: &Bytes,
        members: &[Bytes],
    ) -> Result<usize, &'static str> {
        self.table.sadd_slice_fast(key, members)
    }

    #[inline]
    pub fn sadd_slice(&mut self, key: &[u8], members: &[Bytes]) -> Result<usize, &'static str> {
        self.table.sadd_slice(key, members)
    }

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

    #[inline(always)]
    pub fn sismember_compact(
        &mut self,
        key: &[u8],
        member: &[u8],
    ) -> Result<CompactResp, &'static str> {
        self.table.sismember_compact(key, member)
    }

    #[inline(always)]
    pub fn write_sismember_resp(
        &mut self,
        key: &[u8],
        member: &[u8],
        out: &mut Vec<u8>,
    ) -> Result<(), &'static str> {
        self.table.write_sismember_resp(key, member, out)
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

    #[inline(always)]
    pub fn zadd_slice_fast(
        &mut self,
        key: &Bytes,
        elements: &[(f64, Bytes)],
        flags: crate::table::ZAddFlags,
    ) -> Result<(usize, Option<f64>), &'static str> {
        self.table.zadd_slice_fast(key, elements, flags)
    }

    #[inline]
    pub fn zadd_slice(
        &mut self,
        key: &[u8],
        elements: &[(f64, Bytes)],
        flags: crate::table::ZAddFlags,
    ) -> Result<(usize, Option<f64>), &'static str> {
        self.table.zadd_slice(key, elements, flags)
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

    #[inline(always)]
    pub fn write_zrange_resp(
        &mut self,
        key: &[u8],
        opts: &crate::table::ZRangeOpts,
        is_resp3: bool,
        out: &mut Vec<u8>,
    ) -> Result<(), &'static str> {
        self.table.write_zrange_resp(key, opts, is_resp3, out)
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
        self.save_extended_rdb_chunk(buf);
    }

    pub fn save_extended_rdb_chunk(&self, buf: &mut Vec<u8>) {
        // 1. JSON documents
        for (key, val) in self.json_store.iter() {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.push(7u8);
            let json_str = val.to_string();
            buf.extend_from_slice(&(json_str.len() as u32).to_le_bytes());
            buf.extend_from_slice(json_str.as_bytes());
        }

        // 2. Bloom filters
        for (key, bf) in &self.probabilistic_store.bloom_filters {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.push(8u8);
            buf.extend_from_slice(&(bf.capacity as u64).to_le_bytes());
            buf.extend_from_slice(&bf.error_rate.to_bits().to_le_bytes());
            buf.extend_from_slice(&(bf.num_bits as u64).to_le_bytes());
            buf.extend_from_slice(&(bf.num_hashes as u32).to_le_bytes());
            buf.extend_from_slice(&(bf.count as u64).to_le_bytes());
            buf.extend_from_slice(&(bf.bits.len() as u32).to_le_bytes());
            for word in &bf.bits {
                buf.extend_from_slice(&word.to_le_bytes());
            }
        }

        // 3. Vector indexes
        for (name, index) in &self.vector_indexes {
            for (key, &node_id) in &index.key_to_id {
                if let Some(Some(node)) = index.nodes.get(node_id) {
                    let full_key = format!("vec:{}:{}", name, String::from_utf8_lossy(key));
                    buf.extend_from_slice(&(full_key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(full_key.as_bytes());
                    buf.push(9u8);
                    buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
                    buf.extend_from_slice(name.as_bytes());
                    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key);
                    buf.push(index.metric as u8);
                    buf.extend_from_slice(&(node.vector.len() as u32).to_le_bytes());
                    for &coord in &node.vector {
                        buf.extend_from_slice(&coord.to_bits().to_le_bytes());
                    }
                }
            }
        }

        // 4. Cuckoo filters
        for (key, cf) in &self.probabilistic_store.cuckoo_filters {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.push(10u8);
            buf.extend_from_slice(&(cf.capacity as u64).to_le_bytes());
            buf.extend_from_slice(&(cf.num_buckets as u64).to_le_bytes());
            buf.extend_from_slice(&(cf.count as u64).to_le_bytes());
            buf.extend_from_slice(&(cf.buckets.len() as u32).to_le_bytes());
            for bucket in &cf.buckets {
                for &fp in bucket {
                    buf.extend_from_slice(&fp.to_le_bytes());
                }
            }
        }

        // 5. Count-Min Sketches
        for (key, cms) in &self.probabilistic_store.cms_sketches {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.push(11u8);
            buf.extend_from_slice(&(cms.width as u64).to_le_bytes());
            buf.extend_from_slice(&(cms.depth as u32).to_le_bytes());
            buf.extend_from_slice(&cms.total_count.to_le_bytes());
            for row in &cms.table {
                for &cell in row {
                    buf.extend_from_slice(&cell.to_le_bytes());
                }
            }
        }

        // 6. Top-K trackers
        for (key, topk) in &self.probabilistic_store.topk_trackers {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.push(12u8);
            buf.extend_from_slice(&(topk.k as u64).to_le_bytes());
            buf.extend_from_slice(&(topk.items.len() as u32).to_le_bytes());
            for (item_key, &count_val) in &topk.items {
                buf.extend_from_slice(&(item_key.len() as u32).to_le_bytes());
                buf.extend_from_slice(item_key);
                buf.extend_from_slice(&count_val.to_le_bytes());
            }
        }

        // 7. CRDT sync state
        let crdt_payload = self.crdt_store.export_sync_payload();
        if !crdt_payload.is_empty() {
            let crdt_marker = Bytes::from_static(b"__rudis_crdt_sync__");
            buf.extend_from_slice(&(crdt_marker.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crdt_marker);
            buf.push(13u8);
            buf.extend_from_slice(&(crdt_payload.len() as u32).to_le_bytes());
            buf.extend_from_slice(&crdt_payload);
        }
    }

    pub fn restore_rdb_chunk(&mut self, mut data: &[u8]) -> Result<(), &'static str> {
        let unix_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        while !data.is_empty() {
            let mut expire_at = None;
            if data[0] == 0xFC {
                if data.len() < 9 {
                    return Err("Truncated RDB expire");
                }
                let exp_unix_ms = u64::from_le_bytes(data[1..9].try_into().unwrap());
                data = &data[9..];
                if exp_unix_ms <= unix_now {
                    if data.len() < 4 {
                        return Err("Truncated RDB key");
                    }
                    let k_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                    data = &data[4..];
                    if data.len() < k_len {
                        return Err("Truncated RDB key");
                    }
                    data = &data[k_len..];
                    let (_, consumed) = crate::table::RudisTable::deserialize_val_payload(data)?;
                    data = &data[consumed..];
                    continue;
                }
                let rem_ms = exp_unix_ms - unix_now;
                expire_at =
                    Some(std::time::Instant::now() + std::time::Duration::from_millis(rem_ms));
            }
            if data.len() < 4 {
                return Err("Truncated RDB key");
            }
            let k_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
            data = &data[4..];
            if data.len() < k_len {
                return Err("Truncated RDB key");
            }
            let key = bytes::Bytes::copy_from_slice(&data[..k_len]);
            data = &data[k_len..];

            if data.is_empty() {
                return Err("Truncated RDB type");
            }
            let type_byte = data[0];
            if type_byte == 7 {
                data = &data[1..];
                if data.len() < 4 {
                    return Err("Truncated JSON len");
                }
                let json_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                data = &data[4..];
                if data.len() < json_len {
                    return Err("Truncated JSON payload");
                }
                if let Ok(json_str) = std::str::from_utf8(&data[..json_len])
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(json_str)
                {
                    self.json_store.insert_raw(key, val);
                }
                data = &data[json_len..];
                continue;
            } else if type_byte == 8 {
                data = &data[1..];
                if data.len() < 40 {
                    return Err("Truncated BloomFilter header");
                }
                let capacity = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
                let error_rate =
                    f64::from_bits(u64::from_le_bytes(data[8..16].try_into().unwrap()));
                let num_bits = u64::from_le_bytes(data[16..24].try_into().unwrap()) as usize;
                let num_hashes = u32::from_le_bytes(data[24..28].try_into().unwrap()) as usize;
                let count_val = u64::from_le_bytes(data[28..36].try_into().unwrap()) as usize;
                let bits_len = u32::from_le_bytes(data[36..40].try_into().unwrap()) as usize;
                data = &data[40..];
                if data.len() < bits_len * 8 {
                    return Err("Truncated BloomFilter bits");
                }
                let mut bits = Vec::with_capacity(bits_len);
                for i in 0..bits_len {
                    bits.push(u64::from_le_bytes(
                        data[i * 8..(i + 1) * 8].try_into().unwrap(),
                    ));
                }
                data = &data[bits_len * 8..];
                self.probabilistic_store.bloom_filters.insert(
                    key,
                    crate::probabilistic::BloomFilter {
                        capacity,
                        error_rate,
                        num_bits,
                        num_hashes,
                        count: count_val,
                        bits,
                    },
                );
                continue;
            } else if type_byte == 9 {
                data = &data[1..];
                if data.len() < 4 {
                    return Err("Truncated vector index name len");
                }
                let idx_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                data = &data[4..];
                if data.len() < idx_len {
                    return Err("Truncated vector index name");
                }
                let idx_name = String::from_utf8_lossy(&data[..idx_len]).to_string();
                data = &data[idx_len..];

                if data.len() < 4 {
                    return Err("Truncated vector doc key len");
                }
                let doc_key_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                data = &data[4..];
                if data.len() < doc_key_len {
                    return Err("Truncated vector doc key");
                }
                let doc_key = bytes::Bytes::copy_from_slice(&data[..doc_key_len]);
                data = &data[doc_key_len..];

                if data.len() < 5 {
                    return Err("Truncated vector metric and dim");
                }
                let metric_byte = data[0];
                let metric = match metric_byte {
                    0 => crate::vector::VectorMetric::Cosine,
                    1 => crate::vector::VectorMetric::L2,
                    _ => crate::vector::VectorMetric::IP,
                };
                let vec_len = u32::from_le_bytes(data[1..5].try_into().unwrap()) as usize;
                data = &data[5..];
                if data.len() < vec_len * 4 {
                    return Err("Truncated vector coordinates");
                }
                let mut vector = Vec::with_capacity(vec_len);
                for i in 0..vec_len {
                    let bits = u32::from_le_bytes(data[i * 4..(i + 1) * 4].try_into().unwrap());
                    vector.push(f32::from_bits(bits));
                }
                data = &data[vec_len * 4..];
                let _ = self.vadd(
                    &idx_name,
                    doc_key,
                    vector,
                    Some(metric),
                    false,
                    false,
                    false,
                );
                continue;
            } else if type_byte == 10 {
                data = &data[1..];
                if data.len() < 28 {
                    return Err("Truncated CuckooFilter header");
                }
                let capacity = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
                let num_buckets = u64::from_le_bytes(data[8..16].try_into().unwrap()) as usize;
                let count_val = u64::from_le_bytes(data[16..24].try_into().unwrap()) as usize;
                let buckets_len = u32::from_le_bytes(data[24..28].try_into().unwrap()) as usize;
                data = &data[28..];
                if data.len() < buckets_len * 8 {
                    return Err("Truncated CuckooFilter buckets");
                }
                let mut buckets = Vec::with_capacity(buckets_len);
                for i in 0..buckets_len {
                    let base = i * 8;
                    let fp0 = u16::from_le_bytes(data[base..base + 2].try_into().unwrap());
                    let fp1 = u16::from_le_bytes(data[base + 2..base + 4].try_into().unwrap());
                    let fp2 = u16::from_le_bytes(data[base + 4..base + 6].try_into().unwrap());
                    let fp3 = u16::from_le_bytes(data[base + 6..base + 8].try_into().unwrap());
                    buckets.push([fp0, fp1, fp2, fp3]);
                }
                data = &data[buckets_len * 8..];
                self.probabilistic_store.cuckoo_filters.insert(
                    key,
                    crate::probabilistic::CuckooFilter {
                        capacity,
                        num_buckets,
                        count: count_val,
                        buckets,
                    },
                );
                continue;
            } else if type_byte == 11 {
                data = &data[1..];
                if data.len() < 20 {
                    return Err("Truncated CountMinSketch header");
                }
                let width = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
                let depth = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
                let total_count = u64::from_le_bytes(data[12..20].try_into().unwrap());
                data = &data[20..];
                let total_cells = width * depth;
                if data.len() < total_cells * 8 {
                    return Err("Truncated CountMinSketch cells");
                }
                let mut table = Vec::with_capacity(depth);
                for _ in 0..depth {
                    let mut row = Vec::with_capacity(width);
                    for _ in 0..width {
                        let cell = u64::from_le_bytes(data[0..8].try_into().unwrap());
                        data = &data[8..];
                        row.push(cell);
                    }
                    table.push(row);
                }
                self.probabilistic_store.cms_sketches.insert(
                    key,
                    crate::probabilistic::CountMinSketch {
                        width,
                        depth,
                        total_count,
                        table,
                    },
                );
                continue;
            } else if type_byte == 12 {
                data = &data[1..];
                if data.len() < 12 {
                    return Err("Truncated TopK header");
                }
                let k = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
                let items_len = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
                data = &data[12..];
                let mut items = hashbrown::HashMap::with_capacity(items_len);
                for _ in 0..items_len {
                    if data.len() < 4 {
                        return Err("Truncated TopK item len");
                    }
                    let item_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                    data = &data[4..];
                    if data.len() < item_len + 8 {
                        return Err("Truncated TopK item data");
                    }
                    let item_key = bytes::Bytes::copy_from_slice(&data[..item_len]);
                    data = &data[item_len..];
                    let count_val = u64::from_le_bytes(data[0..8].try_into().unwrap());
                    data = &data[8..];
                    items.insert(item_key, count_val);
                }
                self.probabilistic_store
                    .topk_trackers
                    .insert(key, crate::probabilistic::TopK { k, items });
                continue;
            } else if type_byte == 13 {
                data = &data[1..];
                if data.len() < 4 {
                    return Err("Truncated CRDT payload len");
                }
                let payload_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                data = &data[4..];
                if data.len() < payload_len {
                    return Err("Truncated CRDT payload");
                }
                let payload = &data[..payload_len];
                data = &data[payload_len..];
                let _ = self.crdt_store.merge_sync_payload(payload);
                continue;
            }

            let (val, consumed) = crate::table::RudisTable::deserialize_val_payload(data)?;
            data = &data[consumed..];

            self.table.insert_entry(crate::table::RudisEntry {
                key,
                val,
                expire_at,
            });
        }
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fast_integer_formatting() {
        for v in [
            0, 1, 7, 10, 42, 99, 100, 123, 999, 1000, 1234, 9999, 10000, 123456, -1, -42,
        ] {
            let resp = CompactResp::from_integer(v);
            let expected = format!(":{}\r\n", v);
            assert_eq!(
                resp.as_slice(),
                expected.as_bytes(),
                "Failed for integer {}",
                v
            );
        }
    }

    #[test]
    fn test_extended_rdb_save_and_restore() {
        let mut db = ShardDb::new(0);

        // 1. Add JSON
        let _json_key = Bytes::from("user:101");
        assert!(
            db.json_store
                .json_set(
                    b"user:101",
                    "$",
                    r#"{"name":"alice","age":30}"#,
                    false,
                    false
                )
                .unwrap()
        );

        // 2. Add Bloom filter
        let bf_key = Bytes::from("bloom:test");
        let mut bf = crate::probabilistic::BloomFilter::new(1000, 0.01);
        bf.add(b"item_alpha");
        db.probabilistic_store
            .bloom_filters
            .insert(bf_key.clone(), bf);

        // 3. Add Cuckoo filter
        let cf_key = Bytes::from("cuckoo:test");
        let mut cf = crate::probabilistic::CuckooFilter::new(100);
        cf.add(b"cf_alpha").unwrap();
        db.probabilistic_store
            .cuckoo_filters
            .insert(cf_key.clone(), cf);

        // 4. Add CountMinSketch
        let cms_key = Bytes::from("cms:test");
        let mut cms = crate::probabilistic::CountMinSketch::new(100, 4);
        cms.incr_by(b"packet_loss", 42);
        db.probabilistic_store
            .cms_sketches
            .insert(cms_key.clone(), cms);

        // 5. Add TopK
        let topk_key = Bytes::from("topk:test");
        let mut topk = crate::probabilistic::TopK::new(3);
        topk.add(Bytes::from_static(b"user_vip"), 100);
        db.probabilistic_store
            .topk_trackers
            .insert(topk_key.clone(), topk);

        // 6. Add CRDT registers and counters
        db.crdt_set(
            Bytes::from_static(b"crdt:reg"),
            Bytes::from_static(b"val_crdt"),
        );
        db.crdt_incrby(Bytes::from_static(b"crdt:cnt"), 99);

        // 7. Add Vector
        let vec_doc = Bytes::from("doc:1");
        db.vadd(
            "test_idx",
            vec_doc.clone(),
            vec![1.0, 2.0, 3.0],
            Some(crate::vector::VectorMetric::Cosine),
            false,
            false,
            false,
        )
        .unwrap();

        // Serialize to RDB chunk
        let mut chunk = Vec::new();
        db.save_rdb_chunk(&mut chunk);
        assert!(!chunk.is_empty());

        // Restore into new ShardDb
        let mut new_db = ShardDb::new(0);
        new_db
            .restore_rdb_chunk(&chunk)
            .expect("restore_rdb_chunk should succeed");

        // Verify JSON
        let json_val = new_db
            .json_store
            .json_get(b"user:101", &["$"])
            .expect("JSON document should exist");
        assert!(json_val.contains("alice"));

        // Verify Bloom filter
        let restored_bf = new_db
            .probabilistic_store
            .bloom_filters
            .get(&bf_key)
            .expect("Bloom filter should exist");
        assert!(restored_bf.contains(b"item_alpha"));
        assert!(!restored_bf.contains(b"nonexistent"));

        // Verify Cuckoo filter
        let restored_cf = new_db
            .probabilistic_store
            .cuckoo_filters
            .get(&cf_key)
            .expect("Cuckoo filter should exist");
        assert!(restored_cf.contains(b"cf_alpha"));
        assert!(!restored_cf.contains(b"nonexistent"));

        // Verify CountMinSketch
        let restored_cms = new_db
            .probabilistic_store
            .cms_sketches
            .get(&cms_key)
            .expect("CMS should exist");
        assert_eq!(restored_cms.query(b"packet_loss"), 42);

        // Verify TopK
        let restored_topk = new_db
            .probabilistic_store
            .topk_trackers
            .get(&topk_key)
            .expect("TopK should exist");
        assert!(restored_topk.query(b"user_vip"));

        // Verify CRDT
        assert_eq!(
            new_db.crdt_get(&Bytes::from_static(b"crdt:reg")),
            Some(Bytes::from_static(b"val_crdt"))
        );
        assert_eq!(
            new_db.crdt_counter_get(&Bytes::from_static(b"crdt:cnt")),
            99
        );

        // Verify Vector
        assert!(new_db.vector_indexes.contains_key("test_idx"));
        let neighbors = new_db.vquery("test_idx", &[1.0, 2.0, 3.0], 1, false);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, vec_doc);
    }

    #[test]
    fn test_compact_resp_estimated_len() {
        assert_eq!(CompactResp::OK.estimated_len(), 5); // +OK\r\n
        assert_eq!(CompactResp::INT_1.estimated_len(), 4); // :1\r\n
        assert_eq!(CompactResp::NULL.estimated_len(), 5); // $-1\r\n

        let bulk = CompactResp::Bulk(Bytes::from("hello"));
        assert!(bulk.estimated_len() >= 5);

        let big = CompactResp::from_vec(vec![0u8; 100]);
        assert_eq!(big.estimated_len(), 100);
    }
}
