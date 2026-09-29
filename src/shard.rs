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
    RawBytes(Bytes),
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

    pub const NULL_RESP3: Self = CompactResp::Small {
        len: 3,
        data: [
            b'_', b'\r', b'\n', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0,
        ],
    };

    #[inline(always)]
    pub fn null(is_resp3: bool) -> Self {
        if is_resp3 {
            Self::NULL_RESP3
        } else {
            Self::NULL
        }
    }

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
            CompactResp::RawBytes(bytes) => bytes.len(),
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
            CompactResp::RawBytes(bytes) => out.extend_from_slice(bytes),
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            CompactResp::Small { len, data } => &data[..*len as usize],
            CompactResp::Big(vec) => vec.as_slice(),
            CompactResp::Bulk(bytes)
            | CompactResp::Array1Bulk(bytes)
            | CompactResp::RawBytes(bytes) => bytes.as_ref(),
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
            CompactResp::RawBytes(bytes) => bytes.to_vec(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ScanParams {
    pub slot: usize,
    pub pattern: Option<Bytes>,
    pub count: usize,
    pub key_type: Option<Bytes>,
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
        opts: crate::resp::ExpireOptions,
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
        schema: Box<crate::search::IndexSchema>,
        responder: flume::Sender<()>,
    },
    DropSearchIndex {
        name: String,
        responder: flume::Sender<bool>,
    },
    SearchQuery {
        index: String,
        ast: Box<crate::search::QueryAst>,
        options: Box<crate::search::SearchOptions>,
        responder: flume::Sender<(usize, Vec<crate::search::SearchHit>)>,
    },
    Keys {
        pattern: Bytes,
        responder: flume::Sender<Vec<Bytes>>,
    },
    Scan {
        params: Box<ScanParams>,
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
        cmd: Box<Command>,
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
        condition: Option<Box<(String, Bytes)>>,
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
    pub shard_id: usize,
    pub tier_manager: Option<std::rc::Rc<crate::tiering::ShardTierManager>>,
    pub vector_indexes: std::collections::HashMap<String, crate::vector::HnswIndex>,
    pub semantic_caches: hashbrown::HashMap<Bytes, crate::vector::SemanticCache>,
    pub agent_memories: hashbrown::HashMap<Bytes, crate::agent::AgentMemorySession>,
    pub llm_quotas: hashbrown::HashMap<Bytes, crate::agent::LlmQuotaBucket>,
    pub agent_checkpoints: hashbrown::HashMap<Bytes, crate::agent::AgentCheckpointThread>,
    pub agent_tools: hashbrown::HashMap<Bytes, crate::agent::AgentToolRegistry>,
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
            shard_id: 0,
            tier_manager: None,
            vector_indexes: std::collections::HashMap::new(),
            semantic_caches: hashbrown::HashMap::new(),
            agent_memories: hashbrown::HashMap::new(),
            llm_quotas: hashbrown::HashMap::new(),
            agent_checkpoints: hashbrown::HashMap::new(),
            agent_tools: hashbrown::HashMap::new(),
            crdt_store: crate::crdt::CrdtStore::new(port),
            json_store: crate::json::JsonStore::new(),
            probabilistic_store: crate::probabilistic::ProbabilisticStore::new(),
            sticky_keys: hashbrown::HashSet::new(),
            search_indices: std::collections::HashMap::new(),
        }
    }

    #[inline]
    pub fn with_shard(mut self, shard_id: usize) -> Self {
        self.shard_id = shard_id;
        self
    }

    #[inline(always)]
    pub fn has_search_indices(&self) -> bool {
        !self.search_indices.is_empty()
    }

    pub fn init_search_index(&mut self, schema: crate::search::IndexSchema) {
        let index_name = schema.name.clone();
        let on_type = schema.on_type.to_uppercase();
        let prefixes = schema.prefixes.clone();
        let prev_idx = self.search_indices.remove(&index_name);
        let mut idx = crate::search::InvertedIndex::new(schema.clone());
        let global_idx = crate::search::get_search_index(&index_name);

        if on_type == "HASH" {
            let now = std::time::Instant::now();
            let matching_keys: Vec<Bytes> = self
                .table
                .entries()
                .filter(|e| e.expire_at.is_none_or(|exp| exp > now))
                .filter_map(|e| {
                    let k_str = String::from_utf8_lossy(&e.key);
                    let matched = if prefixes.is_empty() {
                        true
                    } else {
                        prefixes.iter().any(|p| k_str.starts_with(p))
                    };
                    if matched { Some(e.key.clone()) } else { None }
                })
                .collect();
            for key in matching_keys {
                if let Ok(raw) = self.table.hgetall(&key)
                    && !raw.is_empty()
                {
                    let key_str = String::from_utf8_lossy(&key);
                    idx.add_hash_document(&key_str, &raw);
                    if let Some(ref g) = global_idx
                        && let Ok(mut g_idx) = g.write()
                    {
                        g_idx.add_hash_document(&key_str, &raw);
                    }
                }
            }
        } else if on_type == "JSON" {
            for (k, doc) in self.json_store.iter() {
                let k_str = String::from_utf8_lossy(k);
                let matched = if prefixes.is_empty() {
                    true
                } else {
                    prefixes.iter().any(|p| k_str.starts_with(p))
                };
                if matched {
                    let (extracted_fields, extracted_vectors) =
                        crate::search::extract_json_fields(&schema, doc);
                    idx.add_document(&k_str, extracted_fields.clone(), extracted_vectors.clone());
                    if let Some(ref g) = global_idx
                        && let Ok(mut g_idx) = g.write()
                    {
                        g_idx.add_document(&k_str, extracted_fields, extracted_vectors);
                    }
                }
            }
        }

        if let Some(prev) = prev_idx {
            for meta in prev.id_to_meta.into_values() {
                if !idx.key_to_id.contains_key(&meta.key) {
                    let key_str = String::from_utf8_lossy(&meta.key).to_string();
                    let vecs = if meta.vector_fields.is_empty() {
                        None
                    } else {
                        Some(meta.vector_fields)
                    };
                    idx.add_document(&key_str, meta.fields.clone(), vecs.clone());
                    if let Some(ref g) = global_idx
                        && let Ok(mut g_idx) = g.write()
                    {
                        g_idx.add_document(&key_str, meta.fields, vecs);
                    }
                }
            }
        }

        self.search_indices.insert(index_name, idx);
    }

    pub fn drop_search_index(&mut self, name: &str) -> bool {
        self.search_indices.remove(name).is_some()
    }

    /// Re-indexes the full content of HASH `key` into matching local search indexes and returns
    /// the raw field/value pairs so the caller can feed the global registry too.
    pub fn reindex_hash_local(&mut self, key: &[u8]) -> Vec<(Bytes, Bytes)> {
        let raw = self.table.hgetall(key).unwrap_or_default();
        let key_str = String::from_utf8_lossy(key);
        if raw.is_empty() {
            self.delete_document_local(&key_str);
            return raw;
        }
        for idx in self.search_indices.values_mut() {
            if let Some(schema) = &idx.schema {
                if schema.on_type.to_uppercase() != "HASH" {
                    continue;
                }
                let matched = if schema.prefixes.is_empty() {
                    true
                } else {
                    schema.prefixes.iter().any(|p| key_str.starts_with(p))
                };
                if matched {
                    idx.add_hash_document(&key_str, &raw);
                }
            }
        }
        raw
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
            if self.table.del(key) {
                return true;
            }
            if !self.vector_indexes.is_empty()
                && let Ok(s) = std::str::from_utf8(key)
            {
                return self
                    .vector_indexes
                    .remove(s)
                    .is_some_and(|idx| !idx.is_empty());
            }
            return false;
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
            return true;
        }
        if !self.vector_indexes.is_empty()
            && let Ok(s) = std::str::from_utf8(key)
        {
            return self
                .vector_indexes
                .remove(s)
                .is_some_and(|idx| !idx.is_empty());
        }
        false
    }

    #[inline(always)]
    pub fn del_with_hash(&mut self, key: &[u8], hash: u64) -> bool {
        if self.tier_manager.is_none() && self.sticky_keys.is_empty() {
            if self.table.del_with_hash(key, hash) {
                return true;
            }
            if !self.vector_indexes.is_empty()
                && let Ok(s) = std::str::from_utf8(key)
            {
                return self
                    .vector_indexes
                    .remove(s)
                    .is_some_and(|idx| !idx.is_empty());
            }
            return false;
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
        if self.table.exists(key) {
            return true;
        }
        if !self.vector_indexes.is_empty()
            && let Ok(s) = std::str::from_utf8(key)
        {
            return self
                .vector_indexes
                .get(s)
                .is_some_and(|idx| !idx.is_empty());
        }
        false
    }

    #[inline(always)]
    pub fn exists_with_hash(&mut self, key: &[u8], hash: u64) -> bool {
        if self.table.exists_with_hash(key, hash) {
            return true;
        }
        if !self.vector_indexes.is_empty()
            && let Ok(s) = std::str::from_utf8(key)
        {
            return self
                .vector_indexes
                .get(s)
                .is_some_and(|idx| !idx.is_empty());
        }
        false
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
    pub fn expire(
        &mut self,
        key: &[u8],
        duration: Duration,
        opts: crate::resp::ExpireOptions,
    ) -> bool {
        self.table.expire(key, duration, opts)
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
        no_values: bool,
    ) -> Result<(usize, Vec<Bytes>), &'static str> {
        self.table.hscan(key, cursor, pattern, count, no_values)
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

    #[inline]
    pub fn lmovem(
        &mut self,
        source: &[u8],
        destination: Bytes,
        where_from: crate::table::ListDirection,
        where_to: crate::table::ListDirection,
        mode: crate::resp::LmovemMode,
        count: usize,
        ordering: crate::resp::LmovemOrdering,
    ) -> Result<Option<Vec<Bytes>>, &'static str> {
        self.table.lmovem(
            source,
            destination,
            where_from,
            where_to,
            mode,
            count,
            ordering,
        )
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
        cursor: u64,
        pattern: Option<&[u8]>,
        count: usize,
    ) -> Result<(u64, Vec<Bytes>), &'static str> {
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
        self.vector_indexes.clear();
        self.semantic_caches.clear();
        self.agent_memories.clear();
        self.llm_quotas.clear();
        self.agent_checkpoints.clear();
        self.agent_tools.clear();
    }

    #[inline]
    pub fn dbsize(&mut self) -> usize {
        self.table.dbsize()
    }

    #[inline]
    pub fn type_of(&mut self, key: &[u8]) -> &'static str {
        let t = self.table.type_of(key);
        if t == "none"
            && !self.vector_indexes.is_empty()
            && let Ok(s) = std::str::from_utf8(key)
            && self
                .vector_indexes
                .get(s)
                .is_some_and(|idx| !idx.is_empty())
        {
            return "vectorset";
        }
        t
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
    pub fn copy(&mut self, src: &[u8], dst: Bytes, replace: bool) -> Result<bool, &'static str> {
        self.table.copy(src, dst, replace)
    }

    #[inline]
    pub fn next_rand(&mut self) -> usize {
        self.table.next_rand()
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
    pub fn increx(
        &mut self,
        key: Bytes,
        increment: crate::resp::IncrexIncrement,
        lbound: Option<crate::resp::IncrexBound>,
        ubound: Option<crate::resp::IncrexBound>,
        saturate: bool,
        expire: Option<crate::resp::IncrexExpire>,
        enx: bool,
    ) -> Result<crate::table::IncrexOutput, &'static str> {
        self.table
            .increx(key, increment, lbound, ubound, saturate, expire, enx)
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
        key_type: Option<&[u8]>,
    ) -> (usize, Vec<Bytes>) {
        self.table.scan(cursor, pattern, count, key_type)
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
    pub fn setbit(
        &mut self,
        key: Bytes,
        offset: usize,
        value: u8,
    ) -> Result<(u8, bool), &'static str> {
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
        is_bit: bool,
    ) -> Result<usize, &'static str> {
        self.table.bitcount(key, start, end, is_bit)
    }

    #[inline]
    pub fn bitpos(
        &mut self,
        key: &[u8],
        bit: u8,
        start: Option<i64>,
        end: Option<i64>,
        is_bit: bool,
    ) -> Result<i64, &'static str> {
        self.table.bitpos(key, bit, start, end, is_bit)
    }

    #[inline]
    pub fn bitfield(
        &mut self,
        key: Bytes,
        ops: &[crate::resp::BitfieldSubOp],
    ) -> Result<(Vec<Option<i64>>, usize), &'static str> {
        self.table.bitfield(key, ops)
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
    pub fn xinfo_stream(&mut self, key: &[u8]) -> Result<crate::table::StreamInfo, &'static str> {
        self.table.xinfo_stream(key)
    }

    #[inline]
    pub fn xinfo_groups(
        &mut self,
        key: &[u8],
    ) -> Result<Vec<crate::table::StreamGroupInfo>, &'static str> {
        self.table.xinfo_groups(key)
    }

    #[inline]
    pub fn xinfo_consumers(
        &mut self,
        key: &[u8],
        group: &[u8],
    ) -> Result<Vec<crate::table::StreamConsumerInfo>, &'static str> {
        self.table.xinfo_consumers(key, group)
    }

    #[inline]
    pub fn save_rdb_chunk(&mut self, buf: &mut Vec<u8>) {
        let now = std::time::Instant::now();
        let unix_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        for entry in self.table.entries() {
            if let Some(exp) = entry.expire_at {
                if exp <= now {
                    continue;
                }
                let rem_ms = exp.duration_since(now).as_millis() as u64;
                let expire_unix_ms = unix_now + rem_ms;
                buf.push(0xFC);
                buf.extend_from_slice(&expire_unix_ms.to_le_bytes());
            }
            match &entry.val {
                crate::table::RudisValue::Tiered(ptr) => {
                    if let Some(ref tm) = self.tier_manager
                        && let Ok((_, raw)) = tm.read_ptr_sync(*ptr)
                    {
                        buf.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
                        buf.extend_from_slice(&entry.key);
                        crate::table::RudisTable::serialize_val_payload(
                            &crate::table::RudisValue::String(bytes::Bytes::from(raw)),
                            buf,
                        );
                    }
                }
                crate::table::RudisValue::Cooled { val, .. } => {
                    buf.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&entry.key);
                    crate::table::RudisTable::serialize_val_payload(val, buf);
                }
                other => {
                    buf.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(&entry.key);
                    crate::table::RudisTable::serialize_val_payload(other, buf);
                }
            }
        }

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
                    buf.push((index.metric as u8) | 0x80);
                    let full_v = index.node_vector_cow(node);
                    buf.extend_from_slice(&(full_v.len() as u32).to_le_bytes());
                    for &coord in full_v.iter() {
                        buf.extend_from_slice(&coord.to_bits().to_le_bytes());
                    }
                    let mut vset_flags = 0u8;
                    if index.is_redis_vset {
                        vset_flags |= 0x01;
                    }
                    if node.quantized.is_some() {
                        vset_flags |= 0x02;
                    }
                    if node.pq.is_some() {
                        vset_flags |= 0x04;
                    }
                    if node.is_tiered {
                        vset_flags |= 0x08;
                    }
                    buf.push(vset_flags);
                    let quant_byte = match index.quant {
                        crate::vector::VQuant::NoQuant => 0u8,
                        crate::vector::VQuant::Q8 => 1u8,
                        crate::vector::VQuant::Bin => 2u8,
                    };
                    buf.push(quant_byte);
                    buf.extend_from_slice(&(index.m as u32).to_le_bytes());
                    if let Some(attr) = index.attributes.get(key) {
                        buf.extend_from_slice(&(attr.len() as u32).to_le_bytes());
                        buf.extend_from_slice(attr.as_bytes());
                    } else {
                        buf.extend_from_slice(&0u32.to_le_bytes());
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

        // 8. Hash field expirations (Redis 7.4 / Valkey 8 HEXPIRE)
        if !self.table.hash_field_expires.is_empty() {
            let now = std::time::Instant::now();
            let unix_now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            for (key, fmap) in &self.table.hash_field_expires {
                let active: Vec<(&Bytes, u64)> = fmap
                    .iter()
                    .filter_map(|(f, &exp)| {
                        if exp > now {
                            let rem_ms = exp.duration_since(now).as_millis() as u64;
                            Some((f, unix_now + rem_ms))
                        } else {
                            None
                        }
                    })
                    .collect();
                if !active.is_empty() {
                    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    buf.extend_from_slice(key);
                    buf.push(14u8);
                    buf.extend_from_slice(&(active.len() as u32).to_le_bytes());
                    for (field, exp_unix_ms) in active {
                        buf.extend_from_slice(&(field.len() as u32).to_le_bytes());
                        buf.extend_from_slice(field);
                        buf.extend_from_slice(&exp_unix_ms.to_le_bytes());
                    }
                }
            }
        }

        let write_bytes = |b: &mut Vec<u8>, slice: &[u8]| {
            b.extend_from_slice(&(slice.len() as u32).to_le_bytes());
            b.extend_from_slice(slice);
        };
        let write_opt_bytes = |b: &mut Vec<u8>, opt: Option<&Bytes>| {
            if let Some(s) = opt {
                b.push(1u8);
                write_bytes(b, s);
            } else {
                b.push(0u8);
            }
        };
        let write_f32_slice = |b: &mut Vec<u8>, v: &[f32]| {
            b.extend_from_slice(&(v.len() as u32).to_le_bytes());
            for &coord in v {
                b.extend_from_slice(&coord.to_bits().to_le_bytes());
            }
        };

        // 9. Semantic caches (SEMANTIC.*)
        if !self.semantic_caches.is_empty() {
            let now = std::time::Instant::now();
            let unix_now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            for (ns, cache) in &self.semantic_caches {
                #[allow(clippy::type_complexity)]
                let active_entries: Vec<(
                    &Bytes,
                    &crate::vector::SemanticEntry,
                    std::borrow::Cow<'_, [f32]>,
                    bool,
                    u64,
                )> = cache
                    .entries
                    .iter()
                    .filter_map(|(id, entry)| {
                        let exp_unix_ms = if let Some(exp) = entry.expire_at {
                            if exp <= now {
                                return None;
                            }
                            unix_now + exp.duration_since(now).as_millis().max(1) as u64
                        } else {
                            0
                        };
                        let &node_id = cache.index.key_to_id.get(id)?;
                        let node = cache.index.nodes.get(node_id)?.as_ref()?;
                        let vec = cache.index.node_vector_cow(node);
                        let quantize = node.quantized.is_some();
                        Some((id, entry, vec, quantize, exp_unix_ms))
                    })
                    .collect();
                if !active_entries.is_empty() || cache.hits > 0 || cache.misses > 0 {
                    write_bytes(buf, ns);
                    buf.push(15u8);
                    buf.extend_from_slice(&(cache.index.dim as u32).to_le_bytes());
                    buf.extend_from_slice(&cache.hits.to_le_bytes());
                    buf.extend_from_slice(&cache.misses.to_le_bytes());
                    buf.extend_from_slice(&cache.tokens_saved.to_le_bytes());
                    buf.extend_from_slice(&cache.evicted_expired.to_le_bytes());
                    buf.extend_from_slice(&(active_entries.len() as u32).to_le_bytes());
                    for (id, entry, vec, quantize, exp_unix_ms) in active_entries {
                        write_bytes(buf, id);
                        write_bytes(buf, &entry.prompt);
                        write_bytes(buf, &entry.response);
                        write_opt_bytes(buf, entry.scope.as_ref());
                        buf.extend_from_slice(&exp_unix_ms.to_le_bytes());
                        buf.extend_from_slice(&entry.tokens.to_le_bytes());
                        buf.push(u8::from(quantize));
                        write_f32_slice(buf, &vec);
                    }
                }
            }
        }

        // 10. Agent memory sessions (AGENT.MEM.*)
        for (sess_key, session) in &self.agent_memories {
            write_bytes(buf, sess_key);
            buf.push(16u8);
            buf.extend_from_slice(&session.next_turn_id.to_le_bytes());
            buf.extend_from_slice(&session.active_tokens.to_le_bytes());
            buf.extend_from_slice(&session.compactions.to_le_bytes());
            buf.extend_from_slice(&(session.turns.len() as u32).to_le_bytes());
            for turn in &session.turns {
                buf.extend_from_slice(&turn.id.to_le_bytes());
                write_bytes(buf, &turn.role);
                write_bytes(buf, &turn.content);
                buf.extend_from_slice(&turn.tokens.to_le_bytes());
                write_opt_bytes(buf, turn.meta.as_ref());
                buf.push(u8::from(turn.compacted));
                let vec_cow = session
                    .index
                    .as_ref()
                    .and_then(|idx| idx.get_vector_cow(&Bytes::from(turn.id.to_string())));
                if let Some(v) = vec_cow {
                    write_f32_slice(buf, &v);
                } else {
                    buf.extend_from_slice(&0u32.to_le_bytes());
                }
            }
        }

        // 11. Agent DAG checkpoints (AGENT.CHECKPOINT.*)
        for (ck_key, thread) in &self.agent_checkpoints {
            let ordered_nodes: Vec<&crate::agent::AgentCheckpointNode> = thread
                .order
                .iter()
                .filter_map(|step_id| thread.nodes.get(step_id))
                .collect();
            if !ordered_nodes.is_empty() {
                write_bytes(buf, ck_key);
                buf.push(17u8);
                buf.extend_from_slice(&thread.next_seq.to_le_bytes());
                write_opt_bytes(buf, thread.head_step_id.as_ref());
                buf.extend_from_slice(&(ordered_nodes.len() as u32).to_le_bytes());
                for node in ordered_nodes {
                    write_bytes(buf, &node.step_id);
                    write_opt_bytes(buf, node.parent_id.as_ref());
                    buf.extend_from_slice(&node.seq.to_le_bytes());
                    buf.extend_from_slice(&node.timestamp_ms.to_le_bytes());
                    write_bytes(buf, &node.state);
                    write_opt_bytes(buf, node.metadata.as_ref());
                }
            }
        }

        // 12. Agent tool registries (AGENT.TOOL.*)
        if !self.agent_tools.is_empty() {
            let now = std::time::Instant::now();
            let unix_now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            for (tool_key, reg) in &self.agent_tools {
                let active_calls: Vec<(&Bytes, &crate::agent::ToolCallEntry, u64, u64)> = reg
                    .calls
                    .iter()
                    .filter_map(|(call_id, entry)| {
                        let exp_unix_ms = if let Some(exp) = entry.expire_at {
                            if exp <= now {
                                return None;
                            }
                            unix_now + exp.duration_since(now).as_millis().max(1) as u64
                        } else {
                            0
                        };
                        let lease_unix_ms = if let Some(lease) = entry.lease_until
                            && lease > now
                        {
                            unix_now + lease.duration_since(now).as_millis().max(1) as u64
                        } else {
                            0
                        };
                        Some((call_id, entry, lease_unix_ms, exp_unix_ms))
                    })
                    .collect();
                if !active_calls.is_empty() {
                    write_bytes(buf, tool_key);
                    buf.push(18u8);
                    buf.extend_from_slice(&(active_calls.len() as u32).to_le_bytes());
                    for (call_id, entry, lease_unix_ms, exp_unix_ms) in active_calls {
                        write_bytes(buf, call_id);
                        write_opt_bytes(buf, entry.input.as_ref());
                        write_opt_bytes(buf, entry.output.as_ref());
                        buf.extend_from_slice(&entry.attempt.to_le_bytes());
                        buf.extend_from_slice(&lease_unix_ms.to_le_bytes());
                        buf.extend_from_slice(&exp_unix_ms.to_le_bytes());
                    }
                }
            }
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
                let has_vset_ext = (metric_byte & 0x80) != 0;
                let metric = match metric_byte & 0x07 {
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
                if has_vset_ext {
                    if data.len() < 10 {
                        return Err("Truncated vector set extended metadata");
                    }
                    let vset_flags = data[0];
                    let is_redis_vset = (vset_flags & 0x01) != 0;
                    let quantize = (vset_flags & 0x02) != 0;
                    let pq = (vset_flags & 0x04) != 0;
                    let tiered = (vset_flags & 0x08) != 0;
                    let quant = match data[1] {
                        0 => crate::vector::VQuant::NoQuant,
                        1 => crate::vector::VQuant::Q8,
                        _ => crate::vector::VQuant::Bin,
                    };
                    let m = u32::from_le_bytes(data[2..6].try_into().unwrap()) as usize;
                    let attr_len = u32::from_le_bytes(data[6..10].try_into().unwrap()) as usize;
                    data = &data[10..];
                    if data.len() < attr_len {
                        return Err("Truncated vector set attribute");
                    }
                    let setattr = if attr_len > 0 {
                        Some(String::from_utf8_lossy(&data[..attr_len]).to_string())
                    } else {
                        None
                    };
                    data = &data[attr_len..];
                    let _ = self.vadd_ext(
                        &idx_name,
                        doc_key,
                        vector,
                        Some(metric),
                        quantize,
                        pq,
                        tiered,
                        None,
                        Some(quant),
                        None,
                        setattr,
                        Some(m),
                        is_redis_vset,
                    );
                } else {
                    let _ = self.vadd(
                        &idx_name,
                        doc_key,
                        vector,
                        Some(metric),
                        false,
                        false,
                        false,
                    );
                }
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
            } else if type_byte == 14 {
                data = &data[1..];
                if data.len() < 4 {
                    return Err("Truncated hash field expires count");
                }
                let f_count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                data = &data[4..];
                let mut expired_on_disk = Vec::new();
                for _ in 0..f_count {
                    if data.len() < 4 {
                        return Err("Truncated hash field len");
                    }
                    let f_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                    data = &data[4..];
                    if data.len() < f_len + 8 {
                        return Err("Truncated hash field expire payload");
                    }
                    let field = bytes::Bytes::copy_from_slice(&data[..f_len]);
                    let exp_unix_ms =
                        u64::from_le_bytes(data[f_len..f_len + 8].try_into().unwrap());
                    data = &data[f_len + 8..];
                    if exp_unix_ms > unix_now {
                        let rem_ms = exp_unix_ms - unix_now;
                        self.table
                            .hash_field_expires
                            .entry(key.clone())
                            .or_default()
                            .insert(
                                field,
                                std::time::Instant::now()
                                    + std::time::Duration::from_millis(rem_ms),
                            );
                    } else {
                        expired_on_disk.push(field);
                    }
                }
                if !expired_on_disk.is_empty() {
                    let _ = self.table.hdel(&key, &expired_on_disk);
                }
                continue;
            } else if matches!(type_byte, 15..=18) {
                data = &data[1..];
                self.restore_ai_native_rdb_record(type_byte, key, &mut data, unix_now, true)?;
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

    pub(crate) fn restore_ai_native_rdb_record(
        &mut self,
        type_byte: u8,
        key: Bytes,
        data: &mut &[u8],
        unix_now: u64,
        is_owned: bool,
    ) -> Result<(), &'static str> {
        fn read_u8(d: &mut &[u8]) -> Result<u8, &'static str> {
            if d.is_empty() {
                return Err("Truncated RDB u8");
            }
            let v = d[0];
            *d = &d[1..];
            Ok(v)
        }
        fn read_u32(d: &mut &[u8]) -> Result<u32, &'static str> {
            if d.len() < 4 {
                return Err("Truncated RDB u32");
            }
            let v = u32::from_le_bytes(d[0..4].try_into().unwrap());
            *d = &d[4..];
            Ok(v)
        }
        fn read_u64(d: &mut &[u8]) -> Result<u64, &'static str> {
            if d.len() < 8 {
                return Err("Truncated RDB u64");
            }
            let v = u64::from_le_bytes(d[0..8].try_into().unwrap());
            *d = &d[8..];
            Ok(v)
        }
        fn read_bytes(d: &mut &[u8]) -> Result<Bytes, &'static str> {
            let len = read_u32(d)? as usize;
            if d.len() < len {
                return Err("Truncated RDB bytes");
            }
            let b = Bytes::copy_from_slice(&d[..len]);
            *d = &d[len..];
            Ok(b)
        }
        fn read_opt_bytes(d: &mut &[u8]) -> Result<Option<Bytes>, &'static str> {
            let has = read_u8(d)?;
            if has != 0 {
                Ok(Some(read_bytes(d)?))
            } else {
                Ok(None)
            }
        }
        fn read_f32_vec(d: &mut &[u8]) -> Result<Vec<f32>, &'static str> {
            let len = read_u32(d)? as usize;
            if d.len() < len * 4 {
                return Err("Truncated RDB f32 vector");
            }
            let mut v = Vec::with_capacity(len);
            for i in 0..len {
                let bits = u32::from_le_bytes(d[i * 4..(i + 1) * 4].try_into().unwrap());
                v.push(f32::from_bits(bits));
            }
            *d = &d[len * 4..];
            Ok(v)
        }

        match type_byte {
            15 => {
                let dim = read_u32(data)? as usize;
                let hits = read_u64(data)?;
                let misses = read_u64(data)?;
                let tokens_saved = read_u64(data)?;
                let mut evicted_expired = read_u64(data)?;
                let count = read_u32(data)? as usize;
                if is_owned {
                    let ns_str = String::from_utf8_lossy(&key).to_string();
                    self.semantic_caches
                        .entry(key.clone())
                        .or_insert_with(|| crate::vector::SemanticCache::new(ns_str, dim.max(1)));
                }
                for _ in 0..count {
                    let id = read_bytes(data)?;
                    let prompt = read_bytes(data)?;
                    let response = read_bytes(data)?;
                    let scope = read_opt_bytes(data)?;
                    let exp_unix_ms = read_u64(data)?;
                    let tokens = read_u64(data)?;
                    let quantize = read_u8(data)? != 0;
                    let vector = read_f32_vec(data)?;
                    if !is_owned {
                        continue;
                    }
                    if exp_unix_ms != 0 && exp_unix_ms <= unix_now {
                        evicted_expired += 1;
                        continue;
                    }
                    let ttl = if exp_unix_ms > unix_now {
                        Some(std::time::Duration::from_millis(exp_unix_ms - unix_now))
                    } else {
                        None
                    };
                    let _ = self.semantic_set(
                        key.clone(),
                        id,
                        prompt,
                        response,
                        vector,
                        ttl,
                        scope,
                        quantize,
                        Some(tokens),
                    );
                }
                if is_owned && let Some(cache) = self.semantic_caches.get_mut(&key) {
                    cache.hits = hits;
                    cache.misses = misses;
                    cache.tokens_saved = tokens_saved;
                    cache.evicted_expired = evicted_expired;
                }
                Ok(())
            }
            16 => {
                let next_turn_id = read_u64(data)?;
                let active_tokens = read_u64(data)?;
                let compactions = read_u64(data)?;
                let turns_len = read_u32(data)? as usize;
                let sess_str = String::from_utf8_lossy(&key).to_string();
                let mut session = crate::agent::AgentMemorySession::new(sess_str.clone());
                session.next_turn_id = next_turn_id;
                session.active_tokens = active_tokens;
                session.compactions = compactions;
                for pos in 0..turns_len {
                    let id = read_u64(data)?;
                    let role = read_bytes(data)?;
                    let content = read_bytes(data)?;
                    let tokens = read_u64(data)?;
                    let meta = read_opt_bytes(data)?;
                    let compacted = read_u8(data)? != 0;
                    let vec = read_f32_vec(data)?;
                    if !is_owned {
                        continue;
                    }
                    if !vec.is_empty() {
                        let idx = session.index.get_or_insert_with(|| {
                            crate::vector::HnswIndex::new(
                                sess_str.clone(),
                                vec.len(),
                                crate::vector::VectorMetric::Cosine,
                            )
                        });
                        let _ = idx.add(Bytes::from(id.to_string()), vec);
                    }
                    session.turns.push(crate::agent::AgentTurn {
                        id,
                        role,
                        content,
                        tokens,
                        meta,
                        compacted,
                    });
                    session.id_to_pos.insert(id, pos);
                }
                if is_owned {
                    self.agent_memories.insert(key, session);
                }
                Ok(())
            }
            17 => {
                let next_seq = read_u64(data)?;
                let head_step_id = read_opt_bytes(data)?;
                let nodes_len = read_u32(data)? as usize;
                let mut thread = crate::agent::AgentCheckpointThread::new();
                thread.next_seq = next_seq;
                thread.head_step_id = head_step_id;
                for _ in 0..nodes_len {
                    let step_id = read_bytes(data)?;
                    let parent_id = read_opt_bytes(data)?;
                    let seq = read_u64(data)?;
                    let timestamp_ms = read_u64(data)?;
                    let state = read_bytes(data)?;
                    let metadata = read_opt_bytes(data)?;
                    if !is_owned {
                        continue;
                    }
                    thread.order.push(step_id.clone());
                    thread.nodes.insert(
                        step_id.clone(),
                        crate::agent::AgentCheckpointNode {
                            step_id,
                            parent_id,
                            seq,
                            timestamp_ms,
                            state,
                            metadata,
                        },
                    );
                }
                if is_owned {
                    self.agent_checkpoints.insert(key, thread);
                }
                Ok(())
            }
            18 => {
                let calls_len = read_u32(data)? as usize;
                let now = std::time::Instant::now();
                let mut reg = crate::agent::AgentToolRegistry::new();
                for _ in 0..calls_len {
                    let call_id = read_bytes(data)?;
                    let input = read_opt_bytes(data)?;
                    let output = read_opt_bytes(data)?;
                    let attempt = read_u64(data)?;
                    let lease_unix_ms = read_u64(data)?;
                    let exp_unix_ms = read_u64(data)?;
                    if !is_owned || (exp_unix_ms != 0 && exp_unix_ms <= unix_now) {
                        continue;
                    }
                    let lease_until = if lease_unix_ms > unix_now {
                        Some(now + std::time::Duration::from_millis(lease_unix_ms - unix_now))
                    } else {
                        None
                    };
                    let expire_at = if exp_unix_ms > unix_now {
                        Some(now + std::time::Duration::from_millis(exp_unix_ms - unix_now))
                    } else {
                        None
                    };
                    reg.calls.insert(
                        call_id,
                        crate::agent::ToolCallEntry {
                            input,
                            output,
                            attempt,
                            lease_until,
                            expire_at,
                        },
                    );
                }
                if is_owned && !reg.calls.is_empty() {
                    self.agent_tools.insert(key, reg);
                }
                Ok(())
            }
            _ => Err("Unknown AI-native RDB record type"),
        }
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
    pub fn xgroup_setid(
        &mut self,
        key: &[u8],
        group: &[u8],
        id_str: &str,
        entries_read: Option<u64>,
    ) -> Result<(), &'static str> {
        self.table.xgroup_setid(key, group, id_str, entries_read)
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

    #[allow(clippy::too_many_arguments)]
    pub fn vadd_ext(
        &mut self,
        index_name: &str,
        element: Bytes,
        vector: Vec<f32>,
        metric: Option<crate::vector::VectorMetric>,
        quantize: bool,
        pq: bool,
        tiered: bool,
        reduce: Option<usize>,
        quant: Option<crate::vector::VQuant>,
        ef: Option<usize>,
        setattr: Option<String>,
        m: Option<usize>,
        is_redis_vset: bool,
    ) -> Result<bool, String> {
        if self.table.exists(index_name.as_bytes()) {
            return Err(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            );
        }
        if vector.is_empty() {
            return Err("vector dimension must be greater than 0".to_string());
        }
        let target_dim = reduce.unwrap_or(vector.len());
        let is_new_index = !self.vector_indexes.contains_key(index_name);
        let idx = self
            .vector_indexes
            .entry(index_name.to_string())
            .or_insert_with(|| {
                let mut h = crate::vector::HnswIndex::new(
                    index_name.to_string(),
                    target_dim,
                    metric.unwrap_or(crate::vector::VectorMetric::Cosine),
                );
                if is_redis_vset {
                    h.is_redis_vset = true;
                    h.quant = quant.unwrap_or(crate::vector::VQuant::Q8);
                    if let Some(m_val) = m {
                        let m_clamped = m_val.max(2);
                        h.m = m_clamped;
                        h.m0 = m_clamped * 2;
                        h.ml = 1.0 / (m_clamped as f64).ln();
                    }
                    if let Some(ef_val) = ef {
                        h.ef_construction = ef_val.max(1);
                    }
                    if reduce.is_some() {
                        h.set_projection(vector.len());
                    }
                }
                h
            });
        if !is_new_index && is_redis_vset {
            if let Some(q) = quant
                && q != idx.quant
            {
                return Err("Quantization type mismatch".to_string());
            }
            if let Some(r) = reduce
                && (idx.projection.is_none() || r != idx.dim)
            {
                return Err("Projection dimension mismatch".to_string());
            }
            if vector.len() != idx.client_dim() {
                return Err("vector dimension mismatch".to_string());
            }
            if let Some(ef_val) = ef {
                idx.ef_construction = ef_val.max(1);
            }
        }
        let existed = idx.key_to_id.contains_key(&element);
        let projected = idx.project(&vector);
        idx.add_quantized_ext(
            element.clone(),
            projected,
            quantize || idx.quant == crate::vector::VQuant::Q8,
            pq,
            tiered,
        )
        .map_err(|e| e.to_string())?;
        if let Some(attr) = setattr {
            if attr.trim().is_empty() {
                idx.attributes.remove(&element);
            } else {
                idx.attributes.insert(element, attr);
            }
        }
        Ok(!existed)
    }

    pub fn vquery(
        &self,
        index_name: &str,
        query: &[f32],
        k: usize,
        rerank: bool,
    ) -> Vec<(Bytes, f32)> {
        if let Some(idx) = self.vector_indexes.get(index_name) {
            let projected = idx.project(query);
            idx.search_tiered(&projected, k, rerank)
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
            let v1 = idx.get_vector_cow(k1).ok_or("vector 1 not found")?;
            let v2 = idx.get_vector_cow(k2).ok_or("vector 2 not found")?;
            let metric = metric_override.unwrap_or(idx.metric);
            Ok(crate::vector::compute_distance(&v1, &v2, metric))
        } else {
            Err("index not found")
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn vsim_ext(
        &mut self,
        index_name: &str,
        target: &crate::resp::VsimTarget,
        with_attribs: bool,
        count: usize,
        epsilon: Option<f32>,
        ef: Option<usize>,
        filter: Option<&str>,
        filter_ef: Option<usize>,
        truth: bool,
    ) -> Result<Vec<(Bytes, f32, Option<String>)>, String> {
        if self.table.exists(index_name.as_bytes()) {
            return Err(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            );
        }
        let Some(idx) = self.vector_indexes.get(index_name) else {
            return Ok(Vec::new());
        };
        let query_vec: Vec<f32> = match target {
            crate::resp::VsimTarget::Element(elem) => idx
                .get_vector_cow(elem)
                .ok_or_else(|| "Element not found".to_string())?
                .into_owned(),
            crate::resp::VsimTarget::Vector(v) => {
                if v.len() != idx.client_dim() {
                    return Err("vector dimension mismatch".to_string());
                }
                idx.project(v)
            }
        };
        let attrs_ref = &idx.attributes;
        let filter_closure = |elem: &Bytes| -> bool {
            let Some(expr) = filter else {
                return true;
            };
            crate::vector::eval_vset_filter(expr, attrs_ref.get(elem).map(|s| s.as_str()))
                .unwrap_or(false)
        };
        let filter_opt: Option<&dyn Fn(&Bytes) -> bool> = if filter.is_some() {
            Some(&filter_closure)
        } else {
            None
        };
        let rerank = idx.quant == crate::vector::VQuant::NoQuant;
        let raw_hits = if truth {
            idx.search_exact(&query_vec, count, filter_opt)
        } else if filter.is_some() {
            let eff_ef = filter_ef.or(ef).or(Some((count * 10).max(64)));
            let mut hits = idx.search_filtered(&query_vec, count, eff_ef, rerank, filter_opt);
            if hits.len() < count {
                hits = idx.search_exact(&query_vec, count, filter_opt);
            }
            hits
        } else {
            idx.search_ext(&query_vec, count, ef, rerank)
        };
        let mut out = Vec::with_capacity(raw_hits.len());
        for (elem, dist) in raw_hits {
            let score = (1.0 - dist / 2.0).clamp(0.0, 1.0);
            if let Some(ep) = epsilon
                && score < 1.0 - ep - 1e-6
            {
                continue;
            }
            let attr = if with_attribs {
                idx.attributes.get(&elem).cloned()
            } else {
                None
            };
            out.push((elem, score, attr));
        }
        Ok(out)
    }

    pub fn vdel(&mut self, index_name: &str, key: &Bytes) -> bool {
        let mut empty = false;
        let removed = if let Some(idx) = self.vector_indexes.get_mut(index_name) {
            idx.attributes.remove(key);
            let r = idx.remove(key);
            empty = idx.is_empty();
            r
        } else {
            false
        };
        if empty {
            self.vector_indexes.remove(index_name);
        }
        removed
    }

    pub fn vinfo(&self, index_name: &str) -> Option<(usize, usize, &'static str, usize)> {
        self.vector_indexes
            .get(index_name)
            .map(|idx| (idx.len(), idx.dim, idx.metric.as_str(), idx.max_layer))
    }

    pub fn vsetattr(
        &mut self,
        index_name: &str,
        element: &Bytes,
        attr: String,
    ) -> Result<bool, String> {
        if self.table.exists(index_name.as_bytes()) {
            return Err(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            );
        }
        let Some(idx) = self.vector_indexes.get_mut(index_name) else {
            return Ok(false);
        };
        if !idx.key_to_id.contains_key(element) {
            return Ok(false);
        }
        if attr.trim().is_empty() {
            idx.attributes.remove(element);
        } else {
            idx.attributes.insert(element.clone(), attr);
        }
        Ok(true)
    }

    pub fn vgetattr(
        &mut self,
        index_name: &str,
        element: &Bytes,
    ) -> Result<Option<String>, String> {
        if self.table.exists(index_name.as_bytes()) {
            return Err(
                "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            );
        }
        let Some(idx) = self.vector_indexes.get(index_name) else {
            return Ok(None);
        };
        Ok(idx.attributes.get(element).cloned())
    }

    // Semantic Cache operations
    #[allow(clippy::too_many_arguments)]
    pub fn semantic_set(
        &mut self,
        namespace: Bytes,
        id: Bytes,
        prompt: Bytes,
        response: Bytes,
        vector: Vec<f32>,
        ttl: Option<Duration>,
        scope: Option<Bytes>,
        quantize: bool,
        tokens: Option<u64>,
    ) -> Result<(), String> {
        let dim = vector.len();
        let ns_str = String::from_utf8_lossy(&namespace).into_owned();
        let cache = self
            .semantic_caches
            .entry(namespace)
            .or_insert_with(|| crate::vector::SemanticCache::new(ns_str, dim));
        cache.set(id, prompt, response, vector, ttl, scope, quantize, tokens)
    }

    pub fn semantic_get(
        &mut self,
        namespace: &Bytes,
        query: &[f32],
        threshold: f32,
        scope: Option<&[u8]>,
    ) -> Result<Option<crate::vector::SemanticHit>, String> {
        let Some(cache) = self.semantic_caches.get_mut(namespace) else {
            return Ok(None);
        };
        cache.get(query, threshold, scope)
    }

    pub fn semantic_del(&mut self, namespace: &Bytes, ids: &[Bytes]) -> usize {
        let Some(cache) = self.semantic_caches.get_mut(namespace) else {
            return 0;
        };
        let mut removed = 0;
        for id in ids {
            if cache.del(id) {
                removed += 1;
            }
        }
        removed
    }

    pub fn semantic_flush(&mut self, namespace: &Bytes) {
        if let Some(cache) = self.semantic_caches.get_mut(namespace) {
            cache.flush();
        }
    }

    pub fn semantic_info(&mut self, namespace: &Bytes) -> (usize, usize, u64, u64, u64, u64) {
        let Some(cache) = self.semantic_caches.get_mut(namespace) else {
            return (0, 0, 0, 0, 0, 0);
        };
        cache.purge_expired();
        (
            cache.entries.len(),
            cache.index.dim,
            cache.hits,
            cache.misses,
            cache.tokens_saved,
            cache.evicted_expired,
        )
    }

    // Agent Memory operations
    pub fn agent_mem_add(
        &mut self,
        session: Bytes,
        role: Bytes,
        content: Bytes,
        tokens: Option<u64>,
        vector: Option<Vec<f32>>,
        meta: Option<Bytes>,
    ) -> Result<u64, String> {
        let sid = String::from_utf8_lossy(&session).into_owned();
        let mem = self
            .agent_memories
            .entry(session)
            .or_insert_with(|| crate::agent::AgentMemorySession::new(sid));
        mem.add(role, content, tokens, vector, meta)
    }

    pub fn agent_mem_context(
        &self,
        session: &Bytes,
        max_tokens: u64,
        query: Option<&[f32]>,
        recall_k: usize,
    ) -> Result<crate::agent::AgentContextResult, String> {
        let Some(mem) = self.agent_memories.get(session) else {
            return Ok(crate::agent::AgentContextResult {
                recent_turns: Vec::new(),
                recalled_episodes: Vec::new(),
            });
        };
        mem.context(max_tokens, query, recall_k)
    }

    pub fn agent_mem_compact(
        &mut self,
        session: &Bytes,
        keep_recent: usize,
        summary: Bytes,
        tokens: Option<u64>,
        vector: Option<Vec<f32>>,
    ) -> Result<usize, String> {
        let Some(mem) = self.agent_memories.get_mut(session) else {
            return Ok(0);
        };
        mem.compact(keep_recent, summary, tokens, vector)
    }

    pub fn agent_mem_info(&self, session: &Bytes) -> (usize, usize, u64, usize, usize, u64) {
        let Some(mem) = self.agent_memories.get(session) else {
            return (0, 0, 0, 0, 0, 0);
        };
        mem.info()
    }

    pub fn agent_mem_clear(&mut self, session: &Bytes) -> bool {
        self.agent_memories.remove(session).is_some()
    }

    // LLM Quota Governor operations
    pub fn llm_quota_reserve(
        &mut self,
        key: Bytes,
        rpm: usize,
        tpm: u64,
        est_tokens: u64,
        window_ms: Option<u64>,
    ) -> crate::agent::LlmReserveResult {
        let bucket = self
            .llm_quotas
            .entry(key)
            .or_insert_with(|| crate::agent::LlmQuotaBucket::new(window_ms.unwrap_or(60_000)));
        bucket.reserve(rpm, tpm, est_tokens, window_ms)
    }

    pub fn llm_quota_settle(
        &mut self,
        key: &Bytes,
        reservation_id: u64,
        actual_tokens: u64,
    ) -> (bool, i64) {
        let Some(bucket) = self.llm_quotas.get_mut(key) else {
            return (false, 0);
        };
        bucket.settle(reservation_id, actual_tokens)
    }

    pub fn llm_quota_info(&mut self, key: &Bytes) -> (usize, u64, u64, usize, u64) {
        let Some(bucket) = self.llm_quotas.get_mut(key) else {
            return (0, 0, 0, 0, 60_000);
        };
        bucket.info()
    }

    // Agent Checkpoint DAG & Tool Lease operations
    pub fn agent_checkpoint_put(
        &mut self,
        key: Bytes,
        step_id: Bytes,
        parent_id: Option<Bytes>,
        state: Bytes,
        metadata: Option<Bytes>,
    ) -> u64 {
        self.agent_checkpoints
            .entry(key)
            .or_default()
            .put(step_id, parent_id, state, metadata)
    }

    pub fn agent_checkpoint_get(
        &self,
        key: &Bytes,
        step_id: Option<&Bytes>,
    ) -> Option<crate::agent::AgentCheckpointNode> {
        self.agent_checkpoints.get(key)?.get(step_id).cloned()
    }

    pub fn agent_checkpoint_history(
        &self,
        key: &Bytes,
        from_step: Option<&Bytes>,
        limit: usize,
    ) -> Vec<crate::agent::AgentCheckpointNode> {
        let Some(thread) = self.agent_checkpoints.get(key) else {
            return Vec::new();
        };
        thread.history(from_step, limit)
    }

    pub fn agent_tool_claim(
        &mut self,
        key: Bytes,
        call_id: Bytes,
        ttl_ms: u64,
        input: Option<Bytes>,
    ) -> crate::agent::ToolClaimResult {
        self.agent_tools
            .entry(key)
            .or_default()
            .claim(call_id, ttl_ms, input)
    }

    pub fn agent_tool_complete(
        &mut self,
        key: Bytes,
        call_id: Bytes,
        output: Bytes,
        ttl_ms: Option<u64>,
    ) -> bool {
        self.agent_tools
            .entry(key)
            .or_default()
            .complete(call_id, output, ttl_ms)
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

        // 8. Add standard entry
        db.set(Bytes::from("regular_key"), Bytes::from("regular_val"), None);

        // Serialize to RDB chunk
        let mut chunk = Vec::new();
        db.save_rdb_chunk(&mut chunk);
        assert!(!chunk.is_empty());

        // Restore into new ShardDb
        let mut new_db = ShardDb::new(0);
        new_db
            .restore_rdb_chunk(&chunk)
            .expect("restore_rdb_chunk should succeed");

        // Verify standard key
        assert_eq!(
            new_db.get(b"regular_key").unwrap(),
            Bytes::from("regular_val")
        );

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

    #[test]
    fn test_shard_message_size() {
        assert!(std::mem::size_of::<ShardMessage>() <= 88);
    }
}
