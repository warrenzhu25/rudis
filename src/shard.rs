use bytes::Bytes;
use std::time::Duration;

use crate::resp::Command;

/// Inline compact representation of Redis responses for cross-core batch transfers.
/// Avoids heap allocations for responses up to 30 bytes (integers, OK, simple errors, small bulk strings).
/// Bytes of a value [`CompactResp::prefetch`] pulls in; past this the
/// hardware prefetcher has the copy's sequential stream anyway.
const PREFETCH_MAX: usize = 4096;

/// Hints the CPU to load `b`'s cache lines (up to [`PREFETCH_MAX`] bytes).
#[inline(always)]
fn prefetch_bytes(b: &[u8]) {
    #[cfg(target_arch = "x86_64")]
    {
        let n = b.len().min(PREFETCH_MAX);
        let mut off = 0;
        while off < n {
            // SAFETY: a prefetch is only a hint: it never faults or changes
            // memory, and `off < b.len()` keeps the address inside `b`.
            unsafe {
                std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
                    b.as_ptr().add(off) as *const i8,
                );
            }
            off += 64;
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = b;
}

/// The RESP bulk reply of a 7-bit packed value, unpacked into this
/// thread's reply slab (see [`REPLY_SLAB`]): a slice of a shared
/// allocation instead of a `Vec` of its own.
fn packed_reply_from_slab(s: &crate::compact::CompactStr) -> Bytes {
    REPLY_SLAB.with(|cell| {
        let (slab, scratch) = &mut *cell.borrow_mut();
        scratch.clear();
        s.write_resp_bulk(scratch);
        if slab.capacity() - slab.len() < scratch.len() {
            *slab = bytes::BytesMut::with_capacity(REPLY_SLAB_BYTES.max(scratch.len()));
        }
        slab.extend_from_slice(scratch);
        slab.split().freeze()
    })
}

/// Bytes of each reply slab.
const REPLY_SLAB_BYTES: usize = 256 << 10;

thread_local! {
    /// Replies of packed values (unpacked, encoded) for other shards' batches:
    /// carved from one allocation per slab instead of one `Vec` per reply,
    /// which the batch's owner freed on its core. Plus a scratch buffer.
    static REPLY_SLAB: std::cell::RefCell<(bytes::BytesMut, Vec<u8>)> =
        std::cell::RefCell::new((bytes::BytesMut::new(), Vec::new()));
}

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
        Self::from_bulk_with(bytes, || bytes.clone())
    }

    /// Like [`Self::from_bulk`] for borrowed bytes (copied only if long).
    #[inline(always)]
    pub fn from_bulk_slice(bytes: &[u8]) -> Self {
        Self::from_bulk_with(bytes, || Bytes::copy_from_slice(bytes))
    }

    /// Like [`Self::from_bulk`] for a table string value: short values are
    /// encoded inline, long ones share the value's allocation.
    #[inline(always)]
    pub fn from_compact_str(s: &crate::compact::CompactStr) -> Self {
        if s.is_heap_packed() {
            return CompactResp::RawBytes(packed_reply_from_slab(s));
        }
        Self::from_bulk_with(&s.view(), || s.to_bytes())
    }

    #[inline(always)]
    fn from_bulk_with(bytes: &[u8], owned: impl FnOnce() -> Bytes) -> Self {
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
            data[len..len + n].copy_from_slice(bytes);
            len += n;
            data[len] = b'\r';
            data[len + 1] = b'\n';
            len += 2;
            CompactResp::Small {
                len: len as u8,
                data,
            }
        } else {
            CompactResp::Bulk(owned())
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

    /// Starts loading a shared value's bytes into cache ahead of
    /// [`Self::write_to`]. A reply assembled from many values (a pipeline of
    /// GETs or MGETs) otherwise takes each value's cache misses one after
    /// another while copying it; prefetching them all first overlaps them.
    #[inline(always)]
    pub fn prefetch(&self) {
        if let CompactResp::Bulk(b) | CompactResp::Array1Bulk(b) | CompactResp::RawBytes(b) = self {
            prefetch_bytes(b);
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

/// A snapshot (RDB save, full sync) could not read a key's tiered value back
/// from disk. The snapshot must then fail as a whole: writing it without the
/// key would silently lose it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TieredReadError {
    pub key: Bytes,
}

impl std::fmt::Display for TieredReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "failed to read the tiered value of key '{}' from disk",
            String::from_utf8_lossy(&self.key)
        )
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
    Del {
        db: usize,
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    /// See [`crate::router::Router::del_if_unchanged`].
    DelIfUnchanged {
        db: usize,
        key: Bytes,
        payload: Bytes,
        responder: flume::Sender<bool>,
    },
    DelKeys {
        db: usize,
        keys: Vec<Bytes>,
        responder: flume::Sender<usize>,
    },
    ActiveDefrag {
        responder: flume::Sender<usize>,
    },
    Exists {
        db: usize,
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    IncrBy {
        db: usize,
        key: Bytes,
        delta: i64,
        name: crate::resp::IncrName,
        responder: flume::Sender<Result<i64, String>>,
    },
    Expire {
        db: usize,
        key: Bytes,
        duration: Duration,
        opts: crate::resp::ExpireOptions,
        responder: flume::Sender<bool>,
    },
    Persist {
        db: usize,
        key: Bytes,
        responder: flume::Sender<bool>,
    },
    Ttl {
        db: usize,
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
        db: usize,
        items: Vec<(usize, u64, Command)>,
        responder: std::sync::Arc<crate::mailbox::BatchResponder>,
        is_resp3: bool,
        /// The client the commands run for (0 if none), so the owning shard
        /// records tracked reads and NOLOOP writes against it.
        client_id: u64,
        /// The requesting client's `CLIENT NO-TOUCH`.
        no_touch: bool,
        /// Defer tracking broadcasts until the requesting client's EXEC
        /// ends (it flushes them).
        defer_bcast: bool,
    },
    Mget {
        db: usize,
        keys: Vec<(usize, Option<Bytes>)>,
        responder: flume::Sender<Vec<(usize, Option<Bytes>)>>,
    },
    ScatterMget {
        shard_id: usize,
        keys: Vec<(usize, Bytes)>,
        descriptor: std::sync::Arc<crate::mailbox::ScatterMgetDescriptor>,
        /// The requesting client's `CLIENT NO-TOUCH`.
        no_touch: bool,
    },
    Mset {
        db: usize,
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
        db: usize,
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
    /// Answered once every slot update sent earlier on the same ring has
    /// been applied (see `Router::sync_slot_tables`).
    SlotBarrier {
        responder: flume::Sender<()>,
    },
    SyncAof {
        responder: flume::Sender<()>,
    },
    /// Write this shard's pending AOF bytes to the kernel (no fsync), then
    /// reply: see `AofWriter::write_through`.
    WriteAof {
        responder: flume::Sender<()>,
    },
    FlushCommandStats {
        responder: flume::Sender<()>,
    },
    ResetCommandStats {
        responder: flume::Sender<()>,
    },
    SaveRdbChunk {
        /// The chunk, or the failure that makes the snapshot unusable.
        responder: flume::Sender<Result<Vec<u8>, TieredReadError>>,
        /// Replica (client id) whose full sync this chunk is for; the shard
        /// arms the replica's stream cut right after serializing.
        arm_replica: Option<u64>,
    },
    /// SAVE/BGSAVE: stream this shard's keyspace into `sink` in pieces,
    /// yielding in between (see [`save_rdb_chunk_yielding`]), then report on
    /// `done` (`Ok(false)` if the sink hung up or the save was aborted, `Err`
    /// if a tiered value could not be read). Unlike `SaveRdbChunk` this is a
    /// fuzzy snapshot, so it must not be used where an exact cut is needed.
    StreamRdbChunk {
        sink: flume::Sender<Option<Vec<u8>>>,
        done: flume::Sender<Result<bool, TieredReadError>>,
    },
    /// Streamed full sync to replica `replica`: start this shard's
    /// point-in-time image and arm the replica's stream cut, in one step
    /// (see [`start_sync_image`]).
    StartSyncImage {
        replica: u64,
        responder: flume::Sender<Result<(), TieredReadError>>,
    },
    /// The next piece of that image (see [`sync_image_step`]).
    SyncImageStep {
        replica: u64,
        /// The buffer of this shard's previous piece, once sent (see
        /// [`AofImage::give_buffer`]).
        recycle: Option<Vec<u8>>,
        responder: flume::Sender<Result<(Vec<u8>, bool), String>>,
    },
    /// Abandons that image: the full sync failed.
    DropSyncImage {
        replica: u64,
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
        sender: crate::pubsub::SubscriberTx,
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
        responder: flume::Sender<Vec<Bytes>>,
    },
    RemoveClientPubSub {
        client_id: u64,
    },
    /// Defines (or redefines) a search index on the shard. `aof_record` is
    /// the RESP of the FT.* command to append to the shard's AOF: every
    /// shard's AOF holds the index definitions, as each replays on its own.
    InitSearchIndex {
        schema: Box<crate::search::IndexSchema>,
        aof_record: Option<Bytes>,
        responder: flume::Sender<()>,
    },
    /// Drops a search index; `aof_record` as for `InitSearchIndex`, logged
    /// only if the shard had the index.
    DropSearchIndex {
        name: String,
        aof_record: Option<Bytes>,
        responder: flume::Sender<bool>,
    },
    SearchQuery {
        index: String,
        ast: Box<crate::search::QueryAst>,
        options: Box<crate::search::SearchOptions>,
        responder: flume::Sender<(usize, Vec<crate::search::SearchHit>)>,
    },
    Keys {
        db: usize,
        pattern: Bytes,
        responder: flume::Sender<Vec<Bytes>>,
    },
    Scan {
        db: usize,
        params: Box<ScanParams>,
        responder: flume::Sender<(usize, Vec<Bytes>)>,
    },
    RandomKey {
        db: usize,
        responder: flume::Sender<Option<Bytes>>,
    },
    ExpireTime {
        db: usize,
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
    /// Replaces the shard's dataset with its part of a full-sync RDB.
    RestoreRdbChunk {
        data: Bytes,
        /// The AOF directory when AOF is on, to rewrite it after the load.
        aof_dir: Option<std::path::PathBuf>,
        responder: flume::Sender<Result<(), String>>,
    },
    ExecuteReplicaCmd {
        db: usize,
        cmd: Box<Command>,
        responder: flume::Sender<()>,
    },
    /// Replicated commands for this shard, applied in order.
    ExecuteReplicaCmds {
        db: usize,
        cmds: Vec<Command>,
        responder: flume::Sender<()>,
    },
    FlushDb {
        db: usize,
        responder: flume::Sender<()>,
    },
    FlushAll {
        responder: flume::Sender<()>,
    },
    SwapDb {
        db1: usize,
        db2: usize,
        responder: flume::Sender<()>,
    },
    KeyspaceStats {
        responder: flume::Sender<Vec<(usize, usize, usize, u64)>>,
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
    /// This shard's [`crate::table::MemoryBreakdown`] (for `MEMORY STATS`).
    MemoryBreakdown {
        responder: flume::Sender<crate::table::MemoryBreakdown>,
    },
    /// Run `check_auto_tier_local` (spill this shard's hot keys while the
    /// server is over `maxmemory`); replies with the shard's `used_memory`.
    TierAutoSpill {
        responder: flume::Sender<usize>,
    },
    TryEvictOneKey {
        policy: String,
        responder: flume::Sender<Option<usize>>,
    },
    /// Evict this shard's keys under `policy` while the server is over
    /// `maxmemory` and the shard holds more than `floor` bytes; replies with
    /// the shard's `used_memory` and the tracking invalidations owed to
    /// `client_id` (delivered by the requester ahead of its reply).
    EvictUntilUnder {
        policy: String,
        floor: usize,
        client_id: u64,
        responder: flume::Sender<(usize, Vec<u8>)>,
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
        db: usize,
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
    pub current_db: usize,
    pub tables: Vec<Option<crate::table::RudisTable>>,
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
            current_db: 0,
            tables: Vec::new(),
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

    #[inline(always)]
    pub fn select_db(&mut self, target_db: usize) {
        if self.current_db != target_db {
            self.switch_db_slow(target_db);
        }
    }

    #[cold]
    fn switch_db_slow(&mut self, target_db: usize) {
        let old_db = self.current_db;
        let max_idx = old_db.max(target_db);
        if self.tables.len() <= max_idx {
            self.tables.resize_with(max_idx + 1, || None);
        }
        let target_table = self.tables[target_db].take().unwrap_or_default();
        let old_table = std::mem::replace(&mut self.table, target_table);
        self.tables[old_db] = Some(old_table);
        self.current_db = target_db;
    }

    #[inline]
    pub fn table_for_db(&self, db_idx: usize) -> Option<&crate::table::RudisTable> {
        if db_idx == self.current_db {
            Some(&self.table)
        } else {
            self.tables.get(db_idx).and_then(|t| t.as_ref())
        }
    }

    #[inline]
    pub fn table_for_db_mut(&mut self, db_idx: usize) -> &mut crate::table::RudisTable {
        if db_idx == self.current_db {
            return &mut self.table;
        }
        if self.tables.len() <= db_idx {
            self.tables.resize_with(db_idx + 1, || None);
        }
        self.tables[db_idx].get_or_insert_with(crate::table::RudisTable::new)
    }

    #[inline]
    pub fn max_db_len(&self) -> usize {
        self.tables.len().max(self.current_db + 1)
    }

    #[inline]
    pub fn total_used_memory(&self) -> usize {
        let mut total = self.table.used_memory();
        for (idx, opt) in self.tables.iter().enumerate() {
            if idx != self.current_db
                && let Some(t) = opt
            {
                total += t.used_memory();
            }
        }
        total
    }

    /// [`crate::table::RudisTable::eviction_size`] summed over every DB.
    pub fn eviction_size(&self) -> usize {
        let mut total = self.table.eviction_size();
        for (idx, opt) in self.tables.iter().enumerate() {
            if idx != self.current_db
                && let Some(t) = opt
            {
                total += t.eviction_size();
            }
        }
        total
    }

    /// See [`crate::table::RudisTable::refresh_data_step`]; every DB.
    pub fn refresh_data_step(&mut self, budget: usize) {
        self.table.refresh_data_step(budget);
        for (idx, opt) in self.tables.iter_mut().enumerate() {
            if idx != self.current_db
                && let Some(t) = opt
            {
                t.refresh_data_step(budget);
            }
        }
    }

    /// [`crate::table::RudisTable::memory_breakdown`] summed over every DB.
    pub fn memory_breakdown(&self) -> crate::table::MemoryBreakdown {
        let mut total = self.table.memory_breakdown();
        for (idx, opt) in self.tables.iter().enumerate() {
            if idx != self.current_db
                && let Some(t) = opt
            {
                total += t.memory_breakdown();
            }
        }
        total
    }

    #[inline]
    pub fn total_free_arena_bytes(&self) -> usize {
        let mut total = self.table.free_arena_bytes();
        for (idx, opt) in self.tables.iter().enumerate() {
            if idx != self.current_db
                && let Some(t) = opt
            {
                total += t.free_arena_bytes();
            }
        }
        total
    }

    /// Evicts one key under `policy`, trying the selected DB first and then
    /// the others. Like Redis, the eviction is logged as `DEL key` to `aof`
    /// and the replication stream (under the evicted key's DB, which AOF and
    /// replication pick up from `CURRENT_DB` to emit a `SELECT`), so neither
    /// an AOF replay nor a replica keeps a key the master dropped.
    pub fn try_evict_one_key(
        &mut self,
        policy: &str,
        aof: Option<&std::cell::RefCell<crate::aof::AofWriter>>,
    ) -> Option<usize> {
        let (port, shard_id) = (self.port, self.shard_id);
        let log_del = |key: &[u8]| {
            crate::replication::log_shard_mutation(port, shard_id, aof, || {
                crate::resp::Command::Del(smallvec::smallvec![Bytes::copy_from_slice(key)])
            });
        };
        let saved_db = self.current_db;
        let prev_tl = crate::connection::CURRENT_DB.get();
        crate::connection::CURRENT_DB.set(saved_db);
        let res = self.table.try_evict_one_key_with(policy, log_del);
        crate::connection::CURRENT_DB.set(prev_tl);
        if res.is_some() {
            return res;
        }
        let max_db = self.max_db_len();
        for db_idx in 0..max_db {
            if db_idx == saved_db {
                continue;
            }
            if self.table_for_db(db_idx).is_some_and(|t| !t.is_empty()) {
                self.select_db(db_idx);
                crate::connection::CURRENT_DB.set(db_idx);
                let res = self.table.try_evict_one_key_with(policy, log_del);
                self.select_db(saved_db);
                crate::connection::CURRENT_DB.set(prev_tl);
                if let Some(freed) = res {
                    return Some(freed);
                }
            }
        }
        None
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
                .filter(|e| e.expire_at().is_none_or(|exp| exp > now))
                .filter_map(|e| {
                    let k_str = String::from_utf8_lossy(&e.key);
                    let matched = if prefixes.is_empty() {
                        true
                    } else {
                        prefixes.iter().any(|p| k_str.starts_with(p))
                    };
                    if matched {
                        Some(e.key.to_bytes())
                    } else {
                        None
                    }
                })
                .collect();
            for key in matching_keys {
                if let Ok(raw) = self.table.hgetall(&key)
                    && !raw.is_empty()
                {
                    let key_str = String::from_utf8_lossy(&key);
                    idx.add_hash_document(&key_str, &raw);
                    if let Some(ref g) = global_idx {
                        g.write().add_hash_document(&key_str, &raw);
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
                    if let Some(ref g) = global_idx {
                        g.write()
                            .add_document(&k_str, extracted_fields, extracted_vectors);
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
                    if let Some(ref g) = global_idx {
                        g.write().add_document(&key_str, meta.fields, vecs);
                    }
                }
            }
        }

        self.search_indices.insert(index_name, idx);
    }

    pub fn drop_search_index(&mut self, name: &str) -> bool {
        self.search_indices.remove(name).is_some()
    }

    /// Defines `schema` from a persisted `FT.CREATE` (AOF replay, RDB load):
    /// replaces this shard's index of that name, re-indexing its keys, and
    /// makes sure the global registry has the definition.
    pub fn define_search_index(&mut self, schema: crate::search::IndexSchema) {
        crate::search::ensure_search_index(&schema);
        // A stale index of that name must not leak documents into the new one.
        self.search_indices.remove(&schema.name);
        self.init_search_index(schema);
    }

    /// Applies a persisted `FT.ALTER` to this shard's index (see
    /// [`Self::define_search_index`]). Errors if the index is unknown.
    pub fn alter_search_index(
        &mut self,
        index: &str,
        fields: std::collections::HashMap<String, crate::search::FieldType>,
        schema_fields: Vec<crate::search::SchemaField>,
    ) -> Result<(), String> {
        let mut schema = self
            .search_indices
            .get(index)
            .and_then(|idx| idx.schema.clone())
            .ok_or_else(|| format!("Unknown Index name: {}", index))?;
        crate::search::merge_schema_fields(&mut schema, fields, schema_fields);
        crate::search::ensure_search_index(&schema);
        self.init_search_index(schema);
        Ok(())
    }

    /// Re-indexes every search index from this shard's DB 0 keys, after a
    /// load replaced the keyspace without going through the indexing hooks.
    pub fn rebuild_search_indices(&mut self) {
        if self.search_indices.is_empty() {
            return;
        }
        let schemas: Vec<crate::search::IndexSchema> = self
            .search_indices
            .values()
            .filter_map(|idx| idx.schema.clone())
            .collect();
        let saved_db = self.current_db;
        self.select_db(0);
        for schema in schemas {
            self.define_search_index(schema);
        }
        self.select_db(saved_db);
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

    /// GET semantics: like [`ShardDb::get`] but a key holding another type
    /// is a WRONGTYPE error. A tiered key reads as `Ok(None)`; callers load
    /// it from the cold tier.
    #[inline(always)]
    pub fn get_checked(&mut self, key: &[u8]) -> Result<Option<Bytes>, &'static str> {
        match self.table.get(key) {
            Err(_) if self.table.is_tiered(key).is_some() => Ok(None),
            res => res,
        }
    }

    #[inline(always)]
    pub fn get_with_hash(&mut self, key: &[u8], h: u64) -> Option<Bytes> {
        self.table.get_with_hash(key, h).ok().flatten()
    }

    #[inline(always)]
    pub fn write_get_resp(&mut self, key: &[u8], out: &mut Vec<u8>) -> Result<bool, &'static str> {
        self.table.write_get_resp(key, out)
    }

    #[inline(always)]
    pub fn drain_dropped_tier(&mut self) {
        if self.table.dropped_tier.is_empty() {
            return;
        }
        if let Some(tm) = &self.tier_manager {
            for (ptr, is_cooled) in self.table.dropped_tier.drain(..) {
                tm.on_key_overwritten(ptr, is_cooled);
            }
        } else {
            self.table.dropped_tier.clear();
        }
    }

    #[inline]
    pub fn set(&mut self, key: Bytes, value: Bytes, expire_in: Option<Duration>) {
        self.table.set(key, value, expire_in);
        self.drain_dropped_tier();
    }

    #[inline]
    pub fn set_extended(
        &mut self,
        key: Bytes,
        value: Bytes,
        expire_in: Option<Duration>,
        keepttl: bool,
    ) {
        self.table.set_extended(key, value, expire_in, keepttl);
        self.drain_dropped_tier();
    }

    #[inline(always)]
    pub fn del(&mut self, key: &[u8]) -> bool {
        let deleted = self.table.del(key);
        if deleted {
            self.sticky_keys.remove(key);
            self.drain_dropped_tier();
            return true;
        }
        self.drain_dropped_tier();
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
        let deleted = self.table.del_with_hash(key, hash);
        if deleted {
            self.sticky_keys.remove(key);
            self.drain_dropped_tier();
            return true;
        }
        self.drain_dropped_tier();
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
        self.hydrate_key(key);
        let res = self.table.incr_by_slice_fast(key, delta);
        self.drain_dropped_tier();
        res
    }

    #[inline]
    pub fn incr_by_slice(&mut self, key: &[u8], delta: i64) -> Result<i64, &'static str> {
        self.hydrate_key(key);
        let res = self.table.incr_by_slice(key, delta);
        self.drain_dropped_tier();
        res
    }

    #[inline]
    pub fn incr_by(&mut self, key: Bytes, delta: i64) -> Result<i64, String> {
        self.hydrate_key(&key);
        let res = self.table.incr_by(key, delta);
        self.drain_dropped_tier();
        res
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

    /// See `RudisTable::zadd_slice_changes`.
    #[inline]
    pub fn zadd_slice_changes(
        &mut self,
        key: &[u8],
        elements: &[(f64, Bytes)],
        flags: crate::table::ZAddFlags,
    ) -> Result<(usize, usize, Option<f64>), &'static str> {
        self.table.zadd_slice_changes(key, elements, flags)
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

    /// Empties the currently selected database (`self.current_db`).
    /// If `self.current_db == 0`, also clears DB-0-scoped extended stores.
    #[inline]
    pub fn flushdb(&mut self) {
        self.table.flushdb();
        self.drain_dropped_tier();
        if self.current_db == 0 {
            self.sticky_keys.clear();
            self.vector_indexes.clear();
            self.semantic_caches.clear();
            self.agent_memories.clear();
            self.llm_quotas.clear();
            self.agent_checkpoints.clear();
            self.agent_tools.clear();
            self.json_store = crate::json::JsonStore::new();
            self.probabilistic_store = crate::probabilistic::ProbabilisticStore::new();
            self.crdt_store.registers.clear();
            self.crdt_store.sets.clear();
            self.crdt_store.counters.clear();
        }
    }

    /// Empties the shard's whole dataset across ALL databases, every key
    /// family included. The CRDT clock stays, so it never runs backwards.
    #[inline]
    pub fn flushall(&mut self) {
        self.table.flushdb();
        self.table.dropped_tier.clear();
        for t in self.tables.iter_mut().flatten() {
            t.flushdb();
            t.dropped_tier.clear();
        }
        self.tables.clear();
        if let Some(tm) = &self.tier_manager {
            tm.reset();
        }
        self.sticky_keys.clear();
        self.vector_indexes.clear();
        self.semantic_caches.clear();
        self.agent_memories.clear();
        self.llm_quotas.clear();
        self.agent_checkpoints.clear();
        self.agent_tools.clear();
        self.json_store = crate::json::JsonStore::new();
        self.probabilistic_store = crate::probabilistic::ProbabilisticStore::new();
        self.crdt_store.registers.clear();
        self.crdt_store.sets.clear();
        self.crdt_store.counters.clear();
    }

    pub fn swap_dbs(&mut self, db1: usize, db2: usize) {
        if db1 == db2 {
            return;
        }
        poison_aof_image("SWAPDB ran during the rewrite");
        let saved = self.current_db;
        self.select_db(0);
        let max_idx = db1.max(db2);
        if self.tables.len() <= max_idx {
            self.tables.resize_with(max_idx + 1, || None);
        }
        if db1 == 0 {
            let mut t2 = self.tables[db2].take().unwrap_or_default();
            std::mem::swap(&mut self.table, &mut t2);
            self.tables[db2] = Some(t2);
        } else if db2 == 0 {
            let mut t1 = self.tables[db1].take().unwrap_or_default();
            std::mem::swap(&mut self.table, &mut t1);
            self.tables[db1] = Some(t1);
        } else {
            self.tables.swap(db1, db2);
        }
        self.select_db(saved);
        if crate::block::has_blocked_waiters(self.port) {
            let hub_arc = crate::block::get_block_hub_for_port(self.port);
            let mut hub = hub_arc.lock();
            hub.notify_db_keys(self, db1);
            hub.notify_db_keys(self, db2);
        }
    }

    pub fn move_key(&mut self, src_db: usize, dst_db: usize, key: &[u8]) -> bool {
        if src_db == dst_db {
            return false;
        }
        let saved = self.current_db;
        self.select_db(src_db);
        if !self.table.exists(key) {
            self.select_db(saved);
            return false;
        }
        self.select_db(dst_db);
        if self.table.exists(key) {
            self.select_db(saved);
            return false;
        }
        self.select_db(src_db);
        let Some((entry, hfe)) = self.table.take_key_entry(key) else {
            self.select_db(saved);
            return false;
        };
        let key_bytes = entry.key.to_bytes();
        self.select_db(dst_db);
        self.table.insert_entry(entry);
        if let Some(fmap) = hfe
            && !fmap.is_empty()
        {
            self.table.hash_field_expires.insert(key_bytes, fmap);
        }
        self.select_db(saved);
        if crate::block::has_blocked_waiters(self.port) {
            let hub_arc = crate::block::get_block_hub_for_port(self.port);
            let mut hub = hub_arc.lock();
            hub.notify_db_keys(self, dst_db);
        }
        true
    }

    pub fn copy_to_db(
        &mut self,
        src_db: usize,
        dst_db: usize,
        src: &[u8],
        dst: Bytes,
        replace: bool,
    ) -> Result<bool, &'static str> {
        if src_db == dst_db {
            let saved = self.current_db;
            self.select_db(src_db);
            let res = self.copy(src, dst, replace);
            self.select_db(saved);
            return res;
        }
        let saved = self.current_db;
        self.select_db(src_db);
        if !self.table.exists(src) {
            self.select_db(saved);
            return Ok(false);
        }
        let hydrated = self
            .table
            .is_tiered(src)
            .and_then(|ptr| self.hydrate_tiered(ptr));
        let Some(entry) = self.table.peek_entry(src) else {
            self.select_db(saved);
            return Ok(false);
        };
        let val = match &entry.val {
            crate::table::RudisValue::Tiered(_) => match hydrated {
                Some(v) => v,
                None => {
                    self.select_db(saved);
                    return Ok(false);
                }
            },
            crate::table::RudisValue::Cooled(cv) => cv.val.clone(),
            other => other.clone(),
        };
        let expire_at = entry.expire_at();
        let hfe = self.table.hash_field_expires.get(src).cloned();

        self.select_db(dst_db);
        if !replace && self.table.exists(&dst) {
            self.select_db(saved);
            return Ok(false);
        }
        let overwritten = (replace && self.tier_manager.is_some())
            .then(|| {
                self.table
                    .is_tiered(&dst)
                    .map(|p| (p, false))
                    .or_else(|| self.table.is_cooled(&dst).map(|p| (p, true)))
            })
            .flatten();
        let new_entry = crate::table::RudisEntry::new(
            crate::compact::CompactKey::new(&dst),
            val,
            crate::table::Expiry::from(expire_at),
        );
        self.table.insert_entry(new_entry);
        if let Some(fmap) = hfe {
            self.table.hash_field_expires.insert(dst, fmap);
        }
        if let Some((ptr, is_cooled)) = overwritten
            && let Some(tm) = &self.tier_manager
        {
            tm.on_key_overwritten(ptr, is_cooled);
        }
        self.drain_dropped_tier();
        self.select_db(saved);
        if crate::block::has_blocked_waiters(self.port) {
            let hub_arc = crate::block::get_block_hub_for_port(self.port);
            let mut hub = hub_arc.lock();
            hub.notify_db_keys(self, dst_db);
        }
        Ok(true)
    }

    /// Per-DB `(db_idx, keys, expires, ttl_sum_ms)` for `INFO keyspace`.
    pub fn keyspace_stats_by_db(&self) -> Vec<(usize, usize, usize, u64)> {
        let now = std::time::Instant::now();
        let max_db = self.max_db_len();
        let mut out = Vec::new();
        for db_idx in 0..max_db {
            let Some(table) = self.table_for_db(db_idx) else {
                continue;
            };
            let mut keys = table.len();
            if db_idx == 0 {
                keys += self.json_store.len()
                    + self.probabilistic_store.bloom_filters.len()
                    + self.probabilistic_store.cuckoo_filters.len()
                    + self.probabilistic_store.cms_sketches.len()
                    + self.probabilistic_store.topk_trackers.len()
                    + self.vector_indexes.len();
            }
            if keys == 0 {
                continue;
            }
            let expires = table.num_expires;
            let mut ttl_sum_ms = 0u64;
            if expires > 0 {
                let mut sampled = 0usize;
                for entry in table.entries() {
                    if let Some(exp) = entry.expire_at() {
                        if exp > now {
                            ttl_sum_ms += exp.duration_since(now).as_millis() as u64;
                        }
                        sampled += 1;
                        if sampled >= 100 {
                            break;
                        }
                    }
                }
            }
            out.push((db_idx, keys, expires, ttl_sum_ms));
        }
        out
    }

    /// Replaces this shard's dataset with its part of a master's full-sync
    /// RDB. Like Redis, the old dataset goes first, and a load that fails
    /// part-way leaves the shard empty rather than half loaded.
    ///
    /// With `aof` (its directory and the live writer), the shard's AOF is
    /// then rewritten from what is in memory, loaded or emptied, before
    /// anything else runs on the shard: otherwise a restart would replay
    /// the pre-sync AOF instead of the master's dataset.
    pub fn load_full_sync_rdb(
        &mut self,
        data: &[u8],
        shard_id: usize,
        num_shards: usize,
        aof: Option<(
            &std::path::Path,
            &std::rc::Rc<std::cell::RefCell<crate::aof::AofWriter>>,
        )>,
    ) -> Result<(), String> {
        poison_aof_image("a full resync replaced the dataset during the rewrite");
        self.select_db(0);
        self.flushall();
        // The file's index definitions replace ours (a FLUSHALL keeps them).
        self.search_indices.clear();
        let loaded = crate::table::load_rdb_bytes(data, self, shard_id, num_shards)
            .map(|_| ())
            .map_err(|e| {
                self.select_db(0);
                self.flushall();
                e.to_string()
            });
        self.select_db(0);
        if let Some((dir, writer)) = aof {
            crate::aof::rewrite_and_swap_shard_aof(self, dir, shard_id, Some(writer))
                .map_err(|e| format!("rewriting the AOF after the load: {e}"))?;
        }
        loaded
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
    pub fn idletime(&mut self, key: &[u8]) -> Option<u64> {
        self.table.idletime(key)
    }

    #[inline]
    pub fn lfu_freq(&mut self, key: &[u8]) -> Option<u8> {
        self.table.lfu_freq(key)
    }

    #[inline]
    pub fn lru_and_idletime(&mut self, key: &[u8]) -> Option<(u32, u64)> {
        self.table.lru_and_idletime(key)
    }

    #[inline]
    pub fn rename(&mut self, src: &[u8], dst: Bytes, nx: bool) -> Result<bool, &'static str> {
        let overwritten = (src != dst.as_ref() && self.tier_manager.is_some())
            .then(|| {
                self.table
                    .is_tiered(&dst)
                    .map(|p| (p, false))
                    .or_else(|| self.table.is_cooled(&dst).map(|p| (p, true)))
            })
            .flatten();
        let res = self.table.rename(src, dst, nx);
        if matches!(res, Ok(true))
            && let Some((ptr, is_cooled)) = overwritten
            && let Some(tm) = &self.tier_manager
        {
            tm.on_key_overwritten(ptr, is_cooled);
        }
        res
    }

    #[inline]
    pub fn copy(&mut self, src: &[u8], dst: Bytes, replace: bool) -> Result<bool, &'static str> {
        let hydrated = self
            .table
            .is_tiered(src)
            .and_then(|ptr| self.hydrate_tiered(ptr));
        let overwritten = (src != dst.as_ref() && replace && self.tier_manager.is_some())
            .then(|| {
                self.table
                    .is_tiered(&dst)
                    .map(|p| (p, false))
                    .or_else(|| self.table.is_cooled(&dst).map(|p| (p, true)))
            })
            .flatten();
        let res = self.table.copy_with_hydrated(src, dst, replace, hydrated);
        if matches!(res, Ok(true))
            && let Some((ptr, is_cooled)) = overwritten
            && let Some(tm) = &self.tier_manager
        {
            tm.on_key_overwritten(ptr, is_cooled);
        }
        res
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
        let prev_tl = crate::connection::CURRENT_DB.get();
        let saved_db = self.current_db;
        crate::connection::CURRENT_DB.set(saved_db);
        let mut n = self.table.active_expire_cycle();
        self.drain_dropped_tier();
        if !self.tables.is_empty() {
            let max_db = self.max_db_len();
            for db_idx in 0..max_db {
                if db_idx == saved_db {
                    continue;
                }
                let has_exp = self
                    .table_for_db(db_idx)
                    .is_some_and(|t| t.num_expires > 0 || !t.hash_field_expires.is_empty());
                if has_exp {
                    self.select_db(db_idx);
                    crate::connection::CURRENT_DB.set(db_idx);
                    n += self.table.active_expire_cycle();
                    self.drain_dropped_tier();
                }
            }
            self.select_db(saved_db);
        }
        crate::connection::CURRENT_DB.set(prev_tl);
        n
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
    pub fn pfdebug_getreg(&mut self, key: &Bytes) -> Result<[u8; 16384], &'static str> {
        self.table.pfdebug_getreg(key)
    }

    #[inline]
    pub fn pfdebug_encoding(&mut self, key: &Bytes) -> Result<&'static str, &'static str> {
        self.table.pfdebug_encoding(key)
    }

    #[inline]
    pub fn pfdebug_todense(&mut self, key: &Bytes) -> Result<bool, &'static str> {
        self.table.pfdebug_todense(key)
    }

    #[inline]
    pub fn dump(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        // A key still on the tier (it is only loaded back when memory
        // allows) is dumped from its record, decoded without promoting it.
        if let Some(ptr) = self.table.is_tiered(key) {
            let val = self.hydrate_tiered(ptr)?;
            return crate::redis_rdb::dump_value(&val);
        }
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
        approx: bool,
        trim_strategy: crate::table::StreamTrimStrategy,
        idmp: Option<crate::table::StreamIdmpOption>,
        limit: Option<usize>,
    ) -> Result<crate::table::StreamAddResult, &'static str> {
        self.table.xadd(
            key,
            add_id,
            fields,
            nomkstream,
            maxlen,
            minid,
            approx,
            trim_strategy,
            idmp,
            limit,
        )
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
        maxcount: Option<usize>,
        maxsize: Option<usize>,
    ) -> Result<Vec<(Bytes, Vec<(crate::table::StreamId, Vec<(Bytes, Bytes)>)>)>, &'static str>
    {
        self.table.xread(keys, ids, count, maxcount, maxsize)
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
        approx: bool,
        trim_strategy: crate::table::StreamTrimStrategy,
        limit: Option<usize>,
    ) -> Result<usize, &'static str> {
        self.table
            .xtrim(key, maxlen, minid, approx, trim_strategy, limit)
    }

    #[inline]
    pub fn xcfgset(
        &mut self,
        key: &[u8],
        duration: Option<u64>,
        maxsize: Option<usize>,
    ) -> Result<(), &'static str> {
        self.table.xcfgset(key, duration, maxsize)
    }

    #[inline]
    pub fn xsetid(
        &mut self,
        key: &[u8],
        last_id: crate::table::StreamId,
        entries_added: Option<u64>,
        max_deleted_id: Option<crate::table::StreamId>,
    ) -> Result<(), &'static str> {
        self.table
            .xsetid(key, last_id, entries_added, max_deleted_id)
    }

    #[inline]
    pub fn xdelex(
        &mut self,
        key: &[u8],
        strategy: crate::table::StreamTrimStrategy,
        ids: &[crate::table::StreamId],
    ) -> (Vec<i64>, usize) {
        self.table.xdelex(key, strategy, ids)
    }

    #[inline]
    pub fn xackdel(
        &mut self,
        key: &[u8],
        group: &[u8],
        strategy: crate::table::StreamTrimStrategy,
        ids: &[crate::table::StreamId],
    ) -> (Vec<i64>, usize) {
        self.table.xackdel(key, group, strategy, ids)
    }

    #[inline]
    pub fn xidmprecord(
        &mut self,
        key: &[u8],
        pid: Bytes,
        iid: Bytes,
        id_raw: &[u8],
    ) -> Result<(), &'static str> {
        self.table.xidmprecord(key, pid, iid, id_raw)
    }

    #[inline]
    pub fn xinfo_stream(&mut self, key: &[u8]) -> Result<crate::table::StreamInfo, &'static str> {
        self.table.xinfo_stream(key)
    }

    #[inline]
    pub fn xinfo_stream_full(
        &mut self,
        key: &[u8],
        count: Option<usize>,
    ) -> Result<crate::table::StreamFullInfo, &'static str> {
        self.table.xinfo_stream_full(key, count)
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

    /// Serializes this shard's keyspace into `buf`. Fails, with `buf` holding
    /// a partial chunk the caller must discard, if a tiered value can't be
    /// read back from disk.
    #[inline]
    pub fn save_rdb_chunk(&mut self, buf: &mut Vec<u8>) -> Result<(), TieredReadError> {
        let clock = RdbClock::now(self.port);
        if let Some(t0) = self.table_for_db(0) {
            for entry in t0.entries() {
                self.save_rdb_entry(entry, &clock, buf)?;
            }
        }
        self.save_extended_rdb_chunk(buf);
        self.save_other_dbs_rdb_chunk(&clock, buf)
    }

    fn save_other_dbs_rdb_chunk(
        &self,
        clock: &RdbClock,
        buf: &mut Vec<u8>,
    ) -> Result<(), TieredReadError> {
        let max_db = self.max_db_len();
        let mut switched_db = false;
        for db_idx in 1..max_db {
            let Some(table) = self.table_for_db(db_idx) else {
                continue;
            };
            if table.is_empty() && table.hash_field_expires.is_empty() {
                continue;
            }
            crate::redis_rdb::write_selectdb(buf, db_idx);
            switched_db = true;
            for entry in table.entries() {
                self.save_rdb_entry(entry, clock, buf)?;
            }
            if !table.hash_field_expires.is_empty() {
                let mut records = Vec::new();
                Self::write_hash_field_expires_legacy(
                    &table.hash_field_expires,
                    clock,
                    &mut records,
                );
                crate::redis_rdb::write_ext_aux(buf, &records);
            }
        }
        if switched_db {
            crate::redis_rdb::write_selectdb(buf, 0);
        }
        Ok(())
    }

    fn write_hash_field_expires_legacy(
        hfe: &hashbrown::HashMap<Bytes, hashbrown::HashMap<Bytes, std::time::Instant>>,
        clock: &RdbClock,
        buf: &mut Vec<u8>,
    ) {
        for (key, fmap) in hfe {
            // Passed deadlines too: the field itself is saved until it is
            // purged, and without its deadline it came back for good.
            let active: Vec<(&Bytes, u64)> = fmap
                .iter()
                .map(|(f, &exp)| {
                    (
                        f,
                        crate::table::instant_to_unix_ms(exp, clock.now, clock.unix_ms),
                    )
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

    /// Serializes one table segment (see [`crate::table::RudisFlatTable::layout_epoch`]).
    fn save_rdb_segment_db0(
        &self,
        seg: usize,
        clock: &RdbClock,
        buf: &mut Vec<u8>,
    ) -> Result<(), TieredReadError> {
        if let Some(t0) = self.table_for_db(0) {
            for entry in t0.segment_entries(seg) {
                self.save_rdb_entry(entry, clock, buf)?;
            }
        }
        Ok(())
    }

    fn save_rdb_entry(
        &self,
        entry: &crate::table::RudisEntry,
        clock: &RdbClock,
        buf: &mut Vec<u8>,
    ) -> Result<(), TieredReadError> {
        Self::save_rdb_entry_with(entry, clock, buf, |ptr| self.hydrate_tiered(ptr))
    }

    /// [`Self::save_rdb_entry`] reading tiered values with `hydrate`.
    fn save_rdb_entry_with(
        entry: &crate::table::RudisEntry,
        clock: &RdbClock,
        buf: &mut Vec<u8>,
        hydrate: impl FnOnce(crate::table::TieredPointer) -> Option<crate::table::RudisValue>,
    ) -> Result<(), TieredReadError> {
        if let Some(exp) = entry.expire_at()
            && exp <= clock.now
            && !clock.keep_expired
        {
            return Ok(());
        }
        // A tiered record holds the value in `serialize_val_payload` form
        // (see `RudisTable::get_value_for_spill`); it is decoded so it can
        // be written in the Redis encoding. A failed read fails the whole
        // snapshot: like Redis, a save that cannot write every key must not
        // produce (and rename into place) a file missing that key.
        let hydrated = match &entry.val {
            crate::table::RudisValue::Tiered(ptr) => match hydrate(**ptr) {
                Some(val) => Some(val),
                None => {
                    let err = TieredReadError {
                        key: bytes::Bytes::copy_from_slice(&entry.key),
                    };
                    tracing::error!("RDB save failed: {}", err);
                    return Err(err);
                }
            },
            _ => None,
        };
        Self::write_rdb_value(entry, hydrated.as_ref().unwrap_or(&entry.val), clock, buf);
        Ok(())
    }

    fn write_rdb_value(
        entry: &crate::table::RudisEntry,
        val: &crate::table::RudisValue,
        clock: &RdbClock,
        buf: &mut Vec<u8>,
    ) {
        let expire_unix_ms = entry
            .expire_at()
            .map(|exp| clock.unix_ms + exp.duration_since(clock.now).as_millis() as u64);
        crate::redis_rdb::write_record(buf, &entry.key, val, expire_unix_ms);
        let val = match val {
            crate::table::RudisValue::Cooled(cv) => &cv.val,
            v => v,
        };
        if let crate::table::RudisValue::Stream(s) = val
            && crate::redis_rdb::stream_has_extras(s)
        {
            let mut ext = Vec::new();
            crate::redis_rdb::encode_stream_extras(&entry.key, s, &mut ext);
            crate::redis_rdb::write_ext_aux(buf, &ext);
        }
    }

    /// Reads a tiered record's value payload, already in
    /// `serialize_val_payload` form. The record's CRC and header type are
    /// checked by the tier manager; the payload's own type tag must agree.
    fn read_tiered_payload(&self, ptr: crate::table::TieredPointer) -> Option<Vec<u8>> {
        read_tiered_payload_from(self.tier_manager.as_deref()?, ptr)
    }

    /// Reads and decodes a tiered value, for paths that need the value itself
    /// (such as the AOF rewrite).
    pub fn hydrate_tiered(
        &self,
        ptr: crate::table::TieredPointer,
    ) -> Option<crate::table::RudisValue> {
        let raw = self.read_tiered_payload(ptr)?;
        let (val, used) = crate::table::RudisTable::deserialize_val_payload(&raw).ok()?;
        (used == raw.len()).then_some(val)
    }

    /// Brings `cmd`'s keys that live on the tier (spilled to disk, or cooled)
    /// back to plain in-memory values before it runs: the data-type commands
    /// only understand in-memory values and would otherwise answer
    /// WRONGTYPE (or act on the wrong data) for a key auto-tiering spilled.
    /// GET/MGET are left alone, they stream cold values without promoting
    /// them; so are commands that don't read the value. A no-op unless the
    /// server has tiered or cooled keys.
    pub fn hydrate_cmd_keys(&mut self, cmd: &crate::resp::Command) {
        use crate::resp::Command;
        let Some(tm) = self.tier_manager.as_ref() else {
            return;
        };
        // Borrowed, not cloned: this runs for every command on every shard,
        // and an Arc clone/drop is two atomic RMWs on a counter all shards
        // share (3.5% of server time in a pipelined ZPOPMIN profile).
        let stats = &tm.stats;
        if stats.tiered_keys.load(std::sync::atomic::Ordering::Relaxed) == 0
            && stats.cooled_keys.load(std::sync::atomic::Ordering::Relaxed) == 0
        {
            return;
        }
        if matches!(
            cmd,
            Command::Get(_)
                | Command::Mget(_)
                | Command::Del(_)
                | Command::Unlink(_)
                | Command::Exists(_)
                | Command::Type(_)
        ) {
            return;
        }
        let mut keys: smallvec::SmallVec<[&[u8]; 4]> = smallvec::SmallVec::new();
        crate::connection::for_each_cmd_key(cmd, |k| keys.push(k));
        for key in keys {
            self.hydrate_key_unchecked(key);
        }
        self.drain_dropped_tier();
    }

    /// [`ShardDb::hydrate_cmd_keys`] for one key.
    #[inline]
    pub fn hydrate_key(&mut self, key: &[u8]) {
        let Some(tm) = self.tier_manager.as_ref() else {
            return;
        };
        if tm
            .stats
            .tiered_keys
            .load(std::sync::atomic::Ordering::Relaxed)
            == 0
            && tm
                .stats
                .cooled_keys
                .load(std::sync::atomic::Ordering::Relaxed)
                == 0
        {
            return;
        }
        self.hydrate_key_unchecked(key);
        self.drain_dropped_tier();
    }

    fn hydrate_key_unchecked(&mut self, key: &[u8]) {
        let Some(stats) = self.tier_manager.as_ref().map(|tm| tm.stats.clone()) else {
            return;
        };
        if let Some(ptr) = self.table.is_tiered(key) {
            let Some(val) = self.hydrate_tiered(ptr) else {
                return;
            };
            let payload_len = ptr.length as u64;
            if self.table.restore_tiered_value(key, val) {
                use std::sync::atomic::Ordering::Relaxed;
                stats.total_fetches.fetch_add(1, Relaxed);
                stats.tiered_keys.fetch_sub(1, Relaxed);
                stats.cooled_keys.fetch_add(1, Relaxed);
                let _ = stats
                    .ram_saved_bytes
                    .try_update(Relaxed, Relaxed, |v| Some(v.saturating_sub(payload_len)));
            }
        }
        self.table.warm_key(key);
    }

    /// Writes the state Redis has no encoding for (see
    /// [`crate::redis_rdb`]): legacy-format records wrapped in a
    /// `rudis-ext` AUX field, which Redis and Valkey skip.
    pub fn save_extended_rdb_chunk(&self, buf: &mut Vec<u8>) {
        let mut records = Vec::new();
        self.save_extended_legacy_records(&mut records);
        crate::redis_rdb::write_ext_aux(buf, &records);
    }

    /// The legacy-format records behind [`Self::save_extended_rdb_chunk`].
    fn save_extended_legacy_records(&self, buf: &mut Vec<u8>) {
        // 1-6. JSON documents, Bloom filters, vector indexes, Cuckoo
        // filters, Count-Min Sketches and Top-K trackers, one item each.
        for (kind, key) in self.ext_item_keys() {
            self.save_ext_item(kind, &key, buf);
        }
        self.save_extended_small_records(buf);
    }

    /// The extended structures written one at a time (JSON documents,
    /// probabilistic structures, vector indexes): each with its key, in the
    /// order [`Self::save_extended_legacy_records`] writes them.
    pub(crate) fn ext_item_keys(&self) -> Vec<(ExtKind, Bytes)> {
        let mut keys: Vec<(ExtKind, Bytes)> = Vec::new();
        keys.extend(
            self.json_store
                .iter()
                .map(|(k, _)| (ExtKind::Json, k.clone())),
        );
        let ps = &self.probabilistic_store;
        keys.extend(ps.bloom_filters.keys().map(|k| (ExtKind::Bloom, k.clone())));
        keys.extend(
            self.vector_indexes
                .keys()
                .map(|n| (ExtKind::Vector, Bytes::copy_from_slice(n.as_bytes()))),
        );
        keys.extend(
            ps.cuckoo_filters
                .keys()
                .map(|k| (ExtKind::Cuckoo, k.clone())),
        );
        keys.extend(ps.cms_sketches.keys().map(|k| (ExtKind::Cms, k.clone())));
        keys.extend(ps.topk_trackers.keys().map(|k| (ExtKind::Topk, k.clone())));
        keys
    }

    /// Writes the legacy record(s) of extended structure `key` of `kind`,
    /// if it exists.
    pub(crate) fn save_ext_item(&self, kind: ExtKind, key: &[u8], buf: &mut Vec<u8>) {
        let ps = &self.probabilistic_store;
        let mut put = |tag: u8, encode: &dyn Fn(&mut Vec<u8>)| {
            buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buf.extend_from_slice(key);
            buf.push(tag);
            encode(buf);
        };
        match kind {
            ExtKind::Json => {
                if let Some(val) = self.json_store.get(key) {
                    put(7, &|buf| {
                        let json_str = val.to_string();
                        buf.extend_from_slice(&(json_str.len() as u32).to_le_bytes());
                        buf.extend_from_slice(json_str.as_bytes());
                    });
                }
            }
            ExtKind::Bloom => {
                if let Some(bf) = ps.bloom_filters.get(key) {
                    put(8, &|buf| bf.encode(buf));
                }
            }
            ExtKind::Cuckoo => {
                if let Some(cf) = ps.cuckoo_filters.get(key) {
                    put(10, &|buf| cf.encode(buf));
                }
            }
            ExtKind::Cms => {
                if let Some(cms) = ps.cms_sketches.get(key) {
                    put(11, &|buf| cms.encode(buf));
                }
            }
            ExtKind::Topk => {
                if let Some(topk) = ps.topk_trackers.get(key) {
                    put(12, &|buf| topk.encode(buf));
                }
            }
            ExtKind::Vector => {
                let Ok(name_str) = std::str::from_utf8(key) else {
                    return;
                };
                if let Some((name, index)) = self.vector_indexes.get_key_value(name_str) {
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
            }
        }
    }

    /// The extended state other than [`Self::ext_item_keys`]: CRDT state,
    /// DB 0's hash field deadlines, semantic caches, agent state, search
    /// index definitions, LLM quotas.
    fn save_extended_small_records(&self, buf: &mut Vec<u8>) {
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

        // 8. Hash field expirations (Redis 7.4 / Valkey 8 HEXPIRE) for DB 0
        if let Some(t0) = self.table_for_db(0)
            && !t0.hash_field_expires.is_empty()
        {
            let clock = RdbClock::now(self.port);
            Self::write_hash_field_expires_legacy(&t0.hash_field_expires, &clock, buf);
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

        // 13. Search index definitions (FT.CREATE), as their canonical
        // FT.CREATE command. Only the definition: the documents are this
        // shard's HASH/JSON keys, re-indexed once the load is done (see
        // `rebuild_search_indices`). Every shard writes the same records;
        // each shard loading the file keeps one copy of each.
        let mut names: Vec<&String> = self.search_indices.keys().collect();
        names.sort();
        for name in names {
            if let Some(schema) = &self.search_indices[name].schema {
                write_bytes(buf, name.as_bytes());
                buf.push(crate::redis_rdb::EXT_TYPE_SEARCH_INDEX);
                write_bytes(buf, &crate::aof::resp_argv(&schema.ft_create_args()));
            }
        }

        // 14. LLM quota buckets (LLM.QUOTA.*)
        if !self.llm_quotas.is_empty() {
            let now = std::time::Instant::now();
            let unix_now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            for (quota_key, bucket) in &self.llm_quotas {
                let active_reqs: Vec<(u64, u64)> = bucket
                    .requests
                    .iter()
                    .filter_map(|&(ts, tokens)| {
                        let elapsed = now.duration_since(ts).as_millis() as u64;
                        if elapsed < bucket.window_ms {
                            Some((unix_now + (bucket.window_ms - elapsed).max(1), tokens))
                        } else {
                            None
                        }
                    })
                    .collect();
                let mut active_res: Vec<(u64, u64, u64)> = bucket
                    .reservations
                    .iter()
                    .filter_map(|(&res_id, &(ts, est_tokens))| {
                        let elapsed = now.duration_since(ts).as_millis() as u64;
                        if elapsed < bucket.window_ms {
                            Some((
                                res_id,
                                unix_now + (bucket.window_ms - elapsed).max(1),
                                est_tokens,
                            ))
                        } else {
                            None
                        }
                    })
                    .collect();
                active_res.sort_by_key(|(id, _, _)| *id);
                if !active_reqs.is_empty()
                    || !active_res.is_empty()
                    || bucket.next_reservation_id > 1
                {
                    write_bytes(buf, quota_key);
                    buf.push(21u8);
                    buf.extend_from_slice(&bucket.window_ms.to_le_bytes());
                    buf.extend_from_slice(&bucket.next_reservation_id.to_le_bytes());
                    buf.extend_from_slice(&(active_reqs.len() as u32).to_le_bytes());
                    for (exp_unix_ms, tokens) in active_reqs {
                        buf.extend_from_slice(&exp_unix_ms.to_le_bytes());
                        buf.extend_from_slice(&tokens.to_le_bytes());
                    }
                    buf.extend_from_slice(&(active_res.len() as u32).to_le_bytes());
                    for (res_id, exp_unix_ms, est_tokens) in active_res {
                        buf.extend_from_slice(&res_id.to_le_bytes());
                        buf.extend_from_slice(&exp_unix_ms.to_le_bytes());
                        buf.extend_from_slice(&est_tokens.to_le_bytes());
                    }
                }
            }
        }
    }

    /// Loads a [`Self::save_rdb_chunk`] record stream, keeping every key.
    pub fn restore_rdb_chunk(&mut self, data: &[u8]) -> Result<(), String> {
        crate::redis_rdb::load_chunk(data, self).map_err(|e| e.to_string())?;
        self.rebuild_search_indices();
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
                        // Saturating: the starting count comes from the file.
                        evicted_expired = evicted_expired.saturating_add(1);
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
            21 => {
                let window_ms = read_u64(data)?.max(1);
                let next_reservation_id = read_u64(data)?;
                let reqs_len = read_u32(data)? as usize;
                let now = std::time::Instant::now();
                let mut bucket = crate::agent::LlmQuotaBucket::new(window_ms);
                bucket.next_reservation_id = next_reservation_id;
                for _ in 0..reqs_len {
                    let exp_unix_ms = read_u64(data)?;
                    let tokens = read_u64(data)?;
                    if !is_owned || exp_unix_ms <= unix_now {
                        continue;
                    }
                    let rem_ms = (exp_unix_ms - unix_now).min(window_ms);
                    let age_ms = window_ms.saturating_sub(rem_ms);
                    let ts = now
                        .checked_sub(std::time::Duration::from_millis(age_ms))
                        .unwrap_or(now);
                    bucket.requests.push_back((ts, tokens));
                }
                let res_len = read_u32(data)? as usize;
                for _ in 0..res_len {
                    let res_id = read_u64(data)?;
                    let exp_unix_ms = read_u64(data)?;
                    let est_tokens = read_u64(data)?;
                    if !is_owned || exp_unix_ms <= unix_now {
                        continue;
                    }
                    let rem_ms = (exp_unix_ms - unix_now).min(window_ms);
                    let age_ms = window_ms.saturating_sub(rem_ms);
                    let ts = now
                        .checked_sub(std::time::Duration::from_millis(age_ms))
                        .unwrap_or(now);
                    bucket.reservations.insert(res_id, (ts, est_tokens));
                }
                if is_owned {
                    self.llm_quotas.insert(key, bucket);
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
        entries_read: Option<u64>,
    ) -> Result<(), &'static str> {
        self.table
            .xgroup_create(key, group, id_str, mkstream, entries_read)
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
    ) -> Result<Option<usize>, &'static str> {
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
        claim: Option<u64>,
        max_bytes: usize,
        total_entries: &mut usize,
        total_bytes: &mut usize,
    ) -> Result<
        (
            Vec<(
                crate::table::StreamId,
                Vec<(Bytes, Bytes)>,
                Option<(u64, usize)>,
            )>,
            bool,
        ),
        &'static str,
    > {
        self.table.xreadgroup(
            key,
            group,
            consumer,
            id_str,
            count,
            noack,
            claim,
            max_bytes,
            total_entries,
            total_bytes,
        )
    }

    #[inline]
    pub fn xnack(
        &mut self,
        key: &[u8],
        group: &[u8],
        mode: crate::resp::XnackMode,
        ids: &[crate::table::StreamId],
        retrycount: Option<usize>,
        force: bool,
    ) -> Result<usize, &'static str> {
        self.table.xnack(key, group, mode, ids, retrycount, force)
    }

    #[inline]
    pub fn earliest_claim_wait_ms(
        &mut self,
        key: &[u8],
        group: &[u8],
        min_idle: u64,
    ) -> Option<u64> {
        self.table.earliest_claim_wait_ms(key, group, min_idle)
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
        start: std::ops::Bound<crate::table::StreamId>,
        end: std::ops::Bound<crate::table::StreamId>,
        count: usize,
        consumer: Option<&[u8]>,
        min_idle: Option<u64>,
    ) -> Result<Vec<(crate::table::StreamId, Bytes, i64, usize)>, &'static str> {
        self.table
            .xpending_range(key, group, start, end, count, consumer, min_idle)
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
            let eff_ef = filter_ef.or(ef).or(Some(count.saturating_mul(10).max(64)));
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

    pub fn crdt_incrby(&mut self, key: Bytes, delta: i64) -> Result<i64, &'static str> {
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

    /// Returns the add timestamps the removal tombstoned (empty if `member`
    /// was not in the set).
    pub fn crdt_srem(&mut self, key: &Bytes, member: &Bytes) -> Vec<crate::crdt::HlcTimestamp> {
        self.crdt_store.set_rem(key, member)
    }

    pub fn crdt_dump(&self) -> Vec<u8> {
        self.crdt_store.export_sync_payload()
    }

    pub fn crdt_merge(&mut self, payload: &[u8]) -> Result<usize, String> {
        self.crdt_store.merge_sync_payload(payload)
    }

    /// Prunes tombstones from before `cutoff_ms` (see `GcHorizon::cutoff_ms`).
    pub fn crdt_gc(&mut self, cutoff_ms: u64) -> (usize, usize) {
        self.crdt_store.gc_tombstones(cutoff_ms)
    }
}

/// "Now" for one serialization step: expiries are written as absolute unix
/// milliseconds.
struct RdbClock {
    now: std::time::Instant,
    unix_ms: u64,
    /// Write keys that already expired too, like Redis: a master with a
    /// replication history has not told its replicas about them yet, and
    /// after a restart it propagates their deletion (see
    /// `replication::propagate_expired_on_load`).
    keep_expired: bool,
}

impl RdbClock {
    fn now(port: u16) -> Self {
        RdbClock {
            keep_expired: crate::replication::snapshots_keep_expired(port),
            now: std::time::Instant::now(),
            unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}

/// How long one step of [`save_rdb_chunk_yielding`] may hold the shard.
pub(crate) const RDB_SAVE_STEP: std::time::Duration = std::time::Duration::from_millis(1);

/// [`ShardDb::save_rdb_chunk`] for SAVE/BGSAVE: serializes a few table
/// segments at a time and yields to the shard's other tasks in between, so
/// a big shard does not stall its clients for the whole serialization.
///
/// Like the cross-shard save it is a fuzzy snapshot: each key is written as
/// it is when its segment is reached. Every key that exists for the whole
/// save is written; a key moved to a new segment by a split can be written
/// twice, which loading resolves (the later copy wins). If the table is
/// renumbered mid-way (FLUSHALL, collapsing defrag), the rest of the table
/// is small or empty and is written again in one go.
///
/// Not for full syncs: those arm the replication stream cut right after
/// serializing, which needs the shard not to change in between.
///
/// Output goes to `sink` as `Some(piece)` every ~[`RDB_FLUSH_BYTES`] (and a
/// final tail), which keeps memory bounded and avoids reallocating (copying)
/// one huge buffer on the shard thread. Returns `Ok(false)` if the sink hung
/// up or the save was aborted, and an error if a tiered value can't be read
/// back (the pieces sent so far must then be discarded).
pub async fn save_rdb_chunk_yielding(
    db: &std::cell::RefCell<ShardDb>,
    sink: &flume::Sender<Option<Vec<u8>>>,
) -> Result<bool, TieredReadError> {
    let mut cursor = RdbSaveCursor::new(&db.borrow());
    let mut buf = Vec::with_capacity(RDB_FLUSH_BYTES * 2);
    loop {
        if crate::connection::RDB_BGSAVE_ABORTED.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(false);
        }
        let prev_seg = cursor.seg;
        let done = cursor.step(&db.borrow(), &mut buf, RDB_SAVE_STEP, RDB_FLUSH_BYTES)?;
        if crate::connection::RDB_BGSAVE_ABORTED.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(false);
        }
        if buf.len() >= RDB_FLUSH_BYTES {
            let piece = std::mem::replace(&mut buf, Vec::with_capacity(RDB_FLUSH_BYTES * 2));
            if sink.send_async(Some(piece)).await.is_err() {
                return Ok(false);
            }
        }
        let delay_us =
            crate::connection::RDB_KEY_SAVE_DELAY.load(std::sync::atomic::Ordering::Relaxed);
        if delay_us > 0 {
            let segs_done = cursor.seg.saturating_sub(prev_seg).max(1) as u64;
            let sleep_ms = (delay_us.saturating_mul(segs_done) / 1000).clamp(1, 50);
            monoio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
            if crate::connection::RDB_BGSAVE_ABORTED.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(false);
            }
        }
        if done {
            break;
        }
        // A send that completes at once does not yield; always let the
        // shard's other tasks in between steps.
        crate::mailbox::yield_now().await;
    }
    if crate::connection::RDB_BGSAVE_ABORTED.load(std::sync::atomic::Ordering::SeqCst) {
        return Ok(false);
    }
    Ok(buf.is_empty() || sink.send_async(Some(buf)).await.is_ok())
}

/// Directory patterns (see [`crate::table::RudisFlatTable::segment_pattern`])
/// of the segments a [`RdbSaveCursor`] has written. Keys matching one were
/// already written: Redis and Valkey refuse an RDB with a key twice, so a
/// segment split off a written one (or, after a renumbering, any such key)
/// is skipped.
#[derive(Default)]
struct WrittenPatterns {
    patterns: std::collections::HashSet<(u8, u64)>,
    /// Bit `d` set: some pattern has depth `d`.
    depths: u128,
}

impl WrittenPatterns {
    fn insert(&mut self, depth: u8, bits: u64) {
        self.patterns.insert((depth, bits));
        self.depths |= 1u128 << depth.min(127);
    }

    /// Whether directory bits `bits`, known to depth `max_depth`, match a
    /// written pattern.
    fn covers(&self, max_depth: u8, bits: u64) -> bool {
        (0..=max_depth.min(127)).any(|d| {
            self.depths & (1u128 << d) != 0
                && self
                    .patterns
                    .contains(&(d, bits & crate::table::dir_bits_mask(d)))
        })
    }
}

/// Progress of a [`save_rdb_chunk_yielding`] walk over a shard's table.
struct RdbSaveCursor {
    epoch: u64,
    seg: usize,
    written: WrittenPatterns,
}

impl RdbSaveCursor {
    fn new(db: &ShardDb) -> Self {
        RdbSaveCursor {
            epoch: db.table_for_db(0).map_or(0, |t| t.layout_epoch()),
            seg: 0,
            written: WrittenPatterns::default(),
        }
    }

    /// Serializes at least one segment, then more until `budget` has passed
    /// or `buf` holds `flush_bytes`. Returns true once everything, the
    /// extended state included, is in `buf`; fails if a tiered value can't
    /// be read back.
    fn step(
        &mut self,
        db: &ShardDb,
        buf: &mut Vec<u8>,
        budget: Duration,
        flush_bytes: usize,
    ) -> Result<bool, TieredReadError> {
        let clock = RdbClock::now(db.port);
        let Some(t0) = db.table_for_db(0) else {
            db.save_extended_rdb_chunk(buf);
            db.save_other_dbs_rdb_chunk(&clock, buf)?;
            return Ok(true);
        };
        if t0.layout_epoch() != self.epoch {
            // Renumbered (FLUSHALL, collapsing defrag): what is left is
            // small; write the keys no written segment covered.
            for entry in t0.entries() {
                let bits = crate::table::key_dir_bits(&entry.key);
                if !self.written.covers(u8::MAX, bits) {
                    db.save_rdb_entry(entry, &clock, buf)?;
                }
            }
            db.save_extended_rdb_chunk(buf);
            db.save_other_dbs_rdb_chunk(&clock, buf)?;
            return Ok(true);
        }
        let segments = t0.segment_count();
        let mut first = true;
        while self.seg < segments
            && (first || (clock.now.elapsed() < budget && buf.len() < flush_bytes))
        {
            first = false;
            let pattern = t0.segment_pattern(self.seg);
            if let Some((depth, bits)) = pattern {
                if !self.written.covers(depth, bits) {
                    db.save_rdb_segment_db0(self.seg, &clock, buf)?;
                }
                self.written.insert(depth, bits);
            }
            self.seg += 1;
        }
        if self.seg >= segments {
            db.save_extended_rdb_chunk(buf);
            db.save_other_dbs_rdb_chunk(&clock, buf)?;
            return Ok(true);
        }
        Ok(false)
    }
}

/// A tiered record's value payload read from `tm` (see
/// [`ShardDb::read_tiered_payload`]).
fn read_tiered_payload_from(
    tm: &crate::tiering::ShardTierManager,
    ptr: crate::table::TieredPointer,
) -> Option<Vec<u8>> {
    let (_, raw) = tm.read_ptr_sync(ptr).ok()?;
    (raw.first() == Some(&ptr.value_type)).then_some(raw)
}

/// A tiered value read back from `tm` and decoded (see
/// [`ShardDb::hydrate_tiered`]).
fn hydrate_tiered_from(
    tm: &crate::tiering::ShardTierManager,
    ptr: crate::table::TieredPointer,
) -> Option<crate::table::RudisValue> {
    let raw = read_tiered_payload_from(tm, ptr)?;
    let (val, used) = crate::table::RudisTable::deserialize_val_payload(&raw).ok()?;
    (used == raw.len()).then_some(val)
}

/// An extended structure an AOF image writes on its own (see
/// [`ShardDb::ext_item_keys`]).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum ExtKind {
    Json,
    Bloom,
    Vector,
    Cuckoo,
    Cms,
    Topk,
}

/// A point-in-time RDB image of a shard, built a few table segments at a
/// time between commands: the base of an AOF rewrite (the AOF tail written
/// since its start follows it), like the image Redis's forked child writes.
///
/// Every DB is walked segment by segment, then the larger extended
/// structures (JSON documents, probabilistic structures, vector indexes,
/// [`ExtKind`]) one at a time; the function libraries and the rest of the
/// extended state are taken when it starts. To keep the image as of the
/// start, the first write to a key not written yet first writes the key's
/// unchanged state ([`before_write`]), and the walk skips it later. Writes
/// that are idempotent (SET, DEL, PEXPIREAT...) need no hook: the image
/// then holds their result, which the AOF tail repeats harmlessly.
pub struct AofImage {
    /// One walk per DB that had keys at the start, in DB order.
    walks: Vec<DbWalk>,
    /// The walk in progress.
    cur: usize,
    /// Extended structures not written yet, in order.
    ext_pending: std::collections::VecDeque<(ExtKind, bytes::Bytes)>,
    /// Their keys, for [`before_write`].
    ext_keys: std::collections::HashSet<bytes::Bytes>,
    /// Extended structures written early (by [`before_write`]).
    ext_done: std::collections::HashSet<(ExtKind, bytes::Bytes)>,
    buf: Vec<u8>,
    /// The DB the last `SELECTDB` in the image selected.
    selected_db: usize,
    /// Hash field deadlines of DBs other than 0 (each after its
    /// `SELECTDB`), then the rest of the extended state, as of the start:
    /// written last, as a snapshot writes them (loading a hash after its
    /// field deadlines would drop them).
    deferred_dbs: Vec<u8>,
    deferred_ext: Vec<u8>,
    done: bool,
    /// The shard's port (for its snapshot clock) and tier, to read spilled
    /// values back where only a table is at hand ([`before_write_table`]).
    port: u16,
    tier: Option<std::rc::Rc<crate::tiering::ShardTierManager>>,
    /// Why the image can no longer be trusted (e.g. SWAPDB, a spilled value
    /// that can't be read back): the rewrite then falls back to a blocking
    /// pass.
    poisoned: Option<String>,
    /// A shard's part of a multi-shard RDB (a full sync): no file header
    /// and no `OP_EOF`, like [`ShardDb::save_rdb_chunk`]'s output.
    chunk: bool,
    /// A buffer handed back once its piece was sent (see
    /// [`Self::give_buffer`]), reused for the next piece.
    spare: Option<Vec<u8>>,
    /// This full-sync image's share of [`SYNC_IMAGE_BYTES`].
    held: SyncImageHeld,
}

/// Bytes of replication buffers: the shards' full-sync images (their
/// buffers, pre-images included), pieces in flight to a replica, and
/// replicas' output buffers. Like Redis's replication buffers, they don't
/// count toward maxmemory: [`crate::tiering::TieringStats::real_used_total`] leaves them
/// out, so a replica attaching to a master at its limit doesn't make it
/// evict.
pub static SYNC_IMAGE_BYTES: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

/// A share of [`SYNC_IMAGE_BYTES`] (an image's buffers, or a piece in
/// flight to the replica), given back when dropped.
#[derive(Default)]
pub(crate) struct SyncImageHeld(i64);

impl SyncImageHeld {
    pub(crate) fn set(&mut self, bytes: i64) {
        // Usually unchanged (a replica's buffer between growths): skip the
        // write to the counter every shard shares.
        if bytes != self.0 {
            SYNC_IMAGE_BYTES.fetch_add(bytes - self.0, std::sync::atomic::Ordering::Relaxed);
            self.0 = bytes;
        }
    }
}

impl Drop for SyncImageHeld {
    fn drop(&mut self) {
        self.set(0);
    }
}

/// Initial buffer of a full-sync image: about one piece
/// (`SYNC_IMAGE_PIECE_BYTES`) rather than the 8 MB a rewrite image starts
/// with, since a master holds one per shard while a sync runs.
const SYNC_IMAGE_BUF: usize = (1 << 20) + (256 << 10);

/// An [`AofImage`]'s walk over one DB's table (see [`RdbSaveCursor`]).
struct DbWalk {
    db_idx: usize,
    epoch: u64,
    seg: usize,
    written: WrittenPatterns,
    /// Keys written out of order (by [`before_write`]): skipped.
    skip: std::collections::HashSet<bytes::Bytes>,
    done: bool,
}

impl AofImage {
    /// Starts an image of `db` as of now: the RDB header (with the function
    /// libraries) and what is written last (see [`Self::finish`]).
    pub fn start(db: &ShardDb) -> Result<Self, TieredReadError> {
        Self::start_inner(db, false)
    }

    /// [`Self::start`] for one shard's part of a multi-shard RDB: it starts
    /// and ends with DB 0 selected, and the caller writes the file header,
    /// `OP_EOF` and checksum around the parts.
    pub fn start_chunk(db: &ShardDb) -> Result<Self, TieredReadError> {
        Self::start_inner(db, true)
    }

    fn start_inner(db: &ShardDb, chunk: bool) -> Result<Self, TieredReadError> {
        let mut buf = Vec::with_capacity(if chunk {
            SYNC_IMAGE_BUF
        } else {
            RDB_FLUSH_BYTES * 2
        });
        if !chunk {
            crate::redis_rdb::write_file_header(&mut buf, db.total_used_memory() as u64);
        }
        let clock = RdbClock::now(db.port);
        let mut walks = Vec::new();
        let mut deferred_dbs = Vec::new();
        for db_idx in 0..db.max_db_len() {
            let Some(table) = db.table_for_db(db_idx) else {
                continue;
            };
            if !table.is_empty() {
                walks.push(DbWalk {
                    db_idx,
                    epoch: table.layout_epoch(),
                    seg: 0,
                    written: WrittenPatterns::default(),
                    skip: Default::default(),
                    done: false,
                });
            }
            if db_idx != 0 && !table.hash_field_expires.is_empty() {
                crate::redis_rdb::write_selectdb(&mut deferred_dbs, db_idx);
                let mut records = Vec::new();
                ShardDb::write_hash_field_expires_legacy(
                    &table.hash_field_expires,
                    &clock,
                    &mut records,
                );
                crate::redis_rdb::write_ext_aux(&mut deferred_dbs, &records);
            }
        }
        let mut records = Vec::new();
        db.save_extended_small_records(&mut records);
        let mut deferred_ext = Vec::new();
        crate::redis_rdb::write_ext_aux(&mut deferred_ext, &records);
        let ext_pending: std::collections::VecDeque<_> = db.ext_item_keys().into();
        let ext_keys = ext_pending.iter().map(|(_, k)| k.clone()).collect();
        Ok(AofImage {
            walks,
            cur: 0,
            ext_pending,
            ext_keys,
            ext_done: Default::default(),
            buf,
            selected_db: 0,
            deferred_dbs,
            deferred_ext,
            done: false,
            port: db.port,
            tier: db.tier_manager.clone(),
            poisoned: None,
            chunk,
            spare: None,
            held: SyncImageHeld::default(),
        })
        .map(|mut img| {
            img.report_held();
            img
        })
    }

    /// Updates a full-sync image's share of [`SYNC_IMAGE_BYTES`].
    fn report_held(&mut self) {
        if self.chunk {
            let bytes = self.buf.capacity() + self.spare.as_ref().map_or(0, Vec::capacity);
            self.held.set(bytes as i64);
        }
    }

    /// Hands back the buffer of a piece [`Self::step`] returned, once sent,
    /// to hold the next piece: a full sync's pieces are sent from another
    /// thread, and freeing them there skews the per-thread memory counts
    /// eviction reads.
    pub fn give_buffer(&mut self, buf: Vec<u8>) {
        if buf.capacity() > 0 {
            self.spare = Some(buf);
            self.report_held();
        }
    }

    fn select(&mut self, db_idx: usize) {
        if self.selected_db != db_idx {
            crate::redis_rdb::write_selectdb(&mut self.buf, db_idx);
            self.selected_db = db_idx;
        }
    }

    /// Writes more of the image (at least one segment or structure, then
    /// until `budget` has passed or `flush_bytes` are buffered). Returns the
    /// bytes to write out once that many are buffered, and whether all but
    /// [`Self::finish`]'s part is written.
    pub fn step(
        &mut self,
        db: &ShardDb,
        budget: Duration,
        flush_bytes: usize,
    ) -> Result<(Option<Vec<u8>>, bool), String> {
        if let Some(why) = &self.poisoned {
            return Err(why.clone());
        }
        let clock = RdbClock::now(db.port);
        let mut first = true;
        while !self.done
            && (first || (clock.now.elapsed() < budget && self.buf.len() < flush_bytes))
        {
            first = false;
            if self.cur < self.walks.len() {
                // No deadline for an unbounded budget (a one-pass rewrite).
                let deadline = clock.now.checked_add(budget);
                self.step_walk(db, &clock, deadline)
                    .map_err(|e| e.to_string())?;
            } else if let Some((kind, key)) = self.ext_pending.pop_front() {
                if !self.ext_done.remove(&(kind, key.clone())) {
                    let mut records = Vec::new();
                    db.save_ext_item(kind, &key, &mut records);
                    if !records.is_empty() {
                        crate::redis_rdb::write_ext_aux(&mut self.buf, &records);
                    }
                }
            } else {
                self.done = true;
            }
        }
        let piece = (self.buf.len() >= flush_bytes).then(|| {
            let next = match self.spare.take() {
                Some(mut b) => {
                    b.clear();
                    b
                }
                None if self.chunk => Vec::with_capacity(SYNC_IMAGE_BUF),
                None => Vec::with_capacity(RDB_FLUSH_BYTES * 2),
            };
            std::mem::replace(&mut self.buf, next)
        });
        self.report_held();
        Ok((piece, self.done))
    }

    /// Writes one segment of the current DB walk (or, if its table was
    /// renumbered, all its keys not written yet).
    fn step_walk(
        &mut self,
        db: &ShardDb,
        clock: &RdbClock,
        deadline: Option<std::time::Instant>,
    ) -> Result<(), TieredReadError> {
        let w = &self.walks[self.cur];
        let db_idx = w.db_idx;
        let Some(table) = db.table_for_db(db_idx) else {
            self.walks[self.cur].done = true;
            self.cur += 1;
            return Ok(());
        };
        self.select(db_idx);
        let w = &mut self.walks[self.cur];
        if table.layout_epoch() != w.epoch {
            // Renumbered (FLUSHDB / FLUSHALL, collapsing defrag): what is
            // left is small; write the keys no written segment covered.
            for entry in table.entries() {
                let bits = crate::table::key_dir_bits(&entry.key);
                if !w.written.covers(u8::MAX, bits) && !w.skip.contains(&entry.key[..]) {
                    db.save_rdb_entry(entry, clock, &mut self.buf)?;
                }
            }
            w.done = true;
        } else if w.seg < table.segment_count() {
            if let Some((depth, bits)) = table.segment_pattern(w.seg) {
                let mut written = 0usize;
                let mut paused = false;
                if !w.written.covers(depth, bits) {
                    for entry in table.segment_entries(w.seg) {
                        if w.skip.contains(&entry.key[..]) {
                            continue;
                        }
                        // A spilled value is read back from disk, on this
                        // thread: past the step's budget, stop at one and
                        // finish the segment in a later step.
                        if written > 0
                            && matches!(entry.val, crate::table::RudisValue::Tiered(_))
                            && deadline.is_some_and(|d| std::time::Instant::now() >= d)
                        {
                            paused = true;
                            break;
                        }
                        db.save_rdb_entry(entry, clock, &mut self.buf)?;
                        written += 1;
                    }
                }
                if paused {
                    // What this step wrote counts as written out of order
                    // (the segment is not covered yet). Nothing changed
                    // since, so the same iteration yields the same keys.
                    let done: Vec<bytes::Bytes> = table
                        .segment_entries(w.seg)
                        .filter(|e| !w.skip.contains(&e.key[..]))
                        .take(written)
                        .map(|e| bytes::Bytes::copy_from_slice(&e.key))
                        .collect();
                    w.skip.extend(done);
                    return Ok(());
                }
                w.written.insert(depth, bits);
            }
            w.seg += 1;
        } else {
            w.done = true;
        }
        if w.done {
            self.cur += 1;
        }
        Ok(())
    }

    /// The rest of the image once [`Self::step`] reported it done: what is
    /// still buffered, the deferred part, then `OP_EOF` (the writer adds
    /// the CRC64).
    pub fn finish(mut self) -> Result<Vec<u8>, String> {
        if let Some(why) = self.poisoned {
            return Err(why);
        }
        debug_assert!(self.done);
        if !self.deferred_dbs.is_empty() {
            self.buf.append(&mut self.deferred_dbs);
            self.selected_db = usize::MAX;
        }
        self.select(0);
        self.buf.append(&mut self.deferred_ext);
        if !self.chunk {
            self.buf.push(crate::redis_rdb::OP_EOF);
        }
        Ok(self.buf)
    }

    /// Before `key` in DB `db_idx` changes: writes its current state if
    /// the image does not hold it yet, and makes the walk skip it.
    fn cover_key(
        &mut self,
        db_idx: usize,
        table: &crate::table::RudisTable,
        key: &[u8],
        save: impl FnOnce(&crate::table::RudisEntry, &mut Vec<u8>) -> Result<(), TieredReadError>,
    ) {
        if self.poisoned.is_some() {
            return;
        }
        let Some(wi) = self.walks.iter().position(|w| w.db_idx == db_idx) else {
            // The DB was empty at the start: its keys are all newer.
            return;
        };
        let w = &self.walks[wi];
        if w.done || table.layout_epoch() != w.epoch {
            // A renumbered table (FLUSHDB) is finished by the walk; the AOF
            // tail holds the flush.
            return;
        }
        if w.written.covers(u8::MAX, crate::table::key_dir_bits(key)) || w.skip.contains(key) {
            return;
        }
        if let Some(entry) = table.peek_entry(key) {
            self.select(db_idx);
            if let Err(e) = save(entry, &mut self.buf) {
                self.poisoned = Some(e.to_string());
                return;
            }
        }
        // Also when absent: a key created from now on is not in the image.
        self.walks[wi]
            .skip
            .insert(bytes::Bytes::copy_from_slice(key));
    }

    /// Before extended structures named `key` change: writes those not
    /// written yet.
    fn cover_ext(&mut self, db: &ShardDb, key: &[u8]) {
        if !self.ext_keys.remove(key) {
            return;
        }
        let mut records = Vec::new();
        for (kind, k) in &self.ext_pending {
            if k.as_ref() == key && self.ext_done.insert((*kind, k.clone())) {
                db.save_ext_item(*kind, key, &mut records);
            }
        }
        if !records.is_empty() {
            crate::redis_rdb::write_ext_aux(&mut self.buf, &records);
        }
    }
}

/// What a shard's point-in-time [`AofImage`] is for. A shard can build
/// several at once (an AOF rewrite and full syncs to replicas); the write
/// hooks keep every one of them point-in-time.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ImageOwner {
    /// The live AOF rewrite.
    AofRewrite,
    /// The full sync to the replica with this client id.
    FullSync(u64),
}

thread_local! {
    /// The images this shard thread is building.
    static IMAGES: std::cell::RefCell<Vec<(ImageOwner, AofImage)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static IMAGES_ACTIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Installs `image` for `owner` (replacing any it had), which
/// [`before_write`] keeps point-in-time.
pub fn install_image(owner: ImageOwner, image: AofImage) {
    IMAGES.with(|i| {
        let mut images = i.borrow_mut();
        images.retain(|(o, _)| *o != owner);
        images.push((owner, image));
    });
    IMAGES_ACTIVE.set(true);
}

/// Removes `owner`'s image (when done or abandoned).
pub fn take_image(owner: ImageOwner) -> Option<AofImage> {
    IMAGES.with(|i| {
        let mut images = i.borrow_mut();
        let pos = images.iter().position(|(o, _)| *o == owner)?;
        let (_, image) = images.remove(pos);
        IMAGES_ACTIVE.set(!images.is_empty());
        Some(image)
    })
}

/// Runs `f` on `owner`'s image.
pub fn with_image<R>(owner: ImageOwner, f: impl FnOnce(&mut AofImage) -> R) -> Option<R> {
    IMAGES.with(|i| {
        i.borrow_mut()
            .iter_mut()
            .find(|(o, _)| *o == owner)
            .map(|(_, img)| f(img))
    })
}

/// Runs `f` on every image this thread is building.
fn for_each_image(mut f: impl FnMut(&mut AofImage)) {
    IMAGES.with(|i| {
        for (_, img) in i.borrow_mut().iter_mut() {
            f(img);
        }
    });
}

/// Installs `image` as this shard thread's live AOF rewrite image.
pub fn install_aof_image(image: AofImage) {
    install_image(ImageOwner::AofRewrite, image);
}

/// Removes this thread's live AOF rewrite image (when done or abandoned).
pub fn take_aof_image() -> Option<AofImage> {
    take_image(ImageOwner::AofRewrite)
}

/// Runs `f` on this thread's live AOF rewrite image.
pub fn with_aof_image<R>(f: impl FnOnce(&mut AofImage) -> R) -> Option<R> {
    with_image(ImageOwner::AofRewrite, f)
}

/// Whether any image (an AOF rewrite's or a full sync's) is being built on
/// this thread: the write hooks are a single flag test otherwise.
#[inline(always)]
pub fn aof_image_active() -> bool {
    IMAGES_ACTIVE.get()
}

/// Makes every image being built unusable: its rewrite or full sync falls
/// back to a blocking pass. For changes an image cannot follow (SWAPDB, a
/// full resync replacing the dataset).
pub fn poison_aof_image(why: &str) {
    if aof_image_active() {
        for_each_image(|img| {
            img.poisoned.get_or_insert_with(|| why.to_string());
        });
    }
}

/// Starts this shard's part of a streamed full sync to replica `replica`:
/// a point-in-time chunk image (see [`AofImage::start_chunk`]), and the
/// replica's stream cut armed right then, so the stream it gets after the
/// snapshot starts exactly where the image stands.
pub fn start_sync_image(
    db: &ShardDb,
    shard_id: usize,
    replica: u64,
) -> Result<(), TieredReadError> {
    install_image(ImageOwner::FullSync(replica), AofImage::start_chunk(db)?);
    crate::replication::get_replication_hub(db.port).arm_full_sync(replica, shard_id);
    Ok(())
}

/// Bytes a full-sync image step hands out at most (it may hand out less,
/// or nothing, when its time budget runs out first).
const SYNC_IMAGE_PIECE_BYTES: usize = 1 << 20;

/// Writes about [`RDB_SAVE_STEP`] more of replica `replica`'s image.
/// Returns the bytes to send and whether this shard's part is complete (the
/// image is then removed). An error (the image was poisoned, or a spilled
/// value could not be read) removes it too: the sync must start over.
pub fn sync_image_step(
    db: &ShardDb,
    replica: u64,
    recycle: Option<Vec<u8>>,
) -> Result<(Vec<u8>, bool), String> {
    let owner = ImageOwner::FullSync(replica);
    let step = with_image(owner, |img| {
        if let Some(buf) = recycle {
            img.give_buffer(buf);
        }
        img.step(db, RDB_SAVE_STEP, SYNC_IMAGE_PIECE_BYTES)
    })
    .ok_or_else(|| "the full-sync image is gone".to_string())?;
    let (piece, done) = match step {
        Ok(step) => step,
        Err(e) => {
            take_image(owner);
            return Err(e);
        }
    };
    let mut out = piece.unwrap_or_default();
    if done {
        let image = take_image(owner).expect("present above");
        out.extend(image.finish()?);
    }
    Ok((out, done))
}

/// After a full-sync image grew, shrank or went away on this shard:
/// eviction sums the shards' published allocator counts and subtracts
/// `SYNC_IMAGE_BYTES`, which changes at once. Until this shard published
/// again, freed image buffers left `SYNC_IMAGE_BYTES` but still sat in its
/// published count (and new ones the other way round), so the total read
/// up to ~1.25 MB per shard high and writes evicted (on a busy host, where
/// the next publish came later).
pub fn publish_after_image_change(router: &crate::router::Router) {
    router.publish_memory_state();
}

/// [`sync_image_step`] on `router`'s shard, publishing its memory state
/// with the change (see [`publish_after_image_change`]).
pub fn sync_image_step_published(
    router: &crate::router::Router,
    replica: u64,
    recycle: Option<Vec<u8>>,
) -> Result<(Vec<u8>, bool), String> {
    let step = sync_image_step(&router.local_db.borrow(), replica, recycle);
    publish_after_image_change(router);
    step
}

/// Call before a command may change `key` in the current DB (see
/// [`AofImage`]). A single flag test when no AOF rewrite is running.
#[inline(always)]
pub fn before_write(db: &ShardDb, key: &[u8]) {
    if aof_image_active() {
        before_write_slow(db, key);
    }
}

#[cold]
fn before_write_slow(db: &ShardDb, key: &[u8]) {
    before_write_in(db, crate::connection::CURRENT_DB.get(), key);
}

/// [`before_write`] for `key` in DB `db_idx` (MOVE and COPY ... DB write
/// to another DB than the current one).
pub fn before_write_in(db: &ShardDb, db_idx: usize, key: &[u8]) {
    if !aof_image_active() {
        return;
    }
    let clock = RdbClock::now(db.port);
    for_each_image(|img| {
        if let Some(table) = db.table_for_db(db_idx) {
            img.cover_key(db_idx, table, key, |entry, buf| {
                db.save_rdb_entry(entry, &clock, buf)
            });
        }
        img.cover_ext(db, key);
        img.report_held();
    });
}

/// [`before_write`] where only DB `db_idx`'s table is at hand (serving a
/// blocked client); spilled values are read back through the image's own
/// handle on the shard's tier.
pub fn before_write_table(table: &crate::table::RudisTable, db_idx: usize, key: &[u8]) {
    if !aof_image_active() {
        return;
    }
    for_each_image(|img| {
        let clock = RdbClock::now(img.port);
        let tier = img.tier.clone();
        img.cover_key(db_idx, table, key, |entry, buf| {
            ShardDb::save_rdb_entry_with(entry, &clock, buf, |ptr| {
                hydrate_tiered_from(tier.as_deref()?, ptr)
            })
        });
        img.report_held();
    });
}

/// Piece size [`save_rdb_chunk_yielding`] streams to its sink.
const RDB_FLUSH_BYTES: usize = 4 << 20;

#[cfg(test)]
mod tests {
    use super::*;

    /// Replies of packed values come from a shared slab, but each is its
    /// own, unchanged by later replies (also across a slab refill), and
    /// equals the value's RESP encoding.
    #[test]
    fn test_packed_value_replies_from_the_slab_stay_intact() {
        let mut replies = Vec::new();
        let mut want = Vec::new();
        // ~1.3 MB of replies: several slabs.
        for i in 0..6000usize {
            let n = crate::compact::HEAP_PACK_MIN + i % 200;
            let v: Vec<u8> = (0..n).map(|j| b'a' + ((i + j) % 26) as u8).collect();
            let s = crate::compact::CompactStr::new(&v);
            if !s.is_heap_packed() {
                continue;
            }
            let mut enc = Vec::new();
            s.write_resp_bulk(&mut enc);
            replies.push(CompactResp::from_compact_str(&s));
            want.push(enc);
        }
        assert!(replies.len() > 2000, "{} packed", replies.len());
        for (r, w) in replies.iter().zip(&want) {
            assert_eq!(r.as_slice(), &w[..]);
            let mut out = Vec::new();
            r.write_to(&mut out);
            assert_eq!(out, *w);
        }
        assert_eq!(replies[17].clone().into_vec(), want[17]);
    }

    /// With spilled values, an AOF image's walk pauses inside a segment once
    /// its step's budget is spent (each spilled value is a disk read on the
    /// shard thread; a whole segment of them stalled the shard), and the
    /// blocked-client hook reads a spilled value back itself instead of
    /// giving up on the image. The image still holds every key once.
    #[test]
    fn test_aof_image_with_spilled_values() {
        let dir = std::env::temp_dir().join(format!("rudis-img-tier-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut db = ShardDb::new(0);
            let tm = std::rc::Rc::new(
                crate::tiering::ShardTierManager::open(0, 55561, &dir.join("tier"))
                    .await
                    .unwrap(),
            );
            db.tier_manager = Some(tm.clone());
            let n = 3000;
            for i in 0..n {
                let key = Bytes::from(format!("l{i}"));
                db.rpush(key.clone(), vec![Bytes::from(format!("v{i}"))])
                    .unwrap();
                let (payload, val_type) = db.table.get_value_for_spill(&key).unwrap();
                let ptr = tm.stash_record(&key, &payload, val_type).await.unwrap();
                assert!(db.table.set_tiered_pointer(&key, ptr));
            }
            let segments = db.table.segment_count();
            install_aof_image(AofImage::start(&db).unwrap());
            // A blocked client is served on a spilled key: written as is.
            before_write_table(&db.table, 0, b"l7");
            let mut image = Vec::new();
            let mut steps = 0;
            loop {
                let (piece, done) = with_aof_image(|img| img.step(&db, Duration::ZERO, 1 << 30))
                    .unwrap()
                    .unwrap();
                image.extend(piece.unwrap_or_default());
                steps += 1;
                if done {
                    break;
                }
            }
            assert!(
                steps > segments * 2,
                "{steps} steps for {segments} segments: no pause inside a segment"
            );
            image.extend(take_aof_image().unwrap().finish().unwrap());
            let crc = crate::table::crc64(&image);
            image.extend_from_slice(&crc.to_le_bytes());
            let mut loaded = ShardDb::new(0);
            crate::table::load_rdb_bytes(&image, &mut loaded, 0, 1).unwrap();
            assert_eq!(loaded.table.len(), n);
            for i in [0, 7, 1234, n - 1] {
                assert_eq!(
                    loaded.lrange(format!("l{i}").as_bytes(), 0, -1).unwrap(),
                    vec![Bytes::from(format!("v{i}"))]
                );
            }
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A full-sync image's buffers are counted in `SYNC_IMAGE_BYTES` (which
    /// eviction leaves out) while it exists, are about one piece rather than
    /// the 8 MB a rewrite image starts with, are reused once handed back,
    /// and stop counting when the image is done. They counted toward
    /// maxmemory before, so a replica attaching to a master at its limit
    /// made it evict.
    #[test]
    fn test_sync_image_bytes_are_counted_apart() {
        use std::sync::atomic::Ordering::Relaxed;
        let mut db = ShardDb::new(0);
        for i in 0..20_000 {
            db.rpush(
                Bytes::from(format!("l{i}")),
                vec![Bytes::from(vec![b'x'; 300])],
            )
            .unwrap();
        }
        let base = SYNC_IMAGE_BYTES.load(Relaxed);
        start_sync_image(&db, 0, 78).unwrap();
        let held = SYNC_IMAGE_BYTES.load(Relaxed) - base;
        assert!(
            held > 0 && held < 2 << 20,
            "a new sync image holds {held} bytes"
        );
        let mut recycle: Option<Vec<u8>> = None;
        let mut handed_back = Vec::new();
        let (mut pieces, mut in_reused) = (0, 0);
        loop {
            if let Some(b) = &recycle {
                handed_back.push(b.as_ptr() as usize);
            }
            let (piece, done) = sync_image_step(&db, 78, recycle.take()).unwrap();
            if done {
                break;
            }
            assert!(SYNC_IMAGE_BYTES.load(Relaxed) - base < 4 << 20);
            if piece.is_empty() {
                continue;
            }
            pieces += 1;
            if handed_back.contains(&(piece.as_ptr() as usize)) {
                in_reused += 1;
            }
            recycle = Some(piece);
        }
        assert!(pieces >= 4, "only {pieces} pieces");
        // The first two pieces use the image's own buffers; every later one
        // is built in a buffer handed back.
        assert_eq!(
            in_reused,
            pieces - 2,
            "{in_reused} of {pieces} pieces reused a buffer"
        );
        assert_eq!(
            SYNC_IMAGE_BYTES.load(Relaxed),
            base,
            "a finished image still counts"
        );
    }

    /// An AOF rewrite image and a full sync's chunk image are built at
    /// once on one shard, with writes in between: each stays as of its own
    /// start (the write hooks cover both), and the chunk wrapped in a file
    /// header and `OP_EOF` loads. One image per thread was all the hooks
    /// kept before, so a streamed full sync could not run.
    #[test]
    fn test_concurrent_images_each_stay_point_in_time() {
        let mut db = ShardDb::new(0);
        let key = |i: usize| Bytes::from(format!("l{i}"));
        let n = 2000;
        for i in 0..n {
            db.rpush(key(i), vec![Bytes::from_static(b"a")]).unwrap();
        }
        install_aof_image(AofImage::start(&db).unwrap());
        // Pushed after the rewrite image started, before the sync's.
        before_write(&db, b"l1");
        db.rpush(key(1), vec![Bytes::from_static(b"b")]).unwrap();
        start_sync_image(&db, 0, 99).unwrap();
        // After both started: in neither image.
        for i in [1, 5, n - 1] {
            before_write(&db, format!("l{i}").as_bytes());
            db.rpush(key(i), vec![Bytes::from_static(b"c")]).unwrap();
        }
        let mut chunk = Vec::new();
        let mut rewrite = Vec::new();
        let (mut sync_done, mut rewrite_done) = (false, false);
        let mut round = 0;
        while !(sync_done && rewrite_done) {
            if !sync_done {
                let (piece, done) = sync_image_step(&db, 99, None).unwrap();
                chunk.extend(piece);
                sync_done = done;
            }
            if !rewrite_done {
                let (piece, done) = with_aof_image(|img| img.step(&db, Duration::ZERO, 1 << 30))
                    .unwrap()
                    .unwrap();
                rewrite.extend(piece.unwrap_or_default());
                rewrite_done = done;
            }
            // More writes mid-walk.
            round += 1;
            let i = (round * 37) % n;
            before_write(&db, format!("l{i}").as_bytes());
            db.rpush(key(i), vec![Bytes::from_static(b"d")]).unwrap();
        }
        rewrite.extend(take_aof_image().unwrap().finish().unwrap());
        assert!(!aof_image_active(), "the sync image is removed when done");
        let mut full = Vec::new();
        crate::redis_rdb::write_file_header(&mut full, 0);
        full.extend(chunk);
        full.push(crate::redis_rdb::OP_EOF);
        let load = |mut image: Vec<u8>| {
            let crc = crate::table::crc64(&image);
            image.extend_from_slice(&crc.to_le_bytes());
            let mut loaded = ShardDb::new(0);
            crate::table::load_rdb_bytes(&image, &mut loaded, 0, 1).unwrap();
            assert_eq!(loaded.table.len(), n);
            loaded
        };
        let (from_sync, from_rewrite) = (load(full), load(rewrite));
        let list =
            |db: &mut ShardDb, i: usize| db.lrange(format!("l{i}").as_bytes(), 0, -1).unwrap();
        let (a, b) = (Bytes::from_static(b"a"), Bytes::from_static(b"b"));
        let (mut from_sync, mut from_rewrite) = (from_sync, from_rewrite);
        for i in (0..n).step_by(97).chain([1, 5, n - 1]) {
            let want_rewrite = vec![a.clone()];
            let want_sync = if i == 1 {
                vec![a.clone(), b.clone()]
            } else {
                vec![a.clone()]
            };
            assert_eq!(
                list(&mut from_rewrite, i),
                want_rewrite,
                "rewrite image, l{i}"
            );
            assert_eq!(list(&mut from_sync, i), want_sync, "sync image, l{i}");
        }
    }

    #[test]
    fn test_compact_resp_prefetch_leaves_replies_unchanged() {
        let sizes = [0usize, 1, 21, 63, 64, 65, 1024, 4095, 4096, 4097, 100_000];
        for n in sizes {
            let data: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
            let b = Bytes::from(data.clone());
            let variants = [
                CompactResp::Bulk(b.clone()),
                CompactResp::Array1Bulk(b.clone()),
                CompactResp::RawBytes(b.clone()),
                CompactResp::from_vec(data.clone()),
                CompactResp::from_bulk(&b),
            ];
            for r in variants {
                let mut before = Vec::new();
                r.write_to(&mut before);
                r.prefetch();
                let mut after = Vec::new();
                r.write_to(&mut after);
                assert_eq!(before, after, "size {n}");
            }
        }
        // Small and empty replies have nothing to prefetch.
        CompactResp::empty().prefetch();
        CompactResp::NULL.prefetch();
        // A slice of a larger buffer only touches its own range.
        let big = Bytes::from(vec![7u8; 10_000]);
        CompactResp::Bulk(big.slice(9_990..)).prefetch();
    }

    /// Keys of a `save_rdb_chunk` record stream, in order.
    fn chunk_keys(chunk: &[u8]) -> Vec<Bytes> {
        use crate::redis_rdb::{OP_AUX, OP_EXPIRETIME_MS, Reader, skip_value};
        let mut r = Reader::new(chunk);
        let mut keys = Vec::new();
        while !r.is_empty() {
            match r.u8().unwrap() {
                OP_AUX => {
                    r.skip_string().unwrap();
                    r.skip_string().unwrap();
                }
                OP_EXPIRETIME_MS => {
                    r.take(8).unwrap();
                }
                t => {
                    keys.push(r.bytes().unwrap());
                    skip_value(t, &mut r).unwrap();
                }
            }
        }
        keys
    }

    /// A maxmemory eviction is logged as `DEL key` (AOF and replication
    /// share `log_shard_mutation`), under a `SELECT` of the DB the key was
    /// evicted from, even when that is not the selected DB; otherwise an AOF
    /// replay or a replica would resurrect the key.
    #[test]
    fn eviction_logs_del_in_the_evicted_keys_db() {
        let mut db = ShardDb::new(0);
        db.select_db(3);
        db.table.set(
            Bytes::from_static(b"victim"),
            Bytes::from_static(b"v"),
            None,
        );
        db.select_db(0);
        let prev_tl = crate::connection::CURRENT_DB.get();
        let aof = std::cell::RefCell::new(crate::aof::AofWriter::new_in_memory());

        assert!(db.try_evict_one_key("allkeys-lru", Some(&aof)).is_some());
        assert_eq!(
            aof.borrow().buffer(),
            b"*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n*2\r\n$3\r\nDEL\r\n$6\r\nvictim\r\n"
        );
        assert_eq!(db.current_db, 0, "selected DB restored");
        assert_eq!(crate::connection::CURRENT_DB.get(), prev_tl);
        db.select_db(3);
        assert!(db.table.is_empty());
        db.select_db(0);

        // A key of the selected DB: DEL goes back to DB 0.
        db.table
            .set(Bytes::from_static(b"k0"), Bytes::from_static(b"v"), None);
        assert!(db.try_evict_one_key("allkeys-random", Some(&aof)).is_some());
        assert!(
            aof.borrow()
                .buffer()
                .ends_with(b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*2\r\n$3\r\nDEL\r\n$2\r\nk0\r\n")
        );
        assert!(db.try_evict_one_key("allkeys-random", Some(&aof)).is_none());
    }

    /// Segments split while a yielding save runs must not get their keys
    /// written twice: Redis and Valkey refuse an RDB with a duplicate key.
    #[test]
    fn yielding_save_writes_each_key_once_across_splits() {
        let mut db = ShardDb::new(0);
        for i in 0..6000 {
            db.table
                .set(Bytes::from(format!("k{i}")), Bytes::from_static(b"v"), None);
        }
        let mut cursor = RdbSaveCursor::new(&db);
        let mut buf = Vec::new();
        let mut next = 6000;
        let mut steps = 0;
        while !cursor
            .step(&db, &mut buf, Duration::ZERO, usize::MAX)
            .unwrap()
        {
            steps += 1;
            // Splits while the walk is under way, then let it finish.
            let grow = if steps <= 6 { 1500 } else { 0 };
            for _ in 0..grow {
                db.table.set(
                    Bytes::from(format!("k{next}")),
                    Bytes::from_static(b"v"),
                    None,
                );
                next += 1;
            }
        }
        assert!(steps > 2, "the table has several segments");
        let keys = chunk_keys(&buf);
        let unique: std::collections::HashSet<&Bytes> = keys.iter().collect();
        assert_eq!(unique.len(), keys.len(), "a key was written twice");
        for i in 0..6000 {
            assert!(
                unique.contains(&Bytes::from(format!("k{i}"))),
                "k{i} missing"
            );
        }
    }

    /// After a renumbering, keys of already written segments are recognised
    /// by their hash bits.
    #[test]
    fn written_patterns_cover_every_key_of_written_segments() {
        let mut db = ShardDb::new(0);
        for i in 0..5000 {
            db.table
                .set(Bytes::from(format!("k{i}")), Bytes::from_static(b"v"), None);
        }
        let mut written = WrittenPatterns::default();
        let half = db.table.segment_count() / 2;
        for seg in 0..half {
            let (d, bits) = db.table.segment_pattern(seg).unwrap();
            written.insert(d, bits);
        }
        for seg in 0..db.table.segment_count() {
            for e in db.table.segment_entries(seg) {
                let covered = written.covers(u8::MAX, crate::table::key_dir_bits(&e.key));
                assert_eq!(covered, seg < half);
            }
        }
    }

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
        db.crdt_incrby(Bytes::from_static(b"crdt:cnt"), 99).unwrap();

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
        db.save_rdb_chunk(&mut chunk).unwrap();
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

    /// Moves `key` to the tier the way `Router::spill_local` does.
    async fn spill_for_test(db: &mut ShardDb, key: &[u8]) {
        let (payload, val_type) = db.table.get_value_for_spill(key).unwrap();
        let tm = db.tier_manager.clone().unwrap();
        let ptr = tm
            .stash_record(&Bytes::copy_from_slice(key), &payload, val_type)
            .await
            .unwrap();
        assert!(db.table.set_tiered_pointer(key, ptr));
    }

    #[test]
    fn test_dump_of_tiered_key_restores_its_value() {
        let dir = std::env::temp_dir().join(format!("rudis_tier_dump_{}", std::process::id()));
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut db = ShardDb::new(0);
            db.tier_manager = Some(std::rc::Rc::new(
                crate::tiering::ShardTierManager::open(0, 55558, &dir)
                    .await
                    .unwrap(),
            ));
            db.hset(
                Bytes::from_static(b"h"),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            )
            .unwrap();
            spill_for_test(&mut db, b"h").await;

            let payload = db.dump(b"h").expect("tiered key dumps");
            let mut other = ShardDb::new(0);
            other
                .restore(Bytes::from_static(b"h2"), 0, &payload, false, false)
                .unwrap();
            assert_eq!(
                other.hget(b"h2", b"f").unwrap(),
                Some(Bytes::from_static(b"v"))
            );
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_rdb_save_keeps_tiered_values_and_types() {
        let dir = std::env::temp_dir().join(format!("rudis_tier_rdb_{}", std::process::id()));
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut db = ShardDb::new(0);
            db.tier_manager = Some(std::rc::Rc::new(
                crate::tiering::ShardTierManager::open(0, 55556, &dir)
                    .await
                    .unwrap(),
            ));
            db.set(
                Bytes::from_static(b"s"),
                Bytes::from_static(b"plain-value"),
                Some(Duration::from_secs(1000)),
            );
            db.hset(
                Bytes::from_static(b"h"),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            )
            .unwrap();
            db.zadd(
                Bytes::from_static(b"z"),
                vec![(2.5, Bytes::from_static(b"m"))],
                crate::table::ZAddFlags::default(),
            )
            .unwrap();
            db.set(Bytes::from_static(b"hot"), Bytes::from_static(b"x"), None);
            for k in [&b"s"[..], b"h", b"z"] {
                spill_for_test(&mut db, k).await;
            }

            let mut chunk = Vec::new();
            db.save_rdb_chunk(&mut chunk).unwrap();
            let mut restored = ShardDb::new(0);
            restored.restore_rdb_chunk(&chunk).unwrap();

            assert_eq!(restored.get(b"s"), Some(Bytes::from_static(b"plain-value")));
            assert!(restored.ttl(b"s", false) > 900);
            assert_eq!(
                restored.hget(b"h", b"f").unwrap(),
                Some(Bytes::from_static(b"v"))
            );
            assert_eq!(restored.zscore(b"z", b"m").unwrap(), Some(2.5));
            assert_eq!(restored.get(b"hot"), Some(Bytes::from_static(b"x")));
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_rdb_save_fails_on_unreadable_tiered_key() {
        let dir = std::env::temp_dir().join(format!("rudis_tier_rdb_fail_{}", std::process::id()));
        let mut rt = monoio::RuntimeBuilder::<monoio::FusionDriver>::new()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut db = ShardDb::new(0);
            let tm = std::rc::Rc::new(
                crate::tiering::ShardTierManager::open(0, 55580, &dir)
                    .await
                    .unwrap(),
            );
            db.tier_manager = Some(tm.clone());
            // Big enough to bypass the in-memory small bins: it is read
            // from the backing file.
            db.set(
                Bytes::from_static(b"lost"),
                Bytes::from(vec![b'v'; 64 * 1024]),
                Some(Duration::from_secs(1000)),
            );
            db.set(Bytes::from_static(b"kept"), Bytes::from_static(b"k"), None);
            spill_for_test(&mut db, b"lost").await;

            // Readable: the save succeeds and keeps the tiered key.
            let mut chunk = Vec::new();
            db.save_rdb_chunk(&mut chunk).unwrap();
            let mut restored = ShardDb::new(0);
            restored.restore_rdb_chunk(&chunk).unwrap();
            assert_eq!(restored.get(b"lost").map(|v| v.len()), Some(64 * 1024));

            // A real fault: the backing file loses its data.
            std::fs::OpenOptions::new()
                .write(true)
                .open(&tm.path)
                .unwrap()
                .set_len(0)
                .unwrap();

            let mut chunk = Vec::new();
            let err = db.save_rdb_chunk(&mut chunk).unwrap_err();
            assert_eq!(err.key, Bytes::from_static(b"lost"));
            assert!(err.to_string().contains("'lost'"), "{err}");

            // The streaming SAVE/BGSAVE path fails the same way.
            let cell = std::cell::RefCell::new(db);
            let (tx, rx) = flume::unbounded();
            let err = save_rdb_chunk_yielding(&cell, &tx).await.unwrap_err();
            assert_eq!(err.key, Bytes::from_static(b"lost"));
            drop(rx);
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn ft_schema(line: &str) -> crate::search::IndexSchema {
        let mut buf = bytes::BytesMut::from(format!("{line}\r\n").as_str());
        match crate::resp::parse_command(&mut buf) {
            Ok(Some(crate::resp::Command::FtCreate {
                index,
                on_type,
                prefixes,
                fields,
                schema_fields,
            })) => crate::search::IndexSchema {
                name: index,
                on_type,
                prefixes,
                fields,
                schema_fields,
            },
            other => panic!("{line}: {other:?}"),
        }
    }

    fn ft_hits(db: &ShardDb, index: &str, query: &str) -> Vec<String> {
        let idx = db.search_indices.get(index).expect("index defined");
        let ast = crate::search::parse_query(query);
        let (_, hits) =
            crate::search::execute_search(idx, &ast, &crate::search::SearchOptions::default());
        let mut ids: Vec<String> = hits.into_iter().map(|h| h.doc_id).collect();
        ids.sort();
        ids
    }

    /// Search index definitions are saved in the RDB (the `rudis-ext`
    /// field) and the loaded keys are indexed again, wherever the
    /// definition sits relative to the keys in the file.
    #[test]
    fn test_search_index_definitions_survive_rdb_round_trip() {
        let mut db = ShardDb::new(0);
        db.hset(
            Bytes::from_static(b"rdbft:1"),
            vec![(Bytes::from_static(b"title"), Bytes::from_static(b"hello"))],
        )
        .unwrap();
        db.define_search_index(ft_schema(
            "FT.CREATE idx:rdbft ON HASH PREFIX 1 rdbft: SCHEMA title TEXT WEIGHT 2 n NUMERIC SORTABLE",
        ));
        db.hset(
            Bytes::from_static(b"rdbft:2"),
            vec![
                (Bytes::from_static(b"title"), Bytes::from_static(b"hello")),
                (Bytes::from_static(b"n"), Bytes::from_static(b"7")),
            ],
        )
        .unwrap();
        db.reindex_hash_local(b"rdbft:2");
        assert_eq!(ft_hits(&db, "idx:rdbft", "hello"), ["rdbft:1", "rdbft:2"]);

        let mut chunk = Vec::new();
        db.save_rdb_chunk(&mut chunk).unwrap();
        let mut restored = ShardDb::new(0);
        restored.restore_rdb_chunk(&chunk).unwrap();
        let schema = restored.search_indices["idx:rdbft"].schema.clone().unwrap();
        assert!(schema.same_definition(db.search_indices["idx:rdbft"].schema.as_ref().unwrap()));
        assert_eq!(
            ft_hits(&restored, "idx:rdbft", "hello"),
            ["rdbft:1", "rdbft:2"]
        );
        assert_eq!(ft_hits(&restored, "idx:rdbft", "@n:[5 10]"), ["rdbft:2"]);
        let _ = crate::search::drop_search_index("idx:rdbft");
    }

    /// A full resync (or DEBUG RELOAD) replaces the index definitions with
    /// the file's, dropping indexes the file doesn't have.
    #[test]
    fn test_full_sync_load_replaces_search_indexes() {
        let mut master = ShardDb::new(0);
        master
            .hset(
                Bytes::from_static(b"fsft:1"),
                vec![(Bytes::from_static(b"title"), Bytes::from_static(b"hello"))],
            )
            .unwrap();
        master.define_search_index(ft_schema(
            "FT.CREATE idx:fsft ON HASH PREFIX 1 fsft: SCHEMA title TEXT",
        ));
        let mut file = b"REDIS0011".to_vec();
        master.save_rdb_chunk(&mut file).unwrap();
        file.push(0xFF);
        let crc = crate::table::crc64(&file);
        file.extend_from_slice(&crc.to_le_bytes());

        let mut replica = ShardDb::new(0);
        replica.define_search_index(ft_schema("FT.CREATE idx:fsft_stale ON HASH SCHEMA t TEXT"));
        replica.load_full_sync_rdb(&file, 0, 1, None).unwrap();
        assert!(!replica.search_indices.contains_key("idx:fsft_stale"));
        assert_eq!(ft_hits(&replica, "idx:fsft", "hello"), ["fsft:1"]);
        let _ = crate::search::drop_search_index("idx:fsft");
        let _ = crate::search::drop_search_index("idx:fsft_stale");
    }

    #[test]
    fn test_llm_quotas_round_trip_through_rdb() {
        let mut db = ShardDb::new(0);
        let key = Bytes::from_static(b"quota:gpt4");
        let r1 = db.llm_quota_reserve(key.clone(), 10, 10_000, 500, Some(60_000));
        assert!(r1.allowed);
        assert_eq!(
            db.llm_quota_settle(&key, r1.reservation_id.unwrap(), 420),
            (true, -80)
        );
        let r2 = db.llm_quota_reserve(key.clone(), 10, 10_000, 300, Some(60_000));
        assert!(r2.allowed);

        let mut chunk = Vec::new();
        db.save_rdb_chunk(&mut chunk).unwrap();

        let mut restored = ShardDb::new(0);
        restored.restore_rdb_chunk(&chunk).unwrap();
        let (active_reqs, used_tok, res_tok, pending_res, window_ms) =
            restored.llm_quota_info(&key);
        assert_eq!(active_reqs, 2);
        assert_eq!(used_tok, 420);
        assert_eq!(res_tok, 300);
        assert_eq!(pending_res, 1);
        assert_eq!(window_ms, 60_000);
        assert_eq!(
            restored.llm_quota_settle(&key, r2.reservation_id.unwrap(), 250),
            (true, -50)
        );
    }
}
